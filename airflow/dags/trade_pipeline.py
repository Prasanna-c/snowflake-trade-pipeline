"""
===============================================================================
THE TRADE LIFECYCLE PIPELINE.

Runs hourly. Waits for files, loads them, adjudicates them against the business rules,
publishes the golden record, and proves the result was correct before declaring success.

-------------------------------------------------------------------------------
SHAPE OF THE DAG

    preflight
        └── ingest (group)
              wait_for_files -> load_files -> drain_stream -> gate_load_integrity
                    └── transform (group)
                          dbt_source_freshness -> dbt_seed -> dbt_run_staging
                            -> dbt_run_adjudication -> gate_reject_rate
                            -> dbt_run_marts -> dbt_test -> dbt_snapshot
                                  └── verify (group)
                                        reconcile -> gate_publish_readiness
                                              └── report_health

-------------------------------------------------------------------------------
THE DESIGN DECISIONS WORTH DEFENDING

**max_active_runs = 1.** Two concurrent runs would both MERGE into FCT_TRADE on trade_id.
Snowflake would not error; one run's amendment would simply be lost, non-deterministically,
and the loss would be near-impossible to reproduce. Serialising the DAG is a one-line
guarantee of a single writer, and the pipeline is nowhere near needing the parallelism.

**catchup = False.** A backfill of trade adjudication is meaningless: the rules evaluate
against *current* state, so replaying last Tuesday's hourly run would re-adjudicate today's
data with last Tuesday's business date. Historical reprocessing is a deliberate, separate
operation (`dbt build --full-refresh`), documented in the runbook.

**The sensor uses `reschedule`, not `poke`.** A poking sensor holds a worker slot for its
entire timeout. With a 90-minute file SLA that is 90 minutes of occupied concurrency doing
nothing. In reschedule mode the task frees its slot between checks. On a laptop with two
worker slots this is the difference between a working pipeline and a deadlocked one.

**dbt is split across several tasks rather than one `dbt build`.** A single task would be
simpler, and would also mean a failure told you only "dbt failed". Splitting by layer means
the Airflow graph itself localises the fault, retries resume from the failed layer rather
than the beginning, and the data quality gate can sit *between* adjudication and the marts --
which is the only place it can actually prevent bad data from being published.

**Gates raise `AirflowFailException`, not `AirflowSkipException`.** A tripped quality gate is
a decision to stop, and it must be loud. Skipping would leave the DAG green with the marts
silently un-refreshed, which is the failure mode where someone trades on stale data for a
week before noticing.

**Every task is idempotent, so retries are safe.** RAW is immutable, COPY deduplicates on
load history, the stream drain is transactional, and every incremental model merges on a
surrogate key. That property is what allows automatic retry with no human in the loop, and
it is the reason this pipeline can be left unattended.
===============================================================================
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue
from airflow.utils.trigger_rule import TriggerRule

from utils import alerting
from utils import snowflake as sf

log = logging.getLogger(__name__)


def _json(payload: Any) -> str:
    """Serialise a gate's detail dict for logging.

    Every one of these dicts is assembled from a Snowflake result, and the connector returns
    NUMBER as `Decimal` and TIMESTAMP as `datetime`. A bare `json.dumps` raises on both, which
    turns a diagnostic log line into a task failure -- so the tolerant encoder is the default
    here rather than something to remember at each call site.
    """
    return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Configuration. Read from the environment so the DAG file has no hard-coded
# values and the same file works in dev, CI and prod.
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

#: Where dbt writes compiled artefacts. Read from the environment because compose points it away
#: from the project directory: that directory is a host mount, and artefacts written there by this
#: container are owned by this container's user, which breaks the next dbt command run on the host.
#: dbt honours DBT_TARGET_PATH itself; this constant exists so the tasks that read an artefact back
#: -- source freshness, below -- look where dbt actually put it.
DBT_TARGET_PATH = os.environ.get("DBT_TARGET_PATH", f"{DBT_PROJECT_DIR}/target")

# Simulation only. In production files arrive from an upstream system and the DAG merely
# waits for them; here the DAG generates them so the demo is self-contained.
SIMULATE_ARRIVALS = os.environ.get("PIPELINE_SIMULATE_ARRIVALS", "true").lower() == "true"
SIMULATE_TRADES_PER_BATCH = int(os.environ.get("PIPELINE_SIMULATE_TRADES", "3000"))

# Gate thresholds. Defaults match DataQualitySettings and MONITORING.VW_PIPELINE_SLA.
DQ_MAX_REJECT_RATE = float(os.environ.get("DQ_MAX_REJECT_RATE", "0.25"))
DQ_MIN_EVENTS_FOR_GATE = int(os.environ.get("DQ_MIN_EVENTS_FOR_GATE", "50"))
DQ_MAX_PARSE_ERROR_RATE = float(os.environ.get("DQ_MAX_PARSE_ERROR_RATE", "0.05"))

FILE_WAIT_TIMEOUT_MINUTES = int(os.environ.get("DQ_MAX_FILE_DELAY_MINUTES", "90"))

#: Shared by every dbt task. `--no-use-colors` because ANSI codes in the Airflow log UI are
#: unreadable; `--fail-fast` deliberately omitted so a failing model does not hide the other
#: failures in the same run, which would turn one debugging session into four.
DBT_BASE = f"cd {DBT_PROJECT_DIR} && dbt --no-use-colors --log-format json"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}"


default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # Three retries with exponential backoff. Snowflake surfaces warehouse-resume races and
    # brief network faults as retryable errors that clear in seconds; a fixed short delay
    # would hammer a warehouse that is genuinely busy, and no retry at all would page a human
    # for something that fixes itself.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    # Email is sent by the callback, which can build an actionable message. Airflow's
    # built-in email_on_failure sends a bare stack trace, so it is off.
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": alerting.on_failure,
    "on_retry_callback": alerting.on_retry,
    # A task still running after an hour is wedged, not slow. Killing it frees the slot and
    # surfaces the problem instead of blocking every subsequent run.
    "execution_timeout": timedelta(minutes=60),
}


with DAG(
    dag_id="trade_pipeline",
    description="Ingest, validate, adjudicate and publish trade lifecycle events.",
    default_args=default_args,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    sla_miss_callback=alerting.on_sla_miss,
    on_success_callback=alerting.on_success,
    tags=["trades", "snowflake", "dbt", "production"],
    doc_md=__doc__,
    params={
        # Surfaced in the "Trigger DAG w/ config" UI so an operator can run a one-off
        # backfill or a larger simulated batch without editing the DAG.
        "business_date": None,
        "trades_per_batch": SIMULATE_TRADES_PER_BATCH,
        "skip_reconciliation": False,
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # =======================================================================
    # PREFLIGHT
    # =======================================================================
    @task(task_id="preflight", retries=2, execution_timeout=timedelta(minutes=5))
    def preflight(**context: Any) -> dict[str, Any]:
        """Confirm the platform is deployable-into before doing any work.

        Fails in seconds with a clear message when the Snowflake layer has not been deployed,
        rather than failing twenty minutes later inside dbt with a "table does not exist"
        error that reads like a modelling bug.
        """
        run_id = context["dag_run"].run_id
        required_objects = {
            "RAW.TRADE_EVENT": "table",
            "RAW.TRADE_EVENT_QUEUE": "table",
            "RAW.LOAD_BATCH": "table",
            "RAW.COPY_ERROR": "table",
        }

        with sf.session(task_id="preflight", run_id=run_id) as session:
            database = session.settings.database

            version = session.scalar("select current_version()")
            log.info("connected to Snowflake %s, database %s", version, database)

            found = session.execute(
                f"""
                select table_schema || '.' || table_name as object_name
                from {database}.information_schema.tables
                where table_schema = 'RAW'
                """
            )
            existing = {row["OBJECT_NAME"] for row in found}
            missing = [name for name in required_objects if name not in existing]

            if missing:
                raise AirflowFailException(
                    "The Snowflake ingestion layer is not deployed. Missing objects: "
                    f"{', '.join(missing)}.\n"
                    "Run `make deploy-sql` (or python scripts/deploy_snowflake_sql.py) "
                    "before running this DAG."
                )

            # Warehouse reachability. A suspended warehouse auto-resumes, but a warehouse the
            # role cannot USE fails every downstream task with a confusing privilege error.
            session.execute("select 1")

            stream_lag = session.scalar(f"select count(*) from {database}.raw.trade_event_queue")

        return {
            "snowflake_version": str(version),
            "database": database,
            "queue_depth_at_start": int(stream_lag or 0),
        }

    # =======================================================================
    # INGEST
    # =======================================================================
    @task_group(group_id="ingest")
    def ingest_group(upstream: Any) -> Any:
        """Ingest group.

        Takes its upstream explicitly rather than being chained from outside with
        `preflight >> ingest_group()`. The reason is subtle and cost an afternoon once: a
        `@task_group` function returns whatever its body returns -- here the final gate task --
        not the TaskGroup object. So `preflight >> ingest` wires preflight to the *gate*, leaving
        the group's own first task with no upstream at all, and it starts immediately in
        parallel with preflight. The DAG renders as though it were correct.

        Passing the upstream in and wiring it to the first task inside the group makes the
        dependency real. `scripts/validate_dags.py` asserts that every task is downstream of
        `start`, which is what catches this class of mistake mechanically.
        """

        @task(task_id="simulate_arrivals", retries=1)
        def simulate_arrivals(**context: Any) -> dict[str, Any]:
            """Write a batch of trade files, standing in for an upstream feed.

            Exists only so the demo has data to process. In production this task is removed
            and `wait_for_files` watches a stage fed by an external system -- which is why the
            two are separate tasks rather than one.
            """
            if not SIMULATE_ARRIVALS:
                log.info("PIPELINE_SIMULATE_ARRIVALS is false; expecting external arrivals.")
                return {"simulated": False, "files": []}

            from datetime import date as date_type

            from trade_sim.config import simulator_settings
            from trade_sim.generator import TradeBook, TradeGenerator
            from trade_sim.writer import BatchWriter

            params = context["params"]
            count = int(params.get("trades_per_batch") or SIMULATE_TRADES_PER_BATCH)
            business_date = params.get("business_date")
            as_of = date_type.fromisoformat(business_date) if business_date else date_type.today()

            settings = simulator_settings()
            writer = BatchWriter(settings.output_dir, env=DBT_TARGET)
            # Taken from the digits of run_id, so that no Airflow version difference in the
            # context keys can change it. Fourteen digits is exactly YYYYMMDDHHMMSS, and the fixed
            # width matters: manifests and staged files are matched by %batch_ref%, so a
            # reference that prefixed another would pull a second batch into the comparison. The
            # previous form took the last eight characters of run_id, which for a scheduled run is
            # the timezone offset -- ...T07:00:00+00:00 reduced to 000000 -- so every run was
            # labelled af000000 and reconciled all of its predecessors along with itself.
            run_digits = "".join(char for char in context["dag_run"].run_id if char.isdigit())
            batch_ref = f"af{run_digits[:14]}"

            # The scheduler is not the only producer: `make demo` writes to the same state
            # directory, which compose mounts into this container. Without the lock both read
            # the same trade book, mint the same identifiers and race to write it back, leaving
            # the warehouse with two universes claiming one (trade_id, trade_version). Held
            # until the files exist so that the book and what is on disk agree.
            with TradeBook.exclusive(settings.state_dir / "trade_book.json") as book:
                generator = TradeGenerator(settings, book=book)
                events = list(generator.generate(count, as_of=as_of))
                path, materialised = writer.write(events, batch_ref=batch_ref)
                manifest = generator.build_manifest(
                    materialised, batch_ref=batch_ref, file_name=path.name
                )
                writer.write_manifest(manifest, path)

            log.info("simulated %d events into %s", len(materialised), path)
            return {
                "simulated": True,
                "files": [str(path)],
                "batch_ref": batch_ref,
                "event_count": len(materialised),
                "expected_rejected": manifest.expected_rejected,
            }

        @task.sensor(
            task_id="wait_for_files",
            poke_interval=60,
            timeout=FILE_WAIT_TIMEOUT_MINUTES * 60,
            mode="reschedule",
            # An SLA on the sensor is how file-arrival delay becomes an alert. The sensor
            # itself keeps waiting (the file may still be coming); the SLA callback tells
            # someone it is late, which is the actionable half.
            sla=timedelta(minutes=45),
            retries=0,
        )
        def wait_for_files(arrival: dict[str, Any], **context: Any) -> PokeReturnValue:
            """Wait until at least one unprocessed file is present.

            `retries=0` on purpose: a sensor that has genuinely timed out has already waited
            its full window, and retrying restarts that window from zero -- which silently
            triples the effective SLA and delays the alert that matters.
            """
            local_files = arrival.get("files") or []
            if local_files:
                return PokeReturnValue(
                    is_done=True, xcom_value={"source": "local", "files": local_files}
                )

            # No local files: look on the stage, which is where an external feed would land.
            run_id = context["dag_run"].run_id
            with sf.session(task_id="wait_for_files", run_id=run_id) as session:
                staged = session.execute("list @raw.trade_landing")

            if staged:
                names = [row.get("name") for row in staged][:50]
                log.info("found %d staged file(s)", len(staged))
                return PokeReturnValue(is_done=True, xcom_value={"source": "stage", "files": names})

            log.info("no files yet; rescheduling")
            return PokeReturnValue(is_done=False)

        @task(task_id="load_files", retries=3)
        def load_files(detected: dict[str, Any], **context: Any) -> dict[str, Any]:
            """PUT local files to the stage and COPY them into RAW.

            Idempotent through two independent mechanisms: PUT uses OVERWRITE=FALSE so a
            repeat upload fails loudly rather than silently replacing data, and Snowflake's
            COPY load history skips files it has already ingested. A retry therefore cannot
            double-load, which is what makes retries=3 safe here.
            """
            from pathlib import Path

            run_id = context["dag_run"].run_id
            results: list[dict[str, Any]] = []

            with sf.session(task_id="load_files", run_id=run_id) as session:
                loader = sf.loader(session)

                if detected.get("source") == "local":
                    for file_path in detected.get("files", []):
                        path = Path(file_path)
                        if not path.is_file():
                            raise AirflowFailException(f"expected file is missing: {path}")
                        results.append(
                            loader.load_file(
                                path,
                                ingest_date_partition=path.parent.name,
                                orchestrator_run_id=run_id,
                                # The drain is its own task, so it can fail and retry
                                # independently of the load.
                                drain=False,
                            )
                        )
                else:
                    # Files are already staged by an external process; COPY everything not
                    # yet loaded.
                    results.append(
                        loader.copy_into_raw(
                            pattern=".*[.]ndjson([.]gz)?", orchestrator_run_id=run_id
                        )
                    )

            total_loaded = sum(int(r.get("rows_loaded") or 0) for r in results)
            total_errored = sum(int(r.get("rows_errored") or 0) for r in results)
            log.info("loaded %d rows, %d errored", total_loaded, total_errored)

            return {
                "batches": [r.get("batch_id") for r in results],
                "rows_loaded": total_loaded,
                "rows_errored": total_errored,
                "files_loaded": sum(int(r.get("files_loaded") or 0) for r in results),
            }

        @task(task_id="drain_stream", retries=3)
        def drain_stream(load_result: dict[str, Any], **context: Any) -> dict[str, Any]:
            """Move stream rows into the queue table dbt reads.

            The scheduled Snowflake task does this every minute anyway. Draining explicitly
            here removes up to a minute of dead waiting from every run, and -- more
            importantly -- makes the run self-contained: the DAG does not depend on a
            separate scheduler having fired at the right moment, which would otherwise be an
            invisible race.
            """
            run_id = context["dag_run"].run_id
            with sf.session(task_id="drain_stream", run_id=run_id) as session:
                result = sf.loader(session).drain_stream(run_id)
            return result

        @task(task_id="gate_load_integrity")
        def gate_load_integrity(
            load_result: dict[str, Any], drain_result: dict[str, Any], **context: Any
        ) -> dict[str, Any]:
            """First gate: did the load lose data?

            This runs before any transformation because a partially-loaded file is an
            ingestion problem, and discovering it after building the marts means the marts
            were built on incomplete data. Silent partial loads are the single most common
            way file pipelines lose records: COPY with ON_ERROR=CONTINUE is exactly the
            setting that makes ingestion resilient AND makes loss invisible, so the loss has
            to be measured deliberately.
            """
            run_id = context["dag_run"].run_id
            rows_loaded = int(load_result.get("rows_loaded") or 0)
            rows_errored = int(load_result.get("rows_errored") or 0)
            total = rows_loaded + rows_errored

            with sf.session(task_id="gate_load_integrity", run_id=run_id) as session:
                database = session.settings.database
                recent_parse_errors = int(
                    session.scalar(
                        f"""
                        select count(*)
                        from {database}.raw.copy_error
                        where logged_at >= dateadd('hour', -2, current_timestamp())
                        """
                    )
                    or 0
                )
                stuck_batches = int(
                    session.scalar(
                        f"""
                        select count(*)
                        from {database}.raw.load_batch
                        where batch_status = 'RUNNING'
                          and started_at < dateadd('hour', -1, current_timestamp())
                        """
                    )
                    or 0
                )

            parse_error_rate = (rows_errored / total) if total else 0.0
            detail = {
                "rows_loaded": rows_loaded,
                "rows_errored": rows_errored,
                "parse_error_rate": round(parse_error_rate, 4),
                "threshold": DQ_MAX_PARSE_ERROR_RATE,
                "recent_parse_errors": recent_parse_errors,
                "stuck_batches": stuck_batches,
                "rows_drained": drain_result.get("rows_drained"),
            }

            if stuck_batches > 0:
                alerting.notify_dq_gate_breach("load_integrity", detail, blocking=True)
                raise AirflowFailException(
                    f"{stuck_batches} batch(es) have been RUNNING for over an hour. "
                    "A previous run died mid-load. See docs/runbook.md#load-failures."
                )

            if total > 0 and parse_error_rate > DQ_MAX_PARSE_ERROR_RATE:
                alerting.notify_dq_gate_breach("load_integrity", detail, blocking=True)
                raise AirflowFailException(
                    f"Parse error rate {parse_error_rate:.1%} exceeds the "
                    f"{DQ_MAX_PARSE_ERROR_RATE:.1%} threshold. "
                    "Inspect RAW.COPY_ERROR -- this is usually an upstream encoding or "
                    "serialisation change. See docs/runbook.md#load-failures."
                )

            if rows_errored > 0:
                alerting.notify_dq_gate_breach("load_integrity", detail, blocking=False)

            log.info("load integrity gate passed: %s", _json(detail))
            return detail

        arrival = simulate_arrivals()
        detected = wait_for_files(arrival)
        loaded = load_files(detected)
        drained = drain_stream(loaded)
        gate = gate_load_integrity(loaded, drained)

        upstream >> arrival >> detected >> loaded >> drained >> gate
        return gate

    # =======================================================================
    # TRANSFORM
    # =======================================================================
    @task_group(group_id="transform")
    def transform_group(upstream: Any) -> Any:
        """Transform group. Takes its upstream explicitly -- see `ingest_group`."""

        # ---- Freshness -----------------------------------------------------
        # Run before anything is built. A source that is stale means the pipeline is being
        # asked to transform data that never arrived, and finding that out first is cheaper
        # than finding it out after twenty minutes of modelling.
        #
        # `|| true` because dbt exits non-zero on a freshness WARN as well as an ERROR, and a
        # warning is not a reason to stop. The artifact is parsed by the next task, which
        # decides. Swallowing the exit code without then inspecting the result would be the
        # bug; inspecting it is the point.
        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=(
                f"{DBT_BASE} source freshness {DBT_FLAGS} "
                f"--output {DBT_TARGET_PATH}/sources.json || true"
            ),
            retries=1,
            execution_timeout=timedelta(minutes=10),
        )

        @task(task_id="check_source_freshness")
        def check_source_freshness(**context: Any) -> dict[str, Any]:
            """Turn dbt's freshness artifact into a pass/warn/fail decision."""
            from pathlib import Path

            artifact = Path(DBT_TARGET_PATH) / "sources.json"
            if not artifact.is_file():
                log.warning("no sources.json produced; treating freshness as unknown")
                return {"status": "unknown"}

            payload = json.loads(artifact.read_text(encoding="utf-8"))
            results = payload.get("results", [])
            statuses = {r.get("unique_id"): r.get("status") for r in results}

            errored = [uid for uid, status in statuses.items() if status == "error"]
            warned = [uid for uid, status in statuses.items() if status == "warn"]

            detail = {"errored": errored, "warned": warned, "checked": len(results)}

            if errored:
                alerting.notify_dq_gate_breach("source_freshness", detail, blocking=True)
                raise AirflowFailException(
                    f"Source freshness ERROR on {errored}. No new data has arrived within "
                    f"the {FILE_WAIT_TIMEOUT_MINUTES}-minute SLA. "
                    "See docs/runbook.md#file-arrival-delay."
                )
            if warned:
                alerting.notify_dq_gate_breach("source_freshness", detail, blocking=False)

            log.info("source freshness: %s", _json(detail))
            return detail

        # ---- Seeds ---------------------------------------------------------
        # Reference data is small and changes rarely, but it is loaded every run rather than
        # manually, because a rule that validates against reference data is only as correct
        # as the reference data behind it. A stale seed rejects valid trades.
        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"{DBT_BASE} seed {DBT_FLAGS}",
            retries=2,
            execution_timeout=timedelta(minutes=10),
        )

        # ---- Staging and typing -------------------------------------------
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=(
                f"{DBT_BASE} run {DBT_FLAGS} --select stg_trade_event int_trade_event_typed"
            ),
            retries=2,
            execution_timeout=timedelta(minutes=15),
        )

        # ---- Adjudication --------------------------------------------------
        # Its own task because it is the heart of the pipeline and the one place a failure
        # means "the rules did not run", as opposed to "a report is stale". Separating it
        # means the Airflow graph says which of those happened.
        dbt_run_adjudication = BashOperator(
            task_id="dbt_run_adjudication",
            bash_command=(f"{DBT_BASE} run {DBT_FLAGS} --select int_trade_event_adjudicated"),
            retries=2,
            execution_timeout=timedelta(minutes=30),
            sla=timedelta(minutes=20),
        )

        # ---- The gate that matters ----------------------------------------
        @task(task_id="gate_reject_rate")
        def gate_reject_rate(**context: Any) -> dict[str, Any]:
            """Second gate, and the one placed most deliberately.

            It sits AFTER adjudication and BEFORE the marts. That position is the whole
            point: adjudication has happened, so the rejection rate for this batch is
            measurable and every rejected trade is already recorded in the audit log for
            investigation -- but the golden record has not yet been updated, so a batch of
            suspect data can still be stopped before anyone trades on it.

            Placed earlier there would be nothing to measure. Placed later the damage would
            already be done and the gate could only report it.

            The minimum-events floor exists because a percentage over eight events is not a
            rate, it is noise -- and a gate that fires on noise is a gate people disable.
            """
            run_id = context["dag_run"].run_id

            with sf.session(task_id="gate_reject_rate", run_id=run_id) as session:
                database = session.settings.database
                # Both tables are built by dbt, which prefixes its schemas outside prod, so the
                # layer name has to be resolved rather than written literally. Hardcoding
                # `intermediate` works in prod and fails everywhere else.
                intermediate = session.settings.dbt_schema("intermediate")
                core = session.settings.dbt_schema("core")
                rows = session.execute(
                    f"""
                    select
                        count(*) as total_events,
                        count_if(verdict = 'ACCEPTED') as accepted,
                        count_if(verdict = 'REJECTED') as rejected,
                        count_if(verdict = 'SUPERSEDED') as superseded
                    from {database}.{intermediate}.int_trade_event_adjudicated
                    where adjudicated_at >= dateadd('hour', -2, current_timestamp())
                    """
                )
                # Counted from the adjudicated model rather than from AUDIT.TRADE_RULE_RESULT,
                # which is built by dbt_run_marts -- a task that runs AFTER this gate by
                # design. Reading it here returned nothing on the run that mattered: a gate
                # firing on 87% rejects reported "Top rules:" and then an empty string, which
                # is the one moment the breakdown is worth having. The array on the
                # adjudicated model carries the same codes and already exists.
                top_rules = session.execute(
                    f"""
                    with rejected_codes as (
                        select code.value::string as rule_code
                        from {database}.{intermediate}.int_trade_event_adjudicated
                             as adjudicated,
                             lateral flatten(input => adjudicated.violated_rule_codes) as code
                        where adjudicated.verdict = 'REJECTED'
                          and adjudicated.adjudicated_at
                              >= dateadd('hour', -2, current_timestamp())
                    )
                    select
                        rejected_codes.rule_code,
                        coalesce(reason.rule_name, 'unknown rule') as rule_name,
                        count(*) as hits
                    from rejected_codes
                    left join {database}.{core}.ref_rejection_reason as reason
                        on reason.rule_code = rejected_codes.rule_code
                    group by rejected_codes.rule_code, reason.rule_name
                    order by hits desc
                    limit 5
                    """
                )

            metrics = rows[0] if rows else {}
            total = int(metrics.get("TOTAL_EVENTS") or 0)
            rejected = int(metrics.get("REJECTED") or 0)

            # SUPERSEDED is excluded from the numerator: it is business rule 2 working
            # correctly, and folding it in would make this gate fire on healthy amendment
            # traffic.
            reject_rate = (rejected / total) if total else 0.0

            detail = {
                "total_events": total,
                "accepted": int(metrics.get("ACCEPTED") or 0),
                "rejected": rejected,
                "superseded": int(metrics.get("SUPERSEDED") or 0),
                "reject_rate": round(reject_rate, 4),
                "threshold": DQ_MAX_REJECT_RATE,
                "top_rules": [f"{r['RULE_CODE']} {r['RULE_NAME']} x{r['HITS']}" for r in top_rules],
            }

            if total < DQ_MIN_EVENTS_FOR_GATE:
                log.info(
                    "only %d events (floor is %d); gate not meaningful, passing: %s",
                    total,
                    DQ_MIN_EVENTS_FOR_GATE,
                    _json(detail),
                )
                return detail

            if reject_rate > DQ_MAX_REJECT_RATE:
                alerting.notify_dq_gate_breach("reject_rate", detail, blocking=True)
                raise AirflowFailException(
                    f"Reject rate {reject_rate:.1%} exceeds the {DQ_MAX_REJECT_RATE:.1%} "
                    f"threshold ({rejected} of {total} events).\n"
                    f"Top rules: {'; '.join(detail['top_rules'])}\n"
                    "The golden record has NOT been updated. Every rejected event is in "
                    "AUDIT.FCT_TRADE_REJECTED with its reason. "
                    "See docs/runbook.md#data-quality-gate-tripped."
                )

            if reject_rate > DQ_MAX_REJECT_RATE * 0.6:
                alerting.notify_dq_gate_breach("reject_rate", detail, blocking=False)

            log.info("reject rate gate passed: %s", _json(detail))
            return detail

        # ---- Marts ---------------------------------------------------------
        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=(
                f"{DBT_BASE} run {DBT_FLAGS} "
                "--select fct_trade_version fct_trade fct_trade_rejected trade_rule_result "
                "dim_counterparty dim_book dim_product "
                "agg_trade_status_daily agg_rejection_analysis "
                "rpt_trade_expiring_soon rpt_data_quality_scorecard"
            ),
            retries=2,
            execution_timeout=timedelta(minutes=30),
            sla=timedelta(minutes=25),
        )

        # ---- Tests ---------------------------------------------------------
        # Run after the marts, not instead of the gate. The gate asks "is the incoming data
        # plausible"; the tests ask "is what we built internally consistent". Different
        # questions, and the pipeline needs both answered.
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"{DBT_BASE} test {DBT_FLAGS} --store-failures",
            retries=1,
            execution_timeout=timedelta(minutes=25),
        )

        # ---- Snapshot ------------------------------------------------------
        # After the tests. Snapshotting data that has just failed its tests writes a bad
        # state into immutable history, and SCD2 history is not something you can tidy up
        # afterwards without destroying the audit trail.
        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=f"{DBT_BASE} snapshot {DBT_FLAGS}",
            retries=2,
            execution_timeout=timedelta(minutes=20),
        )

        freshness_check = check_source_freshness()
        reject_gate = gate_reject_rate()

        (
            upstream
            >> dbt_source_freshness
            >> freshness_check
            >> dbt_seed
            >> dbt_run_staging
            >> dbt_run_adjudication
            >> reject_gate
            >> dbt_run_marts
            >> dbt_test
            >> dbt_snapshot
        )
        return dbt_snapshot

    # =======================================================================
    # VERIFY
    # =======================================================================
    @task_group(group_id="verify")
    def verify_group(upstream: Any) -> Any:
        """Verify group. Takes its upstream explicitly -- see `ingest_group`."""

        @task(task_id="reconcile", retries=1)
        def reconcile(**context: Any) -> dict[str, Any]:
            """Compare the generator's manifests against the verdicts actually reached.

            This is what turns "the DAG went green" into "the pipeline was correct". Every
            other check in this DAG validates rows that are present; this one is the only
            check that can detect an event which is *absent* -- silent data loss, which is the
            worst outcome in a regulated pipeline and the hardest to notice.

            Only meaningful when the data was simulated, because only then do we know what the
            verdicts should have been. With an external feed there is no ground truth to
            compare against, and the task skips rather than pretending otherwise.
            """
            if not SIMULATE_ARRIVALS or context["params"].get("skip_reconciliation"):
                log.info("reconciliation skipped: no generated ground truth for this run")
                return {"skipped": True}

            from trade_sim.config import simulator_settings
            from trade_sim.reconcile import Reconciler, find_manifests, load_manifest

            run_id = context["dag_run"].run_id
            manifest_dir = simulator_settings().output_dir.parent / "manifests"

            # This run's batch, and only this run's.
            #
            # Reconciling recent history instead was wrong, not merely wasteful. An earlier
            # batch's verdict can legitimately change afterwards: business rule 2 says a resend
            # of a held version overwrites it, so an event this pipeline correctly accepted last
            # hour becomes SUPERSEDED the moment a later batch resends that version. Its
            # manifest, written before that could be known, still expects ACCEPTED. Re-checking
            # it therefore reports a mismatch that reflects nothing wrong -- and it fires in
            # bulk, because one batch amends many of the previous batch's trades.
            #
            # A run verifies its own work. Whatever went wrong in an earlier batch was already
            # reported by the run that produced it, which is the only run that could judge it.
            simulated = context["ti"].xcom_pull(task_ids="ingest.simulate_arrivals") or {}
            batch_ref = simulated.get("batch_ref")
            if not batch_ref:
                log.warning("simulate_arrivals reported no batch_ref; nothing to reconcile")
                return {"skipped": True, "reason": "no batch_ref"}

            manifests = find_manifests(manifest_dir, batch_ref=batch_ref)

            if not manifests:
                log.warning("no manifest for batch %s in %s", batch_ref, manifest_dir)
                return {"skipped": True, "reason": "no manifests"}

            failures: list[str] = []
            summaries: list[str] = []
            with sf.session(task_id="reconcile", run_id=run_id) as session:
                reconciler = Reconciler(session)
                for path in manifests:
                    result = reconciler.reconcile(load_manifest(path))
                    summaries.append(result.summary())
                    if not result.passed:
                        failures.append(result.summary())
                        for discrepancy in result.discrepancies[:10]:
                            log.error(
                                "%s %s v%s: expected %s, got %s (%s)",
                                discrepancy.kind,
                                discrepancy.trade_id,
                                discrepancy.trade_version,
                                discrepancy.expected,
                                discrepancy.actual,
                                discrepancy.detail,
                            )

            detail = {
                "batch_ref": batch_ref,
                "batches_checked": len(manifests),
                "summaries": summaries,
            }

            if failures:
                alerting.notify_dq_gate_breach("reconciliation", detail, blocking=True)
                raise AirflowFailException(
                    "Reconciliation FAILED. The pipeline reached a different verdict than "
                    "the injected faults required, or events are missing entirely:\n"
                    + "\n".join(failures)
                    + "\nSee docs/runbook.md#reconciliation-mismatch."
                )

            log.info("reconciliation passed: %s", _json(detail))
            return detail

        @task(task_id="gate_publish_readiness")
        def gate_publish_readiness(**context: Any) -> dict[str, Any]:
            """Final gate: is the platform in a state we are willing to publish?

            Reads REPORTING.RPT_DATA_QUALITY_SCORECARD -- the same single row the dashboard
            shows and the Snowflake alerts fire on. Using one definition of "healthy" for the
            orchestrator, the dashboard and the alerting means they cannot disagree, and an
            on-call engineer never has to work out which of three sources is lying.
            """
            run_id = context["dag_run"].run_id
            with sf.session(task_id="gate_publish_readiness", run_id=run_id) as session:
                database = session.settings.database
                reporting = session.settings.dbt_schema("reporting")
                rows = session.execute(
                    f"select * from {database}.{reporting}.rpt_data_quality_scorecard"
                )

            if not rows:
                raise AirflowFailException(
                    "The data quality scorecard returned no rows, which should be impossible "
                    "-- it is a single-row aggregate. The reporting layer did not build."
                )

            scorecard = rows[0]
            status = str(scorecard.get("OVERALL_STATUS"))
            overdue = int(scorecard.get("OVERDUE_EXPIRY_TRADES") or 0)

            detail = {
                "overall_status": status,
                "total_trades": scorecard.get("TOTAL_TRADES"),
                "live_trades": scorecard.get("LIVE_TRADES"),
                "expired_trades": scorecard.get("EXPIRED_TRADES"),
                "cancelled_trades": scorecard.get("CANCELLED_TRADES"),
                "reject_rate_pct": scorecard.get("REJECT_RATE_PCT"),
                "pending_events": scorecard.get("PENDING_EVENTS"),
                "overdue_expiry_trades": overdue,
                "rules_never_fired": scorecard.get("RULES_NEVER_FIRED"),
            }

            # A matured trade still marked LIVE after a successful run means the expiry sweep
            # did not do its job -- and business rule 4 is therefore not being honoured.
            if overdue > 0:
                alerting.notify_dq_gate_breach("publish_readiness", detail, blocking=True)
                raise AirflowFailException(
                    f"{overdue} matured trade(s) are still marked LIVE after a successful "
                    "run. The expiry sweep in FCT_TRADE has not applied business rule 4. "
                    "See docs/runbook.md#expiry-sweep-failure."
                )

            if status == "RED":
                alerting.notify_dq_gate_breach("publish_readiness", detail, blocking=True)
                raise AirflowFailException(
                    f"Platform health is RED after the run: {_json(detail)}. "
                    "See docs/runbook.md#platform-health-red."
                )

            if status == "AMBER":
                alerting.notify_dq_gate_breach("publish_readiness", detail, blocking=False)

            log.info("publish readiness gate passed: %s", _json(detail))
            return detail

        @task(task_id="report_health", trigger_rule=TriggerRule.ALL_DONE)
        def report_health(**context: Any) -> None:
            """Log a run summary regardless of what happened upstream.

            `ALL_DONE` so the summary is written even when the run failed -- the state of the
            platform after a failure is exactly what an engineer wants to see first, and
            making them reconstruct it from six task logs wastes the most valuable minutes of
            an incident.
            """
            run_id = context["dag_run"].run_id
            try:
                with sf.session(task_id="report_health", run_id=run_id) as session:
                    database = session.settings.database
                    # MONITORING is deliberately literal. It is created by the versioned SQL
                    # layer rather than by dbt, so it is never prefixed -- passing it through
                    # dbt_schema() would look for DBT_LOCAL_MONITORING, which does not exist.
                    sla = session.execute(f"select * from {database}.monitoring.vw_pipeline_sla")
                    if sla:
                        log.info("platform SLA after run: %s", _json(sla[0]))
            except Exception:
                # Never fail the run on a reporting step.
                log.exception("could not read the SLA view; continuing")

        reconciled = reconcile()
        publish_gate = gate_publish_readiness()
        health = report_health()

        upstream >> reconciled >> publish_gate >> health
        return health

    # =======================================================================
    # WIRING
    #
    # Each group is handed its upstream as an argument rather than chained with `>>` after the
    # fact. See `ingest_group`'s docstring for why: a `@task_group` call evaluates to the
    # function's return value -- the group's last task -- so `a >> group()` attaches to the
    # group's tail and silently leaves its head unconstrained.
    # =======================================================================
    preflight_result = preflight()
    ingest = ingest_group(preflight_result)
    transform = transform_group(ingest)
    verify = verify_group(transform)

    start >> preflight_result
    verify >> end
