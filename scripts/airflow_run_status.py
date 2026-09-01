#!/usr/bin/env python3
"""
Report a verdict on the most recent DAG run, task by task, with an exit code.

    make airflow-status                 # the usual way, from the host
    python scripts/airflow_run_status.py --runs 5 --dag trade_pipeline

WHY THIS EXISTS RATHER THAN "LOOK AT THE UI"
--------------------------------------------
The grid view is the right tool for exploring a run and the wrong one for answering "did it
work". A green DAG run is not proof on its own: a task can be *skipped* rather than run, which
is legitimate for `simulate_arrivals` when `PIPELINE_SIMULATE_ARRIVALS=false` and a silent
hole in the pipeline when it is not. So this prints the state of every task, not a rollup, and
says plainly which tasks were skipped.

It also gives an exit code, which the UI cannot, so the check belongs in a script or a
handover note rather than in someone's memory of what the colours looked like.

WHAT THE EXIT CODES MEAN
------------------------
  0  the latest run succeeded
  1  the latest run failed -- the failing tasks and their log paths are printed
  2  no verdict is available yet: the run is still going, is queued, or has never run

2 is deliberately not 1. "Not finished" and "finished badly" call for different responses, and
collapsing them means a script that waits gets told to escalate.

WHERE IT RUNS
-------------
Inside the container, because that is the only place Airflow and its metadata database exist.
`make airflow-status` handles that; run directly, it explains itself and exits 2.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

# Mounted read-only by compose, so the same path is valid on the host for reading logs.
LOG_ROOT = Path("/opt/airflow/logs")

GREEN, RED, YELLOW, DIM, BOLD, RESET = (
    "\033[32m",
    "\033[31m",
    "\033[33m",
    "\033[2m",
    "\033[1m",
    "\033[0m",
)

# Everything else -- failed, upstream_failed, removed -- is reported as a problem.
GOOD_STATES = {"success"}
BENIGN_STATES = {"skipped"}
PENDING_STATES = {"running", "queued", "scheduled", "deferred", "up_for_retry", "restarting", None}


def _colour(state: str | None) -> str:
    if state in GOOD_STATES:
        return GREEN
    if state in BENIGN_STATES:
        return YELLOW
    if state in PENDING_STATES:
        return DIM
    return RED


def _duration(start: datetime | None, end: datetime | None) -> str:
    if start is None:
        return "-"
    finish = end or datetime.now(UTC)
    seconds = (finish - start).total_seconds()
    if seconds < 0:
        # A running task whose start is in the future: the scheduler's clock and this
        # process's disagree. Reporting nothing beats reporting a negative elapsed time.
        return "-"
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"


def _execution_order(dag_id: str, task_ids: set[str]) -> dict[str, int]:
    """Rank tasks by dependency order, for the ones that have no start time yet.

    Without this, tasks that have not started sort alphabetically, so `reconcile` appears
    above the transform that feeds it and the list implies an order the DAG does not have.
    The serialised DAG in the metadata database is the authority; if it cannot be read the
    caller falls back to task_id, which is arbitrary but at least stable.
    """
    try:
        from airflow.models import DagBag

        dag = DagBag(read_dags_from_db=True).get_dag(dag_id)
        if dag is None:
            return {}
        return {task.task_id: index for index, task in enumerate(dag.topological_sort())}
    # Ordering is cosmetic. A serialisation quirk or a schema change between Airflow versions
    # must not stop the verdict being printed, so this catches broadly and degrades quietly.
    except Exception:  # noqa: BLE001 - see above; the verdict matters more than the ordering
        return {}


def _log_hint(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """Point at the log file, falling back to its directory.

    The path follows Airflow's default log filename template. If that template has been
    changed the file will not be found, so the directory is offered instead of a path that
    looks authoritative and is wrong.
    """
    directory = LOG_ROOT / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    attempt = directory / f"attempt={try_number}.log"
    if attempt.exists():
        return str(attempt)
    if directory.is_dir():
        return f"{directory}/ (no attempt={try_number}.log; the filename template may differ)"
    return f"{directory}/ (nothing here -- check the logs volume is mounted)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag", default="trade_pipeline", help="DAG id to inspect.")
    parser.add_argument("--runs", type=int, default=5, help="How many recent runs to list.")
    args = parser.parse_args()

    try:
        from airflow.models import DagModel, DagRun
        from airflow.utils.session import create_session
    except ImportError:
        print("Airflow is not installed in this interpreter, and it is not meant to be:")
        print("the scheduler owns the metadata database, so the verdict has to be read there.")
        print("-> Run it in the container instead:")
        print("     make airflow-status")
        return 2

    with create_session() as session:
        dag_model = session.query(DagModel).filter(DagModel.dag_id == args.dag).one_or_none()
        if dag_model is None:
            print(f"{RED}No DAG named {args.dag} is registered.{RESET}")
            print("-> The scheduler may still be parsing. Check: make airflow-logs")
            return 2

        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == args.dag)
            .order_by(DagRun.execution_date.desc())
            .limit(args.runs)
            .all()
        )

        paused = f"{YELLOW}paused{RESET}" if dag_model.is_paused else f"{GREEN}active{RESET}"
        print()
        print(f"{BOLD}{args.dag}{RESET}  schedule={dag_model.schedule_interval!r}  {paused}")
        print()

        if not runs:
            print(f"{YELLOW}No runs yet.{RESET}")
            if dag_model.is_paused:
                # The common case after `make pause-writers`, and worth saying rather than
                # leaving someone to wonder why a triggered run never starts.
                print("-> The DAG is paused, so a triggered run stays queued. Unpause first:")
                print("     make resume-writers")
            print("-> Then: make airflow-trigger")
            return 2

        print(f"{DIM}recent runs{RESET}")
        for run in runs:
            print(
                f"  {_colour(run.state)}{(run.state or 'none'):<9}{RESET} "
                f"{run.run_id:<45} {run.run_type:<10} "
                f"{_duration(run.start_date, run.end_date):>7}"
            )
        print()

        latest = runs[0]
        raw_instances = latest.get_task_instances(session=session)

        # Execution order, which is how a failure is read: the first red row is the cause and
        # everything after it is consequence. Tasks that have started sort by when they did;
        # the rest fall in behind them in dependency order.
        order = _execution_order(args.dag, {ti.task_id for ti in raw_instances})
        far_future = datetime.max.replace(tzinfo=UTC)
        instances = sorted(
            raw_instances,
            key=lambda ti: (
                ti.start_date or far_future,
                order.get(ti.task_id, 0),
                ti.task_id,
            ),
        )

        print(
            f"{BOLD}latest run{RESET} {latest.run_id}  state={_colour(latest.state)}{latest.state}{RESET}"
        )
        print()
        for ti in instances:
            retried = f"  {YELLOW}try {ti.try_number}{RESET}" if ti.try_number > 1 else ""
            print(
                f"  {_colour(ti.state)}{(ti.state or 'none'):<14}{RESET} {ti.task_id:<34} "
                f"{_duration(ti.start_date, ti.end_date):>7}{retried}"
            )
        print()

        failed = [ti for ti in instances if ti.state == "failed"]
        blocked = [ti for ti in instances if ti.state == "upstream_failed"]
        skipped = [ti for ti in instances if ti.state == "skipped"]
        pending = [ti for ti in instances if ti.state in PENDING_STATES]

        if skipped:
            print(
                f"{YELLOW}{len(skipped)} task(s) skipped:{RESET} {', '.join(t.task_id for t in skipped)}"
            )
            print(f"{DIM}  Expected for simulate_arrivals when PIPELINE_SIMULATE_ARRIVALS=false,")
            print(f"  and for a branch not taken. Anything else is a hole in the run.{RESET}")
            print()

        if failed or blocked:
            # Counted separately on purpose. An upstream_failed task is a consequence, and
            # reporting it as a failure inflates the number of things to investigate.
            tail = f", {len(blocked)} blocked upstream" if blocked else ""
            print(f"{RED}{len(failed)} task(s) failed{tail}.{RESET}")
            for ti in failed:
                print(f"  {BOLD}{ti.task_id}{RESET}")
                print(f"    {_log_hint(args.dag, latest.run_id, ti.task_id, ti.try_number)}")
            print()
            print(
                "-> docs/runbook.md is indexed by task name; each has a diagnose and resolve section."
            )
            return 1

        if pending or latest.state == "running":
            print(f"{DIM}Run is still in flight; {len(pending)} task(s) not finished.{RESET}")
            return 2

        if latest.state != "success":
            print(f"{RED}Run ended in state {latest.state}.{RESET}")
            return 1

        print(
            f"{GREEN}Run succeeded: {len(instances) - len(skipped)} task(s) ran, "
            f"{len(skipped)} skipped.{RESET}"
        )
        print(f"{DIM}The reconcile task passing is the correctness proof -- it compares every")
        print(f"generated event against the verdict the warehouse reached.{RESET}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
