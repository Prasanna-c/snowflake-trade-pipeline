#!/usr/bin/env python3
"""
Run ad-hoc SQL against the pipeline's Snowflake account.

    python scripts/run_sql.py "select current_version()"
    python scripts/run_sql.py --file snowflake/30_monitoring/01_freshness_and_health.sql
    python scripts/run_sql.py --preset health
    echo "select 1" | python scripts/run_sql.py -

WHY THIS EXISTS ALONGSIDE SNOWSIGHT
-----------------------------------
Snowsight is better for exploration. This is for the cases Snowsight is bad at:

  * Reproducing exactly what the pipeline sees. Same role, same warehouse, same session
    parameters, same query tag. A query that works in Snowsight and fails in the pipeline is
    usually a session-parameter or role difference, and running it through the same session
    settles that in one step instead of an afternoon.
  * Being scriptable, so the Makefile and the runbook can contain literal commands rather than
    instructions to click through a UI.
  * Leaving an audit trail. Every statement is tagged `component=adhoc` with the OS user, so an
    unexplained expensive query in ACCOUNT_USAGE can be traced to a person.

The presets are the queries the runbook tells you to run during an incident. Having them here
by name means an on-call engineer types `--preset backlog` instead of reconstructing a join
under pressure.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
INGESTION_SRC = REPO_ROOT / "ingestion" / "src"
if INGESTION_SRC.is_dir() and str(INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(INGESTION_SRC))


#: Schemas dbt owns, and therefore prefixes with the target schema outside prod
#: (DBT_LOCAL_CORE on a laptop). RAW and MONITORING are built by the SQL layer and keep their
#: bare names everywhere, so presets reference those two literally. Mirrors
#: dbt/macros/utils/generate_schema_name.sql.
_DBT_OWNED_SCHEMAS = ("staging", "intermediate", "core", "reporting", "audit", "snapshots")


def schema_placeholders() -> dict[str, str]:
    """Map `{core}` and friends to the schema they resolve to in the current environment."""
    if os.environ.get("SNOWFLAKE_ENV", "dev").strip().lower() in ("prod", "production"):
        prefix = ""
    else:
        prefix = os.environ.get("DBT_SCHEMA", "DBT_LOCAL").strip().upper() + "_"
    return {schema: f"{prefix}{schema.upper()}" for schema in _DBT_OWNED_SCHEMAS}


#: Named diagnostics, cross-referenced from docs/runbook.md. `{db}` is substituted with the
#: configured database and `{core}`-style placeholders with the resolved schema, so these work
#: unchanged in dev and prod.
PRESETS: dict[str, tuple[str, str]] = {
    "health": (
        "Stage-by-stage pipeline SLA -- start here",
        "select * from {db}.monitoring.vw_pipeline_sla",
    ),
    "scorecard": (
        "The data quality scorecard the Airflow gate reads",
        "select * from {db}.{reporting}.rpt_data_quality_scorecard",
    ),
    "arrivals": (
        "Recent files: staged, loaded, or stalled",
        """
        select file_name, file_state, is_stalled, staged_at, first_row_loaded_at,
               stage_to_load_seconds, rows_in_file
        from {db}.monitoring.vw_file_arrival
        order by coalesce(staged_at, first_row_loaded_at) desc nulls last
        limit 25
        """,
    ),
    "batches": (
        "Recent load batches, including anything stuck",
        """
        select batch_id, batch_type, batch_status, started_at, completed_at,
               duration_seconds, row_count, error_count, is_stuck, error_message
        from {db}.monitoring.vw_batch_health
        order by started_at desc
        limit 25
        """,
    ),
    "backlog": (
        "How far behind is the transform layer",
        """
        select
            (select count(*) from {db}.raw.trade_event_stream) as rows_in_stream,
            (select count(*) from {db}.raw.trade_event_queue) as rows_in_queue,
            (select count(*) from {db}.{intermediate}.int_trade_event_adjudicated)
                as rows_adjudicated,
            (select max(drained_at) from {db}.raw.trade_event_queue) as last_drained_at,
            (select max(adjudicated_at) from {db}.{intermediate}.int_trade_event_adjudicated)
                as last_adjudicated_at
        """,
    ),
    "rejects": (
        "Top rejection reasons in the last 24 hours",
        """
        select rule_code, any_value(rule_name) as rule_name, count(*) as hits,
               count(distinct trade_id) as trades
        from {db}.{audit}.trade_rule_result
        where evaluated_at >= dateadd('hour', -24, current_timestamp())
          and is_blocking
        group by rule_code
        order by hits desc
        """,
    ),
    "parse-errors": (
        "Lines that failed to parse and never reached the rule engine",
        """
        select logged_at, source_file_name, error_message, rejected_record
        from {db}.raw.copy_error
        order by logged_at desc
        limit 25
        """,
    ),
    "dbt-runs": (
        "Recent dbt invocations and their outcomes",
        """
        select invocation_id, any_value(run_status) as run_status,
               min(run_started_at) as started_at,
               count_if(resource_type = 'model') as models,
               count_if(resource_type = 'test'
                        and lower(node_status) in ('fail', 'error')) as tests_failed
        from {db}.{audit}.dbt_run_result
        group by invocation_id
        order by started_at desc
        limit 15
        """,
    ),
    "tasks": (
        "Snowflake task history from the real-time source",
        """
        select name, state, scheduled_time, completed_time, error_message
        from table({db}.information_schema.task_history(
            scheduled_time_range_start => dateadd('hour', -24, current_timestamp())
        ))
        order by scheduled_time desc
        limit 25
        """,
    ),
    "alerts": (
        "Alert evaluation history -- proves an alert was working, not merely defined",
        """
        select name, scheduled_time, state, sql_error_message
        from table({db}.information_schema.alert_history(
            scheduled_time_range_start => dateadd('day', -2, current_timestamp())
        ))
        order by scheduled_time desc
        limit 50
        """,
    ),
    "credits": (
        "Credit burn by warehouse over the last fortnight",
        """
        select usage_date, warehouse_name, workload_class, credits_used
        from {db}.monitoring.vw_warehouse_credits
        where usage_date >= dateadd('day', -14, current_date())
        order by usage_date desc, credits_used desc
        """,
    ),
    "expiry-overdue": (
        "Matured trades still marked LIVE -- must be empty",
        """
        select trade_id, current_version, maturity_date, lifecycle_status,
               counterparty_name, book_name, notional_amount, notional_currency
        from {db}.{core}.fct_trade
        where maturity_date < current_date()
          and lifecycle_status not in ('EXPIRED', 'CANCELLED')
        order by maturity_date
        limit 100
        """,
    ),
}


def render_table(rows: list[dict[str, Any]], max_width: int = 44) -> str:
    """Render rows as a fixed-width table.

    Hand-rolled rather than pulling in `tabulate` or `rich`: this script is the thing you run
    when you are diagnosing a broken environment, and it should have no dependency that could
    itself be the broken thing.
    """
    if not rows:
        return "(no rows)"

    columns = list(rows[0].keys())

    def cell(value: Any) -> str:
        if value is None:
            return "NULL"
        text = str(value).replace("\n", " ").replace("\t", " ")
        return text[: max_width - 3] + "..." if len(text) > max_width else text

    widths = {
        column: min(max_width, max(len(column), *(len(cell(row.get(column))) for row in rows)))
        for column in columns
    }

    header = "  ".join(column.ljust(widths[column]) for column in columns)
    rule = "  ".join("-" * widths[column] for column in columns)
    body = [
        "  ".join(cell(row.get(column)).ljust(widths[column]) for column in columns) for row in rows
    ]
    return "\n".join([header, rule, *body])


def apply_placeholders(sql: str, database: str) -> str:
    """Resolve `{db}` and `{core}`-style placeholders, leaving anything else alone.

    Applied to ad-hoc SQL as well as to the presets. Previously only the presets got this,
    which made the placeholder syntax a trap: the runbook and `--preset` output both teach
    `{db}.{intermediate}.x`, and pasting that into `make sql Q=...` produced
    "syntax error ... unexpected 'db'" from Snowflake rather than anything explanatory.

    Deliberately not `str.format`, which raises KeyError or IndexError on any other brace --
    `parse_json('{"a": 1}')` and `regexp_like(x, 'a{2}')` are both legitimate SQL that
    `format` refuses to leave untouched. An unrecognised placeholder passes through so that
    Snowflake reports it in context.
    """
    values = {"db": database, **schema_placeholders()}
    return re.sub(r"\{(\w+)\}", lambda m: values.get(m.group(1), m.group(0)), sql)


def read_statements(args: argparse.Namespace, database: str) -> list[str]:
    if args.preset:
        _, sql = PRESETS[args.preset]
        return [apply_placeholders(sql, database).strip()]

    if args.file:
        path = Path(args.file)
        if not path.is_file():
            raise SystemExit(f"no such file: {path}")
        text = path.read_text(encoding="utf-8")
        # Reuse the deployer's splitter so a file behaves identically here and there. Two
        # different notions of "where does a statement end" in one repo is a bug waiting to
        # happen, particularly around procedure bodies.
        from deploy_snowflake_sql import build_context, render, split_statements

        rendered = render(text, build_context(os.environ.get("DBT_TARGET", "dev")), path)
        return [statement.sql for statement in split_statements(rendered, path)]

    if args.sql == "-":
        return [apply_placeholders(sys.stdin.read().strip(), database)]

    return [apply_placeholders(args.sql, database)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run SQL against Snowflake using the pipeline's own session settings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Presets:\n"
        + "\n".join(f"  {name:<16} {description}" for name, (description, _) in PRESETS.items()),
    )
    parser.add_argument("sql", nargs="?", help="SQL to run, or - to read stdin")
    parser.add_argument("--file", "-f", help="Run every statement in a .sql file")
    parser.add_argument("--preset", "-p", choices=sorted(PRESETS), help="Run a named diagnostic")
    parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table, for piping to jq"
    )
    parser.add_argument("--limit", type=int, default=100, help="Max rows to display")
    parser.add_argument(
        "--warehouse", help="Override the warehouse (defaults to SNOWFLAKE_WAREHOUSE)"
    )
    parser.add_argument("--role", help="Override the role")
    args = parser.parse_args()

    if not any((args.sql, args.file, args.preset)):
        parser.print_help()
        return 2

    from trade_sim.config import snowflake_settings
    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    settings = snowflake_settings()
    statements = read_statements(args, settings.database)

    if args.preset and not args.json:
        description, _ = PRESETS[args.preset]
        print(f"\033[1m{args.preset}\033[0m -- {description}")
        print()

    session = SnowflakeSession(
        settings,
        warehouse=args.warehouse,
        role=args.role,
        # Naming the OS user in the tag is what makes an unexplained expensive ad-hoc query in
        # ACCOUNT_USAGE attributable to a person rather than to "something, somewhere".
        query_tag_suffix=f"component=adhoc|os_user={getpass.getuser()}",
    )

    exit_code = 0
    with session:
        for position, statement in enumerate(statements, start=1):
            if len(statements) > 1 and not args.json:
                print(f"\033[90m-- statement {position} of {len(statements)}\033[0m")

            started = time.monotonic()
            try:
                rows = session.execute(statement)
            except Exception as exc:  # noqa: BLE001
                print(f"\033[31mfailed:\033[0m {exc}", file=sys.stderr)
                print(f"  statement: {' '.join(statement.split())[:200]}", file=sys.stderr)
                exit_code = 1
                # Keep going through the remaining statements. When running a file, seeing all
                # the failures in one pass is more useful than fixing them one round-trip at a
                # time -- and this script never mutates anything the caller did not ask it to.
                continue

            elapsed = time.monotonic() - started

            if args.json:
                print(json.dumps(rows[: args.limit], indent=2, default=str))
            else:
                print(render_table(rows[: args.limit]))
                suffix = f" (showing {args.limit})" if len(rows) > args.limit else ""
                print()
                print(
                    f"\033[90m{len(rows)} row(s){suffix} in {elapsed:.2f}s"
                    f"  query_id={session.current_query_id}\033[0m"
                )
                print()

    return exit_code


if __name__ == "__main__":
    # The deployer is imported by --file, and it lives beside this script rather than on the
    # path.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
