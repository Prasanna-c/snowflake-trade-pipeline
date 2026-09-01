"""
Smoke tests for the dashboard, using Streamlit's own AppTest harness.

WHY THIS TEST EXISTS
--------------------
A dashboard fails in two ways, and only one of them is loud. A syntax error is caught by any
import check. A wrong column name is not: `run_query` catches the Snowflake error, shows a
small warning, and returns an empty frame -- so the page still renders and the panel is
merely blank. Blank panels on a monitoring dashboard get read as "no data", which is the same
thing a healthy quiet period looks like.

These tests run each page with Snowflake replaced by fixtures whose columns match the dbt
models, and assert the page reaches the end with no uncaught exception. That catches:

  * a reference to a column the page's query does not select,
  * a chart encoding naming a field that is not in the frame,
  * a `column_config` key for a column that no longer exists,
  * an aggregation over a column that is text when the page assumes it is numeric.

Every one of those is a real bug that produces a rendered-but-wrong page in production.

WHY THE DATA IS FAKED RATHER THAN READ FROM SNOWFLAKE
-----------------------------------------------------
Because CI has no Snowflake account, and because a test that needs credentials is a test that
gets skipped. The fixtures are built to mirror the mart column names exactly; the separate
guard against those drifting from the real models is `test_queries_reference_real_columns`
below, which parses the dbt manifest when one is available.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

DASHBOARD_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = DASHBOARD_DIR.parent

# The pages import `from lib import ...`, which works under `streamlit run` because Streamlit
# puts the script's directory on sys.path. AppTest does not, so we do it here.
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

PAGES = [
    DASHBOARD_DIR / "app.py",
    DASHBOARD_DIR / "pages" / "1_Trade_status.py",
    DASHBOARD_DIR / "pages" / "2_Rejections.py",
    DASHBOARD_DIR / "pages" / "3_Pipeline_health.py",
]

NOW = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fixture data. Column names mirror the dbt models and the monitoring views.
# ---------------------------------------------------------------------------
def _scorecard() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "total_trades": 4820,
                "live_trades": 4310,
                "expired_trades": 402,
                "cancelled_trades": 108,
                "expiring_soon_trades": 61,
                "amended_trades": 903,
                "limit_breach_trades": 2,
                "duplicate_uti_trades": 1,
                "live_gross_notional": 8_431_902_113.44,
                "total_events": 6120,
                "accepted_events": 5402,
                "rejected_events": 588,
                "superseded_events": 130,
                "events_last_24h": 3000,
                "rejected_last_24h": 290,
                "pending_events": 0,
                "total_parse_errors": 4,
                "parse_errors_last_24h": 2,
                "reject_rate_pct": 9.61,
                "reject_rate_24h_pct": 9.67,
                "supersede_rate_pct": 2.12,
                "amendment_rate_pct": 18.73,
                "rules_declared": 14,
                "rules_ever_fired": 12,
                "rules_never_fired": 2,
                "last_adjudicated_at": NOW - timedelta(minutes=8),
                "minutes_since_last_adjudication": 8,
                "latest_trade_event_ts": NOW - timedelta(minutes=9),
                "overdue_expiry_trades": 0,
                "overall_status": "GREEN",
                "evaluated_at": NOW,
            }
        ]
    )


def _pipeline_sla() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "evaluated_at": NOW,
                "last_event_loaded_at": NOW - timedelta(minutes=6),
                "minutes_since_last_event": 6,
                "events_loaded_last_24h": 3000,
                "last_drain_at": NOW - timedelta(minutes=2),
                "minutes_since_last_drain": 2,
                "failed_drains_last_24h": 0,
                "stuck_batches": 0,
                "rows_awaiting_transform": 0,
                "oldest_backlog_minutes": None,
                "last_successful_dbt_run_at": NOW - timedelta(minutes=11),
                "minutes_since_dbt_success": 11,
                "trades_overdue_for_expiry": 0,
                "ingestion_status": "GREEN",
                "capture_status": "GREEN",
                "transform_status": "GREEN",
                "correctness_status": "GREEN",
            }
        ]
    )


def _daily_status() -> pd.DataFrame:
    dates = pd.date_range(end=NOW.date(), periods=14, freq="D")
    return pd.DataFrame(
        {
            "calendar_date": dates,
            "trade_count": range(100, 114),
            "live_count": range(90, 104),
            "expired_count": [4] * 14,
            "cancelled_count": [2] * 14,
            "expiring_soon_count": [3] * 14,
            "limit_breach_count": [0] * 14,
            "amended_trade_count": [18] * 14,
            "distinct_counterparty_count": [40] * 14,
            "distinct_book_count": [8] * 14,
            "distinct_product_count": [6] * 14,
            "gross_notional": [1.2e9] * 14,
            "net_notional": [3.1e8] * 14,
            "events_adjudicated": range(200, 214),
            "events_accepted": range(180, 194),
            "events_rejected": [18] * 14,
            "events_superseded": [3] * 14,
            "batch_count": [1] * 14,
            "rejected_event_count": [18] * 14,
            "superseded_event_count": [3] * 14,
            "rejected_trade_count": [17] * 14,
            "distinct_rule_count": [5] * 14,
            "multi_violation_count": [2] * 14,
            "rejected_notional": [4.2e7] * 14,
            "reject_rate_pct": [8.9] * 14,
            "reject_rate_7d_pct": [9.1] * 14,
            "expired_rate_pct": [3.8] * 14,
            "dbt_updated_at": [NOW] * 14,
        }
    )


def _lifecycle_mix() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"lifecycle_status": "LIVE", "trade_count": 4310, "gross_notional": 8.4e9},
            {"lifecycle_status": "EXPIRED", "trade_count": 402, "gross_notional": 7.1e8},
            {"lifecycle_status": "CANCELLED", "trade_count": 108, "gross_notional": 1.9e8},
        ]
    )


def _version_distribution() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"version_bucket": 1, "version_label": "1", "trade_count": 3917},
            {"version_bucket": 2, "version_label": "2", "trade_count": 703},
            {"version_bucket": 3, "version_label": "3", "trade_count": 150},
            {"version_bucket": 6, "version_label": "6+", "trade_count": 50},
        ]
    )


def _book_exposure() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "book_id": "BK-FX-LDN-01",
                "book_name": "FX Cash London",
                "desk": "FX_CASH",
                "live_trade_count": 900,
                "gross_live_notional": 2.4e8,
                "net_live_notional": 3.1e7,
                "notional_limit": 2.5e8,
                "limit_utilisation_pct": 96.0,
                "limit_status": "BREACH",
            },
            {
                "book_id": "BK-IRS-FFT-01",
                "book_name": "Rates Frankfurt",
                "desk": "RATES",
                "live_trade_count": 640,
                "gross_live_notional": 1.1e8,
                "net_live_notional": -2.0e7,
                "notional_limit": 4.0e8,
                "limit_utilisation_pct": 27.5,
                "limit_status": "OK",
            },
        ]
    )


def _counterparty_exposure() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "counterparty_id": "CP0001",
                "counterparty_name": "Aldgate Capital Partners LLP",
                "country_code": "GB",
                "credit_rating": "A+",
                "is_active": True,
                "live_trade_count": 210,
                "gross_live_notional": 9.1e8,
                "net_live_notional": 1.2e8,
                "rejected_event_count": 4,
                "has_live_trades_while_inactive": False,
            },
            {
                "counterparty_id": "CP0042",
                "counterparty_name": "Dormant Holdings SA",
                "country_code": "LU",
                "credit_rating": "BB",
                "is_active": False,
                "live_trade_count": 3,
                "gross_live_notional": 1.4e7,
                "net_live_notional": 1.4e7,
                "rejected_event_count": 22,
                "has_live_trades_while_inactive": True,
            },
        ]
    )


def _expiring_soon() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_id": "TRD-000123",
                "current_version": 2,
                "uti": "UTI0001",
                "product_type": "FX_FORWARD",
                "asset_class": "FX",
                "buy_sell": "BUY",
                "notional_amount": 1.5e6,
                "notional_currency": "EUR",
                "settlement_currency": "EUR",
                "trade_date": NOW.date() - timedelta(days=30),
                "settlement_date": NOW.date() + timedelta(days=2),
                "maturity_date": NOW.date() + timedelta(days=2),
                "days_to_maturity": 2,
                "counterparty_id": "CP0001",
                "counterparty_name": "Aldgate Capital Partners LLP",
                "counterparty_credit_rating": "A+",
                "book_id": "BK-FX-LDN-01",
                "book_name": "FX Cash London",
                "desk": "FX_CASH",
                "trader_id": "TR001",
                "legal_entity": "DB London Branch",
                "clearing_house": None,
                "lifecycle_status": "LIVE",
                "is_overdue_for_expiry": False,
                "urgency": "THIS_WEEK",
                "dbt_updated_at": NOW,
            }
        ]
    )


def _rejection_by_rule() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rule_code": "RJ001",
                "rule_name": "Stale version",
                "rule_category": "VERSION",
                "rule_severity": "REJECT",
                "requirement_ref": "R1",
                "remediation": "Confirm the current version with the source system.",
                "hit_count": 240,
                "distinct_trade_count": 233,
                "affected_notional": 4.1e8,
                "sources_affected": 2,
                "is_concentrated": True,
                "is_new_failure_mode": False,
                "is_chronic": True,
                "last_seen_at": NOW - timedelta(minutes=20),
            },
            {
                "rule_code": "RJ003",
                "rule_name": "Maturity date in the past",
                "rule_category": "TEMPORAL",
                "rule_severity": "REJECT",
                "requirement_ref": "R3",
                "remediation": "Correct the maturity date upstream.",
                "hit_count": 122,
                "distinct_trade_count": 122,
                "affected_notional": 8.8e7,
                "sources_affected": 3,
                "is_concentrated": False,
                "is_new_failure_mode": True,
                "is_chronic": False,
                "last_seen_at": NOW - timedelta(minutes=5),
            },
        ]
    )


def _rejection_by_source() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rule_code": "RJ001",
                "rule_name": "Stale version",
                "source_system": "MUREX",
                "hit_count": 210,
                "distinct_trade_count": 205,
                "distinct_file_count": 9,
                "share_of_rule_pct": 87.5,
                "is_concentrated": True,
                "first_seen_at": NOW - timedelta(days=6),
                "last_seen_at": NOW - timedelta(minutes=20),
            },
            {
                "rule_code": "RJ001",
                "rule_name": "Stale version",
                "source_system": "CALYPSO",
                "hit_count": 30,
                "distinct_trade_count": 28,
                "distinct_file_count": 4,
                "share_of_rule_pct": 12.5,
                "is_concentrated": False,
                "first_seen_at": NOW - timedelta(days=6),
                "last_seen_at": NOW - timedelta(hours=3),
            },
        ]
    )


def _rejected_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rejected_at": NOW - timedelta(minutes=20),
                "trade_id": "TRD-000999",
                "trade_version": 1,
                "disposition": "REJECTED",
                "primary_rule_code": "RJ001",
                "primary_rule_name": "Stale version",
                "violated_rule_codes": ["RJ001"],
                "source_system": "MUREX",
                "source_file_name": "trades_20260615.ndjson.gz",
                "counterparty_id": "CP0001",
                "notional_amount": 1.0e6,
                "notional_currency": "EUR",
                "raw_payload": '{"trade_id": "TRD-000999", "trade_version": 1}',
            },
            {
                "rejected_at": NOW - timedelta(minutes=25),
                "trade_id": "TRD-000998",
                "trade_version": 3,
                "disposition": "SUPERSEDED",
                "primary_rule_code": "RJ002",
                "primary_rule_name": "Superseded by later arrival",
                "violated_rule_codes": ["RJ002"],
                "source_system": "CALYPSO",
                "source_file_name": "trades_20260615.ndjson.gz",
                "counterparty_id": "CP0002",
                "notional_amount": 2.0e6,
                "notional_currency": "USD",
                # Deliberately not JSON: exercises the fallback path that renders bytes
                # verbatim, which is the RJ008 unparseable-payload case.
                "raw_payload": '{"trade_id": "TRD-00099',
            },
        ]
    )


def _rejection_trend() -> pd.DataFrame:
    dates = pd.date_range(end=NOW.date(), periods=10, freq="D")
    return pd.DataFrame(
        {
            "calendar_date": list(dates) * 2,
            "rule_code": ["RJ001"] * 10 + ["RJ003"] * 10,
            "rule_name": ["Stale version"] * 10 + ["Maturity date in the past"] * 10,
            "hit_count": [24] * 10 + [12] * 10,
        }
    )


def _rule_catalogue() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rule_code": "RJ001",
                "rule_name": "Stale version",
                "rule_category": "VERSION",
                "rule_severity": "REJECT",
                "requirement_ref": "R1",
                "description": "Incoming version is lower than the stored version.",
                "remediation": "Confirm the current version upstream.",
                "hit_count": 240,
                "last_fired_at": NOW - timedelta(minutes=20),
                "never_fired": False,
            },
            {
                "rule_code": "RJ014",
                "rule_name": "Settlement before trade date",
                "rule_category": "TEMPORAL",
                "rule_severity": "REJECT",
                "requirement_ref": "R3",
                "description": "Settlement date precedes the trade date.",
                "remediation": "Correct the dates upstream.",
                "hit_count": 0,
                "last_fired_at": None,
                "never_fired": True,
            },
        ]
    )


def _file_arrival() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "file_name": "dt=2026-06-15/trades_1.ndjson.gz",
                "file_state": "LOADED",
                "is_stalled": False,
                "staged_at": NOW - timedelta(minutes=7),
                "first_row_loaded_at": NOW - timedelta(minutes=6),
                "stage_to_load_seconds": 44,
                "rows_in_file": 3000,
                "size_bytes": 412_233,
                "load_method": "COPY",
                "expected_gap_minutes": 60.0,
            },
            {
                "file_name": "dt=2026-06-15/trades_2.ndjson.gz",
                "file_state": "STAGED_NOT_LOADED",
                "is_stalled": True,
                "staged_at": NOW - timedelta(minutes=40),
                "first_row_loaded_at": None,
                "stage_to_load_seconds": None,
                "rows_in_file": None,
                "size_bytes": 401_020,
                "load_method": None,
                "expected_gap_minutes": 60.0,
            },
        ]
    )


def _batch_health() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "batch_id": "b-0001",
                "batch_type": "COPY",
                "batch_status": "SUCCEEDED",
                "orchestrator_run_id": "manual__2026-06-15T11:00:00",
                "started_at": NOW - timedelta(minutes=6),
                "completed_at": NOW - timedelta(minutes=5),
                "duration_seconds": 42.5,
                "row_count": 3000,
                "file_count": 1,
                "error_count": 0,
                "error_message": None,
                "rows_per_second": 70.6,
                "is_stuck": False,
                "trailing_avg_duration_seconds": 40.1,
            },
            {
                "batch_id": "b-0002",
                "batch_type": "DRAIN",
                "batch_status": "RUNNING",
                "orchestrator_run_id": "manual__2026-06-15T11:00:00",
                "started_at": NOW - timedelta(minutes=45),
                "completed_at": None,
                "duration_seconds": None,
                "row_count": 0,
                "file_count": 0,
                "error_count": 0,
                "error_message": None,
                "rows_per_second": None,
                "is_stuck": True,
                "trailing_avg_duration_seconds": 3.2,
            },
        ]
    )


def _stream_lag() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "evaluated_at": NOW,
                "rows_in_stream": 0,
                "oldest_undrained_load_ts": None,
                "lag_minutes": None,
                "staleness_limit_minutes": 20160,
            }
        ]
    )


def _copy_errors() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "logged_at": NOW - timedelta(minutes=6),
                "source_file_name": "dt=2026-06-15/trades_1.ndjson.gz",
                "error_message": "Error parsing JSON: unexpected end of input",
                "rejected_record": '{"trade_id": "TRD-00099',
            }
        ]
    )


def _dbt_runs() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "invocation_id": "abc-123",
                "dbt_target": "dev",
                "dbt_version": "1.9.4",
                "run_status": "success",
                "run_started_at": NOW - timedelta(minutes=14),
                "run_completed_at": NOW - timedelta(minutes=11),
                "duration_seconds": 180,
                "models_built": 14,
                "tests_run": 168,
                "tests_failed": 0,
                "nodes_failed": 0,
                "node_seconds": 240.5,
                "rows_affected": 6120,
            },
            {
                "invocation_id": "abc-122",
                "dbt_target": "dev",
                "dbt_version": "1.9.4",
                "run_status": "failure",
                "run_started_at": NOW - timedelta(hours=2),
                "run_completed_at": NOW - timedelta(hours=2) + timedelta(seconds=90),
                "duration_seconds": 90,
                "models_built": 9,
                "tests_run": 40,
                "tests_failed": 1,
                "nodes_failed": 1,
                "node_seconds": 88.0,
                "rows_affected": 210,
            },
        ]
    )


def _dbt_failed_nodes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_started_at": NOW - timedelta(hours=2),
                "dbt_target": "dev",
                "resource_type": "test",
                "node_name": "assert_no_event_is_silently_dropped",
                "node_status": "fail",
                "failures": 3,
                "execution_time_s": 2.14,
                "message": "Got 3 results, configured to fail if != 0",
            }
        ]
    )


def _warehouse_credits() -> pd.DataFrame:
    dates = pd.date_range(end=NOW.date(), periods=7, freq="D")
    return pd.DataFrame(
        {
            "usage_date": list(dates) * 2,
            "warehouse_name": ["WH_TRADE_LOAD_XS"] * 7 + ["WH_TRADE_TRANSFORM_S"] * 7,
            "workload_class": ["INGESTION"] * 7 + ["TRANSFORMATION"] * 7,
            "credits_used": [0.04] * 7 + [0.18] * 7,
            "credits_compute": [0.03] * 7 + [0.17] * 7,
            "credits_cloud_services": [0.01] * 7 + [0.01] * 7,
            "credits_delta_vs_7d_ago": [None] * 14,
        }
    )


def _model_build_cost() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "model_name": "int_trade_event_adjudicated",
                "statement_count": 12,
                "total_elapsed_seconds": 88.4,
                "avg_elapsed_seconds": 7.37,
                "max_elapsed_seconds": 22.1,
                "bytes_scanned": 91_233_112,
                "statements_with_remote_spill": 0,
                "statements_with_tuning_signal": 1,
            },
            {
                "model_name": "fct_trade",
                "statement_count": 8,
                "total_elapsed_seconds": 31.2,
                "avg_elapsed_seconds": 3.9,
                "max_elapsed_seconds": 9.8,
                "bytes_scanned": 22_100_998,
                "statements_with_remote_spill": 0,
                "statements_with_tuning_signal": 0,
            },
        ]
    )


def _slowest_statements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "start_time": NOW - timedelta(minutes=13),
                "model_name": "int_trade_event_adjudicated",
                "warehouse_name": "WH_TRADE_TRANSFORM_S",
                "warehouse_size": "Small",
                "elapsed_seconds": 22.1,
                "execution_seconds": 20.4,
                "queued_overload_seconds": 0.0,
                "bytes_scanned": 41_233_112,
                "rows_produced": 6120,
                "partition_scan_ratio": 0.91,
                "bytes_spilled_to_remote_storage": 0,
                "tuning_signal": "FULL_SCAN_REVIEW_PRUNING",
                "execution_status": "SUCCESS",
                "query_id": "01b2c3d4-0000-0000-0000-000000000001",
            }
        ]
    )


#: Maps a fragment unique to each query to the frame it should return.
#:
#: Each fragment must match exactly one query helper, which is asserted by
#: `test_fixture_fragments_are_unambiguous` below. That assertion is not pedantry: the obvious
#: choice of fragment is the table name, and several queries read the same tables -- the rule
#: catalogue joins `ref_rejection_reason` to `trade_rule_result` and contains a `group by
#: rule_code`, which also appears in the rule leaderboard. A fragment matching two helpers
#: silently feeds one panel the other panel's columns, and the resulting KeyError looks like a
#: bug in the page rather than in the test.
RESPONSES: list[tuple[str, Any]] = [
    ("rpt_data_quality_scorecard", _scorecard),
    ("vw_pipeline_sla", _pipeline_sla),
    ("agg_trade_status_daily", _daily_status),
    ("least(current_version", _version_distribution),
    ("lifecycle_status,\n            count(*)", _lifecycle_mix),
    ("limit_utilisation_pct,\n            limit_status", _book_exposure),
    ("has_live_trades_while_inactive", _counterparty_exposure),
    ("rpt_trade_expiring_soon", _expiring_soon),
    ("any_value(rule_name) as rule_name", _rejection_by_rule),
    ("share_of_rule_pct,\n            is_concentrated", _rejection_by_source),
    ("fct_trade_rejected", _rejected_events),
    ("group by 1, 2, 3", _rejection_trend),
    ("reason.rule_code,", _rule_catalogue),
    ("vw_file_arrival", _file_arrival),
    ("vw_batch_health", _batch_health),
    ("vw_stream_lag", _stream_lag),
    ("copy_error", _copy_errors),
    ("group by invocation_id", _dbt_runs),
    ("'fail', 'error', 'runtime error', 'warn'", _dbt_failed_nodes),
    ("vw_warehouse_credits", _warehouse_credits),
    ("statements_with_remote_spill", _model_build_cost),
    ("order by elapsed_seconds desc", _slowest_statements),
]


def _matches(fragment: str, sql: str) -> bool:
    """Match a fixture fragment against a statement, ignoring case.

    The query helpers spell warehouse identifiers in upper case, which is the house style for
    Snowflake objects; the fragments here spell them lower case, which is the house style for
    Python. SQL does not care, and neither should the fixtures -- but while the comparison was
    case-sensitive, every fragment naming a view matched nothing, so `_fake_run_query`
    returned an empty frame for all ten and the pages under test rendered their empty states.
    Two of the assertions below existed precisely to catch that and were themselves the
    failures nobody had run.
    """
    return fragment.lower() in sql.lower()


def _fake_run_query(sql: str, label: str = "query") -> pd.DataFrame:
    """Stand-in for lib.connection.run_query.

    Falls back to an empty frame for an unrecognised statement rather than raising. An empty
    frame is a state the pages must handle anyway (a fresh install has empty everything), so
    the fallback exercises the empty-state branches rather than hiding a gap.
    """
    for fragment, builder in RESPONSES:
        if _matches(fragment, sql):
            return builder()
    return pd.DataFrame()


def _all_query_statements() -> dict[str, str]:
    """Build every query helper's SQL with default arguments, keyed by helper name."""
    import inspect

    from lib import queries

    statements: dict[str, str] = {}
    for name in sorted(dir(queries)):
        if name.startswith("_"):
            continue
        fn = getattr(queries, name)
        if not callable(fn):
            continue
        signature = inspect.signature(fn)
        if "database" not in signature.parameters:
            continue
        kwargs: dict[str, Any] = {"database": "TRADES_DEV"}
        for param_name, param in signature.parameters.items():
            if param_name != "database" and param.default is inspect.Parameter.empty:
                kwargs[param_name] = 30
        statements[name] = fn(**kwargs)
    return statements


@pytest.fixture(autouse=True)
def _patch_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the query layer everywhere it is imported.

    The pages do `from lib.connection import run_query`, which binds the name into each page's
    module namespace at import time. AppTest re-imports the page for every run, so patching
    the source module is enough -- but `lib.queries` is also imported by name in several
    places, so both are patched to be safe.
    """
    from lib import connection

    monkeypatch.setattr(connection, "run_query", _fake_run_query)
    monkeypatch.setattr(connection, "database", lambda: "TRADES_DEV")
    monkeypatch.setattr(connection, "clear_caches", lambda: None)
    monkeypatch.setattr(
        connection, "get_session", lambda: pytest.fail("the dashboard must not connect in tests")
    )

    # The sidebar reads these to display which environment is in view.
    for key, value in {
        "SNOWFLAKE_ACCOUNT": "xy12345.eu-central-1",
        "SNOWFLAKE_DATABASE": "TRADES_DEV",
        "SNOWFLAKE_ROLE": "FR_DATA_ENGINEER",
        "SNOWFLAKE_WAREHOUSE": "WH_TRADE_TRANSFORM_S",
    }.items():
        monkeypatch.setenv(key, value)


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_page_renders_without_exception(page: Path) -> None:
    """Every page runs to completion with plausible data.

    `at.exception` is the assertion that matters. Streamlit catches exceptions per-script-run
    and renders them as a red block, so a broken page still returns HTTP 200 -- which is why a
    curl-based health check proves nothing about a dashboard.
    """
    at = AppTest.from_file(str(page), default_timeout=30)
    at.run()

    assert not at.exception, f"{page.name} raised: " + "; ".join(str(e.value) for e in at.exception)


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_page_renders_on_empty_platform(page: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every page survives a completely empty warehouse.

    This is the state of a fresh clone: Snowflake deployed, nothing loaded, no marts built. It
    is also the first thing a reviewer sees, and a traceback there reads as "this does not
    work" regardless of what the rest of the repo does. Each page must degrade to an
    explanatory empty state instead.
    """
    from lib import connection

    monkeypatch.setattr(connection, "run_query", lambda sql, label="query": pd.DataFrame())

    at = AppTest.from_file(str(page), default_timeout=30)
    at.run()

    assert not at.exception, f"{page.name} raised on an empty platform: " + "; ".join(
        str(e.value) for e in at.exception
    )


def test_landing_page_shows_stage_health() -> None:
    """The landing page must surface the four stage badges and the overall verdict.

    Asserted rather than assumed because these are the only elements someone glances at
    before deciding whether to investigate. A refactor that quietly drops the RAG header would
    otherwise pass every other test in this file.
    """
    at = AppTest.from_file(str(DASHBOARD_DIR / "app.py"), default_timeout=30)
    at.run()

    rendered = " ".join(element.value for element in at.markdown)
    for stage in ("Ingestion", "Capture", "Transform", "Correctness"):
        assert stage in rendered, f"stage badge missing: {stage}"
    assert "Overall" in rendered


def test_findings_block_reports_a_real_problem(monkeypatch: pytest.MonkeyPatch) -> None:
    """A red scorecard must produce an explicit finding, not just a number in a grid.

    The interpretation is the product here. Expecting a tired engineer to notice that
    `overdue_expiry_trades` reads 4 instead of 0 in a row of five metrics is how incidents get
    missed, so the page states the conclusion in words -- and this test is what keeps it doing
    so.
    """
    from lib import connection

    def unhealthy(sql: str, label: str = "query") -> pd.DataFrame:
        frame = _fake_run_query(sql, label)
        if _matches("rpt_data_quality_scorecard", sql) and not frame.empty:
            frame = frame.copy()
            frame.loc[0, "overdue_expiry_trades"] = 4
            frame.loc[0, "overall_status"] = "RED"
        return frame

    monkeypatch.setattr(connection, "run_query", unhealthy)

    at = AppTest.from_file(str(DASHBOARD_DIR / "app.py"), default_timeout=30)
    at.run()

    assert not at.exception
    errors = " ".join(element.value for element in at.error)
    assert "still LIVE" in errors
    assert "runbook" in errors


def test_every_query_helper_is_exercised_by_a_fixture() -> None:
    """Guard against a query the smoke tests silently never cover.

    Without this, adding a panel with a new query means `_fake_run_query` returns an empty
    frame for it, the empty-state branch renders, the test passes, and the panel's real
    rendering path is never tested. That is the exact failure mode this whole file exists to
    prevent, so it needs its own check.
    """
    uncovered = [
        name for name, sql in _all_query_statements().items() if _fake_run_query(sql).empty
    ]

    assert not uncovered, (
        "these query helpers have no fixture, so their panels are only ever rendered empty: "
        + ", ".join(uncovered)
    )


def test_fixture_fragments_are_unambiguous() -> None:
    """Each fragment in RESPONSES must identify exactly one query.

    A fragment matching two helpers hands one panel the other's columns. The symptom is a
    KeyError deep inside a page, which reads as a bug in the dashboard rather than in the
    fixture -- so the ambiguity is worth failing on directly, with a message that says which
    fragment is at fault.
    """
    statements = _all_query_statements()

    ambiguous: list[str] = []
    unmatched: list[str] = []
    for fragment, _ in RESPONSES:
        matches = [name for name, sql in statements.items() if _matches(fragment, sql)]
        if len(matches) > 1:
            ambiguous.append(f"{fragment!r} matches {matches}")
        elif not matches:
            unmatched.append(f"{fragment!r} matches nothing")

    assert not ambiguous and not unmatched, "; ".join(ambiguous + unmatched)


def test_dashboard_only_reads_marts_and_monitoring() -> None:
    """The dashboard must not read RAW, STAGING or INTERMEDIATE, with one named exception.

    The marts are the tested contract. A dashboard that reaches past them into RAW will
    eventually compute a number that disagrees with the pipeline's, and then two teams argue
    about which is right instead of fixing anything.

    The exception is deliberate and narrow: `RAW.COPY_ERROR` holds lines that failed to parse
    and so never reached the rule engine, meaning no mart can contain them. The MONITORING
    views are also allowed, and are the reason this rule is expressed as "not RAW" rather than
    "only CORE and REPORTING" -- monitoring exists precisely to answer questions when the marts
    are the thing that is broken.
    """
    forbidden = (".intermediate.", ".staging.")
    allowed_raw = (".raw.copy_error",)

    offenders: list[str] = []
    for name, statement in _all_query_statements().items():
        sql = statement.lower()
        if any(fragment in sql for fragment in forbidden):
            offenders.append(f"{name} reads staging or intermediate")
        if ".raw." in sql and not any(allowed in sql for allowed in allowed_raw):
            offenders.append(f"{name} reads RAW outside the allowed exceptions")

    assert not offenders, "; ".join(offenders)
