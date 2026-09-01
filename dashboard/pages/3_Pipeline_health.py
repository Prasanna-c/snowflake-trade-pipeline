"""
Pipeline health: file arrival, load batches, stream lag, dbt runs, parse errors and cost.

The engineer's page. It exists to answer one question fast during an incident -- which stage
broke -- and it is laid out in pipeline order so the answer is found by reading top to bottom.

A note on freshness, stated on the page as well as here because it matters: the first four
sections read project tables and are current to the second. The cost section reads
SNOWFLAKE.ACCOUNT_USAGE, which lags by up to 45 minutes and is empty on a new account.
Comparing the two without knowing that produces confidently wrong conclusions.
"""

from __future__ import annotations

import altair as alt
import streamlit as st

from lib import components as ui
from lib import queries as q
from lib.connection import database, run_query

st.set_page_config(
    page_title="Pipeline health", page_icon=":material/rule_settings:", layout="wide"
)

st.title("Pipeline health")
db = database()

sla = run_query(q.pipeline_sla(db), label="pipeline SLA")
if sla.empty:
    st.error(
        "Could not read `MONITORING.VW_PIPELINE_SLA`. Run `make doctor`, then `make deploy-sql`.",
        icon=":material/error:",
    )
    st.stop()

row = sla.iloc[0]
ui.rag_header(
    {
        "Ingestion": row.get("ingestion_status"),
        "Capture": row.get("capture_status"),
        "Transform": row.get("transform_status"),
        "Correctness": row.get("correctness_status"),
    }
)
st.write("")
st.caption(
    "Stage-level rather than one overall light, on purpose. \u201cThe platform is red\u201d "
    "starts a hunt; \u201ccapture is red, everything else green\u201d points at the stream "
    "drain immediately."
)

st.divider()

# ===========================================================================
# 1. Ingestion -- did the files arrive?
# ===========================================================================
st.subheader("1. File arrival")
st.markdown(
    "Absence produces no event, so a missing file cannot be detected by watching for "
    "failures. `VW_FILE_ARRIVAL` full-outer-joins the stage directory against loaded rows, "
    "which makes three different problems distinguishable: staged but not loaded (Snowpipe or "
    "COPY stalled), loaded but no longer staged (normal, archived), and never arrived at all."
)

c1, c2, c3 = st.columns(3)
with c1:
    ui.metric(
        "Minutes since last event",
        row.get("minutes_since_last_event"),
        help_text="Amber above 45, red above 90.",
    )
with c2:
    ui.metric("Events loaded (24h)", row.get("events_loaded_last_24h"))
with c3:
    ui.metric("Last loaded at", row.get("last_event_loaded_at"), fmt="{}")

files = run_query(q.file_arrival(db), label="file arrival")
if files.empty:
    ui.empty_state("files", "Nothing has been staged or loaded. Run `make demo`.")
else:
    stalled = files[files["is_stalled"].fillna(False).astype(bool)]
    if not stalled.empty:
        st.error(
            f"{len(stalled)} file(s) have sat on the stage unloaded for over 15 minutes. "
            "Snowpipe is stalled or the COPY task is not running. "
            "See `docs/runbook.md#file-arrival-delay`.",
            icon=":material/error:",
        )

    st.dataframe(
        files,
        hide_index=True,
        width="stretch",
        column_config={
            "file_name": st.column_config.TextColumn("File", width="large"),
            "file_state": st.column_config.TextColumn("State", width="small"),
            "is_stalled": st.column_config.CheckboxColumn("Stalled", width="small"),
            "staged_at": st.column_config.DatetimeColumn("Staged"),
            "first_row_loaded_at": st.column_config.DatetimeColumn("Loaded"),
            "stage_to_load_seconds": st.column_config.NumberColumn("Lag (s)", format="%d"),
            "rows_in_file": st.column_config.NumberColumn("Rows", format="%d"),
            "size_bytes": st.column_config.NumberColumn("Bytes", format="%d"),
            "load_method": st.column_config.TextColumn("Method", width="small"),
            "expected_gap_minutes": st.column_config.NumberColumn(
                "Expected gap (min)",
                format="%.1f",
                help=(
                    "Median observed gap between arrivals over the last 7 days. Derived rather "
                    "than hard-coded, so the monitor keeps working when the upstream changes "
                    "from hourly to every 15 minutes."
                ),
            ),
        },
    )
    ui.caption_source(["MONITORING.VW_FILE_ARRIVAL"], "real-time")

st.divider()

# ===========================================================================
# 2. Load batches.
# ===========================================================================
st.subheader("2. Load batches")
st.markdown(
    "Every COPY and every stream drain writes a row to `RAW.LOAD_BATCH` before it starts and "
    "updates it when it finishes. A row still RUNNING long after it started is the signature "
    "of a crashed session, and is the only way to distinguish \u201cstill working\u201d from "
    "\u201cdied silently\u201d -- a failure that leaves no error anywhere."
)

batches = run_query(q.batch_health(db), label="batch health")
if batches.empty:
    ui.empty_state("load batches", "No loads recorded yet.")
else:
    stuck = batches[batches["is_stuck"].fillna(False).astype(bool)]
    if not stuck.empty:
        st.error(
            f"{len(stuck)} batch(es) have been RUNNING for over 15 minutes. A previous run "
            "died mid-load. See `docs/runbook.md#load-failures`.",
            icon=":material/error:",
        )

    failed = batches[batches["batch_status"] == "FAILED"]
    if not failed.empty:
        st.warning(
            f"{len(failed)} failed batch(es) in the window shown.", icon=":material/warning:"
        )

    throughput = batches.dropna(subset=["rows_per_second"])
    if not throughput.empty:
        chart = (
            alt.Chart(throughput)
            .mark_circle(size=90, opacity=0.75)
            .encode(
                x=alt.X("started_at:T", title=None),
                y=alt.Y("rows_per_second:Q", title="Rows / second"),
                color=alt.Color("batch_type:N", title="Batch type"),
                tooltip=[
                    alt.Tooltip("batch_id:N", title="Batch"),
                    alt.Tooltip("batch_type:N", title="Type"),
                    alt.Tooltip("batch_status:N", title="Status"),
                    alt.Tooltip("row_count:Q", title="Rows", format=","),
                    alt.Tooltip("duration_seconds:Q", title="Duration (s)", format=".2f"),
                    alt.Tooltip("rows_per_second:Q", title="Rows/s", format=",.0f"),
                    alt.Tooltip(
                        "trailing_avg_duration_seconds:Q",
                        title="Trailing avg duration (s)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)
        st.caption(
            "Each batch against its own type's recent norm. A batch drifting away from its "
            "trailing average is visible here well before it becomes a timeout."
        )

    st.dataframe(
        batches,
        hide_index=True,
        width="stretch",
        column_config={
            "batch_id": st.column_config.TextColumn("Batch"),
            "batch_type": st.column_config.TextColumn("Type", width="small"),
            "batch_status": st.column_config.TextColumn("Status", width="small"),
            "orchestrator_run_id": st.column_config.TextColumn("Airflow run"),
            "started_at": st.column_config.DatetimeColumn("Started"),
            "completed_at": st.column_config.DatetimeColumn("Completed"),
            "duration_seconds": st.column_config.NumberColumn("Duration (s)", format="%.2f"),
            "row_count": st.column_config.NumberColumn("Rows", format="%d"),
            "file_count": st.column_config.NumberColumn("Files", format="%d"),
            "error_count": st.column_config.NumberColumn("Errors", format="%d"),
            "rows_per_second": st.column_config.NumberColumn("Rows/s", format="%.0f"),
            "is_stuck": st.column_config.CheckboxColumn("Stuck", width="small"),
        },
    )
    ui.caption_source(["MONITORING.VW_BATCH_HEALTH"], "real-time")

st.divider()

# ===========================================================================
# 3. Change capture -- stream lag.
# ===========================================================================
st.subheader("3. Change capture")
st.markdown(
    "`SYSTEM$STREAM_HAS_DATA` returns a boolean, which is not enough to alert on severity, so "
    "the un-drained rows are counted directly. A stream not consumed within its source table's "
    "Time Travel window goes stale, at which point the delta is unrecoverable from the stream "
    "and the only remedy is a full COPY replay -- so the alert threshold sits far below the "
    "limit shown."
)

lag = run_query(q.stream_lag(db), label="stream lag")
if lag.empty:
    ui.empty_state("stream data", "The stream has not been created, or has never had rows.")
else:
    lag_row = lag.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        ui.metric("Rows in stream", lag_row.get("rows_in_stream"))
    with c2:
        ui.metric("Lag (minutes)", lag_row.get("lag_minutes"))
    with c3:
        ui.metric(
            "Staleness limit (minutes)",
            lag_row.get("staleness_limit_minutes"),
            help_text="14 days. Past this the stream cannot be used to recover the delta.",
        )
    with c4:
        ui.metric("Minutes since last drain", row.get("minutes_since_last_drain"))

    st.caption(
        "Selecting from a stream does not advance its offset \u2014 only DML that consumes it "
        "does. This panel is therefore safe to refresh as often as you like."
    )
    ui.caption_source(["MONITORING.VW_STREAM_LAG"], "real-time")

st.divider()

# ===========================================================================
# 4. Transform -- dbt.
# ===========================================================================
st.subheader("4. Transform")
st.markdown(
    "dbt writes its own run results into `AUDIT.DBT_RUN_RESULT` from an `on-run-end` hook. "
    "That is what lets Snowflake notice a stale curated layer when Airflow is down and "
    "reporting nothing \u2014 which is precisely when nobody is watching the Airflow UI."
)

c1, c2, c3 = st.columns(3)
with c1:
    ui.metric("Minutes since dbt success", row.get("minutes_since_dbt_success"))
with c2:
    ui.metric("Rows awaiting transform", row.get("rows_awaiting_transform"))
with c3:
    ui.metric(
        "Oldest backlog (minutes)",
        row.get("oldest_backlog_minutes"),
        help_text="Age of the oldest un-adjudicated queued row. Amber above 60.",
    )

runs = run_query(q.dbt_runs(db), label="dbt runs")
if runs.empty:
    ui.empty_state(
        "dbt runs",
        "No dbt invocation has been recorded. Run `make dbt-build` with a real Snowflake target.",
    )
else:
    failures = runs[runs["run_status"] == "failure"]
    if not failures.empty:
        st.error(
            f"{len(failures)} of the last {len(runs)} dbt invocations failed. "
            "Failed nodes are listed below.",
            icon=":material/error:",
        )

    chart = (
        alt.Chart(runs)
        .mark_bar()
        .encode(
            x=alt.X("run_started_at:T", title=None),
            y=alt.Y("duration_seconds:Q", title="Duration (s)"),
            color=alt.Color(
                "run_status:N",
                title="Status",
                scale=alt.Scale(domain=["success", "failure"], range=["#1a7f37", "#cf222e"]),
            ),
            tooltip=[
                alt.Tooltip("run_started_at:T", title="Started"),
                alt.Tooltip("run_status:N", title="Status"),
                alt.Tooltip("dbt_target:N", title="Target"),
                alt.Tooltip("models_built:Q", title="Models"),
                alt.Tooltip("tests_run:Q", title="Tests"),
                alt.Tooltip("tests_failed:Q", title="Tests failed"),
                alt.Tooltip("duration_seconds:Q", title="Duration (s)"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(chart, use_container_width=True)

    st.dataframe(
        runs,
        hide_index=True,
        width="stretch",
        column_config={
            "invocation_id": st.column_config.TextColumn("Invocation", width="medium"),
            "dbt_target": st.column_config.TextColumn("Target", width="small"),
            "dbt_version": st.column_config.TextColumn("dbt", width="small"),
            "run_status": st.column_config.TextColumn("Status", width="small"),
            "run_started_at": st.column_config.DatetimeColumn("Started"),
            "duration_seconds": st.column_config.NumberColumn("Wall (s)", format="%d"),
            "node_seconds": st.column_config.NumberColumn(
                "Node (s)",
                format="%.1f",
                help="Summed node execution time. Exceeds wall time when threads > 1.",
            ),
            "models_built": st.column_config.NumberColumn("Models", format="%d"),
            "tests_run": st.column_config.NumberColumn("Tests", format="%d"),
            "tests_failed": st.column_config.NumberColumn("Tests failed", format="%d"),
            "nodes_failed": st.column_config.NumberColumn("Nodes failed", format="%d"),
            "rows_affected": st.column_config.NumberColumn("Rows", format="%d"),
        },
    )

    failed_nodes = run_query(q.dbt_failed_nodes(db), label="failed dbt nodes")
    if not failed_nodes.empty:
        with st.expander(f"Failed and warning nodes ({len(failed_nodes)})", expanded=False):
            st.dataframe(
                failed_nodes,
                hide_index=True,
                width="stretch",
                column_config={
                    "run_started_at": st.column_config.DatetimeColumn("Run"),
                    "node_name": st.column_config.TextColumn("Node"),
                    "node_status": st.column_config.TextColumn("Status", width="small"),
                    "resource_type": st.column_config.TextColumn("Type", width="small"),
                    "failures": st.column_config.NumberColumn("Failing rows", format="%d"),
                    "message": st.column_config.TextColumn("Message", width="large"),
                },
            )
            st.caption(
                "Test failures are stored to `DBT_TEST_FAILURES` as well (`--store-failures`), "
                "so the offending rows themselves are queryable rather than merely counted."
            )

    ui.caption_source(["AUDIT.DBT_RUN_RESULT"], "written by dbt's on-run-end hook")

st.divider()

# ===========================================================================
# 5. Parse errors.
# ===========================================================================
st.subheader("5. Parse errors")
st.markdown(
    "COPY runs with `ON_ERROR = CONTINUE`, so one malformed line cannot block a whole file. "
    "That resilience is also what makes loss invisible, so every rejected line is captured "
    "into `RAW.COPY_ERROR` with its bytes. Resilient ingestion without this table is silent "
    "data loss."
)

errors = run_query(q.copy_errors(db), label="copy errors")
if errors.empty:
    st.success("No parse errors recorded.", icon=":material/check_circle:")
else:
    st.warning(
        f"{len(errors)} parse error(s) in the most recent window. These rows never reached "
        "the rule engine, so they appear in neither the accepted nor the rejected counts \u2014 "
        "which is exactly why they need their own panel.",
        icon=":material/warning:",
    )
    st.dataframe(
        errors,
        hide_index=True,
        width="stretch",
        column_config={
            "logged_at": st.column_config.DatetimeColumn("Logged"),
            "source_file_name": st.column_config.TextColumn("File", width="medium"),
            "error_message": st.column_config.TextColumn("Error", width="large"),
            "rejected_record": st.column_config.TextColumn("Raw line", width="large"),
        },
    )
    ui.caption_source(["RAW.COPY_ERROR"], "real-time")

st.divider()

# ===========================================================================
# 6. Cost and performance.
# ===========================================================================
st.subheader("6. Cost and performance")
st.warning(
    "This section reads `SNOWFLAKE.ACCOUNT_USAGE`, which lags by up to 45 minutes and is "
    "empty on a newly created account. Everything above reads project tables and is current "
    "to the second. Comparing across that boundary without knowing it is how people conclude "
    "the pipeline is broken when it is not.",
    icon=":material/schedule:",
)

cost_days = (
    st.segmented_control(
        "Window",
        options=[7, 14, 30],
        default=14,
        format_func=lambda d: f"{d} days",
        key="cost_window",
    )
    or 14
)

credits = run_query(q.warehouse_credits(db, cost_days), label="warehouse credits")
if credits.empty:
    ui.empty_state(
        "credit history",
        "ACCOUNT_USAGE has no rows yet. On a new trial account this takes a few hours to populate.",
    )
else:
    chart = (
        alt.Chart(credits)
        .mark_bar()
        .encode(
            x=alt.X("usage_date:T", title=None, axis=alt.Axis(format="%d %b")),
            y=alt.Y("credits_used:Q", title="Credits"),
            color=alt.Color(
                "workload_class:N",
                title="Workload",
                scale=alt.Scale(
                    domain=["INGESTION", "TRANSFORMATION", "REPORTING", "OTHER"],
                    range=["#4c78a8", "#f58518", "#54a24b", "#9d9d9d"],
                ),
            ),
            tooltip=[
                alt.Tooltip("usage_date:T", title="Date"),
                alt.Tooltip("warehouse_name:N", title="Warehouse"),
                alt.Tooltip("workload_class:N", title="Workload"),
                alt.Tooltip("credits_used:Q", title="Credits", format=".4f"),
                alt.Tooltip("credits_delta_vs_7d_ago:Q", title="vs 7d ago", format="+.4f"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(
        "Split by workload class, which is only possible because ingestion, transformation and "
        "reporting have separate warehouses. On one shared warehouse this chart is a single "
        "bar and cost cannot be attributed to anything."
    )
    ui.caption_source(["MONITORING.VW_WAREHOUSE_CREDITS"], "ACCOUNT_USAGE, up to 3h latency")

model_cost = run_query(q.model_build_cost(db, cost_days), label="model build cost")
if not model_cost.empty:
    st.markdown("**Warehouse-seconds by dbt model**")
    chart = (
        alt.Chart(model_cost.head(20))
        .mark_bar(color="#f58518")
        .encode(
            y=alt.Y("model_name:N", title=None, sort="-x"),
            x=alt.X("total_elapsed_seconds:Q", title="Total elapsed seconds"),
            tooltip=[
                alt.Tooltip("model_name:N", title="Model"),
                alt.Tooltip("statement_count:Q", title="Statements"),
                alt.Tooltip("total_elapsed_seconds:Q", title="Total (s)", format=".1f"),
                alt.Tooltip("avg_elapsed_seconds:Q", title="Avg (s)", format=".2f"),
                alt.Tooltip("max_elapsed_seconds:Q", title="Max (s)", format=".2f"),
                alt.Tooltip("bytes_scanned:Q", title="Bytes scanned", format=","),
                alt.Tooltip("statements_with_remote_spill:Q", title="Remote spills"),
            ],
        )
        .properties(height=min(26 * min(len(model_cost), 20) + 40, 520))
    )
    st.altair_chart(chart, use_container_width=True)
    st.caption(
        "Attributable to a model only because every dbt statement is tagged with its model "
        "name by the `query_tag` pre-hook. Without the tag, `ACCOUNT_USAGE` can say what the "
        "warehouse cost but not what caused it."
    )

slow = run_query(q.slowest_statements(db, cost_days), label="slowest statements")
if not slow.empty:
    tuning = slow[slow["tuning_signal"] != "OK"]
    if not tuning.empty:
        st.info(
            f"{len(tuning)} statement(s) carry a tuning signal. The view translates raw "
            "counters into a diagnosis \u2014 remote spill means size up the warehouse, "
            "queueing means add a cluster, a full scan means review pruning \u2014 because "
            "`bytes_spilled_to_remote_storage = 4.2e9` is not an instruction.",
            icon=":material/lightbulb:",
        )
    with st.expander(f"Slowest {len(slow)} statements", expanded=False):
        st.dataframe(
            slow,
            hide_index=True,
            width="stretch",
            column_config={
                "start_time": st.column_config.DatetimeColumn("Started"),
                "model_name": st.column_config.TextColumn("Model"),
                "warehouse_name": st.column_config.TextColumn("Warehouse", width="small"),
                "warehouse_size": st.column_config.TextColumn("Size", width="small"),
                "elapsed_seconds": st.column_config.NumberColumn("Elapsed (s)", format="%.2f"),
                "execution_seconds": st.column_config.NumberColumn("Exec (s)", format="%.2f"),
                "queued_overload_seconds": st.column_config.NumberColumn(
                    "Queued (s)", format="%.2f"
                ),
                "bytes_scanned": st.column_config.NumberColumn("Bytes scanned", format="%d"),
                "partition_scan_ratio": st.column_config.NumberColumn(
                    "Scan ratio",
                    format="%.3f",
                    help="Partitions scanned over partitions total. Near 1.0 means no pruning.",
                ),
                "tuning_signal": st.column_config.TextColumn("Signal"),
                "query_id": st.column_config.TextColumn("Query ID", width="medium"),
            },
        )
    ui.caption_source(["MONITORING.VW_DBT_QUERY_PERFORMANCE"], "ACCOUNT_USAGE, ~45m latency")
