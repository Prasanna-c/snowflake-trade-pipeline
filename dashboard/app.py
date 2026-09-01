"""
===============================================================================
TRADE LIFECYCLE PIPELINE -- OPERATIONS DASHBOARD

Landing page: is the platform healthy, and if not, which stage is at fault.

Run with:
    streamlit run dashboard/app.py          (or `make dashboard`)

-------------------------------------------------------------------------------
WHY STREAMLIT RATHER THAN TABLEAU OR LOOKER STUDIO

The brief allows any of them. Streamlit was chosen for three reasons that matter to this
particular deliverable:

  * It lives in the repo. The dashboard is version-controlled, code-reviewed and deployed by
    the same pipeline as the models it reads. A Tableau workbook is a binary artifact edited
    outside source control, and its definition of "reject rate" drifts from dbt's the first
    time someone is in a hurry.

  * It runs on a laptop with no server and no licence, which is a hard requirement here.

  * It can express the drill-down that makes this dashboard operationally useful -- from a
    rate, to a rule, to the raw JSON of one rejected trade -- without an extract or a
    published data source.

What is given up is real: Tableau's governed row-level security, scheduled subscriptions and
non-technical authoring are all things a bank genuinely needs. The mitigation is that every
number here comes from a tested dbt mart rather than from dashboard logic, so pointing Tableau
at the same marts later reproduces these figures exactly. Recorded in
docs/adr/0010-dashboard-choice.md.

-------------------------------------------------------------------------------
WHY THE DASHBOARD READS THE SAME SCORECARD THE AIRFLOW GATE READS

REPORTING.RPT_DATA_QUALITY_SCORECARD is one row, and three consumers read it: this dashboard,
the Airflow publish-readiness gate, and the Snowflake alert. That is deliberate. If the
dashboard computed its own health, an on-call engineer would face a dashboard saying GREEN and
a page saying RED, and would have to decide which to believe. One definition of healthy means
that situation cannot arise.
===============================================================================
"""

from __future__ import annotations

from datetime import UTC, datetime

import streamlit as st

from lib import components as ui
from lib import queries as q
from lib.connection import CACHE_TTL_SECONDS, clear_caches, database, run_query

st.set_page_config(
    page_title="Trade Pipeline Operations",
    page_icon=":material/monitoring:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def sidebar() -> None:
    """Shared sidebar: connection facts and a refresh control.

    The account, database and target are shown because the commonest cause of "the dashboard
    is wrong" is that it is pointed at dev while the reporter is looking at prod. Making that
    visible costs three lines and removes a whole category of confusion.
    """
    import os

    with st.sidebar:
        st.markdown("### Trade Pipeline")
        st.caption("Operations and data quality")

        st.divider()
        st.markdown("**Connection**")
        st.code(
            f"account   {os.environ.get('SNOWFLAKE_ACCOUNT', '?')}\n"
            f"database  {os.environ.get('SNOWFLAKE_DATABASE', '?')}\n"
            f"role      {os.environ.get('SNOWFLAKE_ROLE', '?')}\n"
            f"warehouse {os.environ.get('SNOWFLAKE_LOAD_WAREHOUSE') or os.environ.get('SNOWFLAKE_WAREHOUSE', '?')}",
            language="text",
        )

        st.divider()
        if st.button("Refresh data", width="stretch", icon=":material/refresh:"):
            clear_caches()
            st.rerun()
        st.caption(
            f"Results cached for {CACHE_TTL_SECONDS}s. Short on purpose: a monitoring view "
            "that is minutes stale will convince someone a resolved incident is ongoing."
        )

        st.divider()
        st.caption(
            "Every figure comes from a tested dbt mart or a monitoring view. "
            "This app contains no business logic of its own -- see the module docstring."
        )


def main() -> None:
    sidebar()

    st.title("Trade lifecycle pipeline")
    st.caption(f"Rendered {datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC \u00b7 database `{database()}`")

    db = database()

    # -----------------------------------------------------------------------
    # Stage health, from the monitoring layer.
    #
    # This block reads MONITORING.VW_PIPELINE_SLA rather than the dbt scorecard, and the order
    # matters: the monitoring view depends only on load metadata and the stream queue, so it
    # answers even when the transform layer is the thing that is broken. The scorecard below
    # is richer but depends on the marts existing. Showing the resilient one first means the
    # page still tells you something useful on the worst day.
    # -----------------------------------------------------------------------
    sla = run_query(q.pipeline_sla(db), label="pipeline SLA")

    if sla.empty:
        st.error(
            "Could not read `MONITORING.VW_PIPELINE_SLA`. Either the Snowflake layer is not "
            "deployed or the role cannot see it.\n\n"
            "Run `make doctor` to diagnose, then `make deploy-sql`.",
            icon=":material/error:",
        )
    else:
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

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            ui.metric(
                "Minutes since last event",
                row.get("minutes_since_last_event"),
                help_text="Amber above 45, red above 90. Matches the file-arrival SLA.",
            )
        with c2:
            ui.metric(
                "Awaiting transform",
                row.get("rows_awaiting_transform"),
                help_text="Rows drained from the stream but not yet adjudicated.",
            )
        with c3:
            ui.metric(
                "Minutes since dbt success",
                row.get("minutes_since_dbt_success"),
                help_text=(
                    "Read from AUDIT.DBT_RUN_RESULT, which dbt writes itself. So this is "
                    "correct even when Airflow is down and reporting nothing."
                ),
            )
        with c4:
            ui.metric(
                "Overdue for expiry",
                row.get("trades_overdue_for_expiry"),
                help_text=(
                    "Matured trades still marked LIVE. Must be zero: any other value means "
                    "business rule 4 is not being applied, so the pipeline is stalled."
                ),
            )
        ui.caption_source(
            ["MONITORING.VW_PIPELINE_SLA"],
            "real-time (reads project metadata, not ACCOUNT_USAGE)",
        )

    st.divider()

    # -----------------------------------------------------------------------
    # The scorecard: the business view of the same platform.
    # -----------------------------------------------------------------------
    st.subheader("Platform scorecard")

    card = run_query(q.scorecard(db), label="data quality scorecard")
    if card.empty:
        ui.empty_state(
            "curated data",
            "The reporting layer has not been built. Run `make dbt-build`, or trigger the "
            "`trade_pipeline` DAG in Airflow.",
        )
        return

    s = card.iloc[0]

    overall = str(s.get("overall_status", "UNKNOWN"))
    st.markdown(
        f"**Overall** {ui.rag_badge(overall)} "
        f'<span style="color:#57606a;font-size:0.85rem;">'
        f"same row the Airflow publish gate and the Snowflake alert evaluate"
        f"</span>",
        unsafe_allow_html=True,
    )
    st.write("")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        ui.metric("Trades", s.get("total_trades"))
    with c2:
        ui.metric("Live", s.get("live_trades"))
    with c3:
        ui.metric(
            "Live notional",
            s.get("live_gross_notional"),
            fmt="{}",
            help_text="Gross notional of LIVE trades, all currencies summed without FX conversion.",
        )
        st.caption(ui.money(s.get("live_gross_notional")))
    with c4:
        ui.metric(
            "Reject rate",
            s.get("reject_rate_pct"),
            fmt="{:.2f}%",
            help_text=(
                "Rejected events over all events, lifetime. The simulator injects faults "
                "deliberately, so a non-zero rate here is the rule engine working."
            ),
        )
    with c5:
        ui.metric(
            "Reject rate 24h",
            s.get("reject_rate_24h_pct"),
            fmt="{:.2f}%",
            help_text="Amber above 15%, red above 25%. This is the figure the gate acts on.",
        )

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        ui.metric("Events adjudicated", s.get("total_events"))
    with c2:
        ui.metric(
            "Superseded",
            s.get("superseded_events"),
            help_text=(
                "Same-version resends replaced by a later arrival (business rule 2). Not "
                "failures -- excluded from the reject rate for that reason."
            ),
        )
    with c3:
        ui.metric(
            "Amended trades",
            s.get("amended_trades"),
            help_text="Trades now above version 1.",
        )
    with c4:
        ui.metric(
            "Expiring within 7 days",
            s.get("expiring_soon_trades"),
        )
    with c5:
        ui.metric(
            "Rules never fired",
            s.get("rules_never_fired"),
            help_text=(
                "Declared rules with no recorded hit. A rule that has never fired is either "
                "genuinely never violated or silently broken, and a green test suite cannot "
                "tell those apart."
            ),
        )

    ui.caption_source(["REPORTING.RPT_DATA_QUALITY_SCORECARD"], "view, recomputed on read")

    # -----------------------------------------------------------------------
    # Anything actively wrong, stated plainly.
    #
    # The metrics above are the state; this block is the interpretation. On a monitoring
    # dashboard the interpretation belongs on the landing page: expecting a tired engineer to
    # infer "parse errors in the last 24 hours" from a number in a grid is how incidents get
    # missed.
    # -----------------------------------------------------------------------
    findings: list[tuple[str, str]] = []

    if int(s.get("overdue_expiry_trades") or 0) > 0:
        findings.append(
            (
                "error",
                f"{int(s['overdue_expiry_trades'])} matured trade(s) are still LIVE. The expiry "
                "sweep has not run. See `docs/runbook.md#expiry-sweep-failure`.",
            )
        )
    if int(s.get("parse_errors_last_24h") or 0) > 0:
        findings.append(
            (
                "warning",
                f"{int(s['parse_errors_last_24h'])} file row(s) failed to parse in the last 24 "
                "hours. Inspect `RAW.COPY_ERROR` -- usually an upstream serialisation change.",
            )
        )
    if int(s.get("pending_events") or 0) > 10000:
        findings.append(
            (
                "warning",
                f"{int(s['pending_events']):,} events are queued and not yet adjudicated. The "
                "transform layer is behind.",
            )
        )
    if int(s.get("duplicate_uti_trades") or 0) > 0:
        findings.append(
            (
                "warning",
                f"{int(s['duplicate_uti_trades'])} trade(s) share a UTI with another trade. "
                "Reported rather than rejected, because a duplicate UTI is a reconciliation "
                "question for the front office, not a malformed message.",
            )
        )
    if int(s.get("limit_breach_trades") or 0) > 0:
        findings.append(
            (
                "warning",
                f"{int(s['limit_breach_trades'])} trade(s) exceed their book notional limit.",
            )
        )
    if int(s.get("rules_never_fired") or 0) > 0:
        findings.append(
            (
                "info",
                f"{int(s['rules_never_fired'])} declared rule(s) have never fired. Check the rule "
                "catalogue on the Rejections page to confirm each is genuinely unviolated.",
            )
        )

    if findings:
        st.divider()
        st.subheader("Findings")
        for level, message in findings:
            getattr(st, level)(message)

    st.divider()
    st.caption(
        "Pages in the sidebar: **Trade status** for volumes and exposure, **Rejections** for "
        "the rule engine and drill-down to raw payloads, **Data quality** for the scorecard "
        "history and dbt test results, **Pipeline health** for freshness, loads and cost."
    )


main()
