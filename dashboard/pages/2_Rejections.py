"""
Rejections: which rules fire, where the failures come from, and the raw payload behind any one
of them.

THE POINT OF THIS PAGE
----------------------
"Reject rate is 31%" is not actionable. Three questions have to be answerable before anyone can
do anything about it:

  1. Which rule is firing? -> the rule table below.
  2. Is it one upstream system or all of them? -> the concentration analysis. One system means
     a release broke a feed; all systems means our reference data is wrong. Opposite responses,
     invisible from a total count.
  3. What did the message actually look like? -> the drill-down to `raw_payload`.

Retaining the raw payload in AUDIT.FCT_TRADE_REJECTED exists to make question 3 answerable
without going back to the source system, which in a bank means a ticket and two days.
"""

from __future__ import annotations

import json

import altair as alt
import pandas as pd
import streamlit as st

from lib import components as ui
from lib import queries as q
from lib.connection import database, run_query

st.set_page_config(page_title="Rejections", page_icon=":material/block:", layout="wide")

st.title("Rejections and rule performance")
db = database()

by_rule = run_query(q.rejection_by_rule(db), label="rejection analysis")

if by_rule.empty:
    ui.empty_state(
        "rejections",
        "Either nothing has been rejected yet, or the audit layer has not been built. "
        "The simulator injects faults at an 8% rate by default, so a loaded batch should "
        "produce rejections. Run `make demo` then `make dbt-build`.",
    )
    st.stop()

# ---------------------------------------------------------------------------
# Headline counts.
# ---------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    ui.metric("Rule hits", by_rule["hit_count"].sum())
with c2:
    ui.metric("Distinct rules firing", len(by_rule))
with c3:
    ui.metric(
        "New failure modes",
        int(by_rule["is_new_failure_mode"].fillna(False).astype(bool).sum()),
        help_text=(
            "Rules first seen in the last 24 hours. A brand-new failure mode is nearly always "
            "an upstream change, and is the highest-value thing on this page."
        ),
    )
with c4:
    ui.metric(
        "Chronic rules",
        int(by_rule["is_chronic"].fillna(False).astype(bool).sum()),
        help_text=(
            "Firing continuously over a long period. Chronic rejections are usually accepted "
            "as normal and then stop being investigated, which is how a real problem hides."
        ),
    )

st.divider()

# ---------------------------------------------------------------------------
# Rule leaderboard.
# ---------------------------------------------------------------------------
st.subheader("Rules by volume")

chart_data = by_rule.head(15).copy()
chart_data["label"] = chart_data["rule_code"] + " " + chart_data["rule_name"].fillna("")

chart = (
    alt.Chart(chart_data)
    .mark_bar()
    .encode(
        y=alt.Y("label:N", title=None, sort="-x"),
        x=alt.X("hit_count:Q", title="Hits"),
        color=alt.Color(
            "rule_category:N",
            title="Category",
            scale=alt.Scale(scheme="tableau10"),
        ),
        tooltip=[
            alt.Tooltip("rule_code:N", title="Rule"),
            alt.Tooltip("rule_name:N", title="Name"),
            alt.Tooltip("rule_category:N", title="Category"),
            alt.Tooltip("rule_severity:N", title="Severity"),
            alt.Tooltip("hit_count:Q", title="Hits", format=","),
            alt.Tooltip("distinct_trade_count:Q", title="Trades", format=","),
            alt.Tooltip("requirement_ref:N", title="Requirement"),
        ],
    )
    .properties(height=min(28 * len(chart_data) + 40, 480))
)
st.altair_chart(chart, width="stretch")

st.dataframe(
    by_rule[
        [
            "rule_code",
            "rule_name",
            "rule_category",
            "rule_severity",
            "requirement_ref",
            "hit_count",
            "distinct_trade_count",
            "sources_affected",
            "is_concentrated",
            "is_new_failure_mode",
            "is_chronic",
            "last_seen_at",
        ]
    ],
    hide_index=True,
    width="stretch",
    column_config={
        "rule_code": st.column_config.TextColumn("Rule", width="small"),
        "rule_name": st.column_config.TextColumn("Name"),
        "rule_category": st.column_config.TextColumn("Category", width="small"),
        "rule_severity": st.column_config.TextColumn(
            "Severity", width="small", help="REJECT blocks the event; WARN annotates it only."
        ),
        "requirement_ref": st.column_config.TextColumn("Req", width="small"),
        "hit_count": st.column_config.NumberColumn("Hits", format="%d"),
        "distinct_trade_count": st.column_config.NumberColumn("Trades", format="%d"),
        "sources_affected": st.column_config.NumberColumn("Sources", format="%d"),
        "is_concentrated": st.column_config.CheckboxColumn(
            "Concentrated",
            width="small",
            help="Over 80% of hits from one source system: points upstream, not at our data.",
        ),
        "is_new_failure_mode": st.column_config.CheckboxColumn("New", width="small"),
        "is_chronic": st.column_config.CheckboxColumn("Chronic", width="small"),
        "last_seen_at": st.column_config.DatetimeColumn("Last seen"),
    },
)
ui.caption_source(["REPORTING.AGG_REJECTION_ANALYSIS"], "view over AUDIT")

st.divider()

# ---------------------------------------------------------------------------
# Drill-down. One selector drives the three panels below it.
# ---------------------------------------------------------------------------
st.subheader("Drill down")

options = ["(all rules)"] + [
    f"{code} \u2014 {name}"
    for code, name in zip(by_rule["rule_code"], by_rule["rule_name"].fillna(""), strict=True)
]
selection = st.selectbox("Rule", options=options, index=0, key="rule_drilldown")
selected_code = None if selection.startswith("(all") else selection.split(" \u2014 ")[0]

if selected_code:
    detail = by_rule[by_rule["rule_code"] == selected_code].iloc[0]
    st.info(
        f"**{detail['rule_code']} \u2014 {detail['rule_name']}** "
        f"({detail['rule_category']}, {detail['rule_severity']}, "
        f"requirement {detail['requirement_ref']})\n\n"
        f"{detail.get('remediation') or 'No remediation guidance recorded.'}",
        icon=":material/gavel:",
    )

tab_sources, tab_trend, tab_events = st.tabs(["By source system", "Trend", "Rejected events"])

# ---- Concentration --------------------------------------------------------
with tab_sources:
    st.markdown(
        "**Is this one feed or all of them?** Concentrated in one source system means a "
        "release broke that feed. Spread evenly across sources means our reference data or "
        "the rule itself is wrong. The remediation is completely different, and a total hit "
        "count cannot distinguish them."
    )
    sources = run_query(q.rejection_by_source(db, selected_code), label="rejections by source")
    if sources.empty:
        ui.empty_state("source breakdown", "No rows for this selection.")
    else:
        chart = (
            alt.Chart(sources)
            .mark_bar()
            .encode(
                x=alt.X("hit_count:Q", title="Hits", stack="normalize"),
                y=alt.Y("rule_code:N", title=None, sort="-x"),
                color=alt.Color("source_system:N", title="Source", scale=alt.Scale(scheme="set2")),
                tooltip=[
                    alt.Tooltip("rule_code:N", title="Rule"),
                    alt.Tooltip("source_system:N", title="Source"),
                    alt.Tooltip("hit_count:Q", title="Hits", format=","),
                    alt.Tooltip("share_of_rule_pct:Q", title="Share of rule %", format=".1f"),
                    alt.Tooltip("distinct_file_count:Q", title="Files", format=","),
                ],
            )
            .properties(height=min(30 * sources["rule_code"].nunique() + 40, 420))
        )
        st.altair_chart(chart, width="stretch")
        st.caption("Bars normalised to 100% so concentration is visible regardless of volume.")
        st.dataframe(sources, hide_index=True, width="stretch")

# ---- Trend ----------------------------------------------------------------
with tab_trend:
    trend_days = (
        st.segmented_control(
            "Window",
            options=[7, 14, 30],
            default=14,
            format_func=lambda d: f"{d} days",
            key="rejection_trend_window",
        )
        or 14
    )

    trend = run_query(q.rejection_trend(db, trend_days), label="rejection trend")
    if trend.empty:
        ui.empty_state("trend data", "No blocking rule hits in this window.")
    else:
        if selected_code:
            trend = trend[trend["rule_code"] == selected_code]
        chart = (
            alt.Chart(trend)
            .mark_area(opacity=0.75)
            .encode(
                x=alt.X("calendar_date:T", title=None, axis=alt.Axis(format="%d %b")),
                y=alt.Y("hit_count:Q", title="Hits", stack=True),
                color=alt.Color("rule_code:N", title="Rule", scale=alt.Scale(scheme="tableau20")),
                tooltip=[
                    alt.Tooltip("calendar_date:T", title="Date"),
                    alt.Tooltip("rule_code:N", title="Rule"),
                    alt.Tooltip("rule_name:N", title="Name"),
                    alt.Tooltip("hit_count:Q", title="Hits", format=","),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, width="stretch")
        st.caption(
            "Blocking hits only (severity REJECT). WARN-severity rules annotate an event "
            "without refusing it, so including them here would overstate the problem."
        )
        ui.caption_source(["AUDIT.TRADE_RULE_RESULT"], "append-only table")

# ---- Individual events ----------------------------------------------------
with tab_events:
    st.markdown(
        "**The actual messages.** `raw_payload` is retained on every rejection so a rejected "
        "trade can be explained without a request to the source system. Select a row to see "
        "the payload as it arrived."
    )
    events = run_query(q.rejected_events(db, selected_code), label="rejected events")

    if events.empty:
        ui.empty_state("rejected events", "No rejected events match this selection.")
    else:
        table = events.drop(columns=["raw_payload"])
        selected = st.dataframe(
            table,
            hide_index=True,
            width="stretch",
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "rejected_at": st.column_config.DatetimeColumn("Rejected at"),
                "trade_id": st.column_config.TextColumn("Trade"),
                "trade_version": st.column_config.NumberColumn("Ver", format="%d", width="small"),
                "disposition": st.column_config.TextColumn("Disposition", width="small"),
                "primary_rule_code": st.column_config.TextColumn("Rule", width="small"),
                "primary_rule_name": st.column_config.TextColumn("Reason"),
                "violated_rule_codes": st.column_config.ListColumn("All rules"),
                "notional_amount": st.column_config.NumberColumn("Notional", format="%.2f"),
            },
        )

        rows = selected.selection.rows if selected and selected.selection else []
        if rows:
            record = events.iloc[rows[0]]
            st.markdown(f"#### {record['trade_id']} version {record['trade_version']}")

            left, right = st.columns([1, 1])
            with left:
                st.markdown("**Verdict**")
                st.write(
                    pd.DataFrame(
                        {
                            "field": [
                                "Disposition",
                                "Primary rule",
                                "All rules violated",
                                "Source system",
                                "Source file",
                                "Rejected at",
                            ],
                            "value": [
                                record.get("disposition"),
                                f"{record.get('primary_rule_code')} "
                                f"{record.get('primary_rule_name')}",
                                str(record.get("violated_rule_codes")),
                                record.get("source_system"),
                                record.get("source_file_name"),
                                str(record.get("rejected_at")),
                            ],
                        }
                    ),
                    hide_index=True,
                )
                st.caption(
                    "`disposition` distinguishes REJECTED (a rule refused it) from SUPERSEDED "
                    "(a later arrival of the same version replaced it). Both are recorded, "
                    "because losing either would break the "
                    "\u201cevery event reaches exactly one destination\u201d invariant that "
                    "`assert_no_event_is_silently_dropped` asserts."
                )
            with right:
                st.markdown("**Payload as it arrived**")
                payload = record.get("raw_payload")
                try:
                    parsed = json.loads(payload) if isinstance(payload, str) else payload
                    st.json(parsed, expanded=True)
                except (TypeError, ValueError):
                    # Unparseable JSON is a fault the simulator injects on purpose, and this is
                    # exactly the case where showing the bytes verbatim is the whole point.
                    st.code(str(payload), language="text")
                    st.caption(
                        "Not valid JSON. Shown verbatim -- this is the RJ008 case, and the "
                        "bytes are what an upstream investigation needs."
                    )
        else:
            st.caption("Select a row to inspect its payload.")

        ui.caption_source(["AUDIT.FCT_TRADE_REJECTED"], "append-only table")

st.divider()

# ---------------------------------------------------------------------------
# Rule catalogue and coverage.
# ---------------------------------------------------------------------------
st.subheader("Rule catalogue and coverage")
st.markdown(
    "Every declared rule, and whether it has ever fired. A rule that has never fired is "
    "either genuinely never violated or quietly broken, and a passing test suite looks "
    "identical in both cases. The singular test "
    "`assert_rule_catalogue_matches_macro` separately guarantees this list and the executable "
    "rule macro cannot drift apart."
)

catalogue = run_query(q.rule_catalogue(db), label="rule catalogue")
if catalogue.empty:
    ui.empty_state("rule catalogue", "Load the seeds with `dbt seed`.")
else:
    never = catalogue[catalogue["never_fired"].fillna(False).astype(bool)]
    if not never.empty:
        st.warning(
            f"{len(never)} rule(s) have never fired: "
            f"{', '.join(never['rule_code'].astype(str))}. Confirm each is genuinely "
            "unviolated rather than silently broken.",
            icon=":material/help:",
        )
    st.dataframe(
        catalogue,
        hide_index=True,
        width="stretch",
        column_config={
            "rule_code": st.column_config.TextColumn("Rule", width="small"),
            "rule_name": st.column_config.TextColumn("Name"),
            "rule_category": st.column_config.TextColumn("Category", width="small"),
            "rule_severity": st.column_config.TextColumn("Severity", width="small"),
            "requirement_ref": st.column_config.TextColumn("Req", width="small"),
            "description": st.column_config.TextColumn("Description", width="large"),
            "remediation": st.column_config.TextColumn("Remediation", width="large"),
            "hit_count": st.column_config.NumberColumn("Hits", format="%d"),
            "last_fired_at": st.column_config.DatetimeColumn("Last fired"),
            "never_fired": st.column_config.CheckboxColumn("Never fired", width="small"),
        },
    )
    ui.caption_source(["CORE.REF_REJECTION_REASON", "AUDIT.TRADE_RULE_RESULT"], "seed + table")
