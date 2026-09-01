"""
Trade status: volumes, lifecycle mix, exposure and upcoming maturities.

The business-facing page. Everything reads CORE and REPORTING, never RAW, so a number shown
here is a number the dbt tests have already asserted something about.
"""

from __future__ import annotations

import altair as alt
import streamlit as st

from lib import components as ui
from lib import queries as q
from lib.connection import database, run_query

st.set_page_config(page_title="Trade status", page_icon=":material/trending_up:", layout="wide")

st.title("Trade status")
db = database()

# ---------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------
window = st.segmented_control(
    "History window",
    options=[7, 14, 30, 90],
    default=30,
    format_func=lambda days: f"{days} days",
    key="trade_status_window",
)
window = window or 30

daily = run_query(q.daily_status(db, window), label="daily trade status")

if daily.empty:
    ui.empty_state(
        "trade history",
        "Generate and load a batch (`make demo`), then build the marts (`make dbt-build`).",
    )
    st.stop()

# ---------------------------------------------------------------------------
# Volume and reject rate over time.
#
# Two y-axes on one chart, deliberately: the question people actually ask is "did the reject
# rate move because something broke, or because volume moved?", and answering it from two
# separate charts requires eyeballing across a page break.
# ---------------------------------------------------------------------------
st.subheader("Daily volume and reject rate")

base = alt.Chart(daily).encode(
    x=alt.X("calendar_date:T", title=None, axis=alt.Axis(format="%d %b")),
)

bars = base.mark_bar(color="#4c78a8", opacity=0.85).encode(
    y=alt.Y("events_adjudicated:Q", title="Events adjudicated"),
    tooltip=[
        alt.Tooltip("calendar_date:T", title="Date"),
        alt.Tooltip("events_adjudicated:Q", title="Events", format=","),
        alt.Tooltip("events_accepted:Q", title="Accepted", format=","),
        alt.Tooltip("events_rejected:Q", title="Rejected", format=","),
        alt.Tooltip("events_superseded:Q", title="Superseded", format=","),
        alt.Tooltip("reject_rate_pct:Q", title="Reject rate %", format=".2f"),
    ],
)

line = base.mark_line(color="#cf222e", strokeWidth=2, point=True).encode(
    y=alt.Y("reject_rate_pct:Q", title="Reject rate %", scale=alt.Scale(zero=True)),
    tooltip=[
        alt.Tooltip("calendar_date:T", title="Date"),
        alt.Tooltip("reject_rate_pct:Q", title="Reject rate %", format=".2f"),
    ],
)

# The gate threshold drawn on the chart. A rate without its threshold is a number; a rate next
# to the line at which the pipeline stops publishing is a decision.
threshold = (
    alt.Chart(daily.assign(gate=25.0))
    .mark_rule(color="#cf222e", strokeDash=[6, 4], opacity=0.6)
    .encode(y=alt.Y("gate:Q"))
)

st.altair_chart(
    alt.layer(bars, line + threshold).resolve_scale(y="independent").properties(height=320),
    use_container_width=True,
)
st.caption(
    "Dashed line is the 25% reject-rate gate. Above it the Airflow run fails before the "
    "marts are refreshed, so the golden record is never updated from a suspect batch."
)
ui.caption_source(["REPORTING.AGG_TRADE_STATUS_DAILY"], "view over CORE")

st.divider()

# ---------------------------------------------------------------------------
# Lifecycle mix and amendment depth, side by side.
# ---------------------------------------------------------------------------
left, right = st.columns(2)

with left:
    st.subheader("Lifecycle mix")
    mix = run_query(q.lifecycle_mix(db), label="lifecycle mix")
    if mix.empty:
        ui.empty_state("trades", "Build the CORE layer first.")
    else:
        chart = (
            alt.Chart(mix)
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("trade_count:Q"),
                color=alt.Color(
                    "lifecycle_status:N",
                    title=None,
                    scale=alt.Scale(
                        domain=["LIVE", "EXPIRED", "CANCELLED"],
                        range=["#1a7f37", "#57606a", "#bf8700"],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("lifecycle_status:N", title="Status"),
                    alt.Tooltip("trade_count:Q", title="Trades", format=","),
                    alt.Tooltip("gross_notional:Q", title="Gross notional", format=",.0f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)
        st.caption(
            "EXPIRED is set by the sweep in `fct_trade`, not by an incoming event -- which is "
            "why business rule 4 needs a model that reprocesses existing rows rather than only "
            "new ones."
        )

with right:
    st.subheader("Amendment depth")
    versions = run_query(q.version_distribution(db), label="version distribution")
    if versions.empty:
        ui.empty_state("versions", "Build the CORE layer first.")
    else:
        chart = (
            alt.Chart(versions)
            .mark_bar(color="#4c78a8")
            .encode(
                x=alt.X("version_label:N", title="Current version", sort=None),
                y=alt.Y("trade_count:Q", title="Trades"),
                tooltip=[
                    alt.Tooltip("version_label:N", title="Version"),
                    alt.Tooltip("trade_count:Q", title="Trades", format=","),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)
        st.caption(
            "A sudden shift towards high versions usually means an upstream system is "
            "retransmitting. Business rule 1 accepts ascending versions, so this absorbs "
            "silently and only shows up here."
        )

st.divider()

# ---------------------------------------------------------------------------
# Book exposure against limits.
# ---------------------------------------------------------------------------
st.subheader("Book exposure against limits")

books = run_query(q.book_exposure(db), label="book exposure")
if books.empty:
    ui.empty_state("book data", "Load the reference seeds and build `dim_book`.")
else:
    breaches = int((books["limit_status"] == "BREACH").sum())
    warnings = int((books["limit_status"] == "WARNING").sum())
    if breaches:
        st.error(
            f"{breaches} book(s) are over their notional limit. Limit breach is reported, not "
            "rejected: refusing a trade that has already been executed would put the "
            "warehouse out of step with the front office, and the control is a credit decision "
            "rather than a data-validity one.",
            icon=":material/error:",
        )
    elif warnings:
        st.warning(f"{warnings} book(s) are above 80% of their limit.", icon=":material/warning:")

    display = books.copy()
    display["utilisation"] = (display["limit_utilisation_pct"].fillna(0) / 100).clip(upper=1.5)

    st.dataframe(
        display[
            [
                "book_id",
                "book_name",
                "desk",
                "live_trade_count",
                "gross_live_notional",
                "net_live_notional",
                "notional_limit",
                "utilisation",
                "limit_status",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "book_id": st.column_config.TextColumn("Book"),
            "book_name": st.column_config.TextColumn("Name"),
            "desk": st.column_config.TextColumn("Desk"),
            "live_trade_count": st.column_config.NumberColumn("Live", format="%d"),
            "gross_live_notional": st.column_config.NumberColumn("Gross live", format="%.0f"),
            "net_live_notional": st.column_config.NumberColumn(
                "Net live",
                format="%.0f",
                help="Buy positive, sell negative. Netting is what makes the limit meaningful.",
            ),
            "notional_limit": st.column_config.NumberColumn("Limit", format="%.0f"),
            "utilisation": st.column_config.ProgressColumn(
                "Utilisation", min_value=0.0, max_value=1.5, format="%.0f%%"
            ),
            "limit_status": st.column_config.TextColumn("Status"),
        },
    )
    ui.caption_source(["CORE.DIM_BOOK"], "table, rebuilt each run")

st.divider()

# ---------------------------------------------------------------------------
# Counterparty concentration.
# ---------------------------------------------------------------------------
st.subheader("Largest counterparty exposures")

counterparties = run_query(q.counterparty_exposure(db), label="counterparty exposure")
if counterparties.empty:
    ui.empty_state("counterparty data", "Load the reference seeds and build `dim_counterparty`.")
else:
    inactive_with_live = counterparties[
        counterparties["has_live_trades_while_inactive"].fillna(False).astype(bool)
    ]
    if not inactive_with_live.empty:
        names = ", ".join(inactive_with_live["counterparty_id"].astype(str).head(5))
        st.error(
            f"Live trades exist against inactive counterparties ({names}). This is a control "
            "finding, not a data error: rule RJ006 rejects *new* events for inactive "
            "counterparties, but a counterparty deactivated after a trade was booked leaves "
            "the existing position live and in need of a decision.",
            icon=":material/gavel:",
        )

    st.dataframe(
        counterparties,
        hide_index=True,
        width="stretch",
        column_config={
            "counterparty_id": st.column_config.TextColumn("ID"),
            "counterparty_name": st.column_config.TextColumn("Counterparty"),
            "country_code": st.column_config.TextColumn("Country", width="small"),
            "credit_rating": st.column_config.TextColumn("Rating", width="small"),
            "is_active": st.column_config.CheckboxColumn("Active", width="small"),
            "live_trade_count": st.column_config.NumberColumn("Live", format="%d"),
            "gross_live_notional": st.column_config.NumberColumn("Gross live", format="%.0f"),
            "net_live_notional": st.column_config.NumberColumn("Net live", format="%.0f"),
            "rejected_event_count": st.column_config.NumberColumn("Rejected", format="%d"),
            "has_live_trades_while_inactive": st.column_config.CheckboxColumn(
                "Inactive w/ live", width="small"
            ),
        },
    )
    ui.caption_source(["CORE.DIM_COUNTERPARTY"], "table, rebuilt each run")

st.divider()

# ---------------------------------------------------------------------------
# Maturities.
# ---------------------------------------------------------------------------
st.subheader("Maturing soon")

expiring = run_query(q.expiring_soon(db), label="expiring trades")
if expiring.empty:
    st.success(
        "No trades are maturing within the reporting horizon, and none are overdue for expiry.",
        icon=":material/check_circle:",
    )
else:
    overdue = expiring[expiring["is_overdue_for_expiry"].fillna(False).astype(bool)]
    if not overdue.empty:
        st.error(
            f"{len(overdue)} trade(s) matured before today and are still LIVE. This is the "
            "canary for a stalled pipeline: the expiry sweep runs on every dbt build, so a "
            "non-empty result here means no build has completed since those trades matured.",
            icon=":material/error:",
        )

    urgency_filter = st.multiselect(
        "Urgency",
        options=["OVERDUE", "TODAY", "THIS_WEEK", "LATER"],
        default=["OVERDUE", "TODAY", "THIS_WEEK"],
    )
    filtered = expiring[expiring["urgency"].isin(urgency_filter)] if urgency_filter else expiring

    st.dataframe(
        filtered[
            [
                "urgency",
                "trade_id",
                "current_version",
                "maturity_date",
                "days_to_maturity",
                "product_type",
                "buy_sell",
                "notional_amount",
                "notional_currency",
                "counterparty_name",
                "book_name",
                "desk",
                "is_overdue_for_expiry",
            ]
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "urgency": st.column_config.TextColumn("Urgency", width="small"),
            "trade_id": st.column_config.TextColumn("Trade"),
            "current_version": st.column_config.NumberColumn("Ver", format="%d", width="small"),
            "maturity_date": st.column_config.DateColumn("Maturity"),
            "days_to_maturity": st.column_config.NumberColumn("Days", format="%d", width="small"),
            "notional_amount": st.column_config.NumberColumn("Notional", format="%.2f"),
            "is_overdue_for_expiry": st.column_config.CheckboxColumn("Overdue", width="small"),
        },
    )
    ui.caption_source(["REPORTING.RPT_TRADE_EXPIRING_SOON"], "view over CORE")
