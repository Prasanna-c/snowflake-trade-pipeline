"""
Shared UI pieces.

Extracted because the RAG badge and the health header appear on every page, and a monitoring
tool where "GREEN" is drawn slightly differently on two pages is a tool people stop trusting.
The colours are also the same three used in the Snowflake alert emails and the runbook, so
"it was amber" means one thing across the whole platform.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

RAG_COLOURS: dict[str, str] = {
    "GREEN": "#1a7f37",
    "AMBER": "#bf8700",
    "RED": "#cf222e",
    "UNKNOWN": "#57606a",
}

RAG_ICONS: dict[str, str] = {
    "GREEN": "\u25cf",
    "AMBER": "\u25b2",
    "RED": "\u25a0",
    "UNKNOWN": "\u25cb",
}


def rag_badge(status: Any, label: str = "") -> str:
    """An inline coloured badge as HTML.

    Shape as well as colour (circle / triangle / square) because roughly one man in twelve
    cannot reliably distinguish the red from the green, and a status indicator that only some
    people can read is not a status indicator.
    """
    key = str(status).upper() if status is not None else "UNKNOWN"
    if key not in RAG_COLOURS:
        key = "UNKNOWN"
    text = f"{RAG_ICONS[key]} {label or key}"
    return (
        f'<span style="background-color:{RAG_COLOURS[key]};color:#ffffff;'
        f"padding:2px 10px;border-radius:12px;font-size:0.82rem;"
        f'font-weight:600;white-space:nowrap;">{text}</span>'
    )


def rag_header(statuses: dict[str, Any]) -> None:
    """A row of stage badges: ingestion, capture, transform, correctness.

    Stage-level rather than a single overall light. "The platform is red" prompts a
    twenty-minute hunt; "capture is red and everything else is green" points at the stream
    drain immediately, which is the entire value of showing four numbers instead of one.
    """
    columns = st.columns(len(statuses))
    for column, (label, status) in zip(columns, statuses.items(), strict=True):
        with column:
            st.markdown(
                f'<div style="text-align:center;">'
                f'<div style="font-size:0.75rem;color:#57606a;'
                f'text-transform:uppercase;letter-spacing:0.06em;">{label}</div>'
                f'<div style="margin-top:4px;">{rag_badge(status)}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )


def metric(
    label: str,
    value: Any,
    *,
    delta: Any = None,
    help_text: str | None = None,
    fmt: str = "{:,.0f}",
) -> None:
    """st.metric with null-safe formatting.

    Every metric here comes from a SQL aggregate over a possibly-empty table, so NULL is a
    normal value, not an error. Rendering it as an em dash says "no data yet"; letting the
    format string raise would blank the page on a fresh install, which is the worst possible
    first impression.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        display = "\u2014"
    else:
        try:
            display = fmt.format(value)
        except (ValueError, TypeError):
            display = str(value)
    st.metric(label, display, delta=delta, help=help_text)


def money(value: Any, currency: str = "") -> str:
    """Format a notional for a human, not for an accountant.

    Notionals here run to hundreds of millions. Rendering them in full means a column of
    fifteen-digit numbers that nobody compares correctly at a glance, so they are abbreviated
    and the exact figure stays available in the underlying table.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "\u2014"
    amount = float(value)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    prefix = f"{currency} " if currency else ""
    for threshold, suffix in ((1e12, "T"), (1e9, "bn"), (1e6, "m"), (1e3, "k")):
        if amount >= threshold:
            return f"{sign}{prefix}{amount / threshold:,.2f}{suffix}"
    return f"{sign}{prefix}{amount:,.2f}"


def empty_state(what: str, hint: str) -> None:
    """A useful message when a panel has no rows.

    An empty chart with no explanation is indistinguishable from a broken one. Saying what is
    missing and which command produces it turns a dead end into the next step -- which matters
    most on a fresh clone, when every panel is empty and the user has no idea whether they
    have set it up wrong.
    """
    st.info(f"**No {what} yet.** {hint}", icon=":material/info:")


def caption_source(objects: list[str], latency: str = "real-time") -> None:
    """Say which objects a panel reads and how fresh they are.

    Non-negotiable on a monitoring dashboard: half these panels read project tables and are
    current, the other half read ACCOUNT_USAGE and lag by up to 45 minutes. Someone comparing
    the two during an incident and not knowing that will reach a confidently wrong conclusion.
    """
    st.caption(f"Source: {', '.join(f'`{obj}`' for obj in objects)} \u00b7 {latency}")
