"""
Snowflake access for the dashboard.

WHY THIS WRAPS trade_sim.SnowflakeSession RATHER THAN USING st.connection
---------------------------------------------------------------------------
Streamlit ships `st.connection("snowflake")`, which is less code. It also reads credentials
from `.streamlit/secrets.toml`, which means the private key path, the role and the warehouse
would be configured in a second place, with a second format, and could silently disagree
with what the pipeline uses. When a dashboard shows different numbers than the pipeline
produced, the first hour of debugging goes into working out whether it is a data problem or
a connection problem.

Reusing the simulator's session removes that class of question entirely: the dashboard, the
CLI and the Airflow DAG authenticate identically, from one `.env`, with one query tag
prefix. The cost is this ~60-line file.

CACHING
-------
Two different caches, for two different reasons:

* `st.cache_resource` for the connection. Resources are shared across every session and
  never serialised, which is exactly right for a socket. Creating a Snowflake connection
  costs a second or two of TLS and authentication, and doing that per widget interaction
  makes the dashboard feel broken.

* `st.cache_data` for query results, applied at the query layer. Data caches are per-value
  and copied on return, so one user's filter cannot mutate another's results.

The TTL is short (60s by default) rather than absent. A monitoring dashboard whose numbers
are five minutes stale is actively harmful during an incident -- someone will conclude the
pipeline is still broken after it recovered. 60 seconds is under the pipeline's own drain
cadence, so the dashboard cannot show a state the platform was never in.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# The dashboard runs from the repo, so make the simulator importable without installing it.
# Keeps `streamlit run dashboard/app.py` working straight after a clone.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_INGESTION_SRC = _REPO_ROOT / "ingestion" / "src"
if _INGESTION_SRC.is_dir() and str(_INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(_INGESTION_SRC))

# Imported below the path setup because that is what makes them importable, and marked as such
# rather than moved: `trade_sim` is first-party here, so E402 is a real finding and the
# suppression has to state why it is being overruled.
from trade_sim.config import snowflake_settings  # noqa: E402
from trade_sim.loaders.snowflake_loader import SnowflakeSession  # noqa: E402

CACHE_TTL_SECONDS = int(os.environ.get("DASHBOARD_CACHE_TTL_SECONDS", "60"))


@st.cache_resource(show_spinner="Connecting to Snowflake...")
def get_session() -> SnowflakeSession:
    """One Snowflake session for the whole app process.

    Tagged as the dashboard so its credit consumption is separable from the pipeline's in
    ACCOUNT_USAGE. Without that, "why did our warehouse spend double last month" has no
    answer, and BI queries are the usual culprit.
    """
    settings = snowflake_settings()
    session = SnowflakeSession(
        settings,
        query_tag_suffix="component=dashboard",
        # The dashboard reads; it never needs the transform warehouse. Using the smaller
        # load warehouse means a dashboard left open on a wall screen cannot keep an
        # X-SMALL-plus warehouse hot and bill for it.
        warehouse=settings.effective_load_warehouse,
    )
    session.connect()
    return session


def database() -> str:
    return snowflake_settings().database


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def run_query(sql: str, label: str = "query") -> pd.DataFrame:
    """Execute SQL and return a DataFrame, with the result cached briefly.

    `label` is part of the cache key only incidentally; its real purpose is to make the
    Streamlit spinner and any error message say which panel failed. A dashboard that says
    "SQL compilation error" without saying where is a dashboard nobody can fix.

    Errors are surfaced as an empty frame plus a visible warning rather than an exception,
    because one broken panel should not blank the whole page -- the other panels are
    frequently the ones that explain the failure. The exception text is shown in full: this
    is an internal tool, and hiding it only means someone reads the terminal instead.
    """
    try:
        # Inside the try, not above it: a failure to connect at all is the commonest failure
        # in this app (missing .env, unregistered public key, suspended trial) and it should
        # produce the same readable warning as a bad column reference, not a traceback on
        # every panel of the page.
        rows = get_session().execute(sql)
    except Exception as exc:  # noqa: BLE001 - deliberately broad; see docstring
        st.warning(f"`{label}` failed: {exc}", icon=":material/error:")
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    # Snowflake returns upper-case identifiers. Lower-casing once here means every chart and
    # table below refers to columns the way the dbt models spell them, and a column rename
    # in dbt breaks the dashboard loudly at one place instead of subtly in ten.
    frame.columns = [str(column).lower() for column in frame.columns]
    return frame


def scalar(sql: str, label: str = "scalar", default: Any = None) -> Any:
    frame = run_query(sql, label=label)
    if frame.empty:
        return default
    return frame.iloc[0, 0]


def clear_caches() -> None:
    """Drop cached query results but keep the connection.

    Bound to the Refresh button. Clearing the resource cache as well would reconnect on every
    refresh, which is slow and pointless -- the connection is not what went stale.
    """
    run_query.clear()
