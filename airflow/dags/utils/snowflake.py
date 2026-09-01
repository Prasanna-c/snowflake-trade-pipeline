"""Snowflake access for DAG tasks.

DESIGN POINT: the DAG reuses `trade_sim.loaders.SnowflakeSession` rather than Airflow's
SnowflakeHook.

The hook would work, but it would mean connection handling, key-pair loading, retry policy
and session parameters existed twice -- once for the CLI and once for Airflow. They would
drift, and the drift would show up as "it works from my laptop but not in Airflow", which is
among the least pleasant things to debug.

Instead `trade_sim` is pip-installed into the Airflow image (see airflow/Dockerfile) and the
DAG imports the same code a developer runs locally. One implementation of "how do we connect
to Snowflake", one place to fix a bug in it.

The cost is that Snowflake credentials come from environment variables rather than the
Airflow connection store. For production that trade-off would flip -- a secrets backend
(Vault, AWS Secrets Manager) is the right answer there, and `SnowflakeSettings` takes its
values from the environment either way, so the change is in how the environment is populated
rather than in this code.
"""

from __future__ import annotations

import logging
from typing import Any

from trade_sim.config import SnowflakeSettings, snowflake_settings
from trade_sim.loaders import SnowflakeLoader, SnowflakeSession

log = logging.getLogger(__name__)


def settings() -> SnowflakeSettings:
    return snowflake_settings()


def session(
    *,
    warehouse: str | None = None,
    role: str | None = None,
    schema: str | None = None,
    task_id: str = "unknown",
    run_id: str = "unknown",
) -> SnowflakeSession:
    """Build a session tagged with the Airflow task and run that opened it.

    The query tag is the whole point of this wrapper. Without it, a slow query found in
    ACCOUNT_USAGE three days later cannot be traced back to the DAG run that issued it, and
    cost attribution stops at "Airflow did something expensive".
    """
    return SnowflakeSession(
        settings(),
        warehouse=warehouse,
        role=role,
        schema=schema,
        query_tag_suffix=f"component=airflow|task={task_id}|run={run_id}",
    )


def loader(sf_session: SnowflakeSession) -> SnowflakeLoader:
    return SnowflakeLoader(sf_session)


def scalar(sql: str, *, task_id: str = "unknown", run_id: str = "unknown") -> Any:
    """One-shot scalar query. Opens and closes its own session."""
    with session(task_id=task_id, run_id=run_id) as sf:
        return sf.scalar(sql)


def query(sql: str, *, task_id: str = "unknown", run_id: str = "unknown") -> list[dict[str, Any]]:
    """One-shot query returning all rows. Opens and closes its own session."""
    with session(task_id=task_id, run_id=run_id) as sf:
        return sf.execute(sql)
