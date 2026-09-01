"""Snowflake session management and file loading.

Two classes, split on purpose:

  SnowflakeSession -- owns connection lifecycle, authentication and retries. Reused by
                      the loader, the deploy runner, the reconciler and Streamlit, so
                      there is one implementation of "how do we connect" rather than five.

  SnowflakeLoader  -- owns the PUT -> COPY -> verify sequence.

Authentication is key-pair (JWT). See docs/adr/0006-keypair-authentication.md.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, OperationalError

from trade_sim.config import SnowflakeSettings, snowflake_settings

log = logging.getLogger(__name__)

#: Snowflake error codes that are worth retrying. Everything else is a real defect and
#: retrying it only delays the alert.
RETRYABLE_ERROR_CODES: frozenset[int] = frozenset(
    {
        390114,  # authentication token expired
        604,  # statement aborted (often a warehouse resume race)
        606,  # statement queued too long
        629,  # connection reset
    }
)


def load_private_key(path: Path, passphrase: str | None = None) -> bytes:
    """Read a PKCS#8 private key and return it in the DER form the driver expects."""
    key_bytes = Path(path).expanduser().read_bytes()
    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class SnowflakeSession:
    """A configured, retrying Snowflake connection."""

    def __init__(
        self,
        settings: SnowflakeSettings | None = None,
        *,
        warehouse: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        query_tag_suffix: str = "component=ingestion",
    ) -> None:
        self.settings = settings or snowflake_settings()
        self.warehouse = warehouse or self.settings.warehouse
        self.role = role or self.settings.role
        self.schema = schema or self.settings.schema_name
        self.query_tag = f"{self.settings.query_tag_prefix}|{query_tag_suffix}"
        self._conn: snowflake.connector.SnowflakeConnection | None = None
        # Snowflake exposes the query ID on the cursor, not the connection, and cursors here
        # are short-lived context managers. Capturing it at execution time is what lets an
        # error message name a query ID you can look up in QUERY_HISTORY afterwards.
        self._last_query_id: str | None = None

    # -- lifecycle ----------------------------------------------------------
    def _connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "account": self.settings.account,
            "user": self.settings.user,
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.settings.database,
            "schema": self.schema,
            "login_timeout": self.settings.login_timeout_seconds,
            "network_timeout": self.settings.network_timeout_seconds,
            # Tag every statement so QUERY_HISTORY can attribute cost and latency to
            # this component. Without it, "who ran that 40-credit query" is unanswerable.
            "session_parameters": {
                "QUERY_TAG": self.query_tag,
                # Fail rather than silently truncate on a MERGE whose source has
                # duplicate join keys -- that is a data bug we want to see.
                "ERROR_ON_NONDETERMINISTIC_MERGE": True,
                "ERROR_ON_NONDETERMINISTIC_UPDATE": True,
                # UTC everywhere. Mixed session timezones are the root cause of most
                # "the maturity date is off by one" incidents.
                "TIMEZONE": "UTC",
                "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ",
            },
            "client_session_keep_alive": True,
        }

        if self.settings.private_key_path:
            passphrase = (
                self.settings.private_key_passphrase.get_secret_value()
                if self.settings.private_key_passphrase
                else None
            )
            kwargs["private_key"] = load_private_key(
                self.settings.private_key_path, passphrase or None
            )
        elif self.settings.password:
            kwargs["password"] = self.settings.password.get_secret_value()

        return kwargs

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is not None and not self._conn.is_closed():
            return self._conn

        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                log.debug("connecting to Snowflake (attempt %d)", attempt)
                self._conn = snowflake.connector.connect(**self._connect_kwargs())
                log.info(
                    "connected: account=%s user=%s role=%s warehouse=%s database=%s",
                    self.settings.account,
                    self.settings.user,
                    self.role,
                    self.warehouse,
                    self.settings.database,
                )
                return self._conn
            except (OperationalError, DatabaseError) as exc:
                last_error = exc
                # Exponential backoff. A warehouse that is resuming, or a brief network
                # blip, resolves in seconds; hammering it does not help.
                backoff = 2**attempt
                log.warning(
                    "connection attempt %d failed (%s); retrying in %ds", attempt, exc, backoff
                )
                if attempt < self.settings.max_retries:
                    time.sleep(backoff)

        raise RuntimeError(
            f"Could not connect to Snowflake after {self.settings.max_retries} attempts. "
            f"Last error: {last_error}. Run `make doctor` to diagnose."
        ) from last_error

    def close(self) -> None:
        if self._conn is not None and not self._conn.is_closed():
            self._conn.close()
            self._conn = None

    def __enter__(self) -> SnowflakeSession:
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- execution ----------------------------------------------------------
    @contextmanager
    def cursor(self, *, dict_rows: bool = True) -> Iterator[Any]:
        conn = self.connect()
        cur = conn.cursor(DictCursor) if dict_rows else conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(
        self, sql: str, params: Sequence[Any] | dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run one statement and return all rows."""
        with self.cursor() as cur:
            log.debug("executing: %s", sql.strip()[:400])
            cur.execute(sql, params)
            self._last_query_id = cur.sfqid
            try:
                return cur.fetchall()
            except snowflake.connector.errors.NotSupportedError:
                # DDL and some DML return no result set.
                return []

    def execute_script(self, sql: str) -> list[list[dict[str, Any]]]:
        """Run a multi-statement script, returning each statement's rows.

        Uses the driver's multi-statement support rather than splitting on semicolons
        in Python. Naive splitting breaks on semicolons inside `$$ ... $$` stored
        procedure bodies, of which this project has several.
        """
        conn = self.connect()
        results: list[list[dict[str, Any]]] = []
        # Annotated rather than inferred, matching `cursor()` above. The driver's cursor
        # types have changed shape across connector releases, so a `type: ignore` on the
        # `fetchall()` below is redundant in one version and load-bearing in the next --
        # and `warn_unused_ignores` makes the redundant case an error. Naming the cursor as
        # dynamically typed, which is how this module already treats it, settles the
        # question in every version instead of tracking the driver's stubs.
        cur: Any = conn.cursor(DictCursor)
        try:
            cur.execute(sql, num_statements=0)
            self._last_query_id = cur.sfqid
            while True:
                try:
                    results.append(cur.fetchall())
                except snowflake.connector.errors.NotSupportedError:
                    results.append([])
                if not cur.nextset():
                    break
        finally:
            cur.close()
        return results

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = self.execute(sql, params)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    @property
    def current_query_id(self) -> str | None:
        return self._last_query_id


class SnowflakeLoader:
    """PUT staged files, COPY them into RAW.TRADE_EVENT, and verify the result."""

    def __init__(self, session: SnowflakeSession, *, stage: str = "RAW.TRADE_LANDING") -> None:
        self.session = session
        self.stage = stage

    def put(self, local_path: Path, *, stage_subpath: str = "") -> dict[str, Any]:
        """Upload one file to the internal stage.

        AUTO_COMPRESS is disabled because the file is already gzipped; letting
        Snowflake compress it again would produce a double-gzip that the JSON parser
        cannot read. This is a genuinely common and confusing failure.

        OVERWRITE is disabled so a repeated PUT of the same name fails loudly rather
        than silently replacing data that may already have been loaded.
        """
        target = f"@{self.stage}/{stage_subpath}".rstrip("/")
        # PUT does not accept bind parameters, so the path is interpolated. It is
        # generated locally by BatchWriter, never taken from user input.
        sql = (
            f"put 'file://{local_path.resolve()}' {target} "
            "auto_compress = false "
            "overwrite = false "
            "parallel = 8"
        )
        rows = self.session.execute(sql)
        result = rows[0] if rows else {}
        log.info(
            "PUT %s -> %s (status=%s)",
            local_path.name,
            target,
            result.get("status", "unknown"),
        )
        return result

    def copy_into_raw(self, *, pattern: str, orchestrator_run_id: str = "manual") -> dict[str, Any]:
        """Invoke the batch COPY procedure and return its result object.

        The COPY itself lives in a Snowflake stored procedure rather than here, so that
        batch registration, the COPY and the VALIDATE() error capture are one
        transaction on the server. Doing it from Python would leave a window where the
        COPY committed but the batch record did not.
        """
        rows = self.session.execute(
            "call raw.sp_load_trade_files(%s, %s)", (pattern, orchestrator_run_id)
        )
        if not rows:
            raise RuntimeError("sp_load_trade_files returned no result.")
        raw_value = next(iter(rows[0].values()))
        import json as _json

        result: dict[str, Any] = _json.loads(raw_value) if isinstance(raw_value, str) else raw_value
        log.info(
            "COPY batch %s: %s files, %s rows loaded, %s rows errored",
            result.get("batch_id"),
            result.get("files_loaded"),
            result.get("rows_loaded"),
            result.get("rows_errored"),
        )
        return result

    def drain_stream(self, orchestrator_run_id: str = "manual") -> dict[str, Any]:
        """Force a stream drain instead of waiting for the scheduled task.

        Used by the demo and by Airflow so a run does not have to wait up to a minute
        for the task's next tick. The task remains the production path.
        """
        rows = self.session.execute(
            "call raw.sp_drain_trade_event_stream(%s)", (orchestrator_run_id,)
        )
        raw_value = next(iter(rows[0].values())) if rows else "{}"
        import json as _json

        result: dict[str, Any] = _json.loads(raw_value) if isinstance(raw_value, str) else raw_value
        log.info("stream drain: %s", result)
        return result

    def load_file(
        self,
        local_path: Path,
        *,
        ingest_date_partition: str,
        orchestrator_run_id: str = "manual",
        drain: bool = True,
    ) -> dict[str, Any]:
        """The whole path for one file: PUT, COPY, optionally drain the stream."""
        self.put(local_path, stage_subpath=ingest_date_partition)
        # Scope the COPY to just this file. A bare COPY would rescan the entire stage
        # every time -- correct, because load history prevents re-loading, but the file
        # listing cost grows without bound.
        pattern = f".*{local_path.name}"
        copy_result = self.copy_into_raw(pattern=pattern, orchestrator_run_id=orchestrator_run_id)

        if drain:
            copy_result["drain"] = self.drain_stream(orchestrator_run_id)
        return copy_result

    def verify_stage(self) -> list[dict[str, Any]]:
        return self.session.execute(f"list @{self.stage}")

    def pipe_status(self) -> dict[str, Any]:
        raw_value = self.session.scalar(
            f"select system$pipe_status('{self.session.settings.database}.raw.pipe_trade_event')"
        )
        import json as _json

        return _json.loads(raw_value) if isinstance(raw_value, str) else (raw_value or {})
