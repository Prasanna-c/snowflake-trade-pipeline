"""Failure, retry and SLA callbacks.

DESIGN POINT: alerts are built from the Airflow context, not from a static string.

An alert that says "trade_pipeline failed" costs the on-call engineer ten minutes of
clicking before they know anything. Every alert here carries the DAG, task, try number,
logical date, the exception, a direct link to the log, and the runbook section for that
specific class of failure. The aim is that the email alone is enough to decide whether to
act now or in the morning.

DESIGN POINT: notification failures are swallowed, deliberately.

If SMTP is misconfigured, a raising callback would mask the original task failure with an
SMTP traceback -- the real error disappears and the on-call engineer debugs the mail server
instead of the pipeline. So delivery problems are logged and suppressed. The task's own
status is the source of truth for whether the pipeline is healthy; alerting is a
convenience layer on top of it and must never be able to make things worse.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, TypeVar

log = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., None])


def _json(payload: Any) -> str:
    """Serialise an alert payload that came out of a warehouse row.

    `default=str` is not laziness. Every detail dict in this pipeline is built from a
    Snowflake result, and the connector returns NUMBER as `Decimal` and TIMESTAMP as
    `datetime` -- neither of which the json module encodes. The exact repr matters far less
    than the alert arriving, so unknown types are stringified rather than dropped.
    """
    return json.dumps(payload, default=str)


def _never_raises(func: F) -> F:
    """Guarantee that a notification cannot fail the task it is reporting on.

    The individual senders already swallow delivery errors, but that guard was drawn too
    tightly: it covered the network call and not the construction of the message. A
    `Decimal` in a detail dict was therefore enough to turn a passing AMBER gate into a
    failed task -- the alerting layer making the outcome worse, which is exactly what this
    module's design note says it must never do. The boundary belongs at the entry point.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        try:
            func(*args, **kwargs)
        except Exception:
            log.exception(
                "alerting failed in %s (the task's own outcome is unaffected)", func.__name__
            )

    return wrapper  # type: ignore[return-value]


ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080")

#: Maps a task id prefix to the runbook section that explains that failure class. Sending
#: someone to a 40-page runbook with no anchor is barely better than sending nothing.
RUNBOOK_SECTIONS: dict[str, str] = {
    "preflight": "docs/runbook.md#preflight-failures",
    "wait_for": "docs/runbook.md#file-arrival-delay",
    "load": "docs/runbook.md#load-failures",
    "drain": "docs/runbook.md#stream-drain-failures",
    "dq_gate": "docs/runbook.md#data-quality-gate-tripped",
    "dbt_source_freshness": "docs/runbook.md#source-freshness-failure",
    "dbt_seed": "docs/runbook.md#dbt-failures",
    "dbt_run": "docs/runbook.md#dbt-failures",
    "dbt_test": "docs/runbook.md#dbt-test-failures",
    "dbt_snapshot": "docs/runbook.md#snapshot-failures",
    "reconcile": "docs/runbook.md#reconciliation-mismatch",
}


def _runbook_for(task_id: str) -> str:
    for prefix, section in RUNBOOK_SECTIONS.items():
        if task_id.startswith(prefix) or prefix in task_id:
            return section
    return "docs/runbook.md"


def _log_url(context: dict[str, Any]) -> str:
    task_instance = context.get("task_instance")
    if task_instance is None:
        return AIRFLOW_BASE_URL
    try:
        return task_instance.log_url  # type: ignore[no-any-return]
    except Exception:  # noqa: BLE001 - a link is a convenience; never fail an alert over one
        return AIRFLOW_BASE_URL


def _extract(context: dict[str, Any]) -> dict[str, str]:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    return {
        "dag_id": getattr(task_instance, "dag_id", "unknown"),
        "task_id": getattr(task_instance, "task_id", "unknown"),
        "run_id": getattr(dag_run, "run_id", "unknown"),
        "try_number": str(getattr(task_instance, "try_number", "?")),
        "max_tries": str(getattr(task_instance, "max_tries", "?")),
        "logical_date": str(context.get("logical_date") or context.get("execution_date") or ""),
        "duration": str(getattr(task_instance, "duration", "") or ""),
        "hostname": str(getattr(task_instance, "hostname", "") or ""),
        "exception": str(exception)[:3000] if exception else "(no exception recorded)",
        "log_url": _log_url(context),
    }


def _send_email(subject: str, html_body: str) -> None:
    """Send via Airflow's configured SMTP.

    Imported lazily so this module can be imported (and unit-tested) outside an Airflow
    runtime, where `airflow.utils.email` does not exist.
    """
    if not ALERT_EMAIL:
        log.warning("ALERT_EMAIL is not set; skipping email alert: %s", subject)
        return
    try:
        from airflow.utils.email import send_email

        send_email(to=[ALERT_EMAIL], subject=subject, html_content=html_body)
        log.info("alert email sent to %s: %s", ALERT_EMAIL, subject)
    except Exception:
        # Never let a mail failure mask the real error.
        log.exception("failed to send alert email (original task failure is unaffected)")


def _send_slack(text: str, blocks: list[dict[str, Any]] | None = None) -> None:
    """Post to a Slack incoming webhook, if one is configured.

    Uses urllib rather than the Slack provider package so the DAG has no extra dependency
    for what is an optional convenience.
    """
    if not SLACK_WEBHOOK_URL:
        return
    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        request = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=_json(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 300:
                log.warning("Slack webhook returned HTTP %s", response.status)
    except (urllib.error.URLError, TimeoutError):
        log.exception("failed to post Slack alert (original task failure is unaffected)")


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------
@_never_raises
def on_failure(context: dict[str, Any]) -> None:
    """Task failure: the alert that must always be actionable."""
    info = _extract(context)
    runbook = _runbook_for(info["task_id"])
    is_final = info["try_number"] == info["max_tries"]

    subject = (
        f"[{'FAILED' if is_final else 'FAILING'}] {info['dag_id']}.{info['task_id']} "
        f"(attempt {info['try_number']}/{info['max_tries']})"
    )

    body = f"""
    <html><body style="font-family: -apple-system, Segoe UI, sans-serif;">
      <h2 style="color:#b00020;margin-bottom:4px;">Trade pipeline task failure</h2>
      <p style="color:#555;margin-top:0;">
        {
        "This was the final attempt. The DAG run has failed."
        if is_final
        else "Airflow will retry automatically. No action needed unless the retries also fail."
    }
      </p>

      <table cellpadding="6" style="border-collapse:collapse;font-size:13px;">
        <tr><td><b>DAG</b></td><td><code>{info["dag_id"]}</code></td></tr>
        <tr><td><b>Task</b></td><td><code>{info["task_id"]}</code></td></tr>
        <tr><td><b>Run</b></td><td><code>{info["run_id"]}</code></td></tr>
        <tr><td><b>Logical date</b></td><td>{info["logical_date"]}</td></tr>
        <tr><td><b>Attempt</b></td><td>{info["try_number"]} of {info["max_tries"]}</td></tr>
        <tr><td><b>Worker</b></td><td>{info["hostname"]}</td></tr>
        <tr><td><b>Duration</b></td><td>{info["duration"]} s</td></tr>
      </table>

      <h3>Error</h3>
      <pre style="background:#f6f6f6;padding:12px;border-radius:4px;
                  font-size:12px;white-space:pre-wrap;">{info["exception"]}</pre>

      <h3>Next steps</h3>
      <ol>
        <li><a href="{info["log_url"]}">Open the task log</a></li>
        <li>Runbook: <code>{runbook}</code></li>
        <li>Check platform health: <code>SELECT * FROM MONITORING.VW_PIPELINE_SLA;</code></li>
      </ol>

      <p style="color:#888;font-size:11px;">
        RAW is immutable and every step is idempotent, so re-running a failed task is always
        safe. See docs/runbook.md#recovery-principles.
      </p>
    </body></html>
    """

    _send_email(subject, body)

    if is_final:
        _send_slack(
            f":rotating_light: *{info['dag_id']}* failed at `{info['task_id']}`",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f":rotating_light: *{info['dag_id']}* failed\n"
                            f"*Task:* `{info['task_id']}`  *Run:* `{info['run_id']}`\n"
                            f"*Error:* ```{info['exception'][:500]}```\n"
                            f"*Runbook:* `{runbook}`"
                        ),
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open log"},
                            "url": info["log_url"],
                        }
                    ],
                },
            ],
        )


@_never_raises
def on_retry(context: dict[str, Any]) -> None:
    """Retry: logged, not emailed.

    Emailing every retry is how alerting gets muted. A transient warehouse-resume race that
    succeeds on attempt two is not something anyone needs to read about at 3am -- but it does
    belong in the log, because a task that always succeeds on attempt three is a real problem
    hiding behind a green DAG.
    """
    info = _extract(context)
    log.warning(
        "retrying %s.%s (attempt %s/%s): %s",
        info["dag_id"],
        info["task_id"],
        info["try_number"],
        info["max_tries"],
        info["exception"],
    )


@_never_raises
def on_success(context: dict[str, Any]) -> None:
    """DAG-level success: log a one-line summary. No notification."""
    dag_run = context.get("dag_run")
    log.info(
        "dag run succeeded: %s (%s)",
        getattr(dag_run, "dag_id", "?"),
        getattr(dag_run, "run_id", "?"),
    )


@_never_raises
def on_sla_miss(
    dag: Any, task_list: str, blocking_task_list: str, slas: Any, blocking_tis: Any
) -> None:
    """SLA miss: the task is still running, but has taken longer than promised.

    Distinct from a failure and worth a separate alert. A load that normally takes two
    minutes and has been running for twenty has usually hit warehouse queuing or an upstream
    volume spike -- nothing has failed, but the downstream consumers are about to miss their
    own deadline, and someone should look before they do.
    """
    subject = f"[SLA MISS] {getattr(dag, 'dag_id', 'trade_pipeline')}"
    body = f"""
    <html><body style="font-family: -apple-system, Segoe UI, sans-serif;">
      <h2 style="color:#e67e00;">SLA missed</h2>
      <p>These tasks have exceeded their promised duration. They have not failed -- they are
         late, which usually means warehouse queuing or an unusual data volume.</p>
      <p><b>Tasks past SLA:</b><br/><code>{task_list}</code></p>
      <p><b>Blocking tasks:</b><br/><code>{blocking_task_list}</code></p>
      <h3>What to check</h3>
      <ol>
        <li><code>SELECT * FROM MONITORING.VW_BATCH_HEALTH;</code> -- is a batch stuck?</li>
        <li><code>SELECT * FROM MONITORING.VW_STREAM_LAG;</code> -- has the backlog grown?</li>
        <li><code>SELECT * FROM MONITORING.VW_DBT_QUERY_PERFORMANCE;</code> -- spill or queuing?</li>
      </ol>
      <p style="color:#888;font-size:11px;">Runbook: docs/runbook.md#sla-miss</p>
    </body></html>
    """
    _send_email(subject, body)
    _send_slack(
        f":hourglass_flowing_sand: SLA miss on *{getattr(dag, 'dag_id', '?')}*: `{task_list}`"
    )


@_never_raises
def notify_dq_gate_breach(gate_name: str, detail: dict[str, Any], *, blocking: bool) -> None:
    """Data quality gate outcome.

    Called directly by the gate tasks rather than through a callback, because a gate breach
    is a data problem rather than a software failure, and the alert needs the metrics -- not
    a stack trace.
    """
    verdict = "BLOCKED the pipeline" if blocking else "raised a warning"
    subject = f"[DQ GATE] {gate_name} {verdict}"

    rows = "".join(
        f"<tr><td><b>{key}</b></td><td>{value}</td></tr>" for key, value in detail.items()
    )
    body = f"""
    <html><body style="font-family: -apple-system, Segoe UI, sans-serif;">
      <h2 style="color:{"#b00020" if blocking else "#e67e00"};">
        Data quality gate: {gate_name}
      </h2>
      <p>The gate {verdict}.</p>
      <table cellpadding="6" style="border-collapse:collapse;font-size:13px;">{rows}</table>
      <h3>What this means</h3>
      <p>
        {
        "The curated layer has NOT been updated. Upstream data quality has degraded beyond "
        "the configured threshold, and continuing would publish suspect data to the golden "
        "record."
        if blocking
        else "The pipeline continued. The metric is elevated but within tolerance -- worth "
        "watching rather than acting on."
    }
      </p>
      <h3>Investigate</h3>
      <pre style="background:#f6f6f6;padding:12px;border-radius:4px;font-size:12px;">
SELECT rule_code, rule_name, source_system, hits_last_24h, is_concentrated
FROM REPORTING.AGG_REJECTION_ANALYSIS
ORDER BY hits_last_24h DESC
LIMIT 20;</pre>
      <p style="color:#888;font-size:11px;">Runbook: docs/runbook.md#data-quality-gate-tripped</p>
    </body></html>
    """
    _send_email(subject, body)
    icon = ":no_entry:" if blocking else ":warning:"
    _send_slack(f"{icon} DQ gate *{gate_name}* {verdict}: `{_json(detail)[:400]}`")
