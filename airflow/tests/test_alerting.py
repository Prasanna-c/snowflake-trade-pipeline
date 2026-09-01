"""Tests for the alerting layer, whose one hard requirement is that it cannot make things worse.

Every alert payload in this pipeline is built from a Snowflake result, and the connector
returns NUMBER as `Decimal` and TIMESTAMP as `datetime`. Neither is JSON-serialisable, so an
alert about a data quality gate used to raise while *formatting itself* -- turning a gate that
had merely gone AMBER into a failed task. These tests exist because that shipped once.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from utils import alerting


class TestPayloadSerialisation:
    def test_warehouse_types_survive_serialisation(self) -> None:
        """A detail dict straight off a cursor must serialise."""
        payload = {
            "reject_rate_pct": Decimal("4.25"),
            "as_of": datetime(2026, 9, 1, tzinfo=UTC),
            "total_trades": 2957,
            "status": "AMBER",
        }

        encoded = alerting._json(payload)

        assert "4.25" in encoded
        assert "2026-09-01" in encoded
        assert '"total_trades": 2957' in encoded

    def test_an_unknown_object_does_not_stop_the_alert(self) -> None:
        """Stringify rather than raise: an unsendable alert is worse than an imprecise one."""

        class Opaque:
            def __str__(self) -> str:
                return "opaque-value"

        assert "opaque-value" in alerting._json({"thing": Opaque()})


class TestNotificationsCannotFailTheTask:
    """The guarantee in this module's design note, asserted rather than asserted-in-prose."""

    def test_a_decimal_in_a_gate_detail_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exact regression: an AMBER publish gate failed on Decimal formatting."""
        monkeypatch.setattr(alerting, "SLACK_WEBHOOK_URL", "https://hooks.example/none")
        sent: list[str] = []
        monkeypatch.setattr(alerting, "_send_slack", lambda text, blocks=None: sent.append(text))

        alerting.notify_dq_gate_breach(
            "publish_readiness",
            {"overall_status": "AMBER", "reject_rate_pct": Decimal("3.9")},
            blocking=False,
        )

        assert sent, "the alert should still have been composed and dispatched"
        assert "3.9" in sent[0]

    def test_a_broken_sender_is_swallowed_and_logged(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        def explode(*_: object, **__: object) -> None:
            raise RuntimeError("webhook exploded")

        monkeypatch.setattr(alerting, "_send_slack", explode)

        with caplog.at_level(logging.ERROR):
            alerting.notify_dq_gate_breach("reject_rate", {"n": 1}, blocking=True)

        assert "alerting failed in notify_dq_gate_breach" in caplog.text

    def test_a_malformed_context_does_not_raise(self, caplog: pytest.LogCaptureFixture) -> None:
        """Callbacks receive whatever Airflow passes, which in tests and edge cases is partial.

        A KeyError inside on_failure would replace the task's real exception with a callback
        traceback, which is the failure mode this module exists to avoid.
        """
        with caplog.at_level(logging.ERROR):
            alerting.on_failure({})

        # Either it coped or it logged -- what matters is that nothing propagated.
        assert True
