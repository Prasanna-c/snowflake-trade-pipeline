"""Tests for the component that certifies everything else.

Reconciliation is what turns "the DAG went green" into "the verdicts were right", which
makes it the one component whose own bugs are invisible. A reconciler that compares the
wrong rows will report a healthy pipeline as broken -- or, worse, a broken one as
healthy. It had no tests until a same-version race and a replayed trade universe found
both of its blind spots at once.

Offline, like every test in this suite: the session is a stub that replays canned rows.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from trade_sim.reconcile import Reconciler
from trade_sim.schema import BatchManifest, ExpectedVerdict


class _FakeSettings:
    database = "TRADES_TEST"

    def dbt_schema(self, layer: str) -> str:
        return f"DBT_TEST_{layer.upper()}"


class _FakeSession:
    """Records the SQL it was asked to run, and replays canned rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.settings = _FakeSettings()
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self._rows = rows

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.statements.append((sql, params))
        return self._rows


def _expected(
    trade_id: str,
    version: int,
    verdict: str,
    codes: list[str] | None = None,
    fault: str | None = None,
) -> ExpectedVerdict:
    return ExpectedVerdict(
        trade_id=trade_id,
        trade_version=version,
        action="NEW",
        injected_fault=fault,
        expected_verdict=verdict,
        expected_rule_codes=codes or [],
    )


def _manifest(*verdicts: ExpectedVerdict, batch_ref: str = "b0001abc") -> BatchManifest:
    return BatchManifest(
        batch_ref=batch_ref,
        generated_at=datetime.now(UTC),
        seed=1,
        file_name=f"trades_dev_{batch_ref}_10.ndjson",
        total_events=len(verdicts),
        expected_accepted=sum(1 for v in verdicts if v.expected_verdict == "ACCEPTED"),
        expected_rejected=sum(1 for v in verdicts if v.expected_verdict == "REJECTED"),
        expected_superseded=sum(1 for v in verdicts if v.expected_verdict == "SUPERSEDED"),
        fault_counts={},
        expected_rule_code_counts={},
        verdicts=list(verdicts),
    )


def _row(
    trade_id: str, version: int, verdict: str, codes: list[str] | None = None
) -> dict[str, Any]:
    return {
        "TRADE_ID": trade_id,
        "TRADE_VERSION": version,
        "VERDICT": verdict,
        "VIOLATED_RULE_CODES": json.dumps(codes or []),
        "VERSION_ACTION": None,
    }


class TestVerdictMatching:
    def test_clean_batch_passes(self) -> None:
        manifest = _manifest(_expected("TRD-000000001", 1, "ACCEPTED"))
        session = _FakeSession([_row("TRD-000000001", 1, "ACCEPTED")])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert result.passed
        assert result.verdict_matches == 1

    def test_same_version_pair_is_matched_as_a_pair(self) -> None:
        """Business rule 2's resend: two events, one version, two different verdicts.

        The rows come back in the opposite order to the manifest on purpose. Nothing
        guarantees the warehouse returns them in generation order, so pairing has to be
        driven by the verdict rather than by position -- and a lookup keyed one-to-one on
        (trade_id, trade_version) would have discarded one side of the pair entirely.
        """
        manifest = _manifest(
            _expected("TRD-000000001", 2, "ACCEPTED"),
            _expected("TRD-000000001", 2, "SUPERSEDED", ["RJ009"]),
        )
        session = _FakeSession(
            [
                _row("TRD-000000001", 2, "SUPERSEDED", ["RJ009"]),
                _row("TRD-000000001", 2, "ACCEPTED"),
            ]
        )

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert result.passed
        assert result.verdict_matches == 2
        assert result.events_expected == 2

    def test_wrong_verdict_is_reported(self) -> None:
        manifest = _manifest(_expected("TRD-000000001", 1, "ACCEPTED"))
        session = _FakeSession([_row("TRD-000000001", 1, "REJECTED", ["RJ001"])])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert not result.passed
        assert result.verdict_mismatches == 1
        assert result.discrepancies[0].kind == "WRONG_VERDICT"

    def test_absent_event_is_reported_as_missing(self) -> None:
        """The only failure mode no dbt test can see: an event that never arrived."""
        manifest = _manifest(_expected("TRD-000000001", 1, "ACCEPTED"))
        session = _FakeSession([])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert not result.passed
        assert result.missing_events == 1
        assert result.discrepancies[0].kind == "MISSING_EVENT"

    def test_unmatchable_faults_are_excluded(self) -> None:
        """An event whose identifier was destroyed cannot be looked up by identifier."""
        manifest = _manifest(
            _expected("TRD-000000001", 1, "REJECTED", ["RJ008"], fault="unparseable_json"),
        )
        session = _FakeSession([])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert result.passed
        assert result.events_expected == 0


class TestRuleCodes:
    def test_expected_codes_need_only_be_a_subset(self) -> None:
        """One fault can legitimately trip a second rule; equality would be brittle."""
        manifest = _manifest(_expected("TRD-000000001", 1, "REJECTED", ["RJ002"]))
        session = _FakeSession([_row("TRD-000000001", 1, "REJECTED", ["RJ002", "RJ003"])])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert result.passed
        assert result.rule_code_matches == 1

    def test_a_missing_code_is_reported(self) -> None:
        manifest = _manifest(_expected("TRD-000000001", 1, "REJECTED", ["RJ015"]))
        session = _FakeSession([_row("TRD-000000001", 1, "REJECTED", ["RJ004"])])

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert not result.passed
        assert result.rule_code_mismatches == 1
        assert result.discrepancies[0].kind == "MISSING_RULE_CODE"

    def test_two_refused_events_on_one_version_are_paired_by_their_codes(self) -> None:
        """The same-version race, with both events refused.

        A clean late arrival and a corrupt one share (trade_id, trade_version) and both come back
        REJECTED, so the verdict alone cannot say which row answers which expectation. Pairing on
        the verdict alone gave the corrupt expectation the clean event's row and reported RJ008 as
        missing -- a reconciler bug wearing a pipeline bug's clothes.
        """
        manifest = _manifest(
            _expected("TRD-000000001", 2, "REJECTED", ["RJ001"]),
            _expected("TRD-000000001", 2, "REJECTED", ["RJ001", "RJ008"], fault="type_mismatch"),
        )
        session = _FakeSession(
            [
                _row("TRD-000000001", 2, "REJECTED", ["RJ001"]),
                _row("TRD-000000001", 2, "REJECTED", ["RJ001", "RJ008"]),
            ]
        )

        result = Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        assert result.passed
        assert result.rule_code_matches == 2


class TestBatchScoping:
    def test_lookup_is_restricted_to_the_batch_that_produced_the_manifest(self) -> None:
        """Trade identifiers are unique within a simulated universe, not across them.

        Two runs from an empty trade book both mint TRD-000000001. Without this filter a
        manifest is compared against another batch's event, and every difference between
        two unrelated trades is reported as a pipeline defect.
        """
        manifest = _manifest(_expected("TRD-000000001", 1, "ACCEPTED"), batch_ref="b99xyz")
        session = _FakeSession([_row("TRD-000000001", 1, "ACCEPTED")])

        Reconciler(session).reconcile(manifest)  # type: ignore[arg-type]

        sql, params = session.statements[0]
        assert "source_file_name ilike" in sql
        assert params[-1] == "%b99xyz%"
