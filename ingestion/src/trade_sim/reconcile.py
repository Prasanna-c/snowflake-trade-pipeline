"""End-to-end correctness check.

The generator knows what verdict every event *should* receive. This module asks the
warehouse what verdict each event *did* receive, and reports the difference.

This is the piece that turns the project from a demo into a tested system. "The DAG
went green" says nothing about whether business rule 1 works. "We injected 47
stale-version events and the pipeline rejected exactly those 47 with RJ001" does.

It also catches the failure mode that no dbt test can: an event that vanished. A row
present in the manifest but absent from adjudication means it was lost between the file
and the warehouse -- silent data loss, which is the worst outcome in a regulated
pipeline and the hardest to notice.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from trade_sim.loaders.snowflake_loader import SnowflakeSession
from trade_sim.schema import BatchManifest, ExpectedVerdict

log = logging.getLogger(__name__)


@dataclass
class Discrepancy:
    kind: str
    trade_id: str | None
    trade_version: int | None
    expected: str
    actual: str
    detail: str = ""


@dataclass
class ReconciliationResult:
    manifest_ref: str
    events_expected: int
    events_found: int
    verdict_matches: int
    verdict_mismatches: int
    missing_events: int
    unexpected_events: int
    rule_code_matches: int
    rule_code_mismatches: int
    discrepancies: list[Discrepancy] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return (
            self.verdict_mismatches == 0
            and self.missing_events == 0
            and self.rule_code_mismatches == 0
        )

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"[{status}] {self.manifest_ref}: "
            f"{self.verdict_matches}/{self.events_expected} verdicts matched, "
            f"{self.missing_events} missing, "
            f"{self.verdict_mismatches} wrong verdict, "
            f"{self.rule_code_mismatches} wrong rule codes"
        )


#: Events whose injected fault destroys the trade identifier cannot be matched by
#: trade_id. They are reconciled by count against RAW.COPY_ERROR instead, because
#: Snowflake rejects them at parse time and they never reach adjudication.
_UNMATCHABLE_FAULTS = frozenset({"unparseable_json", "missing_trade_id"})


def _as_int(value: object) -> int:
    """Coerce one warehouse cell to an int.

    A row is `dict[str, object]` because a cursor cannot promise a type per column, and
    Snowflake hands back NUMBER as `Decimal`, so `int(value)` does not type-check and a
    cast would only hide that. Routing through `str` accepts Decimal, int and a digit
    string alike, which is the full set this schema can produce for a version number.
    """
    return value if isinstance(value, int) else int(str(value))


def _claim_row(
    unclaimed: list[dict[str, object]], expectation: ExpectedVerdict
) -> dict[str, object] | None:
    """Take the row that best answers this expectation, removing it from the pool.

    Preference order is verdict *and* rule codes, then verdict alone, then whatever is left,
    so that a same-version pair is paired up correctly instead of being reported as two
    mismatches. The codes are needed as a tie-break because both events in that pair can be
    refused, and then the verdict alone cannot say which row belongs to which expectation:
    pairing a corrupt event with the clean event's row reported the fault's code as missing.
    Falling back to any remaining row keeps a genuine wrong verdict visible rather than
    silently reclassifying it as a missing event.
    """
    expected_codes = set(expectation.expected_rule_codes)

    for index, row in enumerate(unclaimed):
        if str(row["VERDICT"]) == expectation.expected_verdict and expected_codes.issubset(
            _rule_codes(row)
        ):
            return unclaimed.pop(index)

    for index, row in enumerate(unclaimed):
        if str(row["VERDICT"]) == expectation.expected_verdict:
            return unclaimed.pop(index)

    return unclaimed.pop(0) if unclaimed else None


def _rule_codes(row: dict[str, object]) -> set[str]:
    """The rule codes on an adjudicated row. Snowflake returns the array as JSON text."""
    return set(json.loads(str(row["VIOLATED_RULE_CODES"] or "[]")))


class Reconciler:
    def __init__(self, session: SnowflakeSession) -> None:
        self.session = session

    def reconcile(self, manifest: BatchManifest) -> ReconciliationResult:
        expected = [
            v
            for v in manifest.verdicts
            if v.trade_id is not None and v.injected_fault not in _UNMATCHABLE_FAULTS
        ]
        # Grouped rather than keyed one-to-one. A trade version can legitimately appear
        # twice in a single batch: business rule 2's same-version resend produces two
        # events sharing (trade_id, trade_version), one accepted and one superseded.
        # A dict keyed on that pair drops one of the two on both sides, so the race the
        # pipeline exists to arbitrate was the one case reconciliation never saw.
        expected_by_key: dict[tuple[str, int], list[ExpectedVerdict]] = defaultdict(list)
        for verdict in expected:
            expected_by_key[(str(verdict.trade_id), int(verdict.trade_version or 0))].append(
                verdict
            )

        actual_rows = self._fetch_adjudicated(
            sorted({key[0] for key in expected_by_key}), manifest.batch_ref
        )
        actual_by_key: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
        for row in actual_rows:
            if row.get("TRADE_ID") is None or row.get("TRADE_VERSION") is None:
                continue
            actual_by_key[(str(row["TRADE_ID"]), _as_int(row["TRADE_VERSION"]))].append(row)

        discrepancies: list[Discrepancy] = []
        verdict_matches = verdict_mismatches = 0
        rule_matches = rule_mismatches = 0
        missing = 0

        for key, expectations in expected_by_key.items():
            unclaimed = list(actual_by_key.get(key, []))

            for exp in expectations:
                actual = _claim_row(unclaimed, exp)
                if actual is None:
                    missing += 1
                    discrepancies.append(
                        Discrepancy(
                            kind="MISSING_EVENT",
                            trade_id=exp.trade_id,
                            trade_version=exp.trade_version,
                            expected=exp.expected_verdict,
                            actual="ABSENT",
                            detail=(
                                "Event is in the manifest but not in "
                                "INT_TRADE_EVENT_ADJUDICATED. Either it never loaded, "
                                "or dbt has not processed its batch yet."
                            ),
                        )
                    )
                    continue

                actual_verdict = str(actual["VERDICT"])
                if actual_verdict == exp.expected_verdict:
                    verdict_matches += 1
                else:
                    verdict_mismatches += 1
                    discrepancies.append(
                        Discrepancy(
                            kind="WRONG_VERDICT",
                            trade_id=exp.trade_id,
                            trade_version=exp.trade_version,
                            expected=exp.expected_verdict,
                            actual=actual_verdict,
                            detail=f"injected_fault={exp.injected_fault or 'none'}",
                        )
                    )

                # Rule codes: assert the expected codes are a subset of what fired. Not
                # equality -- one injected fault can legitimately trip a second rule (a
                # maturity before the trade date is also a maturity in the past), and
                # demanding exact equality would make the check brittle rather than
                # useful.
                actual_codes = _rule_codes(actual)
                expected_codes = set(exp.expected_rule_codes)
                if expected_codes.issubset(actual_codes):
                    rule_matches += 1
                else:
                    rule_mismatches += 1
                    discrepancies.append(
                        Discrepancy(
                            kind="MISSING_RULE_CODE",
                            trade_id=exp.trade_id,
                            trade_version=exp.trade_version,
                            expected=",".join(sorted(expected_codes)),
                            actual=",".join(sorted(actual_codes)) or "none",
                            detail=f"injected_fault={exp.injected_fault or 'none'}",
                        )
                    )

        unexpected = len(set(actual_by_key) - set(expected_by_key))

        return ReconciliationResult(
            manifest_ref=manifest.batch_ref,
            events_expected=sum(len(v) for v in expected_by_key.values()),
            events_found=len(actual_rows),
            verdict_matches=verdict_matches,
            verdict_mismatches=verdict_mismatches,
            missing_events=missing,
            unexpected_events=unexpected,
            rule_code_matches=rule_matches,
            rule_code_mismatches=rule_mismatches,
            discrepancies=discrepancies,
        )

    def _fetch_adjudicated(self, trade_ids: list[str], batch_ref: str) -> list[dict[str, object]]:
        """The adjudicated events that arrived in THIS batch's files.

        Scoping by batch is what makes the comparison mean anything. A trade identifier
        is unique within one simulated universe but not across universes: generate from
        an empty trade book twice and both runs mint TRD-000000001. Loading both into
        one warehouse is perfectly legitimate, and the pipeline is right to treat the
        second arrival as a duplicate version -- but an unscoped lookup by trade_id then
        hands this manifest an event belonging to a different batch, and every
        difference between two unrelated events is reported as a pipeline defect.

        The batch reference is embedded in the file name, the same convention
        parse_error_count relies on.
        """
        if not trade_ids:
            return []
        database = self.session.settings.database
        intermediate = self.session.settings.dbt_schema("intermediate")
        # Chunked IN-lists rather than one enormous predicate. Snowflake's expression
        # limit is generous but finite, and a 50,000-element IN-list compiles slowly.
        rows: list[dict[str, object]] = []
        chunk_size = 5_000
        for start in range(0, len(trade_ids), chunk_size):
            chunk = trade_ids[start : start + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            sql = f"""
                select
                    trade_id,
                    trade_version,
                    verdict,
                    to_json(violated_rule_codes) as violated_rule_codes,
                    version_action
                from {database}.{intermediate}.int_trade_event_adjudicated
                where trade_id in ({placeholders})
                  and source_file_name ilike %s
            """
            rows.extend(self.session.execute(sql, (*chunk, f"%{batch_ref}%")))
        return rows

    def parse_error_count(self, batch_ref: str) -> int:
        """Rows Snowflake rejected at parse time -- the home of unparseable payloads."""
        database = self.session.settings.database
        result = self.session.scalar(
            f"""
            select count(*)
            from {database}.raw.copy_error
            where source_file_name ilike %s
            """,
            (f"%{batch_ref}%",),
        )
        return int(result or 0)


def load_manifest(path: Path) -> BatchManifest:
    return BatchManifest.model_validate_json(path.read_text(encoding="utf-8"))


def find_manifests(manifest_dir: Path, batch_ref: str | None = None) -> list[Path]:
    """Manifests matching `batch_ref`, oldest first.

    Ordered by write time, not by name: a batch reference is random hex, so sorting by
    filename would order the batches arbitrarily and leave a caller asking for the latest
    batch holding whichever one happened to sort last.
    """
    if not manifest_dir.is_dir():
        return []
    pattern = f"*{batch_ref}*.manifest.json" if batch_ref else "*.manifest.json"
    return sorted(manifest_dir.glob(pattern), key=lambda path: path.stat().st_mtime)
