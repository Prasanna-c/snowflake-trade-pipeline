"""Tests for the generator's contract with the pipeline.

These are not "does the code run" tests. Each one pins a property the dbt rules depend
on. If any of them break, a business rule silently stops being exercised, and the
pipeline could regress without any test going red.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

from trade_sim import reference as ref
from trade_sim.config import SimulatorSettings
from trade_sim.generator import (
    FAULT_EXPECTATIONS,
    WARN_ONLY_FAULTS,
    GeneratedEvent,
    TradeBook,
    TradeGenerator,
    _business_time,
)
from trade_sim.schema import TradeEvent

#: The codes the STATE phase issues, per the phase column in trade_validation_rules.sql.
#: These are the only ones a manifest can reach from the ordering of a batch; every other
#: code is a FIELD finding, decided by the fault injected rather than by arbitration.
STATE_PHASE_RULE_CODES = frozenset({"RJ001", "RJ009", "RJ010"})


class TestDeterminism:
    def test_same_seed_produces_identical_output(self, sim_settings: SimulatorSettings) -> None:
        """A reviewer must be able to reproduce a specific scenario exactly."""
        first = TradeGenerator(sim_settings, book=TradeBook(path=sim_settings.state_dir / "a.json"))
        second = TradeGenerator(
            sim_settings, book=TradeBook(path=sim_settings.state_dir / "b.json")
        )

        left = [e.payload for e in first.generate(50, as_of=date(2026, 6, 1))]
        right = [e.payload for e in second.generate(50, as_of=date(2026, 6, 1))]

        assert left == right

    def test_different_seed_produces_different_output(
        self, sim_settings: SimulatorSettings
    ) -> None:
        first = TradeGenerator(sim_settings, book=TradeBook(path=sim_settings.state_dir / "a.json"))
        other_settings = sim_settings.model_copy(update={"seed": 9999})
        second = TradeGenerator(
            other_settings, book=TradeBook(path=sim_settings.state_dir / "b.json")
        )

        left = [e.payload for e in first.generate(50, as_of=date(2026, 6, 1))]
        right = [e.payload for e in second.generate(50, as_of=date(2026, 6, 1))]

        assert left != right


class TestCleanEvents:
    def test_clean_events_validate_against_the_contract(
        self, clean_generator: TradeGenerator
    ) -> None:
        """With error_rate=0 every payload must satisfy the pydantic contract.

        If this fails, the generator is producing accidental faults, which would make
        reject-rate metrics meaningless.
        """
        for event in clean_generator.generate(200, as_of=date(2026, 6, 1)):
            assert event.injected_fault is None
            assert isinstance(event.payload, dict)
            TradeEvent.model_validate(event.payload)

    def test_clean_events_are_expected_to_be_accepted(
        self, clean_generator: TradeGenerator
    ) -> None:
        for event in clean_generator.generate(100, as_of=date(2026, 6, 1)):
            assert event.expected_verdict == "ACCEPTED"
            assert event.expected_rule_codes == []

    def test_maturity_is_never_before_trade_date(self, clean_generator: TradeGenerator) -> None:
        """Rule 2 (RJ002) must only ever fire on injected faults, never on clean data."""
        for event in clean_generator.generate(300, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            trade_date = date.fromisoformat(event.payload["trade_date"])
            maturity = event.payload["maturity_date"]
            if maturity is not None:
                assert date.fromisoformat(maturity) >= trade_date

    def test_settlement_is_never_before_trade_date(self, clean_generator: TradeGenerator) -> None:
        for event in clean_generator.generate(300, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            trade_date = date.fromisoformat(event.payload["trade_date"])
            settlement = event.payload["settlement_date"]
            if settlement is not None:
                assert date.fromisoformat(settlement) >= trade_date

    def test_notional_is_always_positive(self, clean_generator: TradeGenerator) -> None:
        for event in clean_generator.generate(200, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            assert event.payload["notional_amount"] > 0

    def test_counterparties_are_active_and_known(self, clean_generator: TradeGenerator) -> None:
        for event in clean_generator.generate(200, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            assert event.payload["counterparty_id"] in ref.ACTIVE_COUNTERPARTY_IDS

    def test_physically_settled_products_use_deliverable_currencies(
        self, clean_generator: TradeGenerator
    ) -> None:
        """RJ017 must be injectable, never accidental."""
        for event in clean_generator.generate(400, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            if event.payload["product_type"] in ref.PHYSICALLY_SETTLED_PRODUCTS:
                assert event.payload["settlement_currency"] in ref.DELIVERABLE_CURRENCY_CODES

    def test_settlement_dates_fall_on_weekdays(self, clean_generator: TradeGenerator) -> None:
        for event in clean_generator.generate(200, as_of=date(2026, 6, 1)):
            assert isinstance(event.payload, dict)
            settlement = date.fromisoformat(event.payload["settlement_date"])
            assert settlement.weekday() < 5


class TestVersioning:
    def test_new_trades_start_at_version_one(self, clean_generator: TradeGenerator) -> None:
        for event in clean_generator.generate(50, as_of=date(2026, 6, 1)):
            assert event.trade_version == 1
            assert event.action == "NEW"

    def test_amendments_increment_the_version(self, sim_settings: SimulatorSettings) -> None:
        settings = sim_settings.model_copy(update={"amend_rate": 1.0, "cancel_rate": 0.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))

        # Seed the book with one trade, then force amendments.
        list(generator.generate(1, as_of=date(2026, 6, 1)))
        seeded_id = next(iter(generator.book.trades))

        versions = []
        for event in generator.generate(5, as_of=date(2026, 6, 2)):
            if event.trade_id == seeded_id:
                versions.append(event.trade_version)

        # Each amendment of the same trade must be strictly higher than the last.
        assert versions == sorted(versions)
        assert all(v > 1 for v in versions)

    def test_stale_versions_are_expected_to_be_rejected(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """Business rule 1 must be exercised with a correct expectation attached."""
        settings = sim_settings.model_copy(
            update={"amend_rate": 1.0, "cancel_rate": 0.0, "stale_version_rate": 0.0}
        )
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))

        # Build up a trade to version 3 so there is room to go stale.
        list(generator.generate(1, as_of=date(2026, 6, 1)))
        list(generator.generate(4, as_of=date(2026, 6, 2)))

        stale_settings = settings.model_copy(update={"stale_version_rate": 1.0, "amend_rate": 0.0})
        stale_generator = TradeGenerator(stale_settings, book=generator.book)

        events = list(stale_generator.generate(10, as_of=date(2026, 6, 3)))
        rejected = [e for e in events if e.expected_verdict == "REJECTED"]

        assert rejected, "stale_version_rate=1.0 must produce rejections"
        assert all("RJ001" in e.expected_rule_codes for e in rejected)

    def test_stale_versions_do_not_advance_the_book(self, sim_settings: SimulatorSettings) -> None:
        """A rejected event must not move the trade book, or later versions go wrong."""
        settings = sim_settings.model_copy(update={"amend_rate": 1.0, "cancel_rate": 0.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        list(generator.generate(1, as_of=date(2026, 6, 1)))
        list(generator.generate(3, as_of=date(2026, 6, 2)))

        trade_id = next(iter(generator.book.trades))
        version_before = generator.book.trades[trade_id].trade_version

        stale_settings = settings.model_copy(update={"stale_version_rate": 1.0, "amend_rate": 0.0})
        stale_generator = TradeGenerator(stale_settings, book=generator.book)
        list(stale_generator.generate(5, as_of=date(2026, 6, 3)))

        assert generator.book.trades[trade_id].trade_version == version_before

    def test_replacements_keep_the_same_version(self, sim_settings: SimulatorSettings) -> None:
        """Business rule 2: a same-version resend must be accepted, not rejected.

        Generated in one batch on purpose. A resend only races the arrival it replaces if
        both are adjudicated in the same build, so the generator draws replacements from
        trades booked in the current batch and nowhere else.
        """
        settings = sim_settings.model_copy(update={"replace_rate": 0.5, "amend_rate": 0.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(20, as_of=date(2026, 6, 1)))

        by_key: dict[tuple[str | None, int | None], list[GeneratedEvent]] = {}
        for event in events:
            by_key.setdefault((event.trade_id, event.trade_version), []).append(event)
        resent = [group for group in by_key.values() if len(group) > 1]

        assert resent, "replace_rate=0.5 must resend some version within the batch"
        for group in resent:
            versions = {e.trade_version for e in group}
            assert len(versions) == 1

    def test_only_the_last_arrival_of_a_version_is_expected_to_survive(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """The other half of rule 2.

        The pipeline accepts the resend with the latest business time and records the
        earlier arrivals as SUPERSEDED. A manifest that expects all of them to be accepted
        reports a correct pipeline as broken.
        """
        settings = sim_settings.model_copy(update={"replace_rate": 0.5, "amend_rate": 0.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(20, as_of=date(2026, 6, 1)))
        generator.build_manifest(events, batch_ref="b1", file_name="trades.ndjson")

        by_key: dict[tuple[str | None, int | None], list[GeneratedEvent]] = {}
        for event in events:
            by_key.setdefault((event.trade_id, event.trade_version), []).append(event)

        for group in (g for g in by_key.values() if len(g) > 1):
            in_time_order = sorted(
                group, key=lambda e: datetime.fromisoformat(e.payload["event_timestamp"])
            )
            assert in_time_order[-1].expected_verdict == "ACCEPTED"
            for superseded in in_time_order[:-1]:
                assert superseded.expected_verdict == "SUPERSEDED"
                assert superseded.expected_rule_codes == ["RJ009"]

    def test_a_trades_events_are_in_business_time_order(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """An amendment cannot precede the booking it amends.

        Version arbitration orders by business time, so an independently drawn timestamp
        per event yields sequences that cannot happen and verdicts the manifest cannot
        predict.
        """
        settings = sim_settings.model_copy(update={"amend_rate": 1.0, "cancel_rate": 0.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(40, as_of=date(2026, 6, 1)))

        seen: dict[str | None, datetime] = {}
        for event in events:
            stamp = datetime.fromisoformat(event.payload["event_timestamp"])
            previous = seen.get(event.trade_id)
            if previous is not None:
                assert stamp > previous, f"{event.trade_id} v{event.trade_version} went backwards"
            seen[event.trade_id] = stamp

    def test_cancellation_is_terminal(self, sim_settings: SimulatorSettings) -> None:
        """A cancelled trade must never be selected for further amendment."""
        settings = sim_settings.model_copy(update={"amend_rate": 1.0, "cancel_rate": 1.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        list(generator.generate(1, as_of=date(2026, 6, 1)))
        list(generator.generate(3, as_of=date(2026, 6, 2)))

        cancelled = {t.trade_id for t in generator.book.trades.values() if t.is_cancelled}
        assert cancelled, "cancel_rate=1.0 must cancel something"
        assert all(t.trade_id not in cancelled for t in generator.book.amendable())


class TestFaultInjection:
    def test_every_fault_declares_expected_rule_codes(self) -> None:
        """A fault with no expectation cannot be reconciled, so it must not exist."""
        for fault, codes in FAULT_EXPECTATIONS.items():
            assert codes, f"fault {fault} declares no expected rule codes"

    def test_expected_rule_codes_exist_in_the_catalogue(self) -> None:
        """Guards against a typo in a rule code silently disabling reconciliation."""
        for fault, codes in FAULT_EXPECTATIONS.items():
            for code in codes:
                assert code in ref.REJECTION_REASON_BY_CODE, (
                    f"fault {fault} expects unknown rule code {code}"
                )

    def test_no_fault_expects_a_business_date_relative_rule(self) -> None:
        """RJ003 and RJ014 are judged against dbt's business_date, so no manifest may promise them.

        A batch generated on Monday and rebuilt on Wednesday has both rules correctly reach the
        opposite conclusion about the same dates, and reconciliation would report that working
        pipeline as broken. RJ003 also exempts CANCEL, which the fault catalogue cannot see.
        """
        for fault, codes in FAULT_EXPECTATIONS.items():
            assert "RJ003" not in codes, f"{fault} expects RJ003, which depends on the build date"
            assert "RJ014" not in codes, f"{fault} expects RJ014, which depends on the build date"

    def test_warn_only_faults_are_still_accepted(self, sim_settings: SimulatorSettings) -> None:
        """A limit breach is flagged, not rejected -- that distinction is the point."""
        for fault in WARN_ONLY_FAULTS:
            reason = ref.REJECTION_REASON_BY_CODE[FAULT_EXPECTATIONS[fault][0]]
            assert reason.severity == "WARN", (
                f"{fault} is warn-only but rule {reason.rule_code} has severity {reason.severity}"
            )

    def test_all_faults_are_produced_over_enough_events(
        self, faulty_generator: TradeGenerator
    ) -> None:
        """Every fault must actually be reachable, or a rule goes untested."""
        seen: set[str] = set()
        for event in faulty_generator.generate(3_000, as_of=date(2026, 6, 1)):
            if event.injected_fault:
                seen.add(event.injected_fault)
        missing = set(FAULT_EXPECTATIONS) - seen
        assert not missing, f"faults never produced: {sorted(missing)}"

    def test_unparseable_payloads_are_written_as_raw_text(
        self, faulty_generator: TradeGenerator
    ) -> None:
        events = [
            e
            for e in faulty_generator.generate(3_000, as_of=date(2026, 6, 1))
            if e.injected_fault == "unparseable_json"
        ]
        assert events
        for event in events:
            assert isinstance(event.payload, str)
            with pytest.raises(json.JSONDecodeError):
                json.loads(event.payload)

    def test_faulty_events_do_not_advance_the_book(self, sim_settings: SimulatorSettings) -> None:
        """A rejected event must leave the book untouched so versions stay consistent."""
        settings = sim_settings.model_copy(update={"error_rate": 1.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(200, as_of=date(2026, 6, 1)))

        rejected_ids = {
            e.trade_id
            for e in events
            if e.expected_verdict == "REJECTED" and e.trade_id is not None
        }
        # None of the rejected trades should have been written to the book.
        assert not (rejected_ids & set(generator.book.trades))

    @pytest.mark.parametrize(
        ("fault", "assertion"),
        [
            ("negative_notional", lambda p: p["notional_amount"] < 0),
            ("zero_notional", lambda p: p["notional_amount"] == 0),
            ("invalid_currency", lambda p: p["notional_currency"] not in ref.CURRENCY_CODES),
            ("unknown_counterparty", lambda p: p["counterparty_id"] not in ref.COUNTERPARTY_IDS),
            ("unknown_book", lambda p: p["book_id"] not in ref.BOOK_IDS),
            ("unsupported_product", lambda p: p["product_type"] not in ref.PRODUCT_TYPES),
            ("invalid_direction", lambda p: p["buy_sell"] not in {"BUY", "SELL"}),
            ("type_mismatch_notional", lambda p: isinstance(p["notional_amount"], str)),
            (
                "settlement_before_trade_date",
                lambda p: (
                    date.fromisoformat(p["settlement_date"]) < date.fromisoformat(p["trade_date"])
                ),
            ),
            (
                "maturity_before_trade_date",
                lambda p: (
                    date.fromisoformat(p["maturity_date"]) < date.fromisoformat(p["trade_date"])
                ),
            ),
        ],
    )
    def test_fault_actually_corrupts_the_payload(
        self, faulty_generator: TradeGenerator, fault: str, assertion: object
    ) -> None:
        """Each fault must genuinely produce the defect it claims to."""
        matched = [
            e
            for e in faulty_generator.generate(4_000, as_of=date(2026, 6, 1))
            if e.injected_fault == fault
        ]
        assert matched, f"fault {fault} was never produced"
        for event in matched:
            assert isinstance(event.payload, dict)
            assert assertion(event.payload), f"{fault} did not corrupt the payload as claimed"  # type: ignore[operator]


class TestNearMaturity:
    def test_near_maturity_trades_are_produced(self, sim_settings: SimulatorSettings) -> None:
        """Business rule 4 needs trades that will mature soon, or it is never exercised."""
        settings = sim_settings.model_copy(update={"near_maturity_rate": 1.0})
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        as_of = date(2026, 6, 1)

        events = list(generator.generate(50, as_of=as_of))
        for event in events:
            assert isinstance(event.payload, dict)
            maturity = date.fromisoformat(event.payload["maturity_date"])
            # 3 business days can span a weekend, so allow up to 5 calendar days.
            assert maturity - as_of <= timedelta(days=5)


class TestManifest:
    def test_manifest_counts_reconcile_with_events(self, faulty_generator: TradeGenerator) -> None:
        events = list(faulty_generator.generate(500, as_of=date(2026, 6, 1)))
        manifest = faulty_generator.build_manifest(events, batch_ref="test", file_name="f.ndjson")

        assert manifest.total_events == len(events)
        assert (
            manifest.expected_accepted + manifest.expected_rejected + manifest.expected_superseded
            == len(events)
        )
        assert sum(manifest.fault_counts.values()) == sum(1 for e in events if e.injected_fault)

    def test_manifest_round_trips_through_json(self, clean_generator: TradeGenerator) -> None:
        from trade_sim.schema import BatchManifest

        events = list(clean_generator.generate(20, as_of=date(2026, 6, 1)))
        manifest = clean_generator.build_manifest(events, batch_ref="test", file_name="f.ndjson")
        restored = BatchManifest.model_validate_json(manifest.model_dump_json())

        assert restored.total_events == manifest.total_events
        assert restored.verdicts == manifest.verdicts


class TestManifestAgreesWithArbitration:
    """The manifest is ground truth for `trade-sim reconcile`, so a wrong expectation
    reports a correct pipeline as broken. These replay the ordering half of adjudication --
    business time within a trade, and the race within a version -- against a batch at the
    default rates, and catch offline what would otherwise surface as a mass reconciliation
    failure after a full warehouse round trip.
    """

    @staticmethod
    def _batch(sim_settings: SimulatorSettings, count: int, book: TradeBook | None = None):
        settings = sim_settings.model_copy(
            update={
                "error_rate": 0.08,
                "amend_rate": 0.20,
                "cancel_rate": 0.05,
                "replace_rate": 0.04,
                "stale_version_rate": 0.04,
            }
        )
        generator = TradeGenerator(
            settings, book=book or TradeBook(path=settings.state_dir / "book.json")
        )
        events = list(generator.generate(count, as_of=date(2026, 6, 15)))
        generator.build_manifest(events, batch_ref="b", file_name="b.ndjson")
        return generator, events

    @staticmethod
    def _in_time_order(events: list[GeneratedEvent]) -> list[GeneratedEvent]:
        return sorted(events, key=lambda e: datetime.fromisoformat(e.payload["event_timestamp"]))

    @staticmethod
    def _replay_the_model(
        events: list[GeneratedEvent], stored_versions: dict[str, int]
    ) -> dict[int, tuple[str, list[str]]]:
        """The verdict int_trade_event_adjudicated will reach for each event, by position.

        Reproduces the model's two window functions instead of restating one invariant at a
        time. `intra_run_rank` partitions by (trade_id, trade_version, is_field_valid) and
        orders on business time then arrival, so the latest arrival of a version takes rank
        1 and a malformed resend ranks in a partition of its own. `effective_prior_version`
        is the highest version among rank-1, field-valid events that precede the arrival in
        business time, floored by the version the warehouse already stored.

        Position in the batch stands in for event_sk, which the model derives from arrival
        order within the file.
        """
        partitions: dict[tuple[str, int, bool], list[int]] = {}
        for position, event in enumerate(events):
            if event.trade_id and event.trade_version:
                key = (event.trade_id, event.trade_version, event.field_valid)
                partitions.setdefault(key, []).append(position)

        rank: dict[int, int] = {}
        for members in partitions.values():
            latest_first = sorted(
                members, key=lambda p: (_business_time(events[p]), p), reverse=True
            )
            for offset, position in enumerate(latest_first, start=1):
                rank[position] = offset

        by_trade: dict[str, list[int]] = {}
        for position in rank:
            by_trade.setdefault(str(events[position].trade_id), []).append(position)

        predicted: dict[int, tuple[str, list[str]]] = {}
        for trade_id, positions in by_trade.items():
            mark = stored_versions.get(trade_id, 0)
            for position in sorted(positions, key=lambda p: (_business_time(events[p]), p)):
                event = events[position]
                version = event.trade_version or 0

                if not event.field_valid:
                    # The STATE phase is evaluated for every row, so a field-invalid
                    # arrival is still stale when a higher version precedes it. It moves no
                    # mark and enters no race: the mark admits only field-valid rows.
                    predicted[position] = ("REJECTED", ["RJ001"] if version < mark else [])
                elif rank[position] > 1:
                    predicted[position] = ("SUPERSEDED", ["RJ009"])
                elif version < mark:
                    predicted[position] = ("REJECTED", ["RJ001"])
                else:
                    predicted[position] = ("ACCEPTED", [])
                    mark = version
        return predicted

    def test_no_version_is_expected_to_be_accepted_twice(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """Two accepted arrivals for one version is the invariant the warehouse asserts."""
        _, events = self._batch(sim_settings, 1500)

        accepted: dict[tuple[str, int], int] = {}
        for event in events:
            if event.expected_verdict == "ACCEPTED" and event.trade_id and event.trade_version:
                key = (event.trade_id, event.trade_version)
                accepted[key] = accepted.get(key, 0) + 1

        assert not [key for key, n in accepted.items() if n > 1]

    def test_a_version_is_expected_stale_exactly_when_the_mark_has_passed_it(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """RJ001 is a function of business-time ordering, so replay it that way."""
        _, events = self._batch(sim_settings, 1500)

        by_trade: dict[str, list[GeneratedEvent]] = {}
        for event in events:
            if not (event.trade_id and event.trade_version):
                continue
            # A field-phase rejection never reaches version arbitration, so it neither
            # moves the high-water mark nor is judged against it -- unless it declares
            # RJ001, which lives in the STATE phase and is evaluated for every row
            # whatever the field phase decided. Admitting those cannot disturb the mark:
            # an event declaring RJ001 is by definition below it already.
            if event.expected_verdict != "REJECTED" or "RJ001" in event.expected_rule_codes:
                by_trade.setdefault(event.trade_id, []).append(event)

        for trade_id, group in by_trade.items():
            mark = 0
            for event in self._in_time_order(group):
                if event.expected_verdict == "SUPERSEDED":
                    # Two reasons to pass over a superseded arrival. Its own verdict is not
                    # a staleness claim: RJ009 outranks RJ001 deliberately, and the loser
                    # test below covers it. And it does not move the mark either, because
                    # the model's window admits only rank-1 events -- an arrival that lost
                    # its race is not history, so it cannot make a later version stale.
                    continue
                declared_stale = "RJ001" in event.expected_rule_codes
                assert declared_stale == ((event.trade_version or 0) < mark), (
                    f"{trade_id} v{event.trade_version} against mark {mark}: "
                    f"{event.expected_verdict} {event.expected_rule_codes}"
                )
                mark = max(mark, event.trade_version or 0)

    def test_every_expectation_matches_a_replay_of_the_model(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """The whole STATE phase at once, rather than one invariant at a time.

        The tests around this one each replay a single rule, which leaves the corners where
        two rules meet unexamined -- and that is where the manifest was wrong. A resend the
        field phase had already disqualified was still counted into its version's race, so
        the good arrival it never competed with was expected to be superseded; and a resend
        stamped before the amendment that overtook it was expected stale, because the book
        advances in emission order while the model reads the clock. Both survived every
        rule-at-a-time test here and surfaced as a mass reconciliation failure after a full
        warehouse round trip. This replays rank, mark and verdict together instead.

        Against a warm book as well as a cold one: the second batch's mark is floored by the
        versions the first one left behind, which is the path a re-run takes.
        """
        book = TradeBook(path=sim_settings.state_dir / "replayed.json")
        self._batch(sim_settings, 800, book=book)
        stored_versions = {tid: entry.trade_version for tid, entry in book.trades.items()}
        assert stored_versions, (
            "the first batch must leave versions behind, or the floor is untested"
        )

        _, events = self._batch(sim_settings, 1500, book=book)

        for position, (verdict, codes) in self._replay_the_model(events, stored_versions).items():
            event = events[position]
            assert event.expected_verdict == verdict, (
                f"{event.trade_id} v{event.trade_version} "
                f"(fault={event.injected_fault}, field_valid={event.field_valid}): "
                f"the model reaches {verdict}, the manifest promises {event.expected_verdict}"
            )
            promised = set(event.expected_rule_codes) & STATE_PHASE_RULE_CODES
            assert promised <= set(codes), (
                f"{event.trade_id} v{event.trade_version}: the manifest promises "
                f"{sorted(promised)}, the model fires {codes}"
            )

    def test_a_warn_only_fault_does_not_rescue_a_late_arrival(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """WARN severity describes the fault, not the event carrying it.

        notional_over_limit is the only fault the rules merely flag, so it is the only one
        whose event can still be expected ACCEPTED. A stale resend that happens to draw it
        is nonetheless stale, and RJ001 rejects it on the event's own merits.

        The rates here are far from production on purpose. At the default 8% error rate and
        4% stale rate, one fault in seventeen, this combination turns up about once in five
        thousand events -- rare enough that a seeded 1,500-event batch misses it and common
        enough to fail a nightly run of three thousand. That is exactly what happened: the
        invariant above was already asserted and simply never dosed hard enough to fire.
        """
        settings = sim_settings.model_copy(
            update={
                "error_rate": 0.30,
                "amend_rate": 0.40,
                "stale_version_rate": 0.30,
                "cancel_rate": 0.0,
                "replace_rate": 0.0,
            }
        )
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(4000, as_of=date(2026, 6, 15)))
        generator.build_manifest(events, batch_ref="warn", file_name="warn.ndjson")

        # Identifiable events only: a fault that destroys the trade_id leaves nothing for
        # the version rules to judge, and its payload is not even a dict.
        identified = [event for event in events if event.trade_id and event.trade_version]

        mark: dict[str, int] = {}
        late_and_flagged = 0
        for event in self._in_time_order(identified):
            assert event.trade_id is not None and event.trade_version is not None

            if event.trade_version < mark.get(event.trade_id, 0):
                # SUPERSEDED is permitted here and is not a contradiction: a late arrival
                # that also loses a same-version race is reported for losing it, RJ009
                # outranking RJ001. The one verdict a late arrival must never carry is
                # ACCEPTED, whatever severity the fault it happens to carry has.
                assert event.expected_verdict != "ACCEPTED", (
                    f"{event.trade_id} v{event.trade_version} arrived after "
                    f"v{mark[event.trade_id]} yet expects ACCEPTED "
                    f"(fault={event.injected_fault})"
                )
                if event.expected_verdict == "REJECTED":
                    assert "RJ001" in event.expected_rule_codes
                    if event.injected_fault in WARN_ONLY_FAULTS:
                        late_and_flagged += 1
            elif event.expected_verdict == "ACCEPTED":
                mark[event.trade_id] = event.trade_version

        assert late_and_flagged, (
            "these rates must produce late arrivals carrying a warn-only fault, "
            "or this test proves nothing"
        )

    def test_only_the_latest_arrival_of_a_version_keeps_its_own_verdict(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """Everything else sharing the key must be expected SUPERSEDED under RJ009."""
        _, events = self._batch(sim_settings, 1500)

        groups: dict[tuple[str, int], list[GeneratedEvent]] = {}
        for event in events:
            # Field-valid arrivals only, which is how the model partitions the ranking: a
            # malformed resend races nothing and supersedes nothing.
            if event.trade_id and event.trade_version and event.field_valid:
                groups.setdefault((event.trade_id, event.trade_version), []).append(event)

        raced = [group for group in groups.values() if len(group) > 1]
        assert raced, "the default replace_rate must produce some same-version races"
        for group in raced:
            for loser in self._in_time_order(group)[:-1]:
                assert loser.expected_verdict == "SUPERSEDED"
                assert loser.expected_rule_codes == ["RJ009"]

    def test_nothing_is_emitted_for_a_trade_after_it_is_cancelled(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """No injected fault targets RJ010, so the simulator must never provoke it.

        Cancellation is terminal, and an amendment after one is rejected as RJ010. Since
        the manifest never expects that code, emitting such an event is a false mismatch.
        The error rate is high here to exercise the accepted-but-flagged cancellation,
        which advances the book on the corrupt path rather than the clean one.
        """
        settings = sim_settings.model_copy(
            update={"amend_rate": 0.9, "cancel_rate": 0.4, "error_rate": 0.5}
        )
        generator = TradeGenerator(settings, book=TradeBook(path=settings.state_dir / "book.json"))
        events = list(generator.generate(600, as_of=date(2026, 6, 15)))

        cancelled: set[str] = set()
        for event in events:
            if event.trade_id is None:
                continue
            assert event.trade_id not in cancelled, f"{event.trade_id} amended after cancellation"
            # Only an accepted cancellation is terminal. A rejected one never reaches the
            # golden record, so the trade is still live and amending it is correct.
            if event.action == "CANCEL" and event.expected_verdict == "ACCEPTED":
                cancelled.add(event.trade_id)

    def test_consecutive_batches_never_accept_the_same_version_twice(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """The cross-build case, which append-only audit cannot resolve after the fact.

        Repeated `make demo` runs share one trade book, so a resend drawn from an earlier
        batch would leave two accepted events for one version -- legitimate upstream
        behaviour that this platform records rather than arbitrates. See
        docs/known-limitations.md.
        """
        book = TradeBook(path=sim_settings.state_dir / "book.json")
        accepted: set[tuple[str, int]] = set()

        for _ in range(3):
            _, events = self._batch(sim_settings, 800, book=book)
            for event in events:
                if event.expected_verdict == "ACCEPTED" and event.trade_id and event.trade_version:
                    key = (event.trade_id, event.trade_version)
                    assert key not in accepted, f"{key} accepted in two batches"
                    accepted.add(key)


class TestTradeBookPersistence:
    def test_book_survives_a_save_load_cycle(self, sim_settings: SimulatorSettings) -> None:
        path = sim_settings.state_dir / "book.json"
        generator = TradeGenerator(sim_settings, book=TradeBook(path=path))
        list(generator.generate(25, as_of=date(2026, 6, 1)))
        generator.book.save()

        reloaded = TradeBook.load(path)
        assert len(reloaded.trades) == len(generator.book.trades)
        assert reloaded.next_sequence == generator.book.next_sequence

    def test_trade_ids_never_repeat_across_runs(self, sim_settings: SimulatorSettings) -> None:
        """Sequence continuity is what stops a second run colliding with the first."""
        path = sim_settings.state_dir / "book.json"
        first = TradeGenerator(sim_settings, book=TradeBook(path=path))
        ids_a = {e.trade_id for e in first.generate(30, as_of=date(2026, 6, 1))}
        first.book.save()

        second = TradeGenerator(sim_settings, book=TradeBook.load(path))
        ids_b = {e.trade_id for e in second.generate(30, as_of=date(2026, 6, 2))}

        assert not (ids_a & ids_b)


class TestTradeBookLocking:
    """One state directory, one simulator at a time.

    The hourly DAG and a manual run share `data/`, and without a lock both read the same
    book, mint the same identifiers and race to write it back. The warehouse then holds two
    universes claiming the same (trade_id, trade_version).
    """

    def test_the_lock_is_held_while_the_book_is_open(self, sim_settings: SimulatorSettings) -> None:
        """A second holder must not be able to take the lock concurrently.

        flock is associated with the open file description rather than the process, so a
        second open() in this process contends exactly as another process would.
        """
        fcntl = pytest.importorskip("fcntl", reason="POSIX file locking only")
        path = sim_settings.state_dir / "trade_book.json"

        with (
            TradeBook.exclusive(path),
            path.with_suffix(".lock").open("w", encoding="utf-8") as contender,
            pytest.raises(OSError),
        ):
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Released on exit, so the next simulator proceeds immediately.
        with (path.with_suffix(".lock")).open("w", encoding="utf-8") as contender:
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_the_book_is_saved_on_the_way_out(self, sim_settings: SimulatorSettings) -> None:
        path = sim_settings.state_dir / "trade_book.json"

        with TradeBook.exclusive(path) as book:
            generator = TradeGenerator(sim_settings, book=book)
            list(generator.generate(10, as_of=date(2026, 6, 1)))

        assert TradeBook.load(path).next_sequence == book.next_sequence

    def test_identifiers_are_not_reused_after_a_failed_batch(
        self, sim_settings: SimulatorSettings
    ) -> None:
        """A gap is harmless; a reused identifier is the collision the lock exists to stop.

        Files for the failed batch may already be on disk and may already have been loaded,
        so the sequence must not rewind.
        """
        path = sim_settings.state_dir / "trade_book.json"

        # pytest.raises is the outer context deliberately: the error must escape
        # TradeBook.exclusive, so that its save-on-exit has already run when it is caught.
        with pytest.raises(RuntimeError), TradeBook.exclusive(path) as book:
            generator = TradeGenerator(sim_settings, book=book)
            list(generator.generate(10, as_of=date(2026, 6, 1)))
            raise RuntimeError("upload failed")

        assert TradeBook.load(path).next_sequence > 1
