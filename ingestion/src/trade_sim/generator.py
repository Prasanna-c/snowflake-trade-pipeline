"""Trade event generator.

Two things make this more than a random-row producer, and both exist so the pipeline
can be *proved* rather than merely demonstrated:

1. **It is stateful.** A persistent trade book records the current version and
   lifecycle state of every trade ever emitted. Amendments therefore carry genuinely
   correct next versions, cancellations are terminal, and the stale-version and
   same-version faults reference real prior state. Without this, business rules 1 and
   2 could never be exercised, because every trade would be version 1 of something
   the warehouse has never seen.

2. **Every event knows its expected verdict.** Each fault is injected with the rule
   codes it should trigger, and the batch manifest records them. `trade-sim reconcile`
   then compares expectation against what the warehouse actually decided.

Determinism: a fixed seed produces byte-identical output given the same trade book, so
a reviewer can reproduce a specific scenario exactly.
"""

from __future__ import annotations

import json
import logging
import random
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import IO, Any

try:  # POSIX only. The documented environments are WSL, Linux and macOS.
    import fcntl
except ImportError:  # pragma: no cover - native Windows
    fcntl = None  # type: ignore[assignment]

from faker import Faker

from trade_sim import reference as ref
from trade_sim.config import SimulatorSettings
from trade_sim.schema import (
    BatchManifest,
    CorruptTradeEvent,
    Direction,
    ExpectedVerdict,
    TradeAction,
    TradeEvent,
)

log = logging.getLogger(__name__)


def _lock_exclusive(handle: IO[str]) -> None:
    """Take an exclusive advisory lock, waiting if another simulator holds it.

    The non-blocking attempt comes first only so that waiting can be reported: a batch
    that appears to hang for a minute is otherwise indistinguishable from a broken one.
    """
    if fcntl is None:
        log.warning(
            "no file locking on this platform: do not run two simulators against one "
            "state directory, or they will mint overlapping trade identifiers"
        )
        return
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.info("another simulator holds the trade book; waiting for it to finish")
        fcntl.flock(handle, fcntl.LOCK_EX)


# ---------------------------------------------------------------------------
# Trade book: the generator's memory
# ---------------------------------------------------------------------------
@dataclass
class TradeBookEntry:
    trade_id: str
    trade_version: int
    product_type: str
    counterparty_id: str
    book_id: str
    notional_currency: str
    settlement_currency: str
    buy_sell: str
    notional_amount: str
    trade_date: str
    maturity_date: str | None
    uti: str
    is_cancelled: bool = False
    #: Business time of the last event emitted for this trade, ISO-8601. Version
    #: arbitration orders by business time, so the next event for this trade has to be
    #: stamped after this one or the pipeline will see the amendment before the booking.
    last_event_ts: str | None = None


@dataclass
class TradeBook:
    """Persistent record of every trade the generator has emitted.

    Stored as JSON next to the generated data. Delete it to start a fresh universe;
    keep it to build up a realistic multi-day history where amendments and expiries
    accumulate.
    """

    path: Path
    trades: dict[str, TradeBookEntry] = field(default_factory=dict)
    next_sequence: int = 1

    @classmethod
    @contextmanager
    def exclusive(cls, path: Path) -> Iterator[TradeBook]:
        """Load the book under an exclusive lock and save it on the way out.

        Two simulators sharing a state directory -- the hourly DAG and a manual run, for
        instance -- otherwise both read the same book, mint the same trade identifiers and
        race to write it back. The warehouse then holds two universes claiming the same
        (trade_id, trade_version), which the rules are right to reject and which no
        manifest can predict. Holding a lock across the read-modify-write means the second
        simulator continues the universe instead of forking it.

        The lock lives on a sibling file rather than on the book: `save()` writes to a
        temporary file and renames it into place, so a lock taken on the book's own inode
        would be orphaned the moment the book was saved.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_suffix(".lock").open("w", encoding="utf-8") as handle:
            _lock_exclusive(handle)
            book = cls.load(path)
            try:
                yield book
            finally:
                # Saved even when the batch fails. A gap in the identifier sequence is
                # harmless; reusing an identifier already written to a file that may
                # since have been loaded recreates the collision this lock prevents.
                book.save()

    @classmethod
    def load(cls, path: Path) -> TradeBook:
        if not path.is_file():
            return cls(path=path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            path=path,
            trades={k: TradeBookEntry(**v) for k, v in raw.get("trades", {}).items()},
            next_sequence=raw.get("next_sequence", 1),
        )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "next_sequence": self.next_sequence,
            "trades": {k: asdict(v) for k, v in self.trades.items()},
        }
        # Write-then-rename so an interrupted run cannot leave a truncated book that
        # would desynchronise every subsequent version number.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def allocate_trade_id(self) -> str:
        trade_id = f"TRD-{self.next_sequence:09d}"
        self.next_sequence += 1
        return trade_id

    def amendable(self) -> list[TradeBookEntry]:
        return [t for t in self.trades.values() if not t.is_cancelled]

    def record(self, entry: TradeBookEntry) -> None:
        self.trades[entry.trade_id] = entry


# ---------------------------------------------------------------------------
# Fault catalogue
# ---------------------------------------------------------------------------
#: fault name -> rule codes the pipeline is expected to return.
#:
#: Every fault here trips its rules from the payload alone, so the expectation holds whenever
#: the batch is built. Faults targeting RJ003 (maturity in the past) and RJ014 (trade date in
#: the future) are deliberately absent: both rules are relative to dbt's `business_date`, and
#: RJ003 additionally exempts CANCEL, so the verdict for such an event depends on when the
#: warehouse is built and on which action was drawn. A manifest cannot state that
#: reproducibly -- rebuild yesterday's batch today and the rule correctly reaches the
#: opposite conclusion. Those two rules are covered by the dbt unit tests instead, where the
#: business date and the input row are both fixed by hand.
FAULT_EXPECTATIONS: dict[str, list[str]] = {
    "unparseable_json": ["RJ008"],
    "missing_trade_id": ["RJ008"],
    "missing_mandatory_field": ["RJ004"],
    "unknown_counterparty": ["RJ005"],
    "inactive_counterparty": ["RJ016"],
    "invalid_currency": ["RJ006"],
    "negative_notional": ["RJ007"],
    "zero_notional": ["RJ007"],
    # Only RJ002. A maturity before the trade date is usually also a maturity in the past,
    # but not on a CANCEL, which RJ003 excuses -- and the reconciler asserts the expected
    # codes are a subset of those that fired, so naming the certain one is enough.
    "maturity_before_trade_date": ["RJ002"],
    "settlement_before_trade_date": ["RJ012"],
    "unknown_book": ["RJ011"],
    "unsupported_product": ["RJ013"],
    "invalid_direction": ["RJ015"],
    "non_deliverable_physical": ["RJ017"],
    "notional_over_limit": ["RJ018"],  # WARN only -- still accepted
    "type_mismatch_notional": ["RJ008"],
}

#: Faults whose codes are WARN severity, so the event is still ACCEPTED.
WARN_ONLY_FAULTS: frozenset[str] = frozenset({"notional_over_limit"})

#: Faults that destroy the trade identifier, so the event cannot be reconciled by id.
IDENTITY_DESTROYING_FAULTS: frozenset[str] = frozenset({"unparseable_json", "missing_trade_id"})


#: Earliest orderable business time, used when an event has none to offer. Timezone-aware
#: because every real payload timestamp is, and Python refuses to compare the two kinds.
_BEFORE_ANY_EVENT = datetime.min.replace(tzinfo=UTC)


def _business_time(event: GeneratedEvent) -> datetime:
    """The event's own business timestamp, for ordering a same-version race.

    A payload is a string, not a mapping, when the injected fault is malformed JSON, and a
    mapping can be missing the timestamp outright when the fault removed it. Neither kind
    reaches this function -- both are refused in the field phase, long before the version
    rules are consulted -- but a sort key has to be total for every event it is handed, so
    an event with no usable timestamp sorts earliest and loses the race rather than raising
    part-way through a batch.
    """
    if not isinstance(event.payload, dict):
        return _BEFORE_ANY_EVENT
    stamp = event.payload.get("event_timestamp")
    if not isinstance(stamp, str):
        return _BEFORE_ANY_EVENT
    try:
        return datetime.fromisoformat(stamp)
    except ValueError:
        return _BEFORE_ANY_EVENT


def _settle_version_arbitration(
    events: list[GeneratedEvent], stored_versions: dict[str, int]
) -> None:
    """Re-express the batch's expectations the way the STATE phase will reach them.

    Two of the model's decisions are unknowable as an event is emitted, so the batch is
    revisited in place once it is complete.

    Rule 2, the same-version race. Several events can legitimately share a (trade_id,
    trade_version): that is the resend the rule exists for. The model ranks them on
    business time and records every arrival but the latest as SUPERSEDED under RJ009,
    which outranks a rejection deliberately -- losing a race is not a data quality
    failure. The ranking partitions by `is_field_valid`, so a malformed resend is in a
    partition of its own: it neither supersedes a good arrival nor is superseded by one,
    and answers for its own bad field alone.

    Rule 1, staleness. RJ001 compares the arrival against `effective_prior_version`: the
    highest version that precedes it *in business time* among the race winners, floored by
    the version the warehouse already stored. The book cannot answer that, because it
    advances in emission order -- a resend stamped before the amendment that overtook it
    is late to the book and in perfect order on the clock the model reads. Where the two
    disagree the clock wins, because that is what the warehouse will do.

    Ordering mirrors `intra_run_rank`: business time first, and on a tie the later
    position in the file, which becomes the higher event_sk. The sort is stable, so
    position falls out of it without a second key.
    """
    contenders: dict[tuple[str, int], list[GeneratedEvent]] = defaultdict(list)
    for event in events:
        if event.trade_id is not None and event.trade_version is not None and event.field_valid:
            contenders[(event.trade_id, event.trade_version)].append(event)

    # Only the winner of each race moves the mark, matching the rank-1 restriction on the
    # model's window. A superseded event is not history, so a version that lost a race
    # cannot make a later arrival stale.
    winners: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
    for (trade_id, trade_version), competing in contenders.items():
        competing.sort(key=_business_time)
        for loser in competing[:-1]:
            loser.expected_verdict = "SUPERSEDED"
            loser.expected_rule_codes = ["RJ009"]
        winners[trade_id].append((_business_time(competing[-1]), trade_version))

    for event in events:
        if event.trade_id is None or "RJ001" not in event.expected_rule_codes:
            continue
        arrived_at = _business_time(event)
        mark = max(
            [stored_versions.get(event.trade_id, 0)]
            + [
                trade_version
                for when, trade_version in winners.get(event.trade_id, [])
                if when < arrived_at
            ]
        )
        if (event.trade_version or 0) < mark:
            continue

        # Nothing precedes it, so the version rules never reach it and the FIELD phase
        # decides alone: acquittal for an event whose fields are sound, and the fault's
        # own codes for one whose fields are not.
        event.expected_rule_codes = [code for code in event.expected_rule_codes if code != "RJ001"]
        if event.field_valid and not event.expected_rule_codes:
            event.expected_verdict = "ACCEPTED"


def _codes_for_reference_token(token: str, unknown_code: str) -> list[str]:
    """The codes a deliberately bad reference value will actually trip.

    A blank never reaches the reference check. The typed layer nullifies it, so the
    pipeline reports a missing mandatory field rather than an unknown value -- and it is
    right to: "you left book_id empty" and "you sent a book we have never heard of" go
    to different people. The manifest has to say the same thing the pipeline will say,
    or reconciliation reports a defect in the pipeline that is really a defect in the
    expectation.
    """
    return ["RJ004"] if not token.strip() else [unknown_code]


@dataclass
class GeneratedEvent:
    """One event plus the metadata reconciliation needs."""

    payload: dict[str, Any] | str
    trade_id: str | None
    trade_version: int | None
    action: str | None
    injected_fault: str | None
    expected_verdict: str
    expected_rule_codes: list[str]

    #: Whether the FIELD phase will let this event through to version arbitration, which
    #: is `is_field_valid` in int_trade_event_adjudicated. It decides which events compete
    #: in a same-version race and which move the high-water mark, so the manifest cannot
    #: infer it from the expected codes: a late arrival carrying a bad field declares
    #: RJ001 from the STATE phase and would otherwise look field-valid. Not carried into
    #: the manifest -- it is how an expectation is reached, not part of the expectation.
    field_valid: bool = True


class TradeGenerator:
    """Produces trade events against a persistent trade book."""

    def __init__(self, settings: SimulatorSettings, book: TradeBook | None = None) -> None:
        self.settings = settings
        self.rng = random.Random(settings.seed)
        self.faker = Faker()
        Faker.seed(settings.seed)
        self.book = book or TradeBook.load(settings.state_dir / "trade_book.json")
        self._versions_at_batch_start: dict[str, int] = {}

        # Pre-compute the weighted product distribution once.
        self._product_pool: tuple[ref.Product, ...] = tuple(
            p for p in ref.PRODUCTS for _ in range(p.weight)
        )
        self._trader_ids: tuple[str, ...] = tuple(f"TDR{n:04d}" for n in range(1, 41))

    # -- primitives ---------------------------------------------------------
    def _log_uniform(self, low: float, high: float) -> Decimal:
        """Sample a notional log-uniformly.

        Real trade sizes are roughly log-normal: many small tickets, few very large
        ones. A uniform sample would make every trade improbably large and would make
        the desk-limit rule fire on half the book.
        """
        import math

        value = math.exp(self.rng.uniform(math.log(low), math.log(high)))
        # Round to a plausible ticket size rather than a random number of cents.
        magnitude = 10 ** max(0, int(math.log10(value)) - 3)
        return Decimal(int(round(value / magnitude) * magnitude)).quantize(Decimal("0.0001"))

    def _business_days_from(self, start: date, days: int) -> date:
        """Advance `days` business days, skipping weekends.

        Deliberately ignores holiday calendars. Modelling a real holiday calendar is
        the correct production behaviour, but it belongs in reference data loaded from
        a vendor feed, not hard-coded in a generator.
        """
        current = start
        remaining = days
        while remaining > 0:
            current += timedelta(days=1)
            if current.weekday() < 5:
                remaining -= 1
        return current

    def _new_trade_fields(self, as_of: date) -> dict[str, Any]:
        product = self.rng.choice(self._product_pool)
        book = self.rng.choice(ref.BOOKS)

        # Physically-settled products must use a deliverable currency, so the clean
        # path never accidentally trips RJ017 -- that fault has to be injected.
        if product.product_type in ref.PHYSICALLY_SETTLED_PRODUCTS:
            currency = self.rng.choice(ref.DELIVERABLE_CURRENCY_CODES)
        else:
            currency = self.rng.choice(ref.CURRENCY_CODES)

        trade_date = as_of
        settlement_date = self._business_days_from(trade_date, 2)

        if self.rng.random() < self.settings.near_maturity_rate:
            # Deliberately near-dated so that a later run's expiry sweep has
            # something to transition. This is how business rule 4 gets exercised
            # without waiting years.
            tenor_days = self.rng.randint(1, 3)
        else:
            tenor_days = self.rng.randint(product.tenor_days_min, product.tenor_days_max)

        maturity_date = self._business_days_from(trade_date, max(tenor_days, 1))

        notional = self._log_uniform(product.notional_min, product.notional_max)

        return {
            "product_type": product.product_type,
            "asset_class": product.asset_class,
            "buy_sell": self.rng.choice([Direction.BUY, Direction.SELL]).value,
            "notional_amount": notional,
            "notional_currency": currency,
            "settlement_currency": currency,
            "quantity": notional if product.asset_class in {"EQUITY", "CREDIT"} else None,
            "price": Decimal(str(round(self.rng.uniform(0.5, 250.0), 6))),
            "trade_date": trade_date,
            "settlement_date": settlement_date,
            "maturity_date": maturity_date,
            "counterparty_id": self.rng.choice(ref.ACTIVE_COUNTERPARTY_IDS),
            "book_id": book.book_id,
            "trader_id": self.rng.choice(self._trader_ids),
            "execution_venue": self.rng.choice(ref.EXECUTION_VENUES),
            "clearing_house": self.rng.choice(ref.CLEARING_HOUSES),
            "legal_entity": book.legal_entity,
            "source_system": self.rng.choice(ref.SOURCE_SYSTEMS),
        }

    def _make_uti(self, trade_id: str) -> str:
        return f"DB{trade_id.replace('-', '')}{self.rng.randint(1000, 9999)}"

    def _event_timestamp(self, as_of: date, after: str | None = None) -> datetime:
        """A plausible intraday timestamp, never earlier than the event it follows.

        `after` is not a nicety. Version arbitration orders a trade's events by business
        time, deliberately, because network reordering is real and the upstream clock is
        the authority on what happened first. An independent draw per event yields
        amendments stamped *before* their own booking -- a sequence that cannot happen --
        and the rules are then right to reject the earlier version as stale, while a
        manifest reasoning in emission order expects it to be accepted.
        """
        stamp = datetime(
            as_of.year,
            as_of.month,
            as_of.day,
            hour=self.rng.randint(6, 21),
            minute=self.rng.randint(0, 59),
            second=self.rng.randint(0, 59),
            microsecond=self.rng.randint(0, 999) * 1000,
            tzinfo=UTC,
        )
        if after is None:
            return stamp
        previous = datetime.fromisoformat(after)
        if stamp > previous:
            return stamp
        # The draw landed before the previous event. Step forward from that event
        # instead, by a gap short enough to stay plausible for a same-day amendment.
        return previous + timedelta(
            seconds=self.rng.randint(1, 900),
            microseconds=self.rng.randint(0, 999) * 1000,
        )

    # -- event construction -------------------------------------------------
    def _build_new(self, as_of: date) -> TradeEvent:
        trade_id = self.book.allocate_trade_id()
        fields = self._new_trade_fields(as_of)
        return TradeEvent(
            trade_id=trade_id,
            trade_version=1,
            action=TradeAction.NEW,
            uti=self._make_uti(trade_id),
            event_timestamp=self._event_timestamp(as_of),
            **fields,
        )

    def _build_amendment(self, entry: TradeBookEntry, as_of: date, *, cancel: bool) -> TradeEvent:
        """Amend or cancel an existing trade, carrying the correct next version."""
        fields = self._new_trade_fields(as_of)

        # An amendment must keep the trade's identity and economics broadly stable --
        # what changes is typically the notional, the maturity or the settlement
        # details. Regenerating everything would make version history meaningless.
        fields.update(
            {
                "product_type": entry.product_type,
                "counterparty_id": entry.counterparty_id,
                "book_id": entry.book_id,
                "notional_currency": entry.notional_currency,
                "settlement_currency": entry.settlement_currency,
                "buy_sell": entry.buy_sell,
                "trade_date": date.fromisoformat(entry.trade_date),
                "maturity_date": (
                    date.fromisoformat(entry.maturity_date) if entry.maturity_date else None
                ),
            }
        )
        # Settlement must remain consistent with the (unchanged) trade date.
        fields["settlement_date"] = self._business_days_from(fields["trade_date"], 2)

        if not cancel:
            # Nudge the notional -- the most common real amendment.
            adjustment = Decimal(str(round(self.rng.uniform(0.85, 1.15), 4)))
            fields["notional_amount"] = (Decimal(entry.notional_amount) * adjustment).quantize(
                Decimal("0.0001")
            )

        return TradeEvent(
            trade_id=entry.trade_id,
            trade_version=entry.trade_version + 1,
            action=TradeAction.CANCEL if cancel else TradeAction.AMEND,
            uti=entry.uti,
            event_timestamp=self._event_timestamp(as_of, entry.last_event_ts),
            **fields,
        )

    def _build_replacement(self, entry: TradeBookEntry, as_of: date) -> TradeEvent:
        """Re-emit the CURRENT version with changed economics -- business rule 2.

        This is the "corrected resend" pattern: an upstream system realises it sent
        bad economics, and resends the same version rather than incrementing. The
        pipeline must overwrite, not reject.
        """
        event = self._build_amendment(entry, as_of, cancel=False)
        return event.model_copy(update={"trade_version": entry.trade_version})

    def _build_stale(self, entry: TradeBookEntry, as_of: date) -> TradeEvent:
        """Re-emit an OLDER version -- business rule 1. Must be rejected."""
        stale_version = max(1, entry.trade_version - self.rng.randint(1, 2))
        event = self._build_amendment(entry, as_of, cancel=False)
        return event.model_copy(update={"trade_version": stale_version})

    # -- fault injection ----------------------------------------------------
    def _corrupt(self, event: TradeEvent) -> CorruptTradeEvent:
        """Apply exactly one fault to an otherwise valid event.

        One fault per event, not several, because the manifest has to state
        unambiguously which rule codes are expected. Multi-fault events are covered by
        the dbt unit tests instead, where the input is written by hand.
        """
        payload = json.loads(event.model_dump_json())
        fault = self.rng.choice(list(FAULT_EXPECTATIONS.keys()))

        # The catalogue's codes are the default. Faults that choose from a token list
        # override them, because which rule fires depends on the token drawn.
        expected_codes = FAULT_EXPECTATIONS[fault]

        match fault:
            case "unparseable_json":
                # Drop the first colon: the object still opens and closes, and every
                # quote is still paired, but it is not valid JSON. ON_ERROR=CONTINUE
                # records it in RAW.COPY_ERROR and the load continues.
                #
                # It is tempting to truncate mid-object instead, which is what a genuinely
                # interrupted producer would emit. Do not: TYPE=JSON does not treat the
                # newline as a record separator, so an unbalanced object consumes the
                # following line's opening brace and every remaining record in the file
                # becomes unreadable. ON_ERROR=CONTINUE cannot help, because the damage is
                # to the framing rather than to one record. See docs/known-limitations.md.
                text = json.dumps(payload)
                return CorruptTradeEvent(
                    payload=text.replace('":', '" ', 1),
                    injected_fault=fault,
                    expected_rule_codes=FAULT_EXPECTATIONS[fault],
                    trade_id=None,
                )
            case "missing_trade_id":
                payload.pop("trade_id", None)
            case "missing_mandatory_field":
                victim = self.rng.choice(
                    ["counterparty_id", "notional_currency", "trade_date", "book_id", "buy_sell"]
                )
                payload[victim] = None
            case "unknown_counterparty":
                payload["counterparty_id"] = f"CP{self.rng.randint(9000, 9999)}"
            case "inactive_counterparty":
                payload["counterparty_id"] = self.rng.choice(ref.INACTIVE_COUNTERPARTY_IDS)
            case "invalid_currency":
                token = self.rng.choice(ref.INVALID_CURRENCY_TOKENS)
                payload["notional_currency"] = token
                expected_codes = _codes_for_reference_token(token, "RJ006")
            case "negative_notional":
                payload["notional_amount"] = -abs(payload["notional_amount"])
            case "zero_notional":
                payload["notional_amount"] = 0
            case "maturity_before_trade_date":
                trade_date = date.fromisoformat(payload["trade_date"])
                payload["maturity_date"] = (
                    trade_date - timedelta(days=self.rng.randint(1, 30))
                ).isoformat()
            case "settlement_before_trade_date":
                trade_date = date.fromisoformat(payload["trade_date"])
                payload["settlement_date"] = (
                    trade_date - timedelta(days=self.rng.randint(1, 5))
                ).isoformat()
            case "unknown_book":
                token = self.rng.choice(ref.INVALID_BOOK_TOKENS)
                payload["book_id"] = token
                expected_codes = _codes_for_reference_token(token, "RJ011")
            case "unsupported_product":
                payload["product_type"] = self.rng.choice(ref.UNSUPPORTED_PRODUCT_TOKENS)
            case "invalid_direction":
                token = self.rng.choice(ref.INVALID_DIRECTION_TOKENS)
                payload["buy_sell"] = token
                expected_codes = _codes_for_reference_token(token, "RJ015")
            case "non_deliverable_physical":
                payload["product_type"] = "FX_FORWARD"
                payload["settlement_currency"] = self.rng.choice(ref.NON_DELIVERABLE_CURRENCY_CODES)
                payload["notional_currency"] = payload["settlement_currency"]
            case "notional_over_limit":
                limit = ref.BOOK_BY_ID[payload["book_id"]].notional_limit
                payload["notional_amount"] = float(limit) * self.rng.uniform(1.2, 4.0)
            case "type_mismatch_notional":
                # A string where a number belongs -- the commonest real serialisation
                # defect, and one that a naive `::number` cast turns into a silent NULL.
                payload["notional_amount"] = f"{payload['notional_amount']:,.2f}"

        return CorruptTradeEvent(
            payload=payload,
            injected_fault=fault,
            expected_rule_codes=expected_codes,
            trade_id=payload.get("trade_id"),
        )

    # -- public API ---------------------------------------------------------
    def generate(self, count: int, as_of: date | None = None) -> Iterator[GeneratedEvent]:
        """Yield `count` events, updating the trade book as it goes."""
        as_of = as_of or date.today()

        # The versions the warehouse already holds, read before the batch moves them. It is
        # the floor under this batch's high-water mark: a resend of a version an earlier
        # batch published is stale on arrival, with nothing in this batch to say so.
        self._versions_at_batch_start = {
            trade_id: entry.trade_version for trade_id, entry in self.book.trades.items()
        }

        # Identifiers, never entries. `book.record()` replaces the entry object, so a
        # cached entry goes stale the moment its trade is updated, and a stale entry
        # carries a stale version number and a stale business timestamp -- enough to issue
        # a version twice, or to stamp an amendment before the version it amends.
        amendable_ids = [entry.trade_id for entry in self.book.amendable()]

        # Only a trade past version 1 has an earlier version to arrive late. Asking for a
        # stale arrival on a version-1 trade produced a same-version resend instead --
        # accepted here, and accepted again in the next batch, which leaves two accepted
        # events for one version and no append-only way to say which one won.
        resendable_ids = [
            entry.trade_id for entry in self.book.amendable() if entry.trade_version > 1
        ]

        # Same-version resends target only trades booked in THIS batch. Rule 2 is
        # arbitrated within a build: the latest arrival wins and the others are recorded
        # as SUPERSEDED. A build cannot reach back into an earlier one to supersede an
        # event it has already published, because the audit fact is append-only by
        # design, so a cross-batch resend would leave two accepted events for one
        # version -- exactly what adjudicated_one_accepted_event_per_trade_version
        # forbids. Confining the race to one batch demonstrates the rule without
        # manufacturing a violation the pipeline has no honest way to resolve.
        replaceable_ids: list[str] = []

        for _ in range(count):
            roll = self.rng.random()
            event: TradeEvent
            mutates_book = True

            if resendable_ids and roll < self.settings.stale_version_rate:
                # Stale version: rejected, so the book must NOT advance.
                entry = self.book.trades[self.rng.choice(resendable_ids)]
                event = self._build_stale(entry, as_of)
                mutates_book = False
            elif replaceable_ids and roll < (
                self.settings.stale_version_rate + self.settings.replace_rate
            ):
                # Same-version replacement: accepted, book version unchanged.
                entry = self.book.trades[self.rng.choice(replaceable_ids)]
                event = self._build_replacement(entry, as_of)
                mutates_book = False
                self._apply_replacement(entry, event)
            elif amendable_ids and roll < (
                self.settings.stale_version_rate
                + self.settings.replace_rate
                + self.settings.amend_rate
            ):
                entry = self.book.trades[self.rng.choice(amendable_ids)]
                cancel = self.rng.random() < self.settings.cancel_rate
                event = self._build_amendment(entry, as_of, cancel=cancel)
            else:
                event = self._build_new(as_of)

            # Business time advances on every emission, accepted or not. A rejected or
            # superseded event must not move the trade's *version* -- that would corrupt
            # every version number after it -- but it did happen, so the next event for
            # the trade has to be stamped after it. Leaving business time behind on the
            # paths that do not advance the book let a later amendment be stamped before
            # an earlier resend, which reorders the trade's history and makes the
            # pipeline's verdict impossible for the manifest to predict.
            if event.trade_id in self.book.trades:
                self.book.trades[event.trade_id].last_event_ts = event.event_timestamp.isoformat()

            corrupt = self._corrupt(event) if self.rng.random() < self.settings.error_rate else None
            is_warn_only = corrupt is not None and corrupt.injected_fault in WARN_ONLY_FAULTS

            # A rejected event must not advance the trade book, or every subsequent
            # version number would be wrong. A warn-only fault is still accepted, so it
            # does advance it -- and the pools with it. A flagged-but-accepted
            # cancellation that stayed in the amendable pool would be amended again, and
            # the rules would rightly reject that as an amend after cancel.
            if mutates_book and (corrupt is None or is_warn_only):
                self._apply_to_book(event)
                if event.action is TradeAction.NEW:
                    amendable_ids.append(event.trade_id)
                    replaceable_ids.append(event.trade_id)
                elif event.action is TradeAction.AMEND and event.trade_version == 2:
                    # The amendment that creates a version to be late for. Testing for
                    # exactly 2 adds the trade once and needs no membership scan.
                    resendable_ids.append(event.trade_id)
                elif event.action is TradeAction.CANCEL:
                    # Cancellation is terminal, so the trade leaves every pool. Held as
                    # lists rather than recomputed from the book, which would scan the
                    # whole trade history once per event.
                    amendable_ids.remove(event.trade_id)
                    if event.trade_id in replaceable_ids:
                        replaceable_ids.remove(event.trade_id)
                    if event.trade_id in resendable_ids:
                        resendable_ids.remove(event.trade_id)

            # Staleness is the event's own doing and is settled before any fault is
            # considered: a late arrival is late whether or not it also carries a bad
            # field. RJ001 sits in the STATE phase, which the model evaluates for every
            # row, so it stands alongside whatever the FIELD phase found rather than
            # instead of it. Decided here, once, for both paths below -- deciding it
            # separately in each is what let them disagree.
            is_stale = event.trade_version < self._known_version(event.trade_id)

            if corrupt is not None:
                # An identity-destroying fault leaves nothing to place in a trade's
                # history, so the version rules cannot reach the event and RJ001 cannot
                # fire. Every other fault leaves the trade identifiable, and staleness
                # applies on top of it.
                is_identifiable = corrupt.injected_fault not in IDENTITY_DESTROYING_FAULTS
                is_late = is_stale and is_identifiable

                # WARN severity describes the fault, not the event. Reading the verdict off
                # the fault alone promised ACCEPTED for late arrivals that happened to draw
                # notional_over_limit -- the one warn-only fault -- and the version rules
                # refused them, correctly, leaving reconciliation to report a working
                # pipeline as broken.
                yield GeneratedEvent(
                    payload=corrupt.payload,
                    trade_id=corrupt.trade_id,
                    trade_version=event.trade_version if is_identifiable else None,
                    action=event.action.value if is_identifiable else None,
                    injected_fault=corrupt.injected_fault,
                    # Every catalogued fault but the warn-only one trips a REJECT-severity
                    # FIELD rule, which is what puts the event outside the ranking.
                    field_valid=is_warn_only,
                    expected_verdict="ACCEPTED" if is_warn_only and not is_late else "REJECTED",
                    # A late arrival is refused on its version, and RJ001 alone is certain.
                    # Claiming the codes of the fault as well asserts that the field phase
                    # always reports on an event the version rules have already refused,
                    # which is a detail of how the two phases combine rather than a promise
                    # the pipeline makes. The subset check accepts whatever else fires.
                    expected_rule_codes=["RJ001"] if is_late else corrupt.expected_rule_codes,
                )
                continue

            yield GeneratedEvent(
                payload=json.loads(event.model_dump_json()),
                trade_id=event.trade_id,
                trade_version=event.trade_version,
                action=event.action.value,
                injected_fault=None,
                expected_verdict="REJECTED" if is_stale else "ACCEPTED",
                expected_rule_codes=["RJ001"] if is_stale else [],
            )

    def _known_version(self, trade_id: str) -> int:
        entry = self.book.trades.get(trade_id)
        return entry.trade_version if entry else 0

    def _apply_to_book(self, event: TradeEvent) -> None:
        self.book.record(
            TradeBookEntry(
                trade_id=event.trade_id,
                trade_version=event.trade_version,
                product_type=event.product_type,
                counterparty_id=event.counterparty_id,
                book_id=event.book_id,
                notional_currency=event.notional_currency,
                settlement_currency=event.settlement_currency,
                buy_sell=event.buy_sell.value,
                notional_amount=str(event.notional_amount),
                trade_date=event.trade_date.isoformat(),
                maturity_date=event.maturity_date.isoformat() if event.maturity_date else None,
                uti=event.uti,
                is_cancelled=event.action is TradeAction.CANCEL,
                last_event_ts=event.event_timestamp.isoformat(),
            )
        )

    def _apply_replacement(self, entry: TradeBookEntry, event: TradeEvent) -> None:
        """A same-version replace overwrites economics but keeps the version."""
        entry.notional_amount = str(event.notional_amount)
        entry.last_event_ts = event.event_timestamp.isoformat()
        self.book.record(entry)

    def build_manifest(
        self, events: list[GeneratedEvent], *, batch_ref: str, file_name: str
    ) -> BatchManifest:
        """Summarise a batch into the ground truth used by `trade-sim reconcile`."""
        _settle_version_arbitration(events, self._versions_at_batch_start)

        fault_counts: dict[str, int] = {}
        rule_counts: dict[str, int] = {}
        for event in events:
            if event.injected_fault:
                fault_counts[event.injected_fault] = fault_counts.get(event.injected_fault, 0) + 1
            for code in event.expected_rule_codes:
                rule_counts[code] = rule_counts.get(code, 0) + 1

        return BatchManifest(
            batch_ref=batch_ref,
            generated_at=datetime.now(UTC),
            seed=self.settings.seed,
            file_name=file_name,
            total_events=len(events),
            expected_accepted=sum(1 for e in events if e.expected_verdict == "ACCEPTED"),
            expected_rejected=sum(1 for e in events if e.expected_verdict == "REJECTED"),
            expected_superseded=sum(1 for e in events if e.expected_verdict == "SUPERSEDED"),
            fault_counts=dict(sorted(fault_counts.items())),
            expected_rule_code_counts=dict(sorted(rule_counts.items())),
            verdicts=[
                ExpectedVerdict(
                    trade_id=e.trade_id,
                    trade_version=e.trade_version,
                    action=e.action,
                    injected_fault=e.injected_fault,
                    expected_verdict=e.expected_verdict,
                    expected_rule_codes=e.expected_rule_codes,
                )
                for e in events
            ],
        )
