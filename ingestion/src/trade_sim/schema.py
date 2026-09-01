"""Trade event contract.

This is the wire format between the producer and RAW.TRADE_EVENT. Pydantic gives us
one place where the contract is written down and enforced, which matters for two
reasons beyond tidiness:

  1. `TradeEvent.model_json_schema()` is emitted to `docs/trade_event.schema.json`,
     so the contract is publishable to upstream teams rather than being folklore.
  2. The corruption engine produces *deliberately invalid* payloads, so it must be
     able to bypass validation. That is why the writer serialises dicts, not models,
     and why `CorruptTradeEvent` exists as a distinct type -- an invalid payload is
     a first-class object in this system, not an error.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class TradeAction(StrEnum):
    NEW = "NEW"
    AMEND = "AMEND"
    CANCEL = "CANCEL"


class Direction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradeEvent(BaseModel):
    """A single trade lifecycle event as emitted by an upstream booking system."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        # Serialise Decimal as a JSON number, not a string, so Snowflake's VARIANT
        # keeps it numeric and `payload:notional_amount::number` needs no cleanup.
        ser_json_inf_nan="null",
    )

    # ---- Identity and versioning -------------------------------------------
    trade_id: str = Field(
        ...,
        pattern=r"^TRD-\d{9}$",
        description="Stable business identifier. Constant across every version of a trade.",
    )
    trade_version: int = Field(
        ...,
        ge=1,
        description=(
            "Monotonically increasing per trade_id. The arbitration rules in business "
            "rules 1 and 2 turn entirely on this field."
        ),
    )
    action: TradeAction = Field(
        ...,
        description="NEW on first booking, AMEND on correction, CANCEL is terminal.",
    )
    uti: str = Field(
        ...,
        min_length=10,
        max_length=52,
        description="Unique Transaction Identifier as reported to the trade repository.",
    )

    # ---- Economics ---------------------------------------------------------
    product_type: str = Field(..., description="See ref_product seed.")
    asset_class: str
    buy_sell: Direction
    notional_amount: Decimal = Field(
        ...,
        description="Always positive. Direction is carried by buy_sell, never by sign.",
    )
    notional_currency: str = Field(..., min_length=3, max_length=3)
    settlement_currency: str = Field(..., min_length=3, max_length=3)
    quantity: Decimal | None = Field(default=None, description="Units, where the product has them.")
    price: Decimal | None = Field(
        default=None, description="Rate, spread or clean price depending on product."
    )

    # ---- Dates -------------------------------------------------------------
    trade_date: date = Field(..., description="Execution date.")
    settlement_date: date | None = Field(default=None)
    maturity_date: date | None = Field(
        default=None,
        description="Business rules 3 and 4 both turn on this field. Null only for perpetuals.",
    )

    # ---- Attribution -------------------------------------------------------
    counterparty_id: str
    book_id: str
    trader_id: str
    execution_venue: str
    clearing_house: str
    legal_entity: str

    # ---- Provenance --------------------------------------------------------
    source_system: str
    event_timestamp: datetime = Field(
        ...,
        description=(
            "When the upstream system created this event. Ranked ahead of arrival time "
            "when ordering events, because network reordering is real and business time is truth."
        ),
    )

    @field_serializer("notional_amount", "quantity", "price", when_used="json")
    def _serialise_decimal(self, value: Decimal | None) -> float | None:
        return float(value) if value is not None else None

    @field_serializer("trade_date", "settlement_date", "maturity_date", when_used="json")
    def _serialise_date(self, value: date | None) -> str | None:
        return value.isoformat() if value is not None else None

    @field_serializer("event_timestamp", when_used="json")
    def _serialise_timestamp(self, value: datetime) -> str:
        return value.isoformat()


class CorruptTradeEvent(BaseModel):
    """A payload that deliberately violates the contract.

    Kept as an explicit type rather than a loose dict so that a batch manifest can
    record *which* fault was injected and therefore which rule code the pipeline is
    expected to return. That is what makes end-to-end reconciliation possible.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    payload: dict[str, Any] | str = Field(
        ...,
        description="The raw thing to write. A str is written verbatim, which is how unparseable JSON is injected.",
    )
    injected_fault: str = Field(
        ..., description="Identifier of the fault, e.g. 'invalid_currency'."
    )
    expected_rule_codes: list[str] = Field(
        default_factory=list,
        description="Rule codes the pipeline should return. Empty for WARN-only faults.",
    )
    trade_id: str | None = Field(
        default=None, description="Null when the fault removed the identifier."
    )


class ExpectedVerdict(BaseModel):
    """One row of the batch manifest: what the pipeline should decide about one event."""

    trade_id: str | None
    trade_version: int | None
    action: str | None
    injected_fault: str | None = None
    expected_verdict: str = Field(..., description="ACCEPTED | REJECTED | SUPERSEDED")
    expected_rule_codes: list[str] = Field(default_factory=list)


class BatchManifest(BaseModel):
    """Companion to a generated file: the ground truth for reconciliation.

    Written alongside every batch. `trade-sim reconcile` compares it against
    INTERMEDIATE.INT_TRADE_EVENT_ADJUDICATED so the demo can assert correctness
    instead of merely asserting completion.
    """

    batch_ref: str
    generated_at: datetime
    seed: int
    file_name: str
    total_events: int
    expected_accepted: int
    expected_rejected: int
    expected_superseded: int
    fault_counts: dict[str, int]
    expected_rule_code_counts: dict[str, int]
    verdicts: list[ExpectedVerdict]

    @field_serializer("generated_at", when_used="json")
    def _serialise_timestamp(self, value: datetime) -> str:
        return value.isoformat()
