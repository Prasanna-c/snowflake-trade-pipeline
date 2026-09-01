"""Reference data: the single source of truth shared by the simulator and dbt.

Counterparties, currencies, products and books are defined here in Python and
*emitted* to `dbt/seeds/*.csv` by `trade-sim emit-seeds`. CI re-runs that command
with `--check` and fails if the checked-in seeds have drifted.

Why not define them in the CSVs and have Python read those? Because the generator
needs more than the columns dbt needs (maturity tenor ranges, notional
distributions, weights) and splitting the definition across two files is how
"unknown counterparty" bugs appear in a demo. One definition, one direction of
generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Counterparty:
    counterparty_id: str
    counterparty_name: str
    lei: str
    country_code: str
    credit_rating: str
    is_active: bool = True


@dataclass(frozen=True)
class Currency:
    currency_code: str
    currency_name: str
    minor_units: int
    is_deliverable: bool


@dataclass(frozen=True)
class Product:
    product_type: str
    asset_class: str
    #: Inclusive range of tenors in days used to derive a plausible maturity date.
    tenor_days_min: int
    tenor_days_max: int
    #: Log-uniform notional bounds, in the notional currency.
    notional_min: float
    notional_max: float
    requires_maturity: bool = True
    #: Relative likelihood of this product appearing in a generated batch.
    weight: int = 1


@dataclass(frozen=True)
class Book:
    book_id: str
    book_name: str
    desk: str
    legal_entity: str
    #: Notional above which a trade is flagged (not rejected) as breaching desk limit.
    notional_limit: float


# ---------------------------------------------------------------------------
# Counterparties. 24 entries is enough for realistic cardinality without making
# the seed unreadable. CP9999 is deliberately absent so the generator can emit an
# unknown-counterparty fault that no amount of seed drift will accidentally fix.
# ---------------------------------------------------------------------------
COUNTERPARTIES: tuple[Counterparty, ...] = (
    Counterparty("CP0001", "Aldgate Capital Partners LLP", "5493001KJTIIGC8Y1R12", "GB", "A+"),
    Counterparty("CP0002", "Bergstrom Nordic Bank AB", "549300ZJTIIGC8Y1R213", "SE", "AA-"),
    Counterparty("CP0003", "Cortez Iberia Inversiones SA", "5493004KJTIIGC8Y1R14", "ES", "BBB+"),
    Counterparty("CP0004", "Daiwa Meridian Securities KK", "5493005KJTIIGC8Y1R15", "JP", "A"),
    Counterparty("CP0005", "Eiffel Structured Finance SA", "5493006KJTIIGC8Y1R16", "FR", "AA"),
    Counterparty("CP0006", "Frankfurter Handelsbank AG", "5493007KJTIIGC8Y1R17", "DE", "AA+"),
    Counterparty("CP0007", "Gotham Prime Brokerage Inc", "5493008KJTIIGC8Y1R18", "US", "A-"),
    Counterparty("CP0008", "Helvetia Zurich Privatbank AG", "5493009KJTIIGC8Y1R19", "CH", "AAA"),
    Counterparty("CP0009", "Iberville Louisiana Trust", "5493010KJTIIGC8Y1R20", "US", "BBB"),
    Counterparty("CP0010", "Jakarta Sentral Sekuritas", "5493011KJTIIGC8Y1R21", "ID", "BB+"),
    Counterparty("CP0011", "Kowloon Pacific Holdings Ltd", "5493012KJTIIGC8Y1R22", "HK", "A"),
    Counterparty("CP0012", "Lombard Street Asset Mgmt", "5493013KJTIIGC8Y1R23", "GB", "A+"),
    Counterparty("CP0013", "Maastricht Delta Fondsen NV", "5493014KJTIIGC8Y1R24", "NL", "AA-"),
    Counterparty("CP0014", "Nordkapp Energi Trading AS", "5493015KJTIIGC8Y1R25", "NO", "BBB+"),
    Counterparty("CP0015", "Osaka Kansai Shoji Co", "5493016KJTIIGC8Y1R26", "JP", "A-"),
    Counterparty("CP0016", "Piedmont Alpine SGR SpA", "5493017KJTIIGC8Y1R27", "IT", "BBB"),
    Counterparty("CP0017", "Queensway Antipodean Ltd", "5493018KJTIIGC8Y1R28", "AU", "A"),
    Counterparty("CP0018", "Rialto Veneto Banca SpA", "5493019KJTIIGC8Y1R29", "IT", "BB"),
    Counterparty("CP0019", "Stockholm Vasa Kapital AB", "5493020KJTIIGC8Y1R30", "SE", "AA"),
    Counterparty("CP0020", "Tanjong Straits Investments", "5493021KJTIIGC8Y1R31", "SG", "A+"),
    # Two intentionally inactive counterparties: trades against these are a
    # legitimate business rejection, not a data-quality fault.
    Counterparty(
        "CP0021", "Umbria Legacy Holdings Srl", "5493022KJTIIGC8Y1R32", "IT", "D", is_active=False
    ),
    Counterparty(
        "CP0022",
        "Vantage Bridge Ltd (in admin)",
        "5493023KJTIIGC8Y1R33",
        "GB",
        "D",
        is_active=False,
    ),
    Counterparty("CP0023", "Westhafen Rhein Kredit AG", "5493024KJTIIGC8Y1R34", "DE", "A"),
    Counterparty("CP0024", "Yokohama Bay Financial Ltd", "5493025KJTIIGC8Y1R35", "JP", "BBB+"),
)

# ---------------------------------------------------------------------------
# Currencies (ISO 4217). JPY has 0 minor units, which is the classic bug that
# turns a 100 yen trade into a 1 yen trade -- worth having in the data.
# ---------------------------------------------------------------------------
CURRENCIES: tuple[Currency, ...] = (
    Currency("USD", "US Dollar", 2, True),
    Currency("EUR", "Euro", 2, True),
    Currency("GBP", "Pound Sterling", 2, True),
    Currency("JPY", "Japanese Yen", 0, True),
    Currency("CHF", "Swiss Franc", 2, True),
    Currency("AUD", "Australian Dollar", 2, True),
    Currency("CAD", "Canadian Dollar", 2, True),
    Currency("SEK", "Swedish Krona", 2, True),
    Currency("NOK", "Norwegian Krone", 2, True),
    Currency("SGD", "Singapore Dollar", 2, True),
    Currency("HKD", "Hong Kong Dollar", 2, True),
    # Non-deliverable: settles in USD, cannot be the settlement currency of a
    # physically-settled forward. Gives the rule set something non-trivial to check.
    Currency("KRW", "South Korean Won", 0, False),
    Currency("INR", "Indian Rupee", 2, False),
    Currency("BRL", "Brazilian Real", 2, False),
)

#: Values that look like currencies but are not. Used by the corruption engine.
#:
#: Every token must still be invalid AFTER the typed layer trims and upper-cases it.
#: "usd" was here once and was a defect in the simulator, not a fault in the data: it
#: normalises to a perfectly good USD, the pipeline accepts it, and the manifest then
#: claimed a rejection that should never have happened. Case normalisation is asserted
#: where it belongs, in the unit tests for int_trade_event_typed.
#:
#: The empty string is deliberate and is not the same kind of token as the others: it
#: nullifies, so it is reported as a missing mandatory field rather than an unknown
#: value. The generator derives that expectation per token.
INVALID_CURRENCY_TOKENS: tuple[str, ...] = ("US", "XXX", "EURO", "GB£", "", "N/A", "000")

# ---------------------------------------------------------------------------
# Products.
# ---------------------------------------------------------------------------
PRODUCTS: tuple[Product, ...] = (
    Product("FX_SPOT", "FX", 2, 2, 100_000, 50_000_000, weight=25),
    Product("FX_FORWARD", "FX", 7, 730, 250_000, 100_000_000, weight=20),
    Product("FX_SWAP", "FX", 30, 1_095, 500_000, 250_000_000, weight=12),
    Product("NDF", "FX", 30, 365, 250_000, 50_000_000, weight=6),
    Product("IRS", "RATES", 365, 10_950, 1_000_000, 500_000_000, weight=18),
    Product("OIS", "RATES", 90, 3_650, 1_000_000, 250_000_000, weight=6),
    Product("CDS", "CREDIT", 365, 3_650, 1_000_000, 100_000_000, weight=5),
    Product("EQUITY_SWAP", "EQUITY", 90, 1_095, 500_000, 75_000_000, weight=5),
    Product("BOND", "CREDIT", 365, 10_950, 100_000, 200_000_000, weight=3),
)

#: Product codes the pipeline does not support. Used by the corruption engine.
UNSUPPORTED_PRODUCT_TOKENS: tuple[str, ...] = (
    "CRYPTO_PERP",
    "WEATHER_DERIV",
    "UNKNOWN",
    "FX_OPTION_EXOTIC",
)

# ---------------------------------------------------------------------------
# Books and desks.
# ---------------------------------------------------------------------------
BOOKS: tuple[Book, ...] = (
    Book("BK-FX-LDN-01", "FX Cash London", "FX_CASH", "DB London Branch", 250_000_000),
    Book("BK-FX-LDN-02", "FX Forwards London", "FX_FWD", "DB London Branch", 300_000_000),
    Book("BK-FX-SGP-01", "FX Cash Singapore", "FX_CASH", "DB Singapore Branch", 150_000_000),
    Book("BK-RT-FRA-01", "Rates Flow Frankfurt", "RATES_FLOW", "DB AG Frankfurt", 500_000_000),
    Book(
        "BK-RT-FRA-02", "Rates Structured Frankfurt", "RATES_STRUCT", "DB AG Frankfurt", 400_000_000
    ),
    Book("BK-CR-NYC-01", "Credit Flow New York", "CREDIT_FLOW", "DB Securities Inc", 200_000_000),
    Book("BK-EQ-LDN-01", "Equity Derivatives London", "EQD", "DB London Branch", 100_000_000),
)

#: Book identifiers that do not exist. Used by the corruption engine.
INVALID_BOOK_TOKENS: tuple[str, ...] = ("BK-XX-XXX-99", "UNKNOWN_BOOK", "", "BK-FX-LDN-99")

#: Directions that are neither BUY nor SELL once normalised. Kept here with the other
#: corruption tokens rather than inline in the generator, so that the same test which
#: proves a token is genuinely invalid covers this field too.
INVALID_DIRECTION_TOKENS: tuple[str, ...] = ("B", "S", "LONG", "SHRT", "")

EXECUTION_VENUES: tuple[str, ...] = (
    "XOFF",
    "XLON",
    "XETR",
    "BMTF",
    "TRDX",
    "REUT",
    "BLMB",
    "MRKT",
    "TWEB",
)

CLEARING_HOUSES: tuple[str, ...] = ("LCH", "EUREX", "ICE_EU", "CME", "NONE")

SOURCE_SYSTEMS: tuple[str, ...] = ("MUREX", "CALYPSO", "SUMMIT", "FRONT_ARENA", "INTERNAL_EFX")

TRADE_ACTIONS: tuple[str, ...] = ("NEW", "AMEND", "CANCEL")

BUY_SELL: tuple[str, ...] = ("BUY", "SELL")


# ---------------------------------------------------------------------------
# Rejection reason catalogue.
#
# The dbt macro `trade_validation_rules()` is the authority for a rule's *condition
# and severity*. This dict is the authority for its *human description and
# remediation*. A dbt singular test asserts the two sets of codes match exactly, so
# neither can drift without CI failing.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RejectionReason:
    rule_code: str
    rule_name: str
    rule_category: str
    severity: str
    requirement_ref: str
    description: str
    remediation: str


REJECTION_REASONS: tuple[RejectionReason, ...] = (
    RejectionReason(
        "RJ001",
        "Stale version",
        "VERSION",
        "REJECT",
        "R1",
        "Incoming trade version is lower than the version already stored for this trade.",
        "Upstream replayed an old message. Confirm the current version with the source system; no action needed if the stored version is correct.",
    ),
    RejectionReason(
        "RJ002",
        "Maturity before trade date",
        "TEMPORAL",
        "REJECT",
        "OWN",
        "Maturity date is earlier than the trade date, which is economically impossible.",
        "Booking error. Correct the maturity date and resubmit with an incremented version.",
    ),
    RejectionReason(
        "RJ003",
        "Maturity in the past",
        "TEMPORAL",
        "REJECT",
        "R3",
        "Maturity date is earlier than today, so the trade cannot be booked as live.",
        "Either the trade is a late back-booking (submit through the historical load path) or the maturity date is wrong.",
    ),
    RejectionReason(
        "RJ004",
        "Missing mandatory field",
        "COMPLETENESS",
        "REJECT",
        "OWN",
        "One or more mandatory fields are null or absent from the payload.",
        "Fix the upstream mapping. The rule detail column names the missing field.",
    ),
    RejectionReason(
        "RJ005",
        "Unknown counterparty",
        "REFERENCE",
        "REJECT",
        "OWN",
        "counterparty_id does not exist in the counterparty reference data.",
        "Either the counterparty is genuinely new (onboard it, then replay) or the identifier is malformed.",
    ),
    RejectionReason(
        "RJ006",
        "Invalid currency",
        "REFERENCE",
        "REJECT",
        "OWN",
        "notional_currency is not a supported ISO 4217 code.",
        "Correct the currency code upstream. Watch for lower-case codes and two-letter country codes.",
    ),
    RejectionReason(
        "RJ007",
        "Non-positive notional",
        "ECONOMIC",
        "REJECT",
        "OWN",
        "Notional amount is zero, negative or absent.",
        "Direction is carried by buy_sell, never by the sign of the notional. Fix the upstream mapping.",
    ),
    RejectionReason(
        "RJ008",
        "Malformed payload",
        "STRUCTURAL",
        "REJECT",
        "OWN",
        "The payload could not be interpreted as a trade: unparseable JSON, or a field that failed type coercion.",
        "Inspect the raw payload in AUDIT.FCT_TRADE_REJECTED. Usually an encoding or serialisation defect upstream.",
    ),
    RejectionReason(
        "RJ009",
        "Superseded within run",
        "VERSION",
        "SUPERSEDE",
        "R2",
        "A later arrival carried the same trade_id and version, so this event was replaced.",
        "Informational. This is business rule 2 (replace same version) operating correctly; no action required.",
    ),
    RejectionReason(
        "RJ010",
        "Amendment after cancellation",
        "LIFECYCLE",
        "REJECT",
        "OWN",
        "An amendment arrived for a trade that has already been cancelled.",
        "Cancellation is terminal. To reinstate, book a new trade with a new trade_id.",
    ),
    RejectionReason(
        "RJ011",
        "Unknown book",
        "REFERENCE",
        "REJECT",
        "OWN",
        "book_id does not exist in the book reference data.",
        "Onboard the book or correct the identifier. Unmapped books break P&L attribution.",
    ),
    RejectionReason(
        "RJ012",
        "Settlement before trade date",
        "TEMPORAL",
        "REJECT",
        "OWN",
        "Settlement date precedes the trade date.",
        "Booking error, commonly a timezone or date-format defect upstream.",
    ),
    RejectionReason(
        "RJ013",
        "Unsupported product",
        "REFERENCE",
        "REJECT",
        "OWN",
        "product_type is not a product this platform is authorised to process.",
        "Route to the correct platform, or extend the supported product list if in scope.",
    ),
    RejectionReason(
        "RJ014",
        "Trade date in the future",
        "TEMPORAL",
        "REJECT",
        "OWN",
        "Trade date is later than today.",
        "Almost always a timezone bug. Verify the upstream system is emitting UTC.",
    ),
    RejectionReason(
        "RJ015",
        "Invalid direction",
        "ECONOMIC",
        "REJECT",
        "OWN",
        "buy_sell is not one of BUY or SELL.",
        "Fix the upstream enumeration mapping.",
    ),
    RejectionReason(
        "RJ016",
        "Inactive counterparty",
        "REFERENCE",
        "REJECT",
        "OWN",
        "The counterparty exists but is flagged inactive (defaulted, in administration or offboarded).",
        "Trading with an inactive counterparty requires credit approval. Escalate rather than resubmitting.",
    ),
    RejectionReason(
        "RJ017",
        "Non-deliverable currency on physical settlement",
        "ECONOMIC",
        "REJECT",
        "OWN",
        "A physically-settled product was booked in a non-deliverable currency.",
        "Rebook as an NDF, or correct the settlement currency to a deliverable one.",
    ),
    RejectionReason(
        "RJ018",
        "Desk notional limit breached",
        "LIMIT",
        "WARN",
        "OWN",
        "Notional exceeds the book's configured limit.",
        "Warning only -- the trade is accepted and flagged. Risk reviews breaches daily.",
    ),
    RejectionReason(
        "RJ019",
        "Duplicate trade identifier",
        "STRUCTURAL",
        "WARN",
        "OWN",
        "The same UTI was seen on a different trade_id, suggesting a double booking.",
        "Warning only. Operations reconciles UTIs against the trade repository.",
    ),
)

REJECTION_REASON_BY_CODE: dict[str, RejectionReason] = {r.rule_code: r for r in REJECTION_REASONS}


# ---------------------------------------------------------------------------
# Convenience lookups
# ---------------------------------------------------------------------------
COUNTERPARTY_IDS: tuple[str, ...] = tuple(c.counterparty_id for c in COUNTERPARTIES)
ACTIVE_COUNTERPARTY_IDS: tuple[str, ...] = tuple(
    c.counterparty_id for c in COUNTERPARTIES if c.is_active
)
INACTIVE_COUNTERPARTY_IDS: tuple[str, ...] = tuple(
    c.counterparty_id for c in COUNTERPARTIES if not c.is_active
)
CURRENCY_CODES: tuple[str, ...] = tuple(c.currency_code for c in CURRENCIES)
DELIVERABLE_CURRENCY_CODES: tuple[str, ...] = tuple(
    c.currency_code for c in CURRENCIES if c.is_deliverable
)
NON_DELIVERABLE_CURRENCY_CODES: tuple[str, ...] = tuple(
    c.currency_code for c in CURRENCIES if not c.is_deliverable
)
BOOK_IDS: tuple[str, ...] = tuple(b.book_id for b in BOOKS)
PRODUCT_TYPES: tuple[str, ...] = tuple(p.product_type for p in PRODUCTS)
PRODUCT_BY_TYPE: dict[str, Product] = {p.product_type: p for p in PRODUCTS}
BOOK_BY_ID: dict[str, Book] = {b.book_id: b for b in BOOKS}

#: Products that settle physically and therefore require a deliverable currency.
PHYSICALLY_SETTLED_PRODUCTS: frozenset[str] = frozenset(
    {"FX_SPOT", "FX_FORWARD", "FX_SWAP", "BOND"}
)


@dataclass
class SeedSpec:
    """Definition of one dbt seed file emitted from this module."""

    filename: str
    header: tuple[str, ...]
    rows: list[tuple[object, ...]] = field(default_factory=list)


def build_seed_specs() -> list[SeedSpec]:
    """Return every dbt seed derived from this module, in a stable order."""
    return [
        SeedSpec(
            "ref_counterparty.csv",
            (
                "counterparty_id",
                "counterparty_name",
                "lei",
                "country_code",
                "credit_rating",
                "is_active",
            ),
            [
                (
                    c.counterparty_id,
                    c.counterparty_name,
                    c.lei,
                    c.country_code,
                    c.credit_rating,
                    str(c.is_active).lower(),
                )
                for c in COUNTERPARTIES
            ],
        ),
        SeedSpec(
            "ref_currency.csv",
            ("currency_code", "currency_name", "minor_units", "is_deliverable"),
            [
                (c.currency_code, c.currency_name, c.minor_units, str(c.is_deliverable).lower())
                for c in CURRENCIES
            ],
        ),
        SeedSpec(
            "ref_product.csv",
            ("product_type", "asset_class", "requires_maturity", "is_physically_settled"),
            [
                (
                    p.product_type,
                    p.asset_class,
                    str(p.requires_maturity).lower(),
                    str(p.product_type in PHYSICALLY_SETTLED_PRODUCTS).lower(),
                )
                for p in PRODUCTS
            ],
        ),
        SeedSpec(
            "ref_book.csv",
            ("book_id", "book_name", "desk", "legal_entity", "notional_limit"),
            [
                (b.book_id, b.book_name, b.desk, b.legal_entity, f"{b.notional_limit:.2f}")
                for b in BOOKS
            ],
        ),
        SeedSpec(
            "ref_rejection_reason.csv",
            (
                "rule_code",
                "rule_name",
                "rule_category",
                "severity",
                "requirement_ref",
                "description",
                "remediation",
            ),
            [
                (
                    r.rule_code,
                    r.rule_name,
                    r.rule_category,
                    r.severity,
                    r.requirement_ref,
                    r.description,
                    r.remediation,
                )
                for r in REJECTION_REASONS
            ],
        ),
    ]
