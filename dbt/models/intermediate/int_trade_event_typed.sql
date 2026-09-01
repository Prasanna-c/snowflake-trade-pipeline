/*
    Typing: turn the VARIANT payload into typed columns, and turn every failed cast
    into evidence rather than a silent NULL.

    THE CENTRAL IDEA HERE.

    A naive `payload:notional_amount::number` on the string "1,234.00" raises an error
    that fails the whole model. Switching to `try_cast` fixes the failure but creates a
    worse problem: the value silently becomes NULL, and the trade is then rejected for a
    "missing field" it actually sent. The upstream team is told the wrong thing, and
    chases the wrong bug.

    So every cast is paired with a presence check:

        try_to_number(payload:notional_amount::varchar)     -> the value, or NULL
        payload_has_value('payload', 'notional_amount')     -> was it sent at all?

    The presence half is a macro rather than `payload:notional_amount is not null`
    because that spelling counts an explicit JSON null as a value, which turns every
    legitimately absent optional field into a cast failure. See
    macros/utils/payload_presence.sql for the three-way truth table.

    "sent but uncastable" (RJ008 malformed) and "not sent" (RJ004 missing) are then
    distinguishable, and cast_failure_count counts the former. That single distinction
    is the difference between a useful rejection report and a misleading one.

    Two window functions are computed here rather than downstream, because they need the
    whole event population and are cheap to compute once:
      uti_distinct_trade_count -- feeds RJ019 (duplicate identifier)
      cast_failure_count       -- feeds RJ008 (malformed payload)
*/

{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select * from {{ ref('stg_trade_event') }}

),

extracted as (

    select
        source.event_sk,
        source.batch_id,
        source.batch_seq,
        source.raw_payload,
        source.source_file_name,
        source.source_file_row_number,
        source.load_method,
        source.load_ts,
        source.drained_at,

        -- Identity ---------------------------------------------------------
        -- VARCHAR extraction cannot fail, so identity fields are cast directly.
        -- trim() because a trailing space in a trade_id would silently create a
        -- second, parallel trade history.
        nullif(trim(source.raw_payload:trade_id::varchar), '') as trade_id,
        try_to_number(source.raw_payload:trade_version::varchar) as trade_version,
        nullif(trim(upper(source.raw_payload:action::varchar)), '') as action,
        nullif(trim(source.raw_payload:uti::varchar), '') as uti,

        -- Economics --------------------------------------------------------
        nullif(trim(upper(source.raw_payload:product_type::varchar)), '') as product_type,
        nullif(trim(upper(source.raw_payload:asset_class::varchar)), '') as asset_class,
        nullif(trim(upper(source.raw_payload:buy_sell::varchar)), '') as buy_sell,
        try_to_number(source.raw_payload:notional_amount::varchar, 38, 4) as notional_amount,
        nullif(trim(upper(source.raw_payload:notional_currency::varchar)), '') as notional_currency,
        nullif(trim(upper(source.raw_payload:settlement_currency::varchar)), '') as settlement_currency,
        try_to_number(source.raw_payload:quantity::varchar, 38, 4) as quantity,
        try_to_number(source.raw_payload:price::varchar, 38, 8) as price,

        -- Dates ------------------------------------------------------------
        -- Explicit format string, not AUTO. Snowflake's automatic date detection will
        -- happily read 03/04/2026 as either 3 April or 4 March depending on session
        -- settings, and an ambiguous maturity date is a silent economic error.
        try_to_date(source.raw_payload:trade_date::varchar, 'YYYY-MM-DD') as trade_date,
        try_to_date(source.raw_payload:settlement_date::varchar, 'YYYY-MM-DD') as settlement_date,
        try_to_date(source.raw_payload:maturity_date::varchar, 'YYYY-MM-DD') as maturity_date,

        -- Attribution ------------------------------------------------------
        nullif(trim(upper(source.raw_payload:counterparty_id::varchar)), '') as counterparty_id,
        nullif(trim(upper(source.raw_payload:book_id::varchar)), '') as book_id,
        nullif(trim(upper(source.raw_payload:trader_id::varchar)), '') as trader_id,
        nullif(trim(upper(source.raw_payload:execution_venue::varchar)), '') as execution_venue,
        nullif(trim(upper(source.raw_payload:clearing_house::varchar)), '') as clearing_house,
        nullif(trim(source.raw_payload:legal_entity::varchar), '') as legal_entity,

        -- Provenance -------------------------------------------------------
        nullif(trim(upper(source.raw_payload:source_system::varchar)), '') as source_system,
        try_to_timestamp_tz(source.raw_payload:event_timestamp::varchar) as event_timestamp,

        -- ---------------------------------------------------------------
        -- Presence flags: did the payload carry a value for the field?
        -- Compared against the cast result to separate "malformed" from "missing".
        -- An explicit JSON null counts as not sent, which is the whole reason this is
        -- a macro instead of `is not null`.
        -- ---------------------------------------------------------------
        {{ payload_has_value('source.raw_payload', 'trade_version') }} as has_trade_version,
        {{ payload_has_value('source.raw_payload', 'notional_amount') }} as has_notional_amount,
        {{ payload_has_value('source.raw_payload', 'quantity') }} as has_quantity,
        {{ payload_has_value('source.raw_payload', 'price') }} as has_price,
        {{ payload_has_value('source.raw_payload', 'trade_date') }} as has_trade_date,
        {{ payload_has_value('source.raw_payload', 'settlement_date') }} as has_settlement_date,
        {{ payload_has_value('source.raw_payload', 'maturity_date') }} as has_maturity_date,
        {{ payload_has_value('source.raw_payload', 'event_timestamp') }} as has_event_timestamp

    from source

),

cast_diagnostics as (

    select
        extracted.*,

        -- One boolean per field that could fail to cast. Kept as separate columns
        -- rather than only a count, so the rejection report can name the offending
        -- field instead of just saying "something did not parse".
        extracted.has_trade_version and extracted.trade_version is null as cast_failed_trade_version,
        extracted.has_notional_amount and extracted.notional_amount is null as cast_failed_notional_amount,
        extracted.has_quantity and extracted.quantity is null as cast_failed_quantity,
        extracted.has_price and extracted.price is null as cast_failed_price,
        extracted.has_trade_date and extracted.trade_date is null as cast_failed_trade_date,
        extracted.has_settlement_date and extracted.settlement_date is null as cast_failed_settlement_date,
        extracted.has_maturity_date and extracted.maturity_date is null as cast_failed_maturity_date,
        extracted.has_event_timestamp and extracted.event_timestamp is null as cast_failed_event_timestamp

    from extracted

),

final as (

    select
        cast_diagnostics.*,

        -- Feeds RJ008.
        (
            iff(cast_diagnostics.cast_failed_trade_version, 1, 0)
            + iff(cast_diagnostics.cast_failed_notional_amount, 1, 0)
            + iff(cast_diagnostics.cast_failed_quantity, 1, 0)
            + iff(cast_diagnostics.cast_failed_price, 1, 0)
            + iff(cast_diagnostics.cast_failed_trade_date, 1, 0)
            + iff(cast_diagnostics.cast_failed_settlement_date, 1, 0)
            + iff(cast_diagnostics.cast_failed_maturity_date, 1, 0)
            + iff(cast_diagnostics.cast_failed_event_timestamp, 1, 0)
        ) as cast_failure_count,

        -- Names the failed fields so the rejection log is actionable.
        array_construct_compact(
            iff(cast_diagnostics.cast_failed_trade_version, 'trade_version', null),
            iff(cast_diagnostics.cast_failed_notional_amount, 'notional_amount', null),
            iff(cast_diagnostics.cast_failed_quantity, 'quantity', null),
            iff(cast_diagnostics.cast_failed_price, 'price', null),
            iff(cast_diagnostics.cast_failed_trade_date, 'trade_date', null),
            iff(cast_diagnostics.cast_failed_settlement_date, 'settlement_date', null),
            iff(cast_diagnostics.cast_failed_maturity_date, 'maturity_date', null),
            iff(cast_diagnostics.cast_failed_event_timestamp, 'event_timestamp', null)
        ) as cast_failed_fields,

        -- Names the absent mandatory fields, for the same reason.
        array_construct_compact(
            iff(cast_diagnostics.trade_id is null, 'trade_id', null),
            iff(not cast_diagnostics.has_trade_version, 'trade_version', null),
            iff(cast_diagnostics.action is null, 'action', null),
            iff(cast_diagnostics.counterparty_id is null, 'counterparty_id', null),
            iff(cast_diagnostics.book_id is null, 'book_id', null),
            iff(cast_diagnostics.product_type is null, 'product_type', null),
            iff(cast_diagnostics.buy_sell is null, 'buy_sell', null),
            iff(cast_diagnostics.notional_currency is null, 'notional_currency', null),
            iff(not cast_diagnostics.has_notional_amount, 'notional_amount', null),
            iff(not cast_diagnostics.has_trade_date, 'trade_date', null)
        ) as missing_mandatory_fields,

        -- Feeds RJ019. A UTI seen against more than one trade_id suggests a double
        -- booking, which regulators care about more than we do -- hence WARN, and
        -- hence it is detected rather than ignored.
        count(distinct cast_diagnostics.trade_id) over (
            partition by cast_diagnostics.uti
        ) as uti_distinct_trade_count,

        -- Deterministic ordering key for version arbitration. Business time first,
        -- because network reordering is real and the upstream system's clock is the
        -- authority on what happened first. Arrival order is only a tie-breaker.
        coalesce(cast_diagnostics.event_timestamp, cast_diagnostics.load_ts) as effective_event_ts

    from cast_diagnostics

)

select * from final
