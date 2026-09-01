/*
    Every accepted version of every trade -- the complete, immutable version ledger.

    WHY THIS EXISTS SEPARATELY FROM FCT_TRADE.

    A trade amended five times has six accepted versions. FCT_TRADE holds only the
    current one, because that is what risk, P&L and reporting need. But "what did we
    believe about this trade on 14 August, and when did that change?" is a question a
    regulator will ask, and answering it from a table that only keeps the latest version
    is impossible.

    So the grain here is (trade_id, trade_version): one row per accepted version, ever.
    FCT_TRADE is then a trivial projection of the maximum version, and the two can never
    disagree because they derive from the same adjudication log.

    GRAIN AND RULE 2. The unique key is the composite (trade_id, trade_version), with
    merge semantics. That is precisely business rule 2: a same-version resend overwrites
    the version in place rather than creating a second row at the same grain. The
    superseded original is not lost -- it is in AUDIT.FCT_TRADE_REJECTED with
    disposition SUPERSEDED, and in the adjudication log in full.
*/

{{
    config(
        materialized = 'incremental',
        unique_key = ['trade_id', 'trade_version'],
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns',
        cluster_by = ['trade_date'],
        tags = ['core', 'audit']
    )
}}

{% set version_key = ['deduplicated.trade_id', 'deduplicated.trade_version'] %}

with accepted as (

    select * from {{ ref('int_trade_event_adjudicated') }}
    where verdict = 'ACCEPTED'
        and trade_id is not null
        and trade_version is not null

    {% if is_incremental() %}
    -- Bounded incremental scan. Correctness comes from the merge key, so this is a
    -- performance filter rather than a load-bearing one.
    and adjudicated_at > (select coalesce(max(t.dbt_updated_at), '1900-01-01'::timestamp_ltz) from {{ this }} as t)
    {% endif %}

),

deduplicated as (

    /*
        A single run can legitimately contain two accepted events at the same
        (trade_id, trade_version) -- an amendment to v3 followed later by a corrected
        resend of v3. Both are ACCEPTED, and both are correct.

        Snowflake's MERGE refuses a source with duplicate join keys when
        ERROR_ON_NONDETERMINISTIC_MERGE is on (it is -- set in the connection session
        parameters, deliberately, because silent arbitrary-winner behaviour is worse
        than a failed build). So the last writer is selected explicitly here.
    */
    select *
    from accepted
    qualify row_number() over (
            partition by trade_id, trade_version
            order by effective_event_ts desc, batch_seq desc, event_sk desc
        ) = 1

),

final as (

    select
        -- Grain -------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key(version_key) }} as trade_version_sk,
        deduplicated.trade_id,
        deduplicated.trade_version,
        deduplicated.action,
        deduplicated.version_action,
        deduplicated.uti,

        -- Economics ---------------------------------------------------------
        deduplicated.product_type,
        deduplicated.asset_class,
        deduplicated.buy_sell,
        deduplicated.notional_amount,
        deduplicated.notional_currency,
        deduplicated.settlement_currency,
        deduplicated.quantity,
        deduplicated.price,

        -- Signed notional. Precomputed because every downstream net-exposure query
        -- would otherwise re-derive it, and half of them would get the sign wrong.
        case
            when deduplicated.buy_sell = 'BUY' then deduplicated.notional_amount
            else -deduplicated.notional_amount
        end as signed_notional_amount,

        -- Dates -------------------------------------------------------------
        deduplicated.trade_date,
        deduplicated.settlement_date,
        deduplicated.maturity_date,
        datediff('day', deduplicated.trade_date, deduplicated.maturity_date) as tenor_days,

        -- Attribution -------------------------------------------------------
        deduplicated.counterparty_id,
        deduplicated.counterparty_name,
        deduplicated.counterparty_lei,
        deduplicated.counterparty_country_code,
        deduplicated.counterparty_credit_rating,
        deduplicated.book_id,
        deduplicated.book_name,
        deduplicated.desk,
        deduplicated.trader_id,
        deduplicated.execution_venue,
        deduplicated.clearing_house,
        deduplicated.legal_entity,

        -- Flags -------------------------------------------------------------
        array_contains('RJ018'::variant, deduplicated.warning_rule_codes) as is_limit_breach,
        array_contains('RJ019'::variant, deduplicated.warning_rule_codes) as is_duplicate_uti,
        deduplicated.warning_rule_codes,

        -- Provenance --------------------------------------------------------
        deduplicated.source_system,
        deduplicated.event_timestamp,
        deduplicated.source_file_name,
        deduplicated.source_file_row_number,
        deduplicated.event_sk,
        deduplicated.batch_id,

        -- dbt lineage -------------------------------------------------------
        deduplicated.dbt_invocation_id,
        deduplicated.adjudicated_at as dbt_created_at,
        current_timestamp() as dbt_updated_at

    from deduplicated

)

select * from final
