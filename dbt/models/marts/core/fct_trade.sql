/*
    ============================================================================
    THE GOLDEN RECORD. One row per trade, holding its current version and lifecycle
    state. This is the table risk, P&L and every dashboard read.

    ---------------------------------------------------------------------------
    BUSINESS RULE 4 -- "mark trades as expired if the maturity date has passed" --
    IS IMPLEMENTED HERE, AND THE HOW MATTERS.

    The naive approach is a derived column: `case when maturity_date < current_date()
    then 'EXPIRED' end`. In an incremental model that is quietly broken. A trade that
    matured yesterday and received no new events today is not in the incremental batch,
    so its row is never rewritten and it keeps reporting LIVE forever. The model looks
    correct in review and is wrong in production.

    The alternative is a post-hook UPDATE, which works but hides a state transition
    outside the model's SELECT, where no one reviewing the lineage will find it.

    What this model does instead: the incremental source is a UNION of two populations --

        (a) trades with newly accepted events in this run, and
        (b) trades ALREADY IN THIS TABLE whose maturity date has now passed while their
            status still says LIVE.

    Because the materialization merges on trade_id, re-emitting population (b) updates
    those rows in place. The expiry transition is therefore an ordinary part of the
    model's SELECT: visible in the code, covered by the model's tests, and impossible to
    forget. It is also cheap -- (b) is a predicate on maturity_date against a clustered
    table, so it touches only the micro-partitions that can possibly qualify.

    ---------------------------------------------------------------------------
    SINGLE WRITER

    dbt is the only writer to this table. There is deliberately no Snowflake task
    performing the expiry sweep, because two writers merging on the same trade_id will
    eventually lose an amendment in a way that is almost impossible to reproduce.

    The cost of that choice is that the expiry sweep only happens when dbt runs. That is
    covered by a detective control rather than a second writer: ALERT_EXPIRY_OVERDUE
    fires whenever a matured trade is still LIVE, which catches a missed dbt run within
    the hour. Prevention in one place, detection in another.

    ---------------------------------------------------------------------------
    ORDER OF PRECEDENCE FOR STATUS

    CANCELLED beats EXPIRED beats LIVE. A cancelled trade that later passes its maturity
    date must stay CANCELLED -- it was withdrawn, and it never matured. Getting this
    backwards would silently resurrect cancelled trades into the expiring-soon report.
    ============================================================================
*/

{{
    config(
        materialized = 'incremental',
        unique_key = 'trade_id',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns',
        cluster_by = ['maturity_date'],
        tags = ['core', 'critical', 'daily']
    )
}}

{% set business_date = "to_date('" ~ var('business_date') ~ "')" %}

with

-- ---------------------------------------------------------------------------
-- (a) Trades touched by this run. One row per trade: the highest accepted version.
--
-- Ordering by trade_version first, then business time, then arrival: version is the
-- authority on which record is current, and the rest only break ties among equal
-- versions (which business rule 2 permits).
-- ---------------------------------------------------------------------------
newly_accepted as (

    select *
    from {{ ref('fct_trade_version') }}

{% if is_incremental() %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
{% endif %}

    qualify row_number() over (
        partition by trade_id
        order by trade_version desc, event_timestamp desc, event_sk desc
    ) = 1

),

-- ---------------------------------------------------------------------------
-- (b) The expiry sweep. Only on an incremental run -- on a full refresh, population
-- (a) is the entire history and every status is computed from scratch anyway.
-- ---------------------------------------------------------------------------
needs_expiry as (

{% if is_incremental() %}
    select existing.*
    from {{ this }} as existing
    where existing.maturity_date is not null
      and existing.maturity_date < {{ business_date }}
      and existing.lifecycle_status = 'LIVE'
      -- Exclude trades that population (a) is about to rewrite. Without this a trade
      -- both amended today and past maturity would appear twice in the merge source,
      -- and Snowflake would reject the non-deterministic MERGE.
      and not exists (
          select 1 from newly_accepted as na where na.trade_id = existing.trade_id
      )
{% else %}
    select
        cast(null as varchar) as trade_id
    where false
{% endif %}

),

-- ---------------------------------------------------------------------------
-- Population (a), shaped into the final grain.
-- ---------------------------------------------------------------------------
from_new_events as (

    select
        newly_accepted.trade_id,
        newly_accepted.trade_version as current_version,
        newly_accepted.action as last_action,
        newly_accepted.version_action as last_version_action,
        newly_accepted.uti,

        newly_accepted.product_type,
        newly_accepted.asset_class,
        newly_accepted.buy_sell,
        newly_accepted.notional_amount,
        newly_accepted.notional_currency,
        newly_accepted.settlement_currency,
        newly_accepted.signed_notional_amount,
        newly_accepted.quantity,
        newly_accepted.price,

        newly_accepted.trade_date,
        newly_accepted.settlement_date,
        newly_accepted.maturity_date,
        newly_accepted.tenor_days,

        newly_accepted.counterparty_id,
        newly_accepted.counterparty_name,
        newly_accepted.counterparty_lei,
        newly_accepted.counterparty_country_code,
        newly_accepted.counterparty_credit_rating,
        newly_accepted.book_id,
        newly_accepted.book_name,
        newly_accepted.desk,
        newly_accepted.trader_id,
        newly_accepted.execution_venue,
        newly_accepted.clearing_house,
        newly_accepted.legal_entity,

        newly_accepted.is_limit_breach,
        newly_accepted.is_duplicate_uti,
        newly_accepted.warning_rule_codes,

        newly_accepted.source_system,
        newly_accepted.event_timestamp as last_event_timestamp,
        newly_accepted.source_file_name as last_source_file_name,
        newly_accepted.batch_id as last_batch_id,
        newly_accepted.event_sk as last_event_sk,

        -- Lifecycle. CANCELLED wins over EXPIRED wins over LIVE.
        case
            when newly_accepted.action = 'CANCEL' then 'CANCELLED'
            when newly_accepted.maturity_date is null then 'LIVE'
            when newly_accepted.maturity_date < {{ business_date }} then 'EXPIRED'
            else 'LIVE'
        end as lifecycle_status,

        -- Null unless the trade is already terminal on arrival, which only happens for
        -- a cancellation or a back-booked matured trade.
        case
            when newly_accepted.action = 'CANCEL' then current_timestamp()
            when newly_accepted.maturity_date < {{ business_date }} then current_timestamp()
        end as status_changed_at,

        false as is_expiry_sweep_update,
        newly_accepted.dbt_invocation_id

    from newly_accepted

),

-- ---------------------------------------------------------------------------
-- Population (b), re-emitted with the transitioned status. Every other column is
-- carried through untouched: this is a status transition, not a restatement, and
-- rewriting economics here would falsify the record.
-- ---------------------------------------------------------------------------
from_expiry_sweep as (

{% if is_incremental() %}
    select
        needs_expiry.trade_id,
        needs_expiry.current_version,
        needs_expiry.last_action,
        needs_expiry.last_version_action,
        needs_expiry.uti,

        needs_expiry.product_type,
        needs_expiry.asset_class,
        needs_expiry.buy_sell,
        needs_expiry.notional_amount,
        needs_expiry.notional_currency,
        needs_expiry.settlement_currency,
        needs_expiry.signed_notional_amount,
        needs_expiry.quantity,
        needs_expiry.price,

        needs_expiry.trade_date,
        needs_expiry.settlement_date,
        needs_expiry.maturity_date,
        needs_expiry.tenor_days,

        needs_expiry.counterparty_id,
        needs_expiry.counterparty_name,
        needs_expiry.counterparty_lei,
        needs_expiry.counterparty_country_code,
        needs_expiry.counterparty_credit_rating,
        needs_expiry.book_id,
        needs_expiry.book_name,
        needs_expiry.desk,
        needs_expiry.trader_id,
        needs_expiry.execution_venue,
        needs_expiry.clearing_house,
        needs_expiry.legal_entity,

        needs_expiry.is_limit_breach,
        needs_expiry.is_duplicate_uti,
        needs_expiry.warning_rule_codes,

        needs_expiry.source_system,
        needs_expiry.last_event_timestamp,
        needs_expiry.last_source_file_name,
        needs_expiry.last_batch_id,
        needs_expiry.last_event_sk,

        'EXPIRED' as lifecycle_status,
        current_timestamp() as status_changed_at,

        -- Distinguishes "this row changed because a trade event arrived" from "this row
        -- changed because time passed". The SCD2 snapshot uses it to explain a version
        -- that has no corresponding trade event.
        true as is_expiry_sweep_update,
        '{{ invocation_id }}' as dbt_invocation_id

    from needs_expiry
{% else %}
    select * from from_new_events where false
{% endif %}

),

combined as (

    select * from from_new_events
    union all
    select * from from_expiry_sweep

),

final as (

    select
        combined.*,

        -- Convenience flags. Derived rather than stored so they can never disagree
        -- with lifecycle_status.
        combined.lifecycle_status = 'LIVE' as is_live,
        combined.lifecycle_status = 'EXPIRED' as is_expired,
        combined.lifecycle_status = 'CANCELLED' as is_cancelled,

        case
            when combined.maturity_date is null then null
            else datediff('day', {{ business_date }}, combined.maturity_date)
        end as days_to_maturity,

        combined.lifecycle_status = 'LIVE'
        and combined.maturity_date is not null
        and combined.maturity_date
            between {{ business_date }}
            and dateadd('day', {{ var('expiring_soon_days') }}, {{ business_date }})
            as is_expiring_soon,

        current_timestamp() as dbt_updated_at

    from combined

)

select * from final
