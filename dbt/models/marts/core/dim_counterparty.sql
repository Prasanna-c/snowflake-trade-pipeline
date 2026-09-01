/*
    Counterparty dimension: reference attributes enriched with observed trading activity.

    The activity metrics are here rather than in a separate aggregate because they answer
    a question that only makes sense at counterparty grain -- "is this counterparty
    actually trading, and how much of our book is exposed to it" -- and because putting
    them here means a credit officer needs one table, not a join.

    Note `has_rejected_trades`: a counterparty that appears only in the reject table is a
    signal worth surfacing. It usually means an onboarding step was missed, and it is
    invisible if the dimension is built from accepted trades alone.
*/

{{
    config(
        materialized = 'table',
        tags = ['core', 'dimension']
    )
}}

with reference as (

    select * from {{ ref('ref_counterparty') }}

),

trade_activity as (

    select
        counterparty_id,
        count(*) as trade_count,
        count_if(is_live) as live_trade_count,
        count_if(is_expired) as expired_trade_count,
        count_if(is_cancelled) as cancelled_trade_count,
        count_if(is_limit_breach) as limit_breach_count,
        count(distinct product_type) as distinct_product_count,
        count(distinct notional_currency) as distinct_currency_count,
        sum(notional_amount) as gross_notional,
        -- Net exposure uses the signed notional, so a matched buy and sell nets to zero.
        -- This is the number a credit officer actually cares about.
        sum(iff(is_live, signed_notional_amount, 0)) as net_live_notional,
        sum(iff(is_live, notional_amount, 0)) as gross_live_notional,
        min(trade_date) as first_trade_date,
        max(trade_date) as last_trade_date,
        max(maturity_date) as furthest_maturity_date
    from {{ ref('fct_trade') }}
    where counterparty_id is not null
    group by counterparty_id

),

rejection_activity as (

    select
        counterparty_id,
        count(*) as rejected_event_count,
        max(rejected_at) as last_rejected_at
    from {{ ref('fct_trade_rejected') }}
    where counterparty_id is not null
      and disposition = 'REJECTED'
    group by counterparty_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['reference.counterparty_id']) }} as counterparty_sk,
        reference.counterparty_id,
        reference.counterparty_name,
        reference.lei,
        reference.country_code,
        reference.credit_rating,
        reference.is_active,

        -- Investment grade boundary. Encoded once here rather than in every report that
        -- needs it, because the cut-off is a policy decision and policies change.
        reference.credit_rating in (
            'AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'
        ) as is_investment_grade,

        coalesce(trade_activity.trade_count, 0) as trade_count,
        coalesce(trade_activity.live_trade_count, 0) as live_trade_count,
        coalesce(trade_activity.expired_trade_count, 0) as expired_trade_count,
        coalesce(trade_activity.cancelled_trade_count, 0) as cancelled_trade_count,
        coalesce(trade_activity.limit_breach_count, 0) as limit_breach_count,
        coalesce(trade_activity.distinct_product_count, 0) as distinct_product_count,
        coalesce(trade_activity.distinct_currency_count, 0) as distinct_currency_count,
        coalesce(trade_activity.gross_notional, 0) as gross_notional,
        coalesce(trade_activity.net_live_notional, 0) as net_live_notional,
        coalesce(trade_activity.gross_live_notional, 0) as gross_live_notional,
        trade_activity.first_trade_date,
        trade_activity.last_trade_date,
        trade_activity.furthest_maturity_date,

        coalesce(rejection_activity.rejected_event_count, 0) as rejected_event_count,
        rejection_activity.last_rejected_at,
        coalesce(rejection_activity.rejected_event_count, 0) > 0 as has_rejected_trades,

        -- An inactive counterparty with live trades is a control finding, not a data
        -- point. Surfacing it as a flag makes it a one-line query for the risk team.
        not reference.is_active
        and coalesce(trade_activity.live_trade_count, 0) > 0 as has_live_trades_while_inactive,

        current_timestamp() as dbt_updated_at

    from reference
    left join trade_activity
        on reference.counterparty_id = trade_activity.counterparty_id
    left join rejection_activity
        on reference.counterparty_id = rejection_activity.counterparty_id

)

select * from final
