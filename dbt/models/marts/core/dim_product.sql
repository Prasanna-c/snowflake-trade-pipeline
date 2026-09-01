/*
    Product dimension with observed characteristics.

    The reference attributes come from the seed; the observed ones are computed from the
    book. Keeping both here makes a specific class of reference-data error visible:
    `requires_maturity = true` combined with `trades_missing_maturity > 0` means either the
    reference data is wrong or an upstream system is omitting a mandatory field, and
    neither is detectable from the seed alone.
*/

{{
    config(
        materialized = 'table',
        tags = ['core', 'dimension']
    )
}}

with reference as (

    select * from {{ ref('ref_product') }}

),

product_activity as (

    select
        product_type,
        count(*) as trade_count,
        count_if(is_live) as live_trade_count,
        count_if(is_expired) as expired_trade_count,
        count_if(is_cancelled) as cancelled_trade_count,
        count(distinct counterparty_id) as distinct_counterparty_count,
        count(distinct book_id) as distinct_book_count,
        count(distinct notional_currency) as distinct_currency_count,
        sum(notional_amount) as gross_notional,
        sum(iff(is_live, notional_amount, 0)) as gross_live_notional,
        avg(notional_amount) as avg_notional,
        median(notional_amount) as median_notional,
        avg(tenor_days) as avg_tenor_days,
        min(tenor_days) as min_tenor_days,
        max(tenor_days) as max_tenor_days,
        count_if(maturity_date is null) as trades_missing_maturity,
        max(trade_date) as last_trade_date
    from {{ ref('fct_trade') }}
    where product_type is not null
    group by product_type

),

rejection_activity as (

    select
        product_type,
        count(*) as rejected_event_count
    from {{ ref('fct_trade_rejected') }}
    where product_type is not null
      and disposition = 'REJECTED'
    group by product_type

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['reference.product_type']) }} as product_sk,
        reference.product_type,
        reference.asset_class,
        reference.requires_maturity,
        reference.is_physically_settled,

        coalesce(product_activity.trade_count, 0) as trade_count,
        coalesce(product_activity.live_trade_count, 0) as live_trade_count,
        coalesce(product_activity.expired_trade_count, 0) as expired_trade_count,
        coalesce(product_activity.cancelled_trade_count, 0) as cancelled_trade_count,
        coalesce(product_activity.distinct_counterparty_count, 0) as distinct_counterparty_count,
        coalesce(product_activity.distinct_book_count, 0) as distinct_book_count,
        coalesce(product_activity.distinct_currency_count, 0) as distinct_currency_count,
        coalesce(product_activity.gross_notional, 0) as gross_notional,
        coalesce(product_activity.gross_live_notional, 0) as gross_live_notional,
        product_activity.avg_notional,
        product_activity.median_notional,
        product_activity.avg_tenor_days,
        product_activity.min_tenor_days,
        product_activity.max_tenor_days,
        coalesce(product_activity.trades_missing_maturity, 0) as trades_missing_maturity,
        product_activity.last_trade_date,

        coalesce(rejection_activity.rejected_event_count, 0) as rejected_event_count,

        -- A reference-data inconsistency, surfaced as a queryable flag rather than left
        -- for someone to notice.
        reference.requires_maturity
        and coalesce(product_activity.trades_missing_maturity, 0) > 0
            as has_maturity_data_inconsistency,

        current_timestamp() as dbt_updated_at

    from reference
    left join product_activity
        on reference.product_type = product_activity.product_type
    left join rejection_activity
        on reference.product_type = rejection_activity.product_type

)

select * from final
