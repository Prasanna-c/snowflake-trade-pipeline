/*
    Book dimension with limit utilisation.

    `limit_utilisation_pct` is the reason this model earns its place: it turns the
    per-trade RJ018 warning into a book-level position. One trade over the limit is a
    warning; a book at 140% of its limit across many trades is a risk breach that no
    per-trade rule can see, because each individual trade is within limit.
*/

{{
    config(
        materialized = 'table',
        tags = ['core', 'dimension']
    )
}}

with reference as (

    select * from {{ ref('ref_book') }}

),

book_activity as (

    select
        book_id,
        count(*) as trade_count,
        count_if(is_live) as live_trade_count,
        count_if(is_expired) as expired_trade_count,
        count_if(is_cancelled) as cancelled_trade_count,
        count_if(is_limit_breach) as limit_breach_count,
        count(distinct counterparty_id) as distinct_counterparty_count,
        count(distinct trader_id) as distinct_trader_count,
        sum(iff(is_live, notional_amount, 0)) as gross_live_notional,
        sum(iff(is_live, signed_notional_amount, 0)) as net_live_notional,
        max(notional_amount) as largest_trade_notional,
        max(trade_date) as last_trade_date
    from {{ ref('fct_trade') }}
    where book_id is not null
    group by book_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['reference.book_id']) }} as book_sk,
        reference.book_id,
        reference.book_name,
        reference.desk,
        reference.legal_entity,
        reference.notional_limit,

        coalesce(book_activity.trade_count, 0) as trade_count,
        coalesce(book_activity.live_trade_count, 0) as live_trade_count,
        coalesce(book_activity.expired_trade_count, 0) as expired_trade_count,
        coalesce(book_activity.cancelled_trade_count, 0) as cancelled_trade_count,
        coalesce(book_activity.limit_breach_count, 0) as limit_breach_count,
        coalesce(book_activity.distinct_counterparty_count, 0) as distinct_counterparty_count,
        coalesce(book_activity.distinct_trader_count, 0) as distinct_trader_count,
        coalesce(book_activity.gross_live_notional, 0) as gross_live_notional,
        coalesce(book_activity.net_live_notional, 0) as net_live_notional,
        book_activity.largest_trade_notional,
        book_activity.last_trade_date,

        -- Utilisation on the GROSS live position: the limit is a capacity constraint, so
        -- offsetting longs and shorts do not release it.
        round(
            100.0 * coalesce(book_activity.gross_live_notional, 0)
            / nullif(reference.notional_limit, 0),
            2
        ) as limit_utilisation_pct,

        case
            when coalesce(book_activity.gross_live_notional, 0) > reference.notional_limit
                then 'BREACH'
            when coalesce(book_activity.gross_live_notional, 0) > 0.8 * reference.notional_limit
                then 'WARNING'
            else 'OK'
        end as limit_status,

        current_timestamp() as dbt_updated_at

    from reference
    left join book_activity
        on reference.book_id = book_activity.book_id

)

select * from final
