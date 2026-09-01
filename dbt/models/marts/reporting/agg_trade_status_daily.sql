/*
    The requirement's headline report: trade counts by status -- valid, expired, rejected --
    per day. This is the primary input to the Streamlit dashboard.

    ONE THING WORTH BEING PRECISE ABOUT: the two halves of this model are counted on
    different date grains, and conflating them is a genuine reporting error.

      Accepted trades are counted on TRADE_DATE   -- the business date of the trade.
      Rejected events are counted on REJECTED_ON  -- the date we processed them.

    A trade booked on Monday and rejected on Tuesday belongs to Monday's trade population
    and Tuesday's operational workload. Forcing both onto one date would make either the
    business view or the operations view wrong. The columns are named so the distinction
    is visible to whoever reads the output, and both dates are kept.
*/

{{
    config(
        materialized = 'view',
        tags = ['reporting', 'dashboard']
    )
}}

with date_spine as (

    /*
        A spine, so that a day with no trades appears as a zero rather than as a gap. A
        missing row in a time series is read by a human as "nothing happened" and by a
        chart as "no data" -- and the difference between those two matters when the
        question is whether the pipeline stopped.
    */
    select
        dateadd('day', -1 * row_number() over (order by seq4()), current_date() + 1) as calendar_date
    from table(generator(rowcount => 400))

),

accepted_by_trade_date as (

    select
        trade_date as calendar_date,
        count(*) as trade_count,
        count_if(lifecycle_status = 'LIVE') as live_count,
        count_if(lifecycle_status = 'EXPIRED') as expired_count,
        count_if(lifecycle_status = 'CANCELLED') as cancelled_count,
        count_if(is_expiring_soon) as expiring_soon_count,
        count_if(is_limit_breach) as limit_breach_count,
        count(distinct counterparty_id) as distinct_counterparty_count,
        count(distinct book_id) as distinct_book_count,
        count(distinct product_type) as distinct_product_count,
        sum(notional_amount) as gross_notional,
        sum(signed_notional_amount) as net_notional,
        avg(notional_amount) as avg_notional,
        max(notional_amount) as max_notional,
        avg(tenor_days) as avg_tenor_days,
        count_if(current_version > 1) as amended_trade_count
    from {{ ref('fct_trade') }}
    where trade_date is not null
    group by trade_date

),

rejected_by_processing_date as (

    select
        rejected_on as calendar_date,
        count_if(disposition = 'REJECTED') as rejected_event_count,
        count_if(disposition = 'SUPERSEDED') as superseded_event_count,
        count(distinct iff(disposition = 'REJECTED', trade_id, null)) as rejected_trade_count,
        count(distinct iff(disposition = 'REJECTED', primary_rule_code, null)) as distinct_rule_count,
        sum(iff(disposition = 'REJECTED', coalesce(notional_amount, 0), 0)) as rejected_notional,
        count_if(disposition = 'REJECTED' and has_multiple_violations) as multi_violation_count
    from {{ ref('fct_trade_rejected') }}
    where rejected_on is not null
    group by rejected_on

),

events_by_processing_date as (

    -- Total adjudicated volume, which is the denominator of the reject rate. Taken from
    -- the adjudication log rather than summing the two tables above, so the rate is
    -- computed against what was actually processed on that date.
    select
        adjudicated_at::date as calendar_date,
        count(*) as events_adjudicated,
        count_if(verdict = 'ACCEPTED') as events_accepted,
        count_if(verdict = 'REJECTED') as events_rejected,
        count_if(verdict = 'SUPERSEDED') as events_superseded,
        count(distinct batch_id) as batch_count
    from {{ ref('int_trade_event_adjudicated') }}
    group by adjudicated_at::date

),

final as (

    select
        date_spine.calendar_date,

        -- Business view: keyed on trade date -------------------------------
        coalesce(accepted.trade_count, 0) as trade_count,
        coalesce(accepted.live_count, 0) as live_count,
        coalesce(accepted.expired_count, 0) as expired_count,
        coalesce(accepted.cancelled_count, 0) as cancelled_count,
        coalesce(accepted.expiring_soon_count, 0) as expiring_soon_count,
        coalesce(accepted.limit_breach_count, 0) as limit_breach_count,
        coalesce(accepted.amended_trade_count, 0) as amended_trade_count,
        coalesce(accepted.distinct_counterparty_count, 0) as distinct_counterparty_count,
        coalesce(accepted.distinct_book_count, 0) as distinct_book_count,
        coalesce(accepted.distinct_product_count, 0) as distinct_product_count,
        coalesce(accepted.gross_notional, 0) as gross_notional,
        coalesce(accepted.net_notional, 0) as net_notional,
        accepted.avg_notional,
        accepted.max_notional,
        accepted.avg_tenor_days,

        -- Operational view: keyed on processing date -------------------------
        coalesce(events.events_adjudicated, 0) as events_adjudicated,
        coalesce(events.events_accepted, 0) as events_accepted,
        coalesce(events.events_rejected, 0) as events_rejected,
        coalesce(events.events_superseded, 0) as events_superseded,
        coalesce(events.batch_count, 0) as batch_count,
        coalesce(rejected.rejected_event_count, 0) as rejected_event_count,
        coalesce(rejected.superseded_event_count, 0) as superseded_event_count,
        coalesce(rejected.rejected_trade_count, 0) as rejected_trade_count,
        coalesce(rejected.distinct_rule_count, 0) as distinct_rule_count,
        coalesce(rejected.multi_violation_count, 0) as multi_violation_count,
        coalesce(rejected.rejected_notional, 0) as rejected_notional,

        -- Reject rate excludes SUPERSEDED from the numerator: a supersession is business
        -- rule 2 working, not a quality failure. Including it would make the alert
        -- threshold fire on healthy amendment traffic.
        round(
            100.0 * coalesce(events.events_rejected, 0)
            / nullif(coalesce(events.events_adjudicated, 0), 0),
            2
        ) as reject_rate_pct,

        round(
            100.0 * coalesce(accepted.expired_count, 0)
            / nullif(coalesce(accepted.trade_count, 0), 0),
            2
        ) as expired_rate_pct,

        -- 7-day trailing reject rate. A single day is noisy; the trend is what tells you
        -- whether an upstream system has regressed.
        round(
            100.0 * sum(coalesce(events.events_rejected, 0)) over (
                order by date_spine.calendar_date rows between 6 preceding and current row
            ) / nullif(
                sum(coalesce(events.events_adjudicated, 0)) over (
                    order by date_spine.calendar_date rows between 6 preceding and current row
                ), 0
            ),
            2
        ) as reject_rate_7d_pct,

        current_timestamp() as dbt_updated_at

    from date_spine
    left join accepted_by_trade_date as accepted
        on date_spine.calendar_date = accepted.calendar_date
    left join rejected_by_processing_date as rejected
        on date_spine.calendar_date = rejected.calendar_date
    left join events_by_processing_date as events
        on date_spine.calendar_date = events.calendar_date

)

select * from final
