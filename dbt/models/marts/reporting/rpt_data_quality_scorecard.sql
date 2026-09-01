/*
    One row: the current state of the platform, as a scorecard.

    Everything here exists elsewhere in more detail. The point of collapsing it into a
    single row is that a dashboard header, a Slack digest and an on-call engineer at 3am
    all want the same three-second answer, and none of them should have to aggregate four
    tables to get it.

    Health is computed here and again in MONITORING.VW_PIPELINE_SLA, deliberately, so that
    each layer still answers when the other is broken. They are not the same measurement:
    this rollup reads curated-layer facts (reject rate, parse errors, queue depth, adjudication
    recency) while the view reads RAW-layer facts (file arrival, drain, backlog, dbt runs), so
    their headline verdicts can legitimately differ. What must agree is the expiry canary, which
    both implement, and tests/singular/assert_sla_thresholds_agree.sql asserts exactly that.
*/

{{
    config(
        materialized = 'view',
        tags = ['reporting', 'dashboard', 'data_quality', 'monitoring']
    )
}}

{% set business_date = "to_date('" ~ var('business_date') ~ "')" %}

with trade_totals as (

    select
        count(*) as total_trades,
        count_if(lifecycle_status = 'LIVE') as live_trades,
        count_if(lifecycle_status = 'EXPIRED') as expired_trades,
        count_if(lifecycle_status = 'CANCELLED') as cancelled_trades,
        count_if(is_expiring_soon) as expiring_soon_trades,
        count_if(is_limit_breach) as limit_breach_trades,
        count_if(is_duplicate_uti) as duplicate_uti_trades,
        count_if(current_version > 1) as amended_trades,
        sum(iff(lifecycle_status = 'LIVE', notional_amount, 0)) as live_gross_notional,
        max(last_event_timestamp) as latest_trade_event_ts,

        -- The staleness canary.
        count_if(
            lifecycle_status = 'LIVE'
            and maturity_date is not null
            and maturity_date < {{ business_date }}
        ) as overdue_expiry_trades

    from {{ ref('fct_trade') }}

),

event_totals as (

    select
        count(*) as total_events,
        count_if(verdict = 'ACCEPTED') as accepted_events,
        count_if(verdict = 'REJECTED') as rejected_events,
        count_if(verdict = 'SUPERSEDED') as superseded_events,
        max(adjudicated_at) as last_adjudicated_at,

        count_if(adjudicated_at >= dateadd('hour', -24, current_timestamp())) as events_last_24h,
        count_if(
            verdict = 'REJECTED' and adjudicated_at >= dateadd('hour', -24, current_timestamp())
        ) as rejected_last_24h

    from {{ ref('int_trade_event_adjudicated') }}

),

queue_depth as (

    -- Rows drained from the stream but not yet adjudicated: the transform backlog.
    select count(*) as pending_events
    from {{ source('raw', 'trade_event_queue') }} as q
    where not exists (
            select 1
            from {{ ref('int_trade_event_adjudicated') }} as a
            where a.event_sk = q.event_sk
        )

),

parse_errors as (

    select
        count(*) as total_parse_errors,
        count_if(logged_at >= dateadd('hour', -24, current_timestamp())) as parse_errors_last_24h
    from {{ source('raw', 'copy_error') }}

),

rule_coverage as (

    -- Which of the declared rules have ever actually fired. A rule that has never fired
    -- is either genuinely never violated or is silently broken, and the two look
    -- identical from a passing test suite.
    select count(distinct rule_code) as rules_ever_fired
    from {{ ref('trade_rule_result') }}

),

declared_rules as (

    select count(*) as rules_declared
    from {{ ref('ref_rejection_reason') }}

),

final as (

    select
        -- Volumes ----------------------------------------------------------
        trade_totals.total_trades,
        trade_totals.live_trades,
        trade_totals.expired_trades,
        trade_totals.cancelled_trades,
        trade_totals.expiring_soon_trades,
        trade_totals.amended_trades,
        trade_totals.limit_breach_trades,
        trade_totals.duplicate_uti_trades,
        trade_totals.live_gross_notional,

        event_totals.total_events,
        event_totals.accepted_events,
        event_totals.rejected_events,
        event_totals.superseded_events,
        event_totals.events_last_24h,
        event_totals.rejected_last_24h,

        queue_depth.pending_events,
        parse_errors.total_parse_errors,
        parse_errors.parse_errors_last_24h,

        -- Rates ------------------------------------------------------------
        round(
            100.0 * event_totals.rejected_events / nullif(event_totals.total_events, 0), 2
        ) as reject_rate_pct,
        round(
            100.0 * event_totals.rejected_last_24h / nullif(event_totals.events_last_24h, 0), 2
        ) as reject_rate_24h_pct,
        round(
            100.0 * event_totals.superseded_events / nullif(event_totals.total_events, 0), 2
        ) as supersede_rate_pct,
        round(
            100.0 * trade_totals.amended_trades / nullif(trade_totals.total_trades, 0), 2
        ) as amendment_rate_pct,

        -- Coverage ---------------------------------------------------------
        declared_rules.rules_declared,
        rule_coverage.rules_ever_fired,
        declared_rules.rules_declared - rule_coverage.rules_ever_fired as rules_never_fired,

        -- Freshness --------------------------------------------------------
        event_totals.last_adjudicated_at,
        timestampdiff('minute', event_totals.last_adjudicated_at, current_timestamp())
            as minutes_since_last_adjudication,
        trade_totals.latest_trade_event_ts,

        -- Correctness canary -------------------------------------------------
        trade_totals.overdue_expiry_trades,

        -- RAG rollup over curated-layer facts. First match wins, so the ordering is itself the
        -- diagnosis; the expiry canary is first because it is the only condition that positively
        -- proves no build completed. Reordering it below a softer condition is a test failure.
        case
            when trade_totals.overdue_expiry_trades > 0 then 'RED'
            when queue_depth.pending_events > 100000 then 'RED'
            when timestampdiff('minute', event_totals.last_adjudicated_at, current_timestamp()) > 180
                then 'RED'
            when round(
                    100.0 * event_totals.rejected_last_24h / nullif(event_totals.events_last_24h, 0), 2
                ) > 25 then 'RED'
            when queue_depth.pending_events > 10000 then 'AMBER'
            when timestampdiff('minute', event_totals.last_adjudicated_at, current_timestamp()) > 90
                then 'AMBER'
            when round(
                    100.0 * event_totals.rejected_last_24h / nullif(event_totals.events_last_24h, 0), 2
                ) > 15 then 'AMBER'
            when parse_errors.parse_errors_last_24h > 0 then 'AMBER'
            else 'GREEN'
        end as overall_status,

        current_timestamp() as evaluated_at

    from trade_totals
    cross join event_totals
    cross join queue_depth
    cross join parse_errors
    cross join rule_coverage
    cross join declared_rules

)

select * from final
