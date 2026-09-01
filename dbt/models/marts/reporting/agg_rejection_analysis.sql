/*
    Rejection analysis by rule and source system.

    The purpose is to convert a pile of rejections into a short list of conversations. A
    reject rate is a symptom; "RJ006 accounts for 71% of rejections and 94% of those come
    from MUREX" is a diagnosis, and it names the team to talk to.

    `is_concentrated` is the column that does the work: a rule failing overwhelmingly from
    one source is an upstream contract change, whereas the same rule failing evenly across
    all sources is more likely our reference data being stale. Those need opposite
    responses, and the distinction is not visible from a total count.
*/

{{
    config(
        materialized = 'view',
        tags = ['reporting', 'dashboard', 'data_quality']
    )
}}

with rule_hits as (

    select * from {{ ref('trade_rule_result') }}

),

by_rule_and_source as (

    select
        rule_code,
        rule_name,
        rule_category,
        rule_severity,
        requirement_ref,
        remediation,
        rule_phase,
        is_blocking,
        coalesce(source_system, 'UNKNOWN') as source_system,

        count(*) as hit_count,
        count(distinct trade_id) as distinct_trade_count,
        count(distinct source_file_name) as distinct_file_count,
        count(distinct desk) as distinct_desk_count,
        sum(coalesce(notional_amount, 0)) as affected_notional,

        min(evaluated_at) as first_seen_at,
        max(evaluated_at) as last_seen_at,

        count_if(evaluated_at >= dateadd('day', -1, current_timestamp())) as hits_last_24h,
        count_if(evaluated_at >= dateadd('day', -7, current_timestamp())) as hits_last_7d,
        count_if(evaluated_at >= dateadd('day', -30, current_timestamp())) as hits_last_30d

    from rule_hits
    group by all

),

-- Rule-level totals are materialised in their own step because the ranking below needs
-- to order by one of them. Snowflake cannot nest a window function inside another
-- window function's ORDER BY, so the aggregate has to become a column first.
rule_totals as (

    select
        by_rule_and_source.*,

        sum(by_rule_and_source.hit_count) over (partition by by_rule_and_source.rule_code) as rule_total_hits,
        sum(by_rule_and_source.hit_count) over () as all_rules_total_hits,
        sum(by_rule_and_source.hits_last_7d) over (partition by by_rule_and_source.rule_code) as rule_hits_last_7d,
        count(*) over (partition by by_rule_and_source.rule_code) as sources_affected

    from by_rule_and_source

),

with_shares as (

    select
        rule_totals.*,

        -- Share of this rule's hits attributable to this source system.
        round(100.0 * rule_totals.hit_count / nullif(rule_totals.rule_total_hits, 0), 2) as share_of_rule_pct,

        -- Share of all hits attributable to this rule.
        round(100.0 * rule_totals.rule_total_hits / nullif(rule_totals.all_rules_total_hits, 0), 2)
            as rule_share_of_all_pct,

        -- Rank rules by recent volume, so a dashboard can show "top 5 problems now"
        -- rather than "top 5 problems since inception", which stops being actionable
        -- after the first month.
        dense_rank() over (
            order by rule_totals.rule_hits_last_7d desc
        ) as rule_rank_last_7d

    from rule_totals

),

final as (

    select
        with_shares.rule_code,
        with_shares.rule_name,
        with_shares.rule_category,
        with_shares.rule_severity,
        with_shares.requirement_ref,
        with_shares.rule_phase,
        with_shares.is_blocking,
        with_shares.source_system,

        with_shares.hit_count,
        with_shares.distinct_trade_count,
        with_shares.distinct_file_count,
        with_shares.distinct_desk_count,
        with_shares.affected_notional,

        with_shares.hits_last_24h,
        with_shares.hits_last_7d,
        with_shares.hits_last_30d,

        with_shares.share_of_rule_pct,
        with_shares.rule_share_of_all_pct,
        with_shares.rule_total_hits,
        with_shares.sources_affected,
        with_shares.rule_rank_last_7d,

        with_shares.first_seen_at,
        with_shares.last_seen_at,

        -- Concentrated in one source: an upstream contract change. Spread evenly: more
        -- likely our own reference data. Different fix, different owner.
        with_shares.share_of_rule_pct >= 80.0 and with_shares.sources_affected > 1
            as is_concentrated,

        -- Appeared for the first time in the last day. New failure modes deserve
        -- attention out of proportion to their volume.
        with_shares.first_seen_at >= dateadd('day', -1, current_timestamp()) as is_new_failure_mode,

        -- Firing continuously for a month with nobody fixing it. Either the rule is
        -- wrong, or it is being ignored -- both worth knowing.
        with_shares.hits_last_30d > 0
        and with_shares.hits_last_24h > 0
        and with_shares.first_seen_at < dateadd('day', -30, current_timestamp())
            as is_chronic,

        with_shares.remediation,
        current_timestamp() as dbt_updated_at

    from with_shares

)

select * from final
