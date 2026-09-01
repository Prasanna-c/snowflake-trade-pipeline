/*
    The threshold that the dbt scorecard and the Snowflake SLA view both implement must
    produce the same verdict in both.

    WHY HEALTH IS COMPUTED TWICE.

      MONITORING.VW_PIPELINE_SLA            pure SQL, deployed by the Snowflake layer, and
                                            therefore still answerable when dbt is broken --
                                            which is exactly when you want to know.

      REPORTING.RPT_DATA_QUALITY_SCORECARD  a dbt model, so the thresholds are testable,
                                            reviewable and versioned with the transformations.

    WHY THIS TEST DOES NOT COMPARE THE TWO HEADLINE VERDICTS.

    They are not measurements of the same thing. The view reports four per-domain statuses over
    RAW-layer facts: file arrival, stream drain, transform backlog and dbt run history. The
    scorecard reports one rollup over curated-layer facts: reject rate, parse errors, queue depth
    and adjudication recency. Neither is a subset of the other.

    So "worst of the view's four equals the scorecard's rollup" is not an invariant. It would fail
    whenever ingestion is stale while the curated layer is entirely healthy -- the normal state of
    a laptop between demo runs, and of production overnight. A control that fails routinely gets
    relaxed until it means nothing, which is worse than not having it.

    WHAT IS GENUINELY SHARED.

    The expiry canary: a matured trade still marked LIVE means the daily sweep has not run. Both
    layers count it, both call it RED above zero, and the two code paths are independent. If they
    disagree, one has been edited without the other -- which is the drift this test exists to catch.

    It also asserts that the canary is not masked in the scorecard's rollup. That rollup is
    first-match-wins, so moving the expiry condition below a softer one would silently turn a RED
    into an AMBER and nothing else in the platform would notice.

    One accepted false positive: the scorecard measures against `business_date` while the view uses
    `current_date()`. These agree on every scheduled run and diverge only during a deliberate
    backfill (`--vars business_date=...`), where a failure here is a true statement -- the SQL view
    has no way to know you moved the as-of date.

    Requires the monitoring layer to be deployed (`make deploy-sql`). Referenced directly rather
    than through a source, because MONITORING is created by versioned SQL rather than by dbt, and
    declaring it as a source would imply dbt could rebuild it.
*/

{{ config(severity = 'error', tags = ['monitoring', 'consistency']) }}

with dbt_verdict as (

    select
        overall_status,
        overdue_expiry_trades
    from {{ ref('rpt_data_quality_scorecard') }}

),

sql_verdict as (

    select
        correctness_status,
        trades_overdue_for_expiry
    from {{ target.database }}.monitoring.vw_pipeline_sla

),

-- Both sides are single-row by construction, so this is a join of one row to one row.
both_layers as (

    select
        dbt_verdict.overall_status,
        dbt_verdict.overdue_expiry_trades,
        sql_verdict.correctness_status,
        sql_verdict.trades_overdue_for_expiry
    from dbt_verdict
    cross join sql_verdict

),

-- Two independent implementations of "is any matured trade still LIVE" reaching opposite
-- conclusions. Compared as booleans rather than as counts: the counts can legitimately differ
-- by a trade booked between the two evaluations, but the verdict must not.
canary_disagrees as (

    select
        both_layers.overall_status,
        both_layers.correctness_status,
        both_layers.overdue_expiry_trades,
        both_layers.trades_overdue_for_expiry,
        'EXPIRY_CANARY_DISAGREEMENT: the dbt scorecard and MONITORING.VW_PIPELINE_SLA no longer '
        || 'agree on whether the expiry sweep has run. Reconcile the condition in '
        || 'models/marts/reporting/rpt_data_quality_scorecard.sql and '
        || 'snowflake/30_monitoring/01_freshness_and_health.sql.' as discrepancy
    from both_layers
    where (both_layers.overdue_expiry_trades > 0)
        <> (both_layers.correctness_status = 'RED')

),

canary_masked as (

    select
        both_layers.overall_status,
        both_layers.correctness_status,
        both_layers.overdue_expiry_trades,
        both_layers.trades_overdue_for_expiry,
        'EXPIRY_CANARY_MASKED: matured trades are still LIVE but the scorecard rollup is not RED. '
        || 'A softer condition has been ordered ahead of the expiry check in '
        || 'models/marts/reporting/rpt_data_quality_scorecard.sql.' as discrepancy
    from both_layers
    where both_layers.overdue_expiry_trades > 0
        and both_layers.overall_status <> 'RED'

)

select * from canary_disagrees
union all
select * from canary_masked
