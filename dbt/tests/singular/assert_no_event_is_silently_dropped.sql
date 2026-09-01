/*
    THE MOST IMPORTANT TEST IN THE PROJECT.

    Every event that entered the queue must have reached exactly one of three destinations:

        ACCEPTED    -> CORE.FCT_TRADE_VERSION
        REJECTED    -> AUDIT.FCT_TRADE_REJECTED
        SUPERSEDED  -> AUDIT.FCT_TRADE_REJECTED (disposition = 'SUPERSEDED')

    An event that reached none of them has been silently dropped. In a regulated trade
    pipeline that is the worst possible failure: nothing is red, no alert fires, the reject
    rate looks healthy, and a trade has simply ceased to exist. It is also the failure mode
    that every other test misses, because every other test validates rows that ARE present.

    Why a completeness test is needed at all when the models look obviously exhaustive: the
    ways an event disappears are all invisible in review --

      * an incremental watermark that skips a batch when two drains share a batch_seq,
      * a rule condition returning NULL rather than TRUE or FALSE, so the event falls
        through every CASE branch,
      * a downstream model whose own watermark advances past rows it never wrote,
      * an inner join added to a model that was written with a left join.

    Each of those is a one-line change that passes code review. This test catches all four.

    Scoped to the last 7 days. Over the full history the anti-join would grow without bound,
    and a drop older than a week has already been caught by a previous run.
*/

{{ config(severity = 'error', tags = ['completeness', 'critical']) }}

with adjudicated as (

    select
        event_sk,
        trade_id,
        trade_version,
        verdict,
        batch_id,
        source_file_name,
        source_file_row_number,
        adjudicated_at
    from {{ ref('int_trade_event_adjudicated') }}
    where adjudicated_at >= dateadd('day', -7, current_timestamp())

),

landed_in_core as (

    select event_sk from {{ ref('fct_trade_version') }}

),

landed_in_audit as (

    select event_sk from {{ ref('fct_trade_rejected') }}

),

orphaned as (

    select
        adjudicated.event_sk,
        adjudicated.trade_id,
        adjudicated.trade_version,
        adjudicated.verdict,
        adjudicated.batch_id,
        adjudicated.source_file_name,
        adjudicated.source_file_row_number,
        adjudicated.adjudicated_at,
        case
            when adjudicated.verdict = 'ACCEPTED'
                then 'ACCEPTED but absent from FCT_TRADE_VERSION'
            when adjudicated.verdict in ('REJECTED', 'SUPERSEDED')
                then 'REJECTED/SUPERSEDED but absent from FCT_TRADE_REJECTED'
            else 'unrecognised verdict: ' || coalesce(adjudicated.verdict, '<null>')
        end as failure_reason

    from adjudicated
    left join landed_in_core
        on adjudicated.event_sk = landed_in_core.event_sk
    left join landed_in_audit
        on adjudicated.event_sk = landed_in_audit.event_sk

    where
        -- An accepted event must be in CORE. The one legitimate exception: business rule 2
        -- means two accepted events can share a (trade_id, version), and the version ledger
        -- keeps only the winner. Those are excluded by the NOT EXISTS below rather than by
        -- being ignored, so a genuine drop is still caught.
        (
            adjudicated.verdict = 'ACCEPTED'
            and landed_in_core.event_sk is null
            and not exists (
                select 1
                from {{ ref('fct_trade_version') }} as fv
                where fv.trade_id = adjudicated.trade_id
                  and fv.trade_version = adjudicated.trade_version
            )
        )
        -- A rejected or superseded event must be in AUDIT, with no exceptions at all.
        or (
            adjudicated.verdict in ('REJECTED', 'SUPERSEDED')
            and landed_in_audit.event_sk is null
        )
        -- A verdict outside the three known values means the CASE in the adjudication
        -- model has grown a branch nobody accounted for.
        or adjudicated.verdict not in ('ACCEPTED', 'REJECTED', 'SUPERSEDED')
        or adjudicated.verdict is null

)

select * from orphaned
