/*
    Accepted version numbers per trade must be contiguous from 1.

    A trade holding v1, v2 and v4 with no v3 means one of two things, and both matter:

      * v3 was accepted and then lost -- silent data loss in the version ledger, or
      * v3 was rejected, and v4 was then accepted anyway.

    The second is the interesting one. If v3 was rejected for a bad currency and v4 arrives
    valid, accepting v4 is correct: the trade is now at v4 and the gap is a true record of
    the upstream system's numbering. So a gap is not automatically a bug -- which is why
    this test excludes gaps that are explained by a rejection at exactly the missing version.

    What remains after that exclusion is a gap with no explanation anywhere in the pipeline,
    and that is a genuine defect.

    Severity is WARN rather than ERROR. Upstream systems do legitimately skip version numbers
    (a booking system that allocates a version, fails validation internally and allocates the
    next one), so an unexplained gap is a question for the upstream team, not a reason to stop
    the pipeline. Blocking on it would mean one upstream quirk halts every other trade.
*/

{{ config(severity = 'warn', tags = ['completeness'] ) }}

with versions as (

    select
        trade_id,
        trade_version
    from {{ ref('fct_trade_version') }}
    where trade_id is not null

),

with_previous as (

    select
        trade_id,
        trade_version,
        lag(trade_version) over (partition by trade_id order by trade_version) as previous_version
    from versions

),

gaps as (

    select
        trade_id,
        previous_version,
        trade_version as next_version,
        trade_version - previous_version - 1 as missing_version_count
    from with_previous
    where previous_version is not null
      and trade_version > previous_version + 1

),

-- Also catch a trade whose lowest accepted version is not 1: v3 as the first accepted
-- version means v1 and v2 either never arrived or were rejected.
missing_first_version as (

    select
        trade_id,
        0 as previous_version,
        min(trade_version) as next_version,
        min(trade_version) - 1 as missing_version_count
    from versions
    group by trade_id
    having min(trade_version) > 1

),

all_gaps as (

    select * from gaps
    union all
    select * from missing_first_version

),

-- How much of each gap a rejection explains: the distinct rejected versions falling
-- strictly inside it. A left join and an aggregate rather than a correlated subquery,
-- because Snowflake cannot evaluate a subquery whose HAVING clause references the outer
-- query. The join also puts both counts in the failure output, so a triggered test says
-- how much of the gap was explained instead of only that something was wrong.
gap_explanations as (

    select
        all_gaps.trade_id,
        all_gaps.previous_version,
        all_gaps.next_version,
        all_gaps.missing_version_count,
        count(distinct rejected.trade_version) as rejected_version_count
    from all_gaps
    left join {{ ref('fct_trade_rejected') }} as rejected
        on all_gaps.trade_id = rejected.trade_id
        and all_gaps.previous_version < rejected.trade_version
        and all_gaps.next_version > rejected.trade_version
    group by
        all_gaps.trade_id,
        all_gaps.previous_version,
        all_gaps.next_version,
        all_gaps.missing_version_count

),

-- A count comparison rather than mere existence, so a gap of three versions explained by
-- only one rejection is still reported.
unexplained as (

    select *
    from gap_explanations
    where rejected_version_count < missing_version_count

)

select * from unexplained
