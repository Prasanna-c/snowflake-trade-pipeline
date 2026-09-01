-- =============================================================================
-- Operational monitoring, part 1: pipeline freshness and batch health.
--
-- THE KEY DISTINCTION that governs every view in this directory:
--
--   INFORMATION_SCHEMA table functions  -> near real time (seconds), 7-14 day
--                                          retention, scoped to the current
--                                          database/account, no extra privilege.
--                                          USE FOR: alerting and on-call triage.
--
--   SNOWFLAKE.ACCOUNT_USAGE views       -> 45 minutes to 3 hours latency
--                                          (COPY_HISTORY ~2h, QUERY_HISTORY ~45m,
--                                          TASK_HISTORY ~45m), 365 day retention,
--                                          requires IMPORTED PRIVILEGES.
--                                          USE FOR: trends, cost, capacity, audit.
--
-- Alerting off ACCOUNT_USAGE is the classic mistake: you find out about a 3am
-- failure at 5am. Everything time-critical below reads INFORMATION_SCHEMA or our
-- own metadata tables; ACCOUNT_USAGE is reserved for the analytical views in
-- 02_cost_and_performance.sql.
-- =============================================================================

use role {{ transform_role }};
use database {{ database }};
use schema monitoring;

-- -----------------------------------------------------------------------------
-- The single pane of glass. One row, one column per SLA, each with a RAG status.
-- This is what the Streamlit header and the on-call runbook both read.
--
-- Sourced entirely from our own metadata tables + INFORMATION_SCHEMA, so it is
-- accurate to the second.
-- -----------------------------------------------------------------------------
create or replace view vw_pipeline_sla as
with file_arrival as (
    select
        max(load_ts) as last_event_loaded_at,
        count(*) as events_loaded_last_24h
    from {{ database }}.raw.trade_event
    where load_ts >= dateadd('hour', -24, current_timestamp())
),

drain as (
    select
        max(completed_at) as last_drain_at,
        count_if(batch_status = 'FAILED') as failed_drains_last_24h,
        count_if(batch_status = 'RUNNING'
            and started_at < dateadd('minute', -15, current_timestamp())) as stuck_batches
    from {{ database }}.raw.load_batch
    where started_at >= dateadd('hour', -24, current_timestamp())
),

queue as (
    select
        count(*) as rows_awaiting_transform,
        min(drained_at) as oldest_pending_drain_at
    from {{ database }}.raw.trade_event_queue as q
    where not exists (
        select 1
        from {{ database }}.{{ intermediate_schema }}.int_trade_event_adjudicated as a
        where a.event_sk = q.event_sk
    )
),

transform as (
    select
        max(run_completed_at) as last_dbt_run_at,
        max(case when run_status = 'success' then run_completed_at end) as last_successful_dbt_run_at
    from {{ database }}.{{ audit_schema }}.dbt_run_result
),

expiry as (
    -- Detective control for business rule 4. If dbt is running, this is always 0.
    -- A non-zero value means the expiry sweep has not run, which is a silent
    -- correctness failure that no task-level monitor would catch.
    select count(*) as trades_overdue_for_expiry
    from {{ database }}.{{ core_schema }}.fct_trade
    where maturity_date < current_date()
      and lifecycle_status not in ('EXPIRED', 'CANCELLED')
)

select
    current_timestamp() as evaluated_at,

    -- Ingestion freshness
    fa.last_event_loaded_at,
    datediff('minute', fa.last_event_loaded_at, current_timestamp()) as minutes_since_last_event,
    fa.events_loaded_last_24h,

    -- Capture freshness
    d.last_drain_at,
    datediff('minute', d.last_drain_at, current_timestamp()) as minutes_since_last_drain,
    d.failed_drains_last_24h,
    d.stuck_batches,

    -- Transform backlog
    q.rows_awaiting_transform,
    datediff('minute', q.oldest_pending_drain_at, current_timestamp()) as oldest_backlog_minutes,

    -- Transform freshness
    t.last_successful_dbt_run_at,
    datediff('minute', t.last_successful_dbt_run_at, current_timestamp()) as minutes_since_dbt_success,

    -- Correctness
    e.trades_overdue_for_expiry,

    -- ---------------------------------------------------------------------
    -- RAG rollup. Thresholds are the documented SLAs, in one place, so that the
    -- dashboard, the alerts and the runbook cannot drift apart.
    -- ---------------------------------------------------------------------
    case
        when fa.last_event_loaded_at is null then 'RED'
        when datediff('minute', fa.last_event_loaded_at, current_timestamp()) > 90 then 'RED'
        when datediff('minute', fa.last_event_loaded_at, current_timestamp()) > 45 then 'AMBER'
        else 'GREEN'
    end as ingestion_status,

    case
        when d.stuck_batches > 0 or d.failed_drains_last_24h > 0 then 'RED'
        when coalesce(datediff('minute', d.last_drain_at, current_timestamp()), 999) > 15 then 'AMBER'
        else 'GREEN'
    end as capture_status,

    case
        when coalesce(datediff('minute', t.last_successful_dbt_run_at, current_timestamp()), 9999) > 180 then 'RED'
        when coalesce(datediff('minute', q.oldest_pending_drain_at, current_timestamp()), 0) > 60 then 'AMBER'
        else 'GREEN'
    end as transform_status,

    case
        when e.trades_overdue_for_expiry > 0 then 'RED'
        else 'GREEN'
    end as correctness_status

from file_arrival as fa
cross join drain as d
cross join queue as q
cross join transform as t
cross join expiry as e;

comment on view vw_pipeline_sla is
    'Single-pane pipeline health. Real-time (reads project metadata, not ACCOUNT_USAGE). Drives alerts and the Streamlit header.';

-- -----------------------------------------------------------------------------
-- File-arrival monitoring.
--
-- Detecting "the file did not arrive" is harder than detecting "the file failed",
-- because absence produces no event. The technique is to compare actual arrivals
-- against an *expected* schedule, which we derive from the observed historical
-- cadence rather than hard-coding -- so the monitor keeps working when the upstream
-- changes from hourly to every 15 minutes.
-- -----------------------------------------------------------------------------
create or replace view vw_file_arrival as
with staged as (
    -- The directory table on the stage: what is physically sitting there right now.
    select
        relative_path,
        size as size_bytes,
        last_modified,
        md5
    from directory(@{{ database }}.raw.trade_landing)
),

loaded as (
    select
        source_file_name,
        count(*) as rows_in_file,
        min(load_ts) as first_row_loaded_at,
        max(load_ts) as last_row_loaded_at,
        max(load_method) as load_method
    from {{ database }}.raw.trade_event
    group by source_file_name
),

cadence as (
    -- Median gap between consecutive file arrivals over the last 7 days. Median,
    -- not mean, so one overnight gap does not inflate the expectation.
    select median(gap_minutes) as median_gap_minutes
    from (
        select datediff(
            'minute',
            lag(first_row_loaded_at) over (order by first_row_loaded_at),
            first_row_loaded_at
        ) as gap_minutes
        from loaded
        where first_row_loaded_at >= dateadd('day', -7, current_timestamp())
    )
    where gap_minutes is not null
)

select
    coalesce(s.relative_path, l.source_file_name) as file_name,
    s.size_bytes,
    s.last_modified as staged_at,
    l.rows_in_file,
    l.first_row_loaded_at,
    l.load_method,
    datediff('second', s.last_modified, l.first_row_loaded_at) as stage_to_load_seconds,

    case
        when l.source_file_name is null then 'STAGED_NOT_LOADED'
        when s.relative_path is null then 'LOADED_AND_ARCHIVED'
        else 'LOADED'
    end as file_state,

    -- A file that has sat on the stage unloaded for more than 15 minutes means
    -- Snowpipe is stalled or the COPY step is not running.
    case
        when l.source_file_name is null
             and s.last_modified < dateadd('minute', -15, current_timestamp())
            then true
        else false
    end as is_stalled,

    c.median_gap_minutes as expected_gap_minutes

from staged as s
full outer join loaded as l
    on s.relative_path = l.source_file_name
cross join cadence as c;

comment on view vw_file_arrival is
    'Per-file reconciliation of stage contents against loaded rows. Real-time. Detects stalled files and derives the expected arrival cadence.';

-- -----------------------------------------------------------------------------
-- Batch health and throughput.
-- -----------------------------------------------------------------------------
create or replace view vw_batch_health as
select
    batch_id,
    batch_type,
    batch_status,
    orchestrator_run_id,
    started_at,
    completed_at,
    datediff('millisecond', started_at, completed_at) / 1000.0 as duration_seconds,
    row_count,
    file_count,
    error_count,
    error_message,

    case
        when row_count > 0 and completed_at is not null
            then row_count / nullif(datediff('millisecond', started_at, completed_at) / 1000.0, 0)
    end as rows_per_second,

    -- Flag batches that are still RUNNING well past the point where they should
    -- have finished. A crashed session leaves exactly this signature.
    case
        when batch_status = 'RUNNING' and started_at < dateadd('minute', -15, current_timestamp())
            then true
        else false
    end as is_stuck,

    -- Rolling comparison against this batch type's own recent norm, so an
    -- unusually slow batch is visible before it becomes an outage.
    avg(datediff('millisecond', started_at, completed_at) / 1000.0)
        over (partition by batch_type order by started_at rows between 20 preceding and 1 preceding)
        as trailing_avg_duration_seconds

from {{ database }}.raw.load_batch;

comment on view vw_batch_health is
    'Per-batch duration, throughput and stuck detection. Real-time.';

-- -----------------------------------------------------------------------------
-- Stream lag: how far behind is change capture?
--
-- SYSTEM$STREAM_HAS_DATA is a boolean, which is not enough to alert on severity.
-- Counting the un-drained rows directly gives us a number to threshold against.
-- -----------------------------------------------------------------------------
create or replace view vw_stream_lag as
select
    current_timestamp() as evaluated_at,
    count(*) as rows_in_stream,
    min(load_ts) as oldest_undrained_load_ts,
    datediff('minute', min(load_ts), current_timestamp()) as lag_minutes,
    -- Streams go stale if not consumed within the source table's Time Travel
    -- window; past that the delta is unrecoverable from the stream and we must
    -- replay via COPY. Alert long before this.
    14 * 24 * 60 as staleness_limit_minutes
from {{ database }}.raw.trade_event_stream;

comment on view vw_stream_lag is
    'Un-drained row count and age in RAW.TRADE_EVENT_STREAM. Real-time. NOTE: selecting from a stream does not advance its offset -- only DML that reads it does.';
