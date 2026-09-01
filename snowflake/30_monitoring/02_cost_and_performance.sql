-- =============================================================================
-- Operational monitoring, part 2: cost, performance and history.
--
-- Everything here reads SNOWFLAKE.ACCOUNT_USAGE, so it carries 45 minutes to 3
-- hours of latency and 365 days of retention. Use it for trends, capacity planning
-- and audit -- never for alerting. Requires IMPORTED PRIVILEGES on the SNOWFLAKE
-- database, which Terraform grants to FR_TRADES_PLATFORM and FR_TRADES_TRANSFORM.
-- =============================================================================

use role {{ transform_role }};
use database {{ database }};
use schema monitoring;

-- -----------------------------------------------------------------------------
-- Credit consumption by warehouse and day.
--
-- This is the view that answers "what is this pipeline costing us, and which part
-- of it". Splitting compute by workload class (the three warehouses) is what makes
-- the answer actionable -- "ingestion is 4% and transformation is 91%" tells you
-- exactly where to optimise.
-- -----------------------------------------------------------------------------
create or replace view vw_warehouse_credits as
select
    date_trunc('day', wmh.start_time) as usage_date,
    wmh.warehouse_name,
    case
        when wmh.warehouse_name ilike '%\\_LOAD\\_%' then 'INGESTION'
        when wmh.warehouse_name ilike '%\\_TRANSFORM\\_%' then 'TRANSFORMATION'
        when wmh.warehouse_name ilike '%\\_BI\\_%' then 'REPORTING'
        else 'OTHER'
    end as workload_class,
    sum(wmh.credits_used) as credits_used,
    sum(wmh.credits_used_compute) as credits_compute,
    sum(wmh.credits_used_cloud_services) as credits_cloud_services,

    -- Week-over-week movement. A step change here is either a data volume change
    -- or a regression someone merged; either way it needs an explanation.
    sum(wmh.credits_used) - lag(sum(wmh.credits_used), 7) over (
        partition by wmh.warehouse_name order by date_trunc('day', wmh.start_time)
    ) as credits_delta_vs_7d_ago

from snowflake.account_usage.warehouse_metering_history as wmh
where wmh.start_time >= dateadd('day', -90, current_timestamp())
group by 1, 2, 3;

comment on view vw_warehouse_credits is
    'Daily credit burn by warehouse and workload class. ACCOUNT_USAGE -- up to 3h latency, 365d retention.';

-- -----------------------------------------------------------------------------
-- Serverless credit consumption (tasks, Snowpipe, automatic clustering).
--
-- These do not appear in WAREHOUSE_METERING_HISTORY, which is why serverless cost
-- is so often missed. On this pipeline the drain task and Snowpipe are entirely
-- serverless, so omitting this view would understate the true cost.
-- -----------------------------------------------------------------------------
create or replace view vw_serverless_credits as
select
    date_trunc('day', start_time) as usage_date,
    'TASK' as service,
    task_name as object_name,
    sum(credits_used) as credits_used
from snowflake.account_usage.serverless_task_history
where start_time >= dateadd('day', -90, current_timestamp())
group by 1, 2, 3

union all

select
    date_trunc('day', start_time) as usage_date,
    'SNOWPIPE' as service,
    pipe_name as object_name,
    sum(credits_used) as credits_used
from snowflake.account_usage.pipe_usage_history
where start_time >= dateadd('day', -90, current_timestamp())
group by 1, 2, 3

union all

select
    date_trunc('day', start_time) as usage_date,
    'AUTO_CLUSTERING' as service,
    table_name as object_name,
    sum(credits_used) as credits_used
from snowflake.account_usage.automatic_clustering_history
where start_time >= dateadd('day', -90, current_timestamp())
group by 1, 2, 3;

comment on view vw_serverless_credits is
    'Serverless credits (tasks, Snowpipe, auto-clustering) -- invisible in WAREHOUSE_METERING_HISTORY. ACCOUNT_USAGE latency applies.';

-- -----------------------------------------------------------------------------
-- dbt query performance.
--
-- Every dbt statement is tagged via the query_tag session parameter (set in
-- dbt_project.yml), so QUERY_HISTORY can be filtered to just this pipeline and
-- attributed to a specific model and dbt invocation.
--
-- The columns that matter for tuning, in priority order:
--   bytes_spilled_to_remote_storage -- the single strongest signal that the
--       warehouse is too small. Remote spill is orders of magnitude slower than
--       local; any non-zero value is worth investigating before anything else.
--   partitions_scanned / partitions_total -- pruning effectiveness. A ratio near
--       1.0 on a large table means the filter is not using the clustering key.
--   queued_overload_time -- concurrency pressure; the case for multi-cluster.
-- -----------------------------------------------------------------------------
create or replace view vw_dbt_query_performance as
select
    qh.query_id,
    qh.query_tag,
    -- dbt writes a JSON query comment; the model name is the useful part.
    regexp_substr(qh.query_tag, 'model=([^|]+)', 1, 1, 'e') as model_name,
    regexp_substr(qh.query_tag, 'invocation=([^|]+)', 1, 1, 'e') as dbt_invocation_id,
    qh.warehouse_name,
    qh.warehouse_size,
    qh.start_time,
    qh.total_elapsed_time / 1000.0 as elapsed_seconds,
    qh.execution_time / 1000.0 as execution_seconds,
    qh.compilation_time / 1000.0 as compilation_seconds,
    qh.queued_overload_time / 1000.0 as queued_overload_seconds,
    qh.bytes_scanned,
    qh.rows_produced,
    qh.partitions_scanned,
    qh.partitions_total,
    round(qh.partitions_scanned / nullif(qh.partitions_total, 0), 4) as partition_scan_ratio,
    qh.bytes_spilled_to_local_storage,
    qh.bytes_spilled_to_remote_storage,
    qh.credits_used_cloud_services,
    qh.execution_status,
    qh.error_code,
    qh.error_message,

    -- Actionable diagnosis rather than raw numbers.
    case
        when qh.bytes_spilled_to_remote_storage > 0 then 'REMOTE_SPILL_SIZE_UP_WAREHOUSE'
        when qh.queued_overload_time > 30000 then 'QUEUEING_ADD_CLUSTER'
        when qh.partitions_total > 1000
             and qh.partitions_scanned / nullif(qh.partitions_total, 0) > 0.8
            then 'FULL_SCAN_REVIEW_PRUNING'
        when qh.compilation_time > qh.execution_time and qh.compilation_time > 5000
            then 'COMPILE_BOUND_SIMPLIFY_SQL'
        else 'OK'
    end as tuning_signal

from snowflake.account_usage.query_history as qh
where qh.start_time >= dateadd('day', -30, current_timestamp())
  and qh.query_tag ilike '%project=trade-pipeline%';

comment on view vw_dbt_query_performance is
    'Per-statement dbt performance with a tuning diagnosis. ACCOUNT_USAGE -- ~45m latency, 365d retention.';

-- -----------------------------------------------------------------------------
-- Task execution history.
--
-- Two sources, deliberately:
--   INFORMATION_SCHEMA.TASK_HISTORY -> real-time, 7 days. Used by the alert.
--   ACCOUNT_USAGE.TASK_HISTORY      -> ~45m latency, 365 days. Used for trends.
-- This view is the long-retention one.
-- -----------------------------------------------------------------------------
create or replace view vw_task_history as
select
    th.name as task_name,
    th.database_name,
    th.schema_name,
    th.scheduled_time,
    th.query_start_time,
    th.completed_time,
    datediff('millisecond', th.query_start_time, th.completed_time) / 1000.0 as duration_seconds,
    th.state,
    th.return_value,
    th.error_code,
    th.error_message,
    th.attempt_number,
    -- SKIPPED is the normal, healthy state for a conditional task with nothing to
    -- do. Counting it as a problem is a common false-positive source.
    case
        when th.state = 'FAILED' then 'FAILURE'
        when th.state = 'SKIPPED' then 'IDLE_NO_DATA'
        when th.state = 'SUCCEEDED' then 'OK'
        else th.state
    end as health

from snowflake.account_usage.task_history as th
where th.scheduled_time >= dateadd('day', -30, current_timestamp())
  and th.database_name = '{{ database }}';

comment on view vw_task_history is
    'Task run history for trend analysis. ACCOUNT_USAGE -- ~45m latency. Alerts read INFORMATION_SCHEMA instead.';

-- -----------------------------------------------------------------------------
-- Load history reconciliation: files Snowflake saw vs. rows we hold.
--
-- This is the control that catches partial loads. COPY with ON_ERROR = CONTINUE
-- will happily report success on a file where 30% of rows were skipped.
-- -----------------------------------------------------------------------------
create or replace view vw_copy_history as
select
    ch.file_name,
    ch.stage_location,
    ch.last_load_time,
    ch.row_count as rows_loaded,
    ch.row_parsed as rows_parsed,
    ch.row_parsed - ch.row_count as rows_skipped,
    round(100.0 * (ch.row_parsed - ch.row_count) / nullif(ch.row_parsed, 0), 2) as skip_pct,
    ch.file_size,
    ch.error_count,
    ch.error_limit,
    ch.status,
    ch.first_error_message,
    ch.pipe_name,
    case when ch.pipe_name is null then 'COPY' else 'SNOWPIPE' end as load_method,

    case
        when ch.status = 'Load failed' then 'FAILED'
        when ch.row_parsed > ch.row_count then 'PARTIAL_LOAD'
        else 'OK'
    end as load_outcome

from snowflake.account_usage.copy_history as ch
where ch.last_load_time >= dateadd('day', -30, current_timestamp())
  and ch.table_catalog_name = '{{ database }}'
  and ch.table_name = 'TRADE_EVENT';

comment on view vw_copy_history is
    'File-level load reconciliation, including partial loads hidden by ON_ERROR=CONTINUE. ACCOUNT_USAGE -- up to 2h latency.';

-- -----------------------------------------------------------------------------
-- Storage growth by table, for capacity planning.
-- -----------------------------------------------------------------------------
create or replace view vw_storage_growth as
select
    tsm.table_catalog as database_name,
    tsm.table_schema as schema_name,
    tsm.table_name,
    tsm.active_bytes / power(1024, 3) as active_gb,
    tsm.time_travel_bytes / power(1024, 3) as time_travel_gb,
    tsm.failsafe_bytes / power(1024, 3) as failsafe_gb,
    (tsm.active_bytes + tsm.time_travel_bytes + tsm.failsafe_bytes) / power(1024, 3) as total_gb,
    -- Fail-safe is 7 days, non-configurable, and billed. Transient tables have
    -- none -- which is exactly why the derived layers are transient.
    round(100.0 * tsm.failsafe_bytes
        / nullif(tsm.active_bytes + tsm.time_travel_bytes + tsm.failsafe_bytes, 0), 1) as failsafe_pct
from snowflake.account_usage.table_storage_metrics as tsm
where tsm.table_catalog = '{{ database }}'
  and tsm.deleted = false;

comment on view vw_storage_growth is
    'Per-table storage split across active / Time Travel / Fail-safe. ACCOUNT_USAGE -- ~2h latency.';
