-- =============================================================================
-- Snowflake ALERT objects.
--
-- An alert is a scheduled condition + action pair evaluated by Snowflake itself.
-- Using native alerts rather than putting every check in Airflow is deliberate:
-- these must keep working when Airflow is the thing that is broken. An orchestrator
-- cannot alert on its own death.
--
-- So the monitoring responsibility is split:
--   Snowflake ALERTs  -> platform-level truths (is data arriving? are tasks alive?
--                        is the warehouse burning credits? is the data correct?).
--                        Survive an Airflow outage.
--   Airflow callbacks -> run-level truths (did THIS dbt invocation fail? which
--                        model? which test?). Have the run context Snowflake lacks.
--
-- COST: each alert is a scheduled query on a warehouse. Seven alerts at 5-15 minute
-- cadences on an XSMALL, each running for well under a second, is a small fraction
-- of one credit per day -- the WHEN-style conditions are cheap metadata reads. The
-- schedules below are staggered so they do not all resume the warehouse at once.
-- =============================================================================

use role {{ transform_role }};
use database {{ database }};
use schema monitoring;

-- -----------------------------------------------------------------------------
-- P1 -- Ingestion stalled.
--
-- Only fires during the hours when trades are expected. Alerting at 03:00 on a
-- Sunday because no trades arrived is how alerts get muted, and a muted alert is
-- worse than no alert.
-- -----------------------------------------------------------------------------
create or replace alert alert_ingestion_stall
    warehouse = {{ load_warehouse }}
    schedule = '15 minute'
    comment = 'P1: no trade events loaded for 90+ minutes during expected trading hours.'
if (exists (
    select 1
    from {{ database }}.monitoring.vw_pipeline_sla
    where ingestion_status = 'RED'
      -- Weekdays 06:00-22:00 UTC. Adjust to the desk's actual trading window.
      and dayofweekiso(current_timestamp()) <= 5
      and hour(convert_timezone('UTC', current_timestamp())) between 6 and 22
))
then call {{ database }}.monitoring.sp_alert_ingestion_stall();

-- -----------------------------------------------------------------------------
-- P1 -- Task failure. Reads INFORMATION_SCHEMA for real-time detection.
-- -----------------------------------------------------------------------------
create or replace alert alert_task_failure
    warehouse = {{ load_warehouse }}
    schedule = '5 minute'
    comment = 'P1: any Snowflake task in this database failed in the last 30 minutes.'
if (exists (
    select 1
    from table({{ database }}.information_schema.task_history(
        scheduled_time_range_start => dateadd('minute', -30, current_timestamp())
    ))
    where state = 'FAILED'
))
then call {{ database }}.monitoring.sp_alert_task_failure();

-- -----------------------------------------------------------------------------
-- P1 -- Stuck batch. A batch left in RUNNING means a session died mid-flight.
-- -----------------------------------------------------------------------------
create or replace alert alert_stuck_batch
    warehouse = {{ load_warehouse }}
    schedule = '10 minute'
    comment = 'P1: a load batch has been RUNNING for more than 15 minutes.'
if (exists (
    select 1
    from {{ database }}.monitoring.vw_batch_health
    where is_stuck
))
then call {{ database }}.monitoring.sp_notify(
    'P1',
    'Load batch stuck in RUNNING',
    'A batch has been RUNNING for over 15 minutes, which means the session that '
    || 'created it died without committing or rolling back.\n\n'
    || 'Investigate: select * from {{ database }}.monitoring.vw_batch_health where is_stuck;\n\n'
    || 'The drain procedure is transactional, so no partial data was committed. '
    || 'Close out the batch row and let the next scheduled drain re-read the stream.'
);

-- -----------------------------------------------------------------------------
-- P2 -- Transform backlog.
--
-- Threshold is deliberately generous (60 minutes of backlog) because a short
-- backlog is normal between dbt runs. What we are catching is a *growing* one.
-- -----------------------------------------------------------------------------
create or replace alert alert_transform_backlog
    warehouse = {{ load_warehouse }}
    schedule = '15 minute'
    comment = 'P2: dbt is not keeping up with change capture.'
if (exists (
    select 1
    from {{ database }}.monitoring.vw_pipeline_sla
    where transform_status in ('RED', 'AMBER')
      and rows_awaiting_transform > 0
))
then call {{ database }}.monitoring.sp_alert_transform_backlog();

-- -----------------------------------------------------------------------------
-- P2 -- Rejection rate spike.
--
-- Guarded by a minimum volume (100 events). Without it, 1 rejection out of 2 events
-- on a quiet morning is a 50% reject rate and a false page.
-- -----------------------------------------------------------------------------
create or replace alert alert_reject_rate_spike
    warehouse = {{ load_warehouse }}
    schedule = '30 minute'
    comment = 'P2: reject rate over the last hour exceeds 25% on meaningful volume.'
if (exists (
    select 1
    from {{ database }}.{{ intermediate_schema }}.int_trade_event_adjudicated
    where adjudicated_at >= dateadd('hour', -1, current_timestamp())
    group by 1
    having count(*) >= 100
       and count_if(verdict = 'REJECTED') / nullif(count(*), 0) > 0.25
))
then call {{ database }}.monitoring.sp_alert_reject_spike();

-- -----------------------------------------------------------------------------
-- P2 -- Expiry overdue. The correctness backstop for business rule 4.
--
-- Runs at 06:00 UTC, after the overnight dbt build has had time to complete.
-- -----------------------------------------------------------------------------
create or replace alert alert_expiry_overdue
    warehouse = {{ load_warehouse }}
    schedule = 'using cron 0 6 * * * UTC'
    comment = 'P2: matured trades still flagged LIVE -- the daily dbt expiry sweep did not run.'
if (exists (
    select 1
    from {{ database }}.{{ core_schema }}.fct_trade
    where maturity_date < current_date()
      and lifecycle_status not in ('EXPIRED', 'CANCELLED')
))
then call {{ database }}.monitoring.sp_alert_expiry_overdue();

-- -----------------------------------------------------------------------------
-- P3 -- Partial load.
-- -----------------------------------------------------------------------------
create or replace alert alert_partial_load
    warehouse = {{ load_warehouse }}
    schedule = '30 minute'
    comment = 'P3: rows were skipped at COPY time in the last hour.'
if (exists (
    select 1
    from {{ database }}.raw.copy_error
    where logged_at >= dateadd('hour', -1, current_timestamp())
))
then call {{ database }}.monitoring.sp_alert_partial_load();

-- -----------------------------------------------------------------------------
-- P3 -- Credit burn. Daily, after ACCOUNT_USAGE has caught up.
-- -----------------------------------------------------------------------------
create or replace alert alert_credit_burn
    warehouse = {{ load_warehouse }}
    schedule = 'using cron 0 7 * * * UTC'
    comment = 'P3: yesterday total credit consumption exceeded the daily budget.'
if (exists (
    select 1
    from {{ database }}.monitoring.vw_warehouse_credits
    where usage_date = current_date()
    group by usage_date
    having sum(credits_used) > {{ daily_credit_budget }}
))
then call {{ database }}.monitoring.sp_alert_credit_burn({{ daily_credit_budget }});

-- -----------------------------------------------------------------------------
-- Alerts are created suspended and must be explicitly resumed.
-- -----------------------------------------------------------------------------
alter alert alert_ingestion_stall resume;
alter alert alert_task_failure resume;
alter alert alert_stuck_batch resume;
alter alert alert_transform_backlog resume;
alter alert alert_reject_rate_spike resume;
alter alert alert_expiry_overdue resume;
alter alert alert_partial_load resume;
alter alert alert_credit_burn resume;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show alerts in schema monitoring;

-- Every evaluation, whether it fired or not. This is how you prove an alert was
-- actually working during an incident post-mortem, and how you find the alert that
-- has been silently erroring for a month.
select
    name,
    scheduled_time,
    state,
    sql_error_message
from table(information_schema.alert_history(
    scheduled_time_range_start => dateadd('hour', -24, current_timestamp())
))
order by scheduled_time desc
limit 50;
