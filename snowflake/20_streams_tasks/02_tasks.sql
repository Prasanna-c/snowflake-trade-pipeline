-- =============================================================================
-- Snowflake Tasks.
--
-- SCOPE DECISION -- worth being explicit about, because it is the first thing an
-- interviewer will probe:
--
--   Snowflake tasks own only what must happen *continuously and independently of
--   the orchestrator*: draining the stream, and housekeeping. They deliberately do
--   NOT write to CORE.FCT_TRADE.
--
--   dbt is the single writer to every curated table. Two writers to a fact table is
--   how you get a lost update nobody can reproduce -- a task and a dbt merge racing
--   on the same trade_id would silently drop an amendment.
--
--   So: tasks capture, dbt transforms, Airflow schedules dbt. Where a native task
--   would duplicate a dbt responsibility, we add a *detective* control instead --
--   an alert that fires if dbt has not done its job (see 40_alerts).
--
-- SERVERLESS vs. WAREHOUSE TASKS:
--   These use serverless compute (no WAREHOUSE clause). For a task that runs for two
--   seconds every minute, serverless is materially cheaper than resuming a warehouse
--   1,440 times a day, and Snowflake right-sizes it automatically.
--
-- WHY NO `ERROR_INTEGRATION`:
--   Snowflake's task ERROR_INTEGRATION accepts only a TYPE = QUEUE notification
--   integration (SNS / Event Grid / Pub-Sub). An EMAIL integration is not valid
--   there, and a trial account has no cloud messaging to point at. Task failures are
--   therefore surfaced by ALERT_TASK_FAILURE in 40_alerts, which polls
--   TASK_HISTORY and emails via SYSTEM$SEND_EMAIL. That is a detective control
--   rather than a push notification, with a detection latency equal to the alert
--   schedule (5 minutes) -- an acceptable trade for zero cloud dependencies, and it
--   is the one place where the local build differs from what I would run in
--   production with an SNS topic wired in.
-- =============================================================================

use role {{ transform_role }};
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- TASK 1: drain the stream.
--
-- Runs every minute, but the WHEN clause means it only *bills* when there is
-- something to do. A skipped task run costs nothing. This is what gives us
-- near-real-time capture without paying for a warehouse that idles all night.
-- -----------------------------------------------------------------------------
create or replace task task_drain_trade_event_stream
    schedule = '1 minute'
    user_task_managed_initial_warehouse_size = 'XSMALL'
    -- If a run hangs, kill it rather than letting the next 60 runs queue behind it.
    user_task_timeout_ms = 300000
    -- Do not let two drains overlap. The procedure is transactional so an overlap
    -- could not corrupt data, but it would fragment batches for no reason.
    allow_overlapping_execution = false
    -- Retry transient failures automatically before escalating to a human.
    task_auto_retry_attempts = 2
    comment = 'Every minute: move new RAW.TRADE_EVENT rows into the dbt queue. No-op when the stream is empty.'
when
    system$stream_has_data('{{ database }}.raw.trade_event_stream')
as
    call raw.sp_drain_trade_event_stream('snowflake_task');

-- -----------------------------------------------------------------------------
-- TASK 2: queue housekeeping.
--
-- The queue is an operational buffer, not a data store -- RAW.TRADE_EVENT is the
-- system of record. Once dbt has adjudicated a batch the queue rows are dead
-- weight, and an ever-growing queue makes every incremental dbt run scan more.
--
-- Retains 7 days regardless of processing state, so a week-long dbt outage is
-- survivable and an operator can always re-drive recent batches.
--
-- Runs on its own daily schedule rather than AFTER the drain task. Chaining it to
-- a task that fires every minute would attempt to delete week-old rows sixty times
-- an hour, and a DAG child cannot carry its own serverless warehouse size -- it
-- inherits the root's. Housekeeping is daily work, so it gets a daily cron,
-- staggered ahead of the stage archive below.
-- -----------------------------------------------------------------------------
create or replace procedure sp_prune_trade_event_queue(p_retain_days number)
returns variant
language sql
comment = 'Delete queue rows older than p_retain_days that dbt has already adjudicated.'
execute as caller
as
$$
declare
    v_deleted number default 0;
    v_adjudicated_exists boolean default false;
begin
    -- The adjudication table is owned by dbt, so on a freshly provisioned account
    -- it does not exist yet. Checking rather than assuming keeps the task green
    -- during the window between `terraform apply` and the first `dbt build`,
    -- instead of emitting a failure that an operator has to learn to ignore.
    select count(*) > 0
    into :v_adjudicated_exists
    from {{ database }}.information_schema.tables
    where table_schema = '{{ intermediate_schema }}'
      and table_name = 'INT_TRADE_EVENT_ADJUDICATED';

    if (not v_adjudicated_exists) then
        return object_construct('rows_deleted', 0, 'skipped_reason', 'adjudication table not built yet');
    end if;

    delete from raw.trade_event_queue as q
    where q.drained_at < dateadd('day', -1 * :p_retain_days, current_timestamp())
      and exists (
          select 1
          from {{ database }}.{{ intermediate_schema }}.int_trade_event_adjudicated as a
          where a.event_sk = q.event_sk
      );

    v_deleted := sqlrowcount;
    return object_construct('rows_deleted', :v_deleted);
end;
$$;

create or replace task task_prune_trade_event_queue
    schedule = 'using cron 15 2 * * * UTC'
    user_task_managed_initial_warehouse_size = 'XSMALL'
    user_task_timeout_ms = 600000
    comment = 'Daily 02:15 UTC: prune adjudicated queue rows older than 7 days.'
as
    call raw.sp_prune_trade_event_queue(7);

-- -----------------------------------------------------------------------------
-- TASK 3: stage housekeeping.
--
-- Loaded files are removed from the landing stage after 3 days so the stage does not
-- grow without bound and, more importantly, so that "files present in the landing
-- zone" stays a meaningful signal for the file-arrival monitor.
--
-- Note the guard: only remove files COPY_HISTORY confirms loaded with zero errors.
-- Removing a file that failed to load is data loss.
-- -----------------------------------------------------------------------------
create or replace procedure sp_archive_loaded_files(p_retain_days number)
returns variant
language sql
comment = 'Remove staged files that COPY_HISTORY confirms loaded without error.'
execute as caller
as
$$
declare
    v_removed number default 0;
    c_files cursor for
        select distinct ch.file_name as file_name
        from table(
            information_schema.copy_history(
                table_name => '{{ database }}.RAW.TRADE_EVENT',
                start_time => dateadd('day', -14, current_timestamp())
            )
        ) as ch
        where ch.status = 'Loaded'
          and ch.error_count = 0
          and ch.last_load_time < dateadd('day', -1 * :p_retain_days, current_timestamp());
begin
    for rec in c_files do
        -- REMOVE does not accept a bind variable for the path, so the statement is
        -- assembled dynamically. file_name from COPY_HISTORY is stage-relative and
        -- already excludes the stage name.
        execute immediate 'remove @raw.trade_landing/' || rec.file_name;
        v_removed := v_removed + 1;
    end for;
    return object_construct('files_removed', :v_removed);
end;
$$;

create or replace task task_archive_loaded_files
    schedule = 'using cron 30 2 * * * UTC'
    user_task_managed_initial_warehouse_size = 'XSMALL'
    user_task_timeout_ms = 900000
    comment = 'Daily 02:30 UTC: remove fully-loaded files older than 3 days from @trade_landing.'
as
    call raw.sp_archive_loaded_files(3);

-- -----------------------------------------------------------------------------
-- All three are standalone scheduled tasks, so resume order does not matter. A
-- task is created suspended, and a task nobody resumed is the most common reason
-- "the pipeline captured nothing overnight".
-- -----------------------------------------------------------------------------
alter task task_prune_trade_event_queue resume;
alter task task_archive_loaded_files resume;
alter task task_drain_trade_event_stream resume;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show tasks in schema raw;

select
    th.name,
    th.state,
    th.scheduled_time,
    th.completed_time,
    th.error_message
from table(information_schema.task_history(
    scheduled_time_range_start => dateadd('hour', -1, current_timestamp())
)) as th
order by th.scheduled_time desc
limit 20;
