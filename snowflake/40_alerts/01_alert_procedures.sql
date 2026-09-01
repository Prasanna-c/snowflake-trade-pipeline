-- =============================================================================
-- Alert action procedures.
--
-- Snowflake's ALERT object takes a single statement in its THEN clause. Rather
-- than cramming string formatting into that clause, each alert calls a procedure
-- here. That buys three things:
--   * the email body can be composed properly, with the actual offending rows in it
--     ("3 tasks failed" is a page; "TASK_DRAIN_TRADE_EVENT_STREAM failed 3x with
--     'insufficient privileges'" is a fix),
--   * the procedures are independently testable -- you can CALL one to verify email
--     delivery without waiting for a real failure,
--   * changing the message never touches the alert definition, so the alert does
--     not have to be suspended and resumed to edit copy.
--
-- SYSTEM$SEND_EMAIL requires:
--   1. an EMAIL notification integration (created by Terraform),
--   2. USAGE on that integration (granted by Terraform),
--   3. recipients that are VERIFIED emails on Snowflake users in this account.
--      Unverified or external addresses fail at runtime -- this is the single most
--      common reason "my alerts do not arrive".
-- =============================================================================

use role {{ transform_role }};
use warehouse {{ load_warehouse }};
use database {{ database }};
use schema monitoring;

-- -----------------------------------------------------------------------------
-- Shared sender. Centralising this means the integration name, the recipient list
-- and the subject-line convention exist in exactly one place.
-- -----------------------------------------------------------------------------
create or replace procedure sp_notify(
    p_severity varchar,
    p_title varchar,
    p_body varchar
)
returns varchar
language sql
comment = 'Send an operational alert email through the project notification integration.'
execute as caller
as
$$
declare
    v_subject varchar;
    v_body varchar;
begin
    -- Severity and environment in the subject so an on-call engineer can triage
    -- from a phone notification without opening the mail.
    v_subject := '[' || :p_severity || '][TRADES-{{ env }}] ' || :p_title;

    v_body := :p_body
        || '\n\n---\n'
        || 'Environment : {{ env }}\n'
        || 'Database    : {{ database }}\n'
        || 'Detected at : ' || current_timestamp()::varchar || '\n'
        || 'Runbook     : docs/runbook.md\n'
        || 'Dashboard   : select * from {{ database }}.monitoring.vw_pipeline_sla;';

    call system$send_email(
        '{{ notification_integration }}',
        '{{ alert_email }}',
        :v_subject,
        :v_body
    );

    return 'sent: ' || :v_subject;
end;
$$;

-- -----------------------------------------------------------------------------
-- P1: change capture has stopped. Trades are arriving but not being picked up, or
-- nothing is arriving at all.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_ingestion_stall()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'Ingestion has stalled.\n\n'
        || 'Last trade event loaded : ' || coalesce(last_event_loaded_at::varchar, 'NEVER') || '\n'
        || 'Minutes since last event: ' || coalesce(minutes_since_last_event::varchar, 'n/a') || '\n'
        || 'Events in last 24h      : ' || events_loaded_last_24h::varchar || '\n'
        || 'Rows awaiting transform : ' || rows_awaiting_transform::varchar || '\n\n'
        || 'Likely causes, in order of probability:\n'
        || '  1. Upstream producer stopped -- check whether files are reaching @RAW.TRADE_LANDING:\n'
        || '       list @{{ database }}.raw.trade_landing;\n'
        || '  2. Snowpipe stalled -- check: select system$pipe_status(''{{ database }}.raw.pipe_trade_event'');\n'
        || '  3. Drain task suspended -- check: show tasks in schema {{ database }}.raw;\n'
        || '  4. Files present but unloaded -- check: select * from {{ database }}.monitoring.vw_file_arrival where is_stalled;'
    into :v_body
    from {{ database }}.monitoring.vw_pipeline_sla;

    call monitoring.sp_notify('P1', 'Ingestion stalled', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P1: a Snowflake task failed. Reads INFORMATION_SCHEMA (real-time), not
-- ACCOUNT_USAGE, so detection latency is the alert schedule and not ~45 minutes.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_task_failure()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'One or more Snowflake tasks failed in the last 30 minutes.\n\n'
        || listagg(
            '  * ' || name
            || ' @ ' || scheduled_time::varchar
            || ' (attempt ' || coalesce(attempt_number::varchar, '?') || ')'
            || '\n      ' || coalesce(error_message, 'no message'),
            '\n'
        ) within group (order by scheduled_time desc)
        || '\n\nThe drain task retries twice automatically before this fires, so a failure here\n'
        || 'is persistent rather than transient. Investigate before resuming.'
    into :v_body
    from table({{ database }}.information_schema.task_history(
        scheduled_time_range_start => dateadd('minute', -30, current_timestamp())
    ))
    where state = 'FAILED';

    call monitoring.sp_notify('P1', 'Snowflake task failure', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P2: transform backlog. Capture is working but dbt is not keeping up, so the
-- curated layer is going stale.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_transform_backlog()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'The transformation layer is falling behind change capture.\n\n'
        || 'Rows awaiting transform     : ' || rows_awaiting_transform::varchar || '\n'
        || 'Oldest backlog age (minutes): ' || coalesce(oldest_backlog_minutes::varchar, 'n/a') || '\n'
        || 'Last successful dbt run     : ' || coalesce(last_successful_dbt_run_at::varchar, 'NEVER') || '\n'
        || 'Minutes since dbt success   : ' || coalesce(minutes_since_dbt_success::varchar, 'n/a') || '\n\n'
        || 'Check the Airflow scheduler first -- a backlog with healthy capture almost always\n'
        || 'means the orchestrator, not Snowflake, is the problem.'
    into :v_body
    from {{ database }}.monitoring.vw_pipeline_sla;

    call monitoring.sp_notify('P2', 'Transform backlog growing', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P2: the reject rate has spiked.
--
-- This is the alert that actually earns its keep. A sudden jump in rejections
-- almost never means "the trades got worse" -- it means an upstream system changed
-- a field format, a reference data load failed, or a clock is wrong. Catching it in
-- minutes rather than at the next reconciliation is the difference between a
-- re-submission and a regulatory breach.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_reject_spike()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'Trade rejection rate has breached its threshold.\n\n'
        || 'Window            : last 60 minutes\n'
        || 'Events adjudicated: ' || total_events::varchar || '\n'
        || 'Rejected          : ' || rejected_events::varchar || '\n'
        || 'Reject rate       : ' || round(reject_rate * 100, 2)::varchar || '%\n\n'
        || 'Top rules fired:\n' || coalesce(top_rules, '  (none)')
        || '\n\nA spike concentrated in ONE rule is an upstream contract change.\n'
        || 'A spike spread across many rules is usually a bad file or a wrong-day extract.'
    into :v_body
    from (
        select
            count(*) as total_events,
            count_if(verdict = 'REJECTED') as rejected_events,
            count_if(verdict = 'REJECTED') / nullif(count(*), 0) as reject_rate,
            (
                select listagg('  * ' || rule_code || ' (' || hits::varchar || ')', '\n')
                       within group (order by hits desc)
                from (
                    select rule_code, count(*) as hits
                    from {{ database }}.{{ audit_schema }}.trade_rule_result
                    where evaluated_at >= dateadd('hour', -1, current_timestamp())
                      and rule_severity = 'REJECT'
                    group by rule_code
                    order by hits desc
                    limit 5
                )
            ) as top_rules
        from {{ database }}.{{ intermediate_schema }}.int_trade_event_adjudicated
        where adjudicated_at >= dateadd('hour', -1, current_timestamp())
    );

    call monitoring.sp_notify('P2', 'Trade rejection rate spike', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P2: expiry sweep overdue -- the detective control for business rule 4.
--
-- Nothing in Snowflake fails when dbt does not run. FCT_TRADE simply keeps
-- reporting matured trades as LIVE, which is a silent correctness bug and exactly
-- the kind of thing that is found in a quarterly review rather than in production.
-- This alert converts that silence into a page.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_expiry_overdue()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'Trades have passed their maturity date but are still flagged LIVE.\n\n'
        || 'Overdue trade count : ' || count(*)::varchar || '\n'
        || 'Oldest maturity date: ' || min(maturity_date)::varchar || '\n'
        || 'Total notional      : ' || to_varchar(sum(notional_amount), '999,999,999,999.00') || '\n\n'
        || 'CORE.FCT_TRADE derives lifecycle_status during the dbt build, so a non-zero count here\n'
        || 'means the daily dbt run has not completed. Verify the Airflow DAG, then run:\n'
        || '  dbt build --select +fct_trade'
    into :v_body
    from {{ database }}.{{ core_schema }}.fct_trade
    where maturity_date < current_date()
      and lifecycle_status not in ('EXPIRED', 'CANCELLED');

    call monitoring.sp_notify('P2', 'Trade expiry sweep overdue', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P3: partial load. COPY with ON_ERROR = CONTINUE reports success on a file where
-- rows were skipped. Without this the loss is invisible.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_partial_load()
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'Rows were skipped during load and are not present in RAW.TRADE_EVENT.\n\n'
        || 'Affected files: ' || count(distinct source_file_name)::varchar || '\n'
        || 'Skipped rows  : ' || count(*)::varchar || '\n\n'
        || 'Sample errors:\n'
        || listagg('  * ' || source_file_name || ' row ' || coalesce(source_file_row_number::varchar, '?')
                   || ': ' || left(coalesce(error_message, ''), 200), '\n')
           within group (order by logged_at desc)
        || '\n\nFull detail: select * from {{ database }}.raw.copy_error order by logged_at desc;'
    into :v_body
    from (
        select *
        from {{ database }}.raw.copy_error
        where logged_at >= dateadd('hour', -1, current_timestamp())
        limit 10
    );

    call monitoring.sp_notify('P3', 'Partial file load -- rows skipped', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- P3: credit burn. On a 30-day trial this is the alert that stops the project
-- dying halfway through; in production it is the one that stops a runaway backfill
-- from becoming a budget conversation.
-- -----------------------------------------------------------------------------
create or replace procedure sp_alert_credit_burn(p_daily_threshold float)
returns varchar
language sql
execute as caller
as
$$
declare
    v_body varchar;
begin
    select
        'Daily credit consumption has exceeded its threshold.\n\n'
        || 'Threshold  : ' || :p_daily_threshold::varchar || ' credits/day\n'
        || 'Actual     : ' || round(sum(credits_used), 2)::varchar || ' credits\n\n'
        || 'By warehouse:\n'
        || listagg('  * ' || warehouse_name || ': ' || round(credits_used, 2)::varchar, '\n')
           within group (order by credits_used desc)
        || '\n\nNOTE: this reads ACCOUNT_USAGE and therefore lags reality by up to 3 hours.\n'
        || 'Resource monitors are the preventive control; this is only the notification.'
    into :v_body
    from {{ database }}.monitoring.vw_warehouse_credits
    where usage_date = current_date();

    call monitoring.sp_notify('P3', 'Credit burn above threshold', :v_body);
    return :v_body;
end;
$$;

-- -----------------------------------------------------------------------------
-- Smoke test. Run this after deploying to prove email delivery works BEFORE
-- relying on it. If this does not arrive, the recipient is not a verified
-- Snowflake user email.
-- -----------------------------------------------------------------------------
-- call monitoring.sp_notify('TEST', 'Alerting smoke test', 'If you can read this, SYSTEM$SEND_EMAIL is configured correctly.');
