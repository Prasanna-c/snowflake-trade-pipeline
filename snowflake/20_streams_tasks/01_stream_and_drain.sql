-- =============================================================================
-- Snowflake Streams + Tasks: the native change-capture engine.
--
-- This is the "use a Snowflake feature as the processing engine to consume
-- trades" requirement. The stream is the consumer; the task is the scheduler; the
-- procedure is the transactional hand-off to dbt.
-- =============================================================================

use role {{ transform_role }};
use warehouse {{ load_warehouse }};
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- Stream on the landing table.
--
-- APPEND_ONLY = TRUE is the important flag. RAW.TRADE_EVENT is insert-only, so an
-- append-only stream is:
--   * cheaper -- it reads only added micro-partitions and never has to compute the
--     delete/update half of the change set,
--   * simpler -- the change set has no METADATA$ISUPDATE rows to reason about,
--   * and it does not silently break if someone ever backfills with a DELETE
--     (an append-only stream just ignores it, rather than emitting phantom rows).
--
-- SHOW_INITIAL_ROWS = TRUE so that the first drain after creation picks up
-- anything already sitting in the table, rather than starting from "now" and
-- stranding it.
--
-- STALENESS WARNING: a stream becomes stale once the source table's Time Travel
-- retention elapses without the stream being consumed (14 days max, or the table's
-- DATA_RETENTION_TIME_IN_DAYS, whichever is greater). If that happens the delta is
-- unrecoverable from the stream. The ALERT_STREAM_STALE alert in 40_alerts fires
-- well before that boundary, and the COPY replay path is the recovery route.
--
-- IF NOT EXISTS, NOT OR REPLACE -- the one deliberate exception to this directory's
-- otherwise uniform CREATE OR REPLACE style.
--
-- A stream's offset is part of its state, not its definition. CREATE OR REPLACE
-- resets that offset, and combined with SHOW_INITIAL_ROWS = TRUE the next drain
-- would re-read every row already in TRADE_EVENT. On a table with a day of history
-- that is a large pointless scan; on a table with a year of it, the drain never
-- finishes.
--
-- Re-draining would not corrupt what has already been adjudicated: EVENT_SK is the
-- adjudication merge key, so a row that reaches the model a second time is updated in
-- place rather than inserted again. But "re-deploying the SQL layer silently reprocesses
-- the entire history" is precisely the kind of surprise that makes people afraid to re-run
-- a deploy. Idempotency has to hold for the objects that carry state, or it is not
-- idempotency.
--
-- Note the boundary: that merge protects against reprocessing, not against two writers.
-- Draining twice is safe; adjudicating twice at the same moment is not. See
-- docs/known-limitations.md.
--
-- The cost of IF NOT EXISTS is that a change to the stream's definition is not
-- picked up by a redeploy. That is a real limitation, and the deliberate answer is
-- that recreating a stream is a data-affecting operation which belongs in the
-- runbook (docs/runbook.md#recreating-the-stream) with the replay it implies, not in
-- an unattended deploy.
-- -----------------------------------------------------------------------------
create stream if not exists trade_event_stream
    on table trade_event
    append_only = true
    show_initial_rows = true
    comment = 'Change capture on RAW.TRADE_EVENT. Consumed exactly once by SP_DRAIN_TRADE_EVENT_STREAM.';

-- -----------------------------------------------------------------------------
-- Drain procedure.
--
-- The whole point is atomicity: the INSERT that reads the stream and the UPDATE
-- that marks the batch complete are in one transaction. Either the rows land in
-- the queue AND the batch is recorded AND the stream offset advances, or none of
-- it happens and the next run picks up the same delta. There is no interleaving
-- that loses a trade.
-- -----------------------------------------------------------------------------
create or replace procedure sp_drain_trade_event_stream(p_orchestrator_run_id varchar)
returns variant
language sql
comment = 'Atomically drain RAW.TRADE_EVENT_STREAM into RAW.TRADE_EVENT_QUEUE and register the batch.'
execute as caller
as
$$
declare
    v_batch_id varchar default uuid_string();
    v_batch_seq number;
    v_row_count number default 0;
    v_drained_at timestamp_ltz default current_timestamp();
begin
    -- Cheap no-op guard. The task also gates on this, but a manual invocation
    -- should not create empty batches that pollute the operational metrics.
    if (not system$stream_has_data('{{ database }}.raw.trade_event_stream')) then
        return object_construct('batch_id', null, 'rows_drained', 0, 'skipped', true);
    end if;

    v_batch_seq := (select raw.seq_batch_order.nextval);

    begin transaction;

        insert into raw.load_batch (
            batch_id, batch_type, batch_status, started_at, orchestrator_run_id
        )
        values (:v_batch_id, 'STREAM_DRAIN', 'RUNNING', :v_drained_at, :p_orchestrator_run_id);

        -- Reading the stream inside this statement is what advances its offset.
        insert into raw.trade_event_queue (
            batch_id, drained_at, batch_seq,
            event_sk, payload,
            source_file_name, source_file_row_number, load_ts, load_method
        )
        select
            :v_batch_id,
            :v_drained_at,
            :v_batch_seq,
            s.event_sk,
            s.payload,
            s.source_file_name,
            s.source_file_row_number,
            s.load_ts,
            s.load_method
        from raw.trade_event_stream as s;

        v_row_count := sqlrowcount;

        update raw.load_batch
        set batch_status = 'SUCCEEDED',
            completed_at = current_timestamp(),
            row_count = :v_row_count
        where batch_id = :v_batch_id;

    commit;

    return object_construct(
        'batch_id', :v_batch_id,
        'batch_seq', :v_batch_seq,
        'rows_drained', :v_row_count,
        'skipped', false
    );

exception
    when other then
        rollback;
        -- Recorded outside the rolled-back transaction so the failure survives.
        insert into raw.load_batch (
            batch_id, batch_type, batch_status, started_at, completed_at, error_message
        )
        values (
            :v_batch_id || '-failed', 'STREAM_DRAIN', 'FAILED',
            :v_drained_at, current_timestamp(),
            -- Colon-bound: inside a SQL statement, bare SQLCODE/SQLERRM resolve as column
            -- names and the handler fails, masking the error it exists to record.
            'SQLCODE ' || :sqlcode || ': ' || :sqlerrm
        );
        raise;
end;
$$;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show streams like 'TRADE_EVENT_STREAM' in schema raw;
select system$stream_has_data('{{ database }}.raw.trade_event_stream') as has_pending_data;
