-- =============================================================================
-- Ingestion mechanics: Snowpipe (continuous) + a COPY procedure (batch).
--
-- Both are built, because they answer different questions and a real platform
-- needs both:
--
--   SNOWPIPE  -- serverless, file-triggered, ~1 minute latency, billed per file
--                plus compute-seconds. No warehouse to size or keep awake. This
--                is the default path for the trickle of trades arriving all day.
--
--   COPY      -- runs on our own warehouse, we control parallelism and we get a
--                synchronous result. This is the path for backfills, for
--                replaying a day, and as the failover when Snowpipe's queue is
--                backed up. Crucially, COPY is *deterministic and observable in
--                the same transaction*, which is what you want for a controlled
--                reload.
--
-- The load-history de-duplication semantics differ and it matters:
--   COPY skips files it has already loaded for 64 days (LOAD_HISTORY).
--   Snowpipe tracks per-pipe file history for 14 days.
-- So a file replayed after 14 days will be re-ingested by Snowpipe but skipped by
-- COPY. Our defence against both is the idempotency of downstream adjudication:
-- re-processing the same event yields the same verdict and merges to the same row.
-- =============================================================================

use role {{ transform_role }};
use warehouse {{ load_warehouse }};
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- Snowpipe.
--
-- On an internal stage, Snowpipe has no cloud event notification to listen to, so
-- it is driven by the REST endpoint (insertFiles) that the Snowflake Python
-- connector's ingest manager calls after PUT. For an external stage this same
-- pipe definition works unchanged with AUTO_INGEST = TRUE plus an SQS/Event Grid
-- notification -- see 04_external_stage_reference.sql.
--
-- ON_ERROR = CONTINUE: one malformed line must not block the other 4,999 trades
-- in the file. The skipped rows are recovered via VALIDATE() into RAW.COPY_ERROR
-- by the reconciliation step, so nothing is lost silently.
-- -----------------------------------------------------------------------------
create or replace pipe pipe_trade_event
    auto_ingest = false
    comment = 'Continuous ingestion of NDJSON trade files from @trade_landing.'
as
copy into raw.trade_event (
    payload,
    source_file_name,
    source_file_row_number,
    source_file_last_modified,
    load_method
)
from (
    select
        $1,
        metadata$filename,
        metadata$file_row_number,
        metadata$file_last_modified,
        'SNOWPIPE'
    from @trade_landing
)
file_format = (format_name = 'ff_trade_ndjson')
on_error = 'continue';

-- -----------------------------------------------------------------------------
-- Batch COPY procedure.
--
-- Wraps COPY in a batch registration so that every load is attributable, and
-- captures VALIDATE() output so that skipped rows land in RAW.COPY_ERROR rather
-- than disappearing.
--
-- Written in Snowflake Scripting (not a Python stored proc) deliberately: it is
-- pure SQL orchestration, so keeping it in SQL means no runtime to version, no
-- packages to pin, and the logic is reviewable by anyone who reads SQL.
-- -----------------------------------------------------------------------------
create or replace procedure sp_load_trade_files(
    p_pattern varchar,
    p_orchestrator_run_id varchar
)
returns variant
language sql
comment = 'COPY matching files from @trade_landing into RAW.TRADE_EVENT, with batch registration and error capture.'
execute as caller
as
$$
declare
    v_batch_id varchar default uuid_string();
    v_rows_loaded number default 0;
    v_files_loaded number default 0;
    v_errors number default 0;
    v_query_id varchar;
    v_result variant;
begin
    -- Register the batch before doing any work, so a crash mid-load still leaves
    -- a RUNNING row that the monitoring alert will surface as a stuck batch.
    insert into raw.load_batch (batch_id, batch_type, batch_status, orchestrator_run_id)
    values (:v_batch_id, 'FILE_LOAD', 'RUNNING', :p_orchestrator_run_id);

    begin
        copy into raw.trade_event (
            payload,
            source_file_name,
            source_file_row_number,
            source_file_last_modified,
            source_file_content_key,
            load_method,
            load_query_id
        )
        from (
            select
                $1,
                metadata$filename,
                metadata$file_row_number,
                metadata$file_last_modified,
                metadata$file_content_key,
                'COPY',
                :v_batch_id
            from @raw.trade_landing
        )
        pattern = :p_pattern
        file_format = (format_name = 'raw.ff_trade_ndjson')
        on_error = 'continue';

        -- RESULT_SCAN over the COPY gives us per-file outcomes: rows parsed,
        -- rows loaded, errors seen. This is the reconciliation evidence.
        v_query_id := sqlid;

        select
            coalesce(sum("rows_loaded"), 0),
            count(*),
            coalesce(sum("errors_seen"), 0)
        into :v_rows_loaded, :v_files_loaded, :v_errors
        from table(result_scan(:v_query_id));

        -- Capture what COPY rejected, so parse failures outlive the 14 days that
        -- COPY_HISTORY retains them.
        --
        -- This is a per-file summary rather than one row per rejected record, because
        -- VALIDATE() -- the only source of row-level detail -- does not support COPY with
        -- a transform, and this COPY needs a transform to attach the METADATA$ columns.
        -- `errors_seen` still carries the true count of rejected rows and `first_error`
        -- identifies the defect, which is what an operator actually triages on.
        if (v_errors > 0) then
            insert into raw.copy_error (
                batch_id, source_file_name, source_file_row_number,
                error_message, rejected_record, error_column_name, byte_offset
            )
            select
                :v_batch_id,
                "file",
                "first_error_line",
                "errors_seen"::varchar || ' row(s) rejected in this file; first error: '
                    || "first_error",
                null,
                "first_error_column_name",
                "first_error_character"
            from table(result_scan(:v_query_id))
            where "errors_seen" > 0;
        end if;

        update raw.load_batch
        set batch_status = 'SUCCEEDED',
            completed_at = current_timestamp(),
            row_count = :v_rows_loaded,
            file_count = :v_files_loaded,
            error_count = :v_errors
        where batch_id = :v_batch_id;

    exception
        when other then
            update raw.load_batch
            set batch_status = 'FAILED',
                completed_at = current_timestamp(),
                -- SQLCODE and SQLERRM need a colon bind inside a SQL statement; without
                -- it Snowflake resolves them as column names and the handler itself fails,
                -- which hides the error it was written to report.
                error_message = 'SQLCODE ' || :sqlcode || ': ' || :sqlerrm
            where batch_id = :v_batch_id;
            -- Re-raise so the caller (Airflow) sees a failure rather than a
            -- silently swallowed error. Swallowing here is how pipelines go
            -- "green" while losing data.
            raise;
    end;

    v_result := object_construct(
        'batch_id', :v_batch_id,
        'files_loaded', :v_files_loaded,
        'rows_loaded', :v_rows_loaded,
        'rows_errored', :v_errors,
        'copy_query_id', :v_query_id
    );
    return :v_result;
end;
$$;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show pipes like 'PIPE_TRADE_EVENT' in schema raw;
select system$pipe_status('{{ database }}.raw.pipe_trade_event') as pipe_status;
