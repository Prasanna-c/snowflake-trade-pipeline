-- =============================================================================
-- RAW layer tables.
--
-- Design principle: RAW is immutable and lossless. We store the payload exactly
-- as received in a VARIANT and never edit it. Everything downstream is derived
-- and can be rebuilt with `dbt build --full-refresh`.
--
-- WHY VARIANT instead of parsing into typed columns at load time:
--   1. A schema change upstream cannot break ingestion. The file still lands;
--      the new field simply appears in the payload and we pick it up when we
--      choose to. Compare with a typed COPY, where an added column fails the load
--      at 3am.
--   2. Compliance needs the original bytes. If we reject a trade, the auditor's
--      question is "what exactly did you receive?" -- not "what did your parser
--      make of it?".
--   3. It costs almost nothing. Snowflake shreds VARIANT into columnar sub-columns
--      internally, so `payload:trade_id::varchar` is close to native column speed
--      and prunes on micro-partition metadata.
-- =============================================================================

use role {{ transform_role }};
use warehouse {{ load_warehouse }};
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- Landing table. Insert-only. One row per line of every file we ever received.
-- -----------------------------------------------------------------------------
create table if not exists trade_event (
    -- Surrogate arrival key. Monotonically increasing per insert, used only as a
    -- deterministic tie-breaker when two events share a business timestamp.
    -- NOTE: IDENTITY is not gap-free and is not a strict global arrival order
    -- under concurrent loads, which is why business timestamps are ranked first.
    event_sk number(38, 0) identity start 1 increment 1 not null,

    -- The payload, untouched.
    payload variant not null,

    -- File lineage. This is the difference between "a trade was rejected" and
    -- "row 4,182 of trades_20260828_1400.ndjson.gz was rejected" -- the second is
    -- actionable, the first is not.
    source_file_name varchar(1000) not null,
    source_file_row_number number(38, 0),
    source_file_last_modified timestamp_ntz,
    -- Snowflake's METADATA$FILE_CONTENT_KEY -- a content hash of the staged file.
    -- Two files with different names but the same content key are a re-delivery,
    -- which is how we detect an upstream replaying yesterday's extract.
    source_file_content_key varchar(200),

    -- Load lineage.
    load_ts timestamp_ltz not null default current_timestamp(),
    load_method varchar(20) not null default 'COPY',  -- COPY | SNOWPIPE
    loaded_by varchar(200) not null default current_user(),
    -- Populated by the loader with the COPY statement's query_id, which is the
    -- join key into ACCOUNT_USAGE.COPY_HISTORY and QUERY_HISTORY. There is no
    -- Snowflake function that yields the current query id as a column default,
    -- so the client supplies it.
    load_query_id varchar(100),

    constraint pk_trade_event primary key (event_sk)
)
-- Clustering is intentionally NOT set here. At this project's volume Snowflake's
-- natural insert-order partitioning already prunes load_ts perfectly, and
-- automatic clustering would burn credits for no benefit. See
-- docs/scalability.md for the threshold at which this changes.
comment = 'Immutable landing zone for trade events. Insert-only, never updated.';

-- -----------------------------------------------------------------------------
-- Batch registry: the unit of processing and the anchor for every operational
-- metric. Written by the stream-drain procedure and by the Python loader.
--
-- Without this, "did the 14:00 batch land?" is unanswerable, and reconciling
-- "rows in the file" against "rows in FCT_TRADE" requires guesswork.
-- -----------------------------------------------------------------------------
create table if not exists load_batch (
    batch_id varchar(36) not null,
    batch_type varchar(30) not null,          -- FILE_LOAD | STREAM_DRAIN
    batch_status varchar(20) not null,        -- RUNNING | SUCCEEDED | FAILED
    started_at timestamp_ltz not null default current_timestamp(),
    completed_at timestamp_ltz,
    file_count number(38, 0) default 0,
    row_count number(38, 0) default 0,
    error_count number(38, 0) default 0,
    error_message varchar(5000),
    -- Correlates a Snowflake batch with the Airflow run that caused it, so an
    -- operator can pivot from a failed DAG task straight to the affected rows.
    orchestrator_run_id varchar(250),
    created_by varchar(200) not null default current_user(),

    constraint pk_load_batch primary key (batch_id)
)
comment = 'One row per processing batch. The join key between Snowflake metrics and Airflow runs.';

-- -----------------------------------------------------------------------------
-- Drain queue: the durable hand-off between the Snowflake Stream and dbt.
--
-- WHY this table exists (this is the crux of the ingestion design):
--   A Snowflake Stream's offset advances when the stream is consumed inside a
--   DML statement. If dbt read the stream directly, then a dbt run that failed
--   *after* the consuming statement committed would have advanced the offset with
--   nothing to show for it -- those trades would be silently lost. A stream also
--   has single-consumer semantics, so two models could not both read it.
--
--   Draining the stream exactly once into an immutable queue table separates
--   "capture the delta" (transactional, done by a Snowflake task) from "transform
--   the delta" (idempotent, retryable, done by dbt). dbt can then be re-run as
--   many times as needed against stable input, which is the property that makes
--   the pipeline safe to retry automatically.
--
--   The alternative -- dbt scanning RAW.TRADE_EVENT with a `load_ts > watermark`
--   filter -- works, but at scale it re-derives max(load_ts) over an
--   ever-growing table on every run and cannot cheaply prove it saw every row.
--   The stream reads the delta from micro-partition metadata instead.
-- -----------------------------------------------------------------------------
create table if not exists trade_event_queue (
    batch_id varchar(36) not null,
    drained_at timestamp_ltz not null,

    -- Carried through verbatim from RAW.TRADE_EVENT.
    event_sk number(38, 0) not null,
    payload variant not null,
    source_file_name varchar(1000),
    source_file_row_number number(38, 0),
    load_ts timestamp_ltz,
    load_method varchar(20),

    -- Monotonic sequence of drains. dbt orders by this to reconstruct the exact
    -- arrival order of events across batches, which the version-arbitration
    -- rules depend on.
    batch_seq number(38, 0) not null,

    constraint pk_trade_event_queue primary key (event_sk)
)
comment = 'Immutable queue drained from RAW.TRADE_EVENT_STREAM. The contract between Snowflake ingestion and dbt.';

-- Sequence backing batch_seq. A sequence (not a timestamp) because two drains in
-- the same second must still have a total order.
create sequence if not exists seq_batch_order
    start = 1
    increment = 1
    order
    comment = 'Total ordering of stream drains.';

-- -----------------------------------------------------------------------------
-- COPY error log. COPY INTO ... ON_ERROR = CONTINUE skips bad rows; without
-- capturing VALIDATE() output those rows vanish without trace, which is exactly
-- the kind of silent data loss a compliance review looks for.
-- -----------------------------------------------------------------------------
create table if not exists copy_error (
    batch_id varchar(36),
    logged_at timestamp_ltz not null default current_timestamp(),
    source_file_name varchar(1000),
    source_file_row_number number(38, 0),
    error_message varchar(5000),
    rejected_record varchar(16777216),
    error_column_name varchar(200),
    byte_offset number(38, 0)
)
comment = 'Rows Snowflake could not parse at COPY time. Populated from VALIDATE() after every load.';

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show tables like '%' in schema raw;
