-- =============================================================================
-- Landing zone: file format + internal stage.
--
-- Placeholders {{ database }} / {{ env }} / {{ load_warehouse }} are substituted
-- by scripts/deploy_snowflake_sql.py. Every statement is CREATE OR REPLACE / IF
-- NOT EXISTS so the whole directory can be re-applied safely -- this is what
-- makes the deploy step idempotent and therefore CI-safe.
-- =============================================================================

use role {{ transform_role }};
use warehouse {{ load_warehouse }};
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- File format: newline-delimited JSON (NDJSON), gzip-compressed.
--
-- WHY NDJSON rather than a JSON array or CSV:
--   * One trade per line means a single malformed record cannot poison the file.
--     With STRIP_OUTER_ARRAY on a big JSON array, one bad byte fails everything.
--   * It streams -- the producer can append and flush without rewriting the file,
--     and Snowflake can split a large file across compute for parallel COPY.
--   * Unlike CSV it is self-describing and schema-evolution tolerant: an upstream
--     system adding a field does not shift column positions and silently corrupt
--     the load.
--
-- STRIP_OUTER_ARRAY = FALSE because each line is already a bare object.
-- -----------------------------------------------------------------------------
create or replace file format ff_trade_ndjson
    type = 'json'
    compression = 'auto'
    strip_outer_array = false
    -- Keep explicit JSON nulls. A field the upstream system deliberately nulled
    -- is different information from a field it never sent, and the validation
    -- rules need to tell those apart.
    strip_null_values = false
    -- Fail loudly on duplicate keys instead of silently keeping the last one.
    allow_duplicate = false
    -- Unparseable bytes become a load error we can see, not a silent NULL.
    replace_invalid_characters = false
    ignore_utf8_errors = false
    -- Producers may write a BOM; tolerate it rather than failing the file.
    skip_byte_order_mark = true
    comment = 'NDJSON trade events, one JSON object per line.';

-- CSV format kept for the "upstream sends us a flat extract" case, which is the
-- common reality when integrating a legacy risk system.
create or replace file format ff_trade_csv
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '"'
    trim_space = true
    -- Distinguish "empty string" from "no value": both map to NULL only for the
    -- tokens we list, so a genuinely empty text field is preserved.
    null_if = ('', 'NULL', 'null', '\\N')
    empty_field_as_null = true
    -- A row with the wrong column count is a contract breach; fail the file.
    error_on_column_count_mismatch = true
    date_format = 'YYYY-MM-DD'
    timestamp_format = 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'
    comment = 'Flat CSV trade extract from legacy upstreams.';

-- -----------------------------------------------------------------------------
-- Internal named stage.
--
-- WHY an internal stage rather than an external S3/GCS/Azure stage:
--   * It needs no cloud account, no storage integration and no IAM trust
--     policy, so the whole project runs from a laptop against a free trial.
--   * The code path is identical to an external stage -- PUT/COPY/Snowpipe all
--     behave the same way. Migrating to external is a change to the stage
--     definition only, which is what 04_external_stage_reference.sql documents.
--
-- DIRECTORY = TRUE maintains a queryable directory table over the stage. This is
-- what lets us answer "which files arrived, when, and how big" in SQL -- the
-- foundation of the file-arrival-delay detection in 30_monitoring.
-- -----------------------------------------------------------------------------
create or replace stage trade_landing
    file_format = ff_trade_ndjson
    directory = (enable = true, refresh_on_create = true)
    -- Server-side encryption with a Snowflake-managed key. SNOWFLAKE_FULL
    -- (client-side) would prevent Snowpipe and the directory table from working.
    encryption = (type = 'SNOWFLAKE_SSE')
    comment = 'Landing zone for inbound trade files. Partitioned by ingest date: /{{ env }}/YYYY-MM-DD/.';

-- A separate stage for files that failed to load, so the landing zone only ever
-- contains work that is pending or done. Operators triage from here.
create or replace stage trade_quarantine
    file_format = ff_trade_ndjson
    directory = (enable = true)
    encryption = (type = 'SNOWFLAKE_SSE')
    comment = 'Files rejected at COPY time (unparseable / wrong format). Manual triage.';

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
show stages like 'TRADE_%' in schema raw;
show file formats like 'FF_TRADE_%' in schema raw;
