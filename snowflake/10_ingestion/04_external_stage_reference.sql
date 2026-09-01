-- =============================================================================
-- REFERENCE ONLY -- NOT DEPLOYED BY scripts/deploy_snowflake_sql.py.
--
-- This file exists to answer the interview question "your demo uses an internal
-- stage; what changes in production?". The answer is: this file, and nothing else.
-- No ingestion code, no dbt model, no Airflow task changes.
--
-- Excluded from the deploy runner because it needs a real cloud account. The runner
-- skips any file whose name contains "_reference".
-- =============================================================================

use role accountadmin;   -- storage integrations are an ACCOUNTADMIN-level object
use database {{ database }};
use schema raw;

-- -----------------------------------------------------------------------------
-- STEP 1: Storage integration.
--
-- The important property: Snowflake never holds a long-lived access key. It assumes
-- an IAM role that trusts a Snowflake-managed principal, so credentials rotate
-- outside our control and there is no secret in Terraform state to leak.
--
-- After creating this, run DESC INTEGRATION to obtain STORAGE_AWS_IAM_USER_ARN and
-- STORAGE_AWS_EXTERNAL_ID, then add those to the trust policy of the IAM role. This
-- two-step handshake cannot be collapsed -- the external ID does not exist until
-- the integration does.
-- -----------------------------------------------------------------------------
create or replace storage integration si_trade_landing
    type = external_stage
    storage_provider = 'S3'
    storage_aws_role_arn = 'arn:aws:iam::123456789012:role/snowflake-trade-landing'
    enabled = true
    -- Restrict to exactly the prefixes we ingest from. Without this, a compromised
    -- Snowflake role could read any bucket the IAM role can reach.
    storage_allowed_locations = ('s3://db-trade-landing/prod/')
    storage_blocked_locations = ('s3://db-trade-landing/prod/archive/')
    comment = 'S3 access for the trade landing zone via IAM role assumption.';

desc integration si_trade_landing;

-- -----------------------------------------------------------------------------
-- STEP 2: External stage.
--
-- Note the path convention: date-partitioned prefixes. This is not cosmetic --
-- COPY with a PATTERN scoped to a single day's prefix lists thousands of files
-- instead of millions, and file listing is the first thing that becomes the
-- bottleneck at scale (see docs/scalability.md).
-- -----------------------------------------------------------------------------
create or replace stage trade_landing_external
    storage_integration = si_trade_landing
    url = 's3://db-trade-landing/prod/'
    file_format = ff_trade_ndjson
    directory = (enable = true, auto_refresh = true)
    comment = 'Production landing zone. Layout: s3://.../prod/ingest_date=YYYY-MM-DD/source=<system>/*.ndjson.gz';

-- -----------------------------------------------------------------------------
-- STEP 3: Snowpipe with auto-ingest.
--
-- THIS is the real production win over the internal-stage demo. AUTO_INGEST = TRUE
-- means S3 event notifications drive loading: a file lands, SQS notifies Snowflake,
-- the pipe loads it within about a minute. There is no polling, no scheduler, no
-- warehouse, and no orchestrator involvement in the hot path at all.
--
-- After creating the pipe, DESC PIPE gives NOTIFICATION_CHANNEL (an SQS ARN). Point
-- the S3 bucket's ObjectCreated event notification at it.
--
-- ERROR_INTEGRATION works here (unlike in our local build) because a production
-- account has an SNS topic to publish to -- this is the push-based task/pipe error
-- notification that our alert-polling approach substitutes for locally.
-- -----------------------------------------------------------------------------
create or replace notification integration ni_trade_pipe_errors
    type = queue
    notification_provider = aws_sns
    direction = outbound
    aws_sns_topic_arn = 'arn:aws:sns:eu-central-1:123456789012:snowflake-pipe-errors'
    aws_sns_role_arn = 'arn:aws:iam::123456789012:role/snowflake-sns-publisher'
    enabled = true;

create or replace pipe pipe_trade_event_external
    auto_ingest = true
    error_integration = ni_trade_pipe_errors
    comment = 'Event-driven ingestion from S3. ~1 minute latency, no warehouse, no orchestrator.'
as
copy into raw.trade_event (
    payload,
    source_file_name,
    source_file_row_number,
    source_file_last_modified,
    source_file_content_key,
    load_method
)
from (
    select
        $1,
        metadata$filename,
        metadata$file_row_number,
        metadata$file_last_modified,
        metadata$file_content_key,
        'SNOWPIPE'
    from @trade_landing_external
)
file_format = (format_name = 'ff_trade_ndjson')
on_error = 'continue';

desc pipe pipe_trade_event_external;   -- copy NOTIFICATION_CHANNEL into the S3 event config

-- -----------------------------------------------------------------------------
-- STEP 4 (optional, for very high volume): Snowpipe Streaming instead of files.
--
-- Above roughly a few thousand files per hour, the per-file overhead of Snowpipe
-- starts to dominate and the economics invert. Snowpipe Streaming writes rows
-- directly from the producer via the SDK -- no files, no stage, seconds of latency,
-- and billed per client-hour rather than per file.
--
-- The migration is confined to the producer: RAW.TRADE_EVENT, the stream, the task
-- and every dbt model stay exactly as they are, because they only ever see rows in
-- the landing table. The only loss is file-level lineage, which is replaced by
-- channel/offset lineage from the streaming client.
--
-- That property -- that the ingestion mechanism is swappable without touching the
-- transformation layer -- is the main reason RAW is a plain insert-only table with a
-- VARIANT column rather than something cleverer.
-- -----------------------------------------------------------------------------
