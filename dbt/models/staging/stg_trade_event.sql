/*
    Staging: rename and expose, nothing else.

    This model does NOT cast the payload. That happens one layer down, in
    int_trade_event_typed, and the separation is deliberate: casting is where data
    quality verdicts are born (a failed cast is rule RJ008), and putting verdict-bearing
    logic in staging would mean the staging layer had opinions. Staging's only job is to
    give every downstream model one place to read the queue from, so that if the
    ingestion contract changes, exactly one model changes.

    Materialised as a view: this is a projection over the queue with no computation, so
    a table would cost storage and build time to save nothing.
*/

with source as (

    select * from {{ source('raw', 'trade_event_queue') }}

),

renamed as (

    select
        -- Identity and ordering -------------------------------------------
        source.event_sk,
        source.batch_id,
        source.batch_seq,

        -- The payload, still untouched ------------------------------------
        source.payload as raw_payload,

        -- Lineage ----------------------------------------------------------
        source.source_file_name,
        source.source_file_row_number,
        source.load_method,
        source.load_ts,
        source.drained_at

    from source

)

select * from renamed
