/*
    ============================================================================
    THE ADJUDICATION MODEL. Every business rule is decided here, once.

    This is an append-style incremental model that holds one row per event ever
    processed, together with the verdict and the rule codes that produced it. Everything
    downstream is a projection of this table:

        CORE.FCT_TRADE            -> the current golden record (max accepted version)
        CORE.FCT_TRADE_VERSION    -> every accepted version
        AUDIT.FCT_TRADE_REJECTED  -> the rejects, for compliance
        AUDIT.TRADE_RULE_RESULT   -> one row per rule that fired, for analysis

    ---------------------------------------------------------------------------
    WHY THE STATE LIVES IN THIS MODEL AND NOT IN FCT_TRADE

    Business rule 1 needs the currently-stored version of a trade in order to reject a
    lower one. The obvious source is CORE.FCT_TRADE -- but referencing a downstream
    model creates a cycle in the dbt DAG, and dbt will refuse to build it.

    The resolution is that this model IS the state. An incremental model may read
    `{{ this }}`, which dbt resolves to the table's contents *as of the start of the
    run* (the model's SELECT populates a temporary relation first, then merges). So the
    set of previously-accepted versions is available here, legally, with no cycle. The
    adjudication log is both the audit trail and the authority on current version --
    which is also conceptually right: the record of what we accepted is what determines
    what we will accept next.

    ---------------------------------------------------------------------------
    THE FOUR-STEP PIPELINE, AND WHY THE ORDER IS LOAD-BEARING

    1. FIELD RULES     Everything decidable from one event plus reference data.
    2. DEDUPLICATION   Among field-valid events only, rank by (trade_id, version).
                       Rank > 1 is SUPERSEDED -- business rule 2's audit trail.
    3. HIGH-WATER MARK For each event, the highest version already accepted, taken as
                       the greater of (a) prior runs, from {{ this }}, and (b) earlier
                       arrivals within this run, via a window function.
    4. STATE RULES     Version arbitration (rule 1) and lifecycle (amend-after-cancel).

    Step 1 must precede step 3. If a malformed version 5 were allowed to set the
    high-water mark, a subsequent perfectly valid version 3 would be rejected as stale
    on the authority of an event we discarded. Filtering to field-valid events before
    computing the mark is the fix, and it is the single subtlest thing in this file.

    Step 3's window handles the multi-version-in-one-batch cases correctly:
        stored v2, batch has v3 then v5  -> both accepted, v5 becomes current
        stored v2, batch has v5 then v3  -> v5 accepted, v3 rejected RJ001
        stored v3, batch has v3          -> accepted as REPLACE (rule 2)

    ---------------------------------------------------------------------------
    IDEMPOTENCY

    Incremental strategy is `merge` on event_sk, not `append`. An append would duplicate
    every row if a batch were reprocessed after a partial failure. With merge, running
    this model twice over the same input is a no-op, which is what makes automatic retry
    safe -- and automatic retry is the only reason the pipeline can be left unattended.
    ============================================================================
*/

{{
    config(
        materialized = 'incremental',
        unique_key = 'event_sk',
        incremental_strategy = 'merge',
        on_schema_change = 'append_new_columns',
        cluster_by = ['adjudicated_at::date', 'verdict'],
        tags = ['adjudication', 'critical']
    )
}}

with

-- ---------------------------------------------------------------------------
-- Prior state: the highest version accepted for each trade before this run, and
-- whether the trade has been cancelled (cancellation is terminal).
--
-- On a full refresh this is empty, which is correct: with no history, every trade is
-- new and nothing can be stale.
-- ---------------------------------------------------------------------------
prior_state as (

    {% if is_incremental() %}
    select
        trade_id,
        max(trade_version) as prior_version,
        max(iff(action = 'CANCEL', 1, 0)) = 1 as prior_is_cancelled
    from {{ this }}
    where verdict = 'ACCEPTED'
        and trade_id is not null
    group by trade_id
    {% else %}
    -- Typed empty relation so the joins below compile identically in both branches.
    select
        cast(null as varchar) as trade_id,
        cast(null as number(38, 0)) as prior_version,
        cast(null as boolean) as prior_is_cancelled
    where false
{% endif %}

),

-- ---------------------------------------------------------------------------
-- Events awaiting adjudication.
--
-- The batch_seq watermark keeps the incremental scan bounded. The merge on event_sk means
-- a watermark that reaches back too far costs a reprocess rather than a corruption: a row
-- already committed here is updated in place, not inserted twice.
--
-- That holds for one writer. It does not hold for two. A merge matches against what is
-- committed in the target, so two runs building this model at the same time both find the
-- key absent and both insert it -- which is how duplicate event_sk gets in, and dbt has no
-- locking to prevent it. One writer per target is an operational invariant, not something
-- this model can enforce; see docs/known-limitations.md.
-- ---------------------------------------------------------------------------
pending as (

    select * from {{ ref('int_trade_event_typed') }}

    {% if is_incremental() %}
    where batch_seq > (select coalesce(max(t.batch_seq), 0) from {{ this }} as t)
    {% endif %}

),

-- ---------------------------------------------------------------------------
-- Reference data joins. LEFT JOIN throughout: a missing match IS the rule violation
-- (RJ005 unknown counterparty, RJ006 invalid currency, and so on), so an inner join
-- would make the offending rows disappear instead of rejecting them.
-- ---------------------------------------------------------------------------
enriched as (

    -- The *_ref_* columns carry the reference table's own key so that "did this join
    -- match?" survives into the downstream CTEs. The rules are evaluated one CTE later,
    -- where the join aliases no longer exist, so a rule cannot say `cp.counterparty_id
    -- is null` -- it has to read a projected column. Every unknown-reference rule
    -- (RJ005, RJ006, RJ008, RJ010) depends on these being here.
    select
        t.*,
        cp.counterparty_id as counterparty_ref_id,
        cp.counterparty_name,
        cp.lei as counterparty_lei,
        cp.country_code as counterparty_country_code,
        cp.credit_rating as counterparty_credit_rating,
        cp.is_active as counterparty_is_active,
        bk.book_id as book_ref_id,
        bk.book_name,
        bk.desk,
        bk.legal_entity as book_legal_entity,
        bk.notional_limit,
        p.product_type as product_ref_type,
        p.asset_class as reference_asset_class,
        p.requires_maturity,
        p.is_physically_settled,
        cur.currency_code as notional_currency_ref_code,
        cur.minor_units as notional_currency_minor_units,
        scur.currency_code as settlement_currency_ref_code,
        scur.is_deliverable as settlement_currency_is_deliverable

    from pending as t
    left join {{ ref('ref_counterparty') }} as cp
        on t.counterparty_id = cp.counterparty_id
    left join {{ ref('ref_book') }} as bk
        on t.book_id = bk.book_id
    left join {{ ref('ref_product') }} as p
        on t.product_type = p.product_type
    left join {{ ref('ref_currency') }} as cur
        on t.notional_currency = cur.currency_code
    left join {{ ref('ref_currency') }} as scur
        on t.settlement_currency = scur.currency_code

),

-- ---------------------------------------------------------------------------
-- STEP 1: field rules. Generated from the rule book by evaluate_rules().
-- ---------------------------------------------------------------------------
field_evaluated as (

    select
        enriched.*,
        {{ evaluate_rules('FIELD', alias='enriched') }} as field_rule_codes

    from enriched

),

field_verdict as (

    select
        field_evaluated.*,

        -- Field-valid means "no REJECT-severity field rule fired". WARN rules such as
        -- a limit breach do not disqualify the event -- that is the entire point of
        -- having a WARN severity.
        not arrays_overlap(
            field_evaluated.field_rule_codes,
            {{ rule_code_array('REJECT') }}
        ) as is_field_valid

    from field_evaluated

),

-- ---------------------------------------------------------------------------
-- STEP 2: deduplication within the run -- business rule 2.
--
-- Ranked only over field-valid events, so a malformed resend cannot supersede a good
-- event. Ordering is business time first, then arrival, then the surrogate key as a
-- final deterministic tie-break: without that last term the winner of a tie would be
-- non-deterministic and the model would not be reproducible.
--
-- Events with no trade_id or no version cannot be ranked at all; they are already
-- rejected by RJ004, and rank 1 is assigned so that the union below stays uniform.
-- ---------------------------------------------------------------------------
ranked as (

    select
        field_verdict.*,
        case
            when
                field_verdict.is_field_valid
                and field_verdict.trade_id is not null
                and field_verdict.trade_version is not null
                then row_number() over (
                        -- is_field_valid belongs in the PARTITION, not only in the CASE that
                        -- consumes the result. A window function ranks every row of its
                        -- partition; filtering afterwards does not remove the malformed
                        -- arrivals from the ordering. With them present, a corrupt resend
                        -- stamped later took rank 1 and pushed the good event to rank 2 --
                        -- superseding a valid trade in favour of a rejected one, which is the
                        -- exact opposite of the intent stated above.
                        partition by
                            field_verdict.trade_id,
                            field_verdict.trade_version,
                            field_verdict.is_field_valid
                        order by
                            field_verdict.effective_event_ts desc,
                            field_verdict.batch_seq desc,
                            field_verdict.event_sk desc
                    )
            else 1
        end as intra_run_rank

    from field_verdict

),

-- ---------------------------------------------------------------------------
-- STEP 3: the high-water mark.
--
-- greatest() of the stored version and the highest version seen earlier in this run.
-- The window is restricted to rank-1, field-valid events, which is what makes the mark
-- trustworthy.
--
-- `rows between unbounded preceding and 1 preceding` deliberately excludes the current
-- row: an event must be compared against what came BEFORE it, never against itself.
-- ---------------------------------------------------------------------------
with_high_water_mark as (

    select
        ranked.*,

        coalesce(prior_state.prior_is_cancelled, false) as prior_is_cancelled,
        coalesce(prior_state.prior_version, 0) as stored_version,

        greatest(
            coalesce(prior_state.prior_version, 0),
            coalesce(
                max(
                    case
                        when ranked.is_field_valid and ranked.intra_run_rank = 1
                            then ranked.trade_version
                    end
                ) over (
                    partition by ranked.trade_id
                    order by ranked.batch_seq, ranked.effective_event_ts, ranked.event_sk
                    rows between unbounded preceding and 1 preceding
                ),
                0
            )
        ) as effective_prior_version

    from ranked
    left join prior_state
        on ranked.trade_id = prior_state.trade_id

),

-- ---------------------------------------------------------------------------
-- STEP 4: state rules -- version arbitration and lifecycle.
-- ---------------------------------------------------------------------------
state_evaluated as (

    select
        with_high_water_mark.*,
        {{ evaluate_rules('STATE', alias='with_high_water_mark') }} as state_rule_codes

    from with_high_water_mark

),

-- ---------------------------------------------------------------------------
-- Final verdict.
-- ---------------------------------------------------------------------------
adjudicated as (

    select
        state_evaluated.*,

        array_cat(
            state_evaluated.field_rule_codes,
            state_evaluated.state_rule_codes
        ) as violated_rule_codes

    from state_evaluated

),

final as (

    select
        -- Identity ---------------------------------------------------------
        adjudicated.event_sk,
        adjudicated.batch_id,
        adjudicated.batch_seq,
        adjudicated.trade_id,
        adjudicated.trade_version,
        adjudicated.action,
        adjudicated.uti,

        -- Economics --------------------------------------------------------
        adjudicated.product_type,
        coalesce(adjudicated.asset_class, adjudicated.reference_asset_class) as asset_class,
        adjudicated.buy_sell,
        adjudicated.notional_amount,
        adjudicated.notional_currency,
        adjudicated.settlement_currency,
        adjudicated.quantity,
        adjudicated.price,

        -- Dates ------------------------------------------------------------
        adjudicated.trade_date,
        adjudicated.settlement_date,
        adjudicated.maturity_date,

        -- Attribution ------------------------------------------------------
        adjudicated.counterparty_id,
        adjudicated.counterparty_name,
        adjudicated.counterparty_lei,
        adjudicated.counterparty_country_code,
        adjudicated.counterparty_credit_rating,
        adjudicated.book_id,
        adjudicated.book_name,
        adjudicated.desk,
        adjudicated.notional_limit,
        adjudicated.trader_id,
        adjudicated.execution_venue,
        adjudicated.clearing_house,
        coalesce(adjudicated.legal_entity, adjudicated.book_legal_entity) as legal_entity,

        -- Provenance -------------------------------------------------------
        adjudicated.source_system,
        adjudicated.event_timestamp,
        adjudicated.effective_event_ts,
        adjudicated.source_file_name,
        adjudicated.source_file_row_number,
        adjudicated.load_method,
        adjudicated.load_ts,
        adjudicated.drained_at,

        -- Arbitration ------------------------------------------------------
        adjudicated.stored_version,
        adjudicated.effective_prior_version,
        adjudicated.intra_run_rank,
        adjudicated.prior_is_cancelled,

        -- Verdict ----------------------------------------------------------
        -- SUPERSEDED is checked before REJECTED: a superseded event is not a data
        -- quality failure, and counting it as one would inflate the reject-rate metric
        -- that the alerting thresholds are calibrated against.
        case
            when array_contains('RJ009'::variant, adjudicated.violated_rule_codes)
                then 'SUPERSEDED'
            when arrays_overlap(adjudicated.violated_rule_codes, {{ rule_code_array('REJECT') }})
                then 'REJECTED'
            else 'ACCEPTED'
        end as verdict,

        adjudicated.violated_rule_codes,
        adjudicated.field_rule_codes,
        adjudicated.state_rule_codes,

        -- WARN codes recorded separately so "accepted but flagged" is queryable
        -- without re-deriving severity.
        array_intersection(
            adjudicated.violated_rule_codes,
            {{ rule_code_array('WARN') }}
        ) as warning_rule_codes,

        array_size(adjudicated.violated_rule_codes) as violated_rule_count,

        -- Delimited forms of the two code arrays. They exist as stored columns rather
        -- than being derived at read time for two reasons.
        --
        -- A rejection report is read by people, and 'RJ005,RJ006' is easier to scan and
        -- to filter with LIKE than a variant array is to unpack.
        --
        -- More importantly, they are what makes the rule codes testable. A dbt unit test
        -- cannot assert an ARRAY column on Snowflake: the dict fixture format casts every
        -- literal with TRY_CAST, which has no string-to-ARRAY conversion, and the sql
        -- fixture format would require every column of this model to be spelled out.
        -- Asserting exactly which codes fired is the highest-value test in the project,
        -- so the model deliberately exposes a form that a fixture can express.
        --
        -- The expressions are repeated rather than referencing the aliases above: a
        -- SELECT list alias is not in scope for its siblings in Snowflake.
        array_to_string(adjudicated.violated_rule_codes, ',') as violated_rule_codes_csv,

        array_to_string(
            array_intersection(
                adjudicated.violated_rule_codes,
                {{ rule_code_array('WARN') }}
            ),
            ','
        ) as warning_rule_codes_csv,

        -- What this event did to the trade's version, for accepted events.
        case
            when array_contains('RJ009'::variant, adjudicated.violated_rule_codes)
                then null
            when arrays_overlap(adjudicated.violated_rule_codes, {{ rule_code_array('REJECT') }})
                then null
            when adjudicated.effective_prior_version = 0 then 'NEW'
            when adjudicated.trade_version = adjudicated.effective_prior_version then 'REPLACE'
            else 'AMEND'
        end as version_action,

        -- Diagnostics for the rejection report ------------------------------
        adjudicated.cast_failed_fields,
        adjudicated.missing_mandatory_fields,
        adjudicated.cast_failure_count,
        adjudicated.uti_distinct_trade_count,

        -- The original payload travels with the verdict. An auditor's first question
        -- about a rejected trade is "what exactly did you receive?", and answering it
        -- by joining back to RAW months later is slow and, once RAW is archived,
        -- sometimes impossible.
        adjudicated.raw_payload,

        -- Lineage of the decision itself ------------------------------------
        '{{ invocation_id }}' as dbt_invocation_id,
        current_timestamp() as adjudicated_at

    from adjudicated

)

select * from final
