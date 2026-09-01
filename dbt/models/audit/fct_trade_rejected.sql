/*
    The compliance record of everything the pipeline refused.

    "Store rejected trades in a separate table for compliance" is the requirement. What
    makes a rejection table actually usable in a compliance conversation, as opposed to
    merely existing, is three things this model does deliberately:

    1. THE ORIGINAL PAYLOAD IS HERE, verbatim. The first question about a rejected trade
       is "what exactly did you receive?" -- not "what did your parser make of it?".
       Joining back to RAW months later is slow and, once RAW is archived, sometimes
       impossible.

    2. REJECTED AND SUPERSEDED ARE IN ONE TABLE, distinguished by `disposition`. They are
       different things -- a rejection is a refusal, a supersession is business rule 2
       working correctly -- but an investigator asking "what happened to this event?"
       should not have to know which of two tables to look in. One table, one column to
       filter on.

    3. IT IS APPEND-ONLY AND NEVER DELETED. `incremental_strategy = 'append'` with no
       unique key, on a non-transient schema with the longest retention in the platform.
       Evidence that can be silently updated is not evidence.

    Grain: one row per rejected or superseded EVENT (not per trade). A trade rejected
    four times has four rows, which is exactly what an audit of upstream data quality
    needs to see.
*/

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        cluster_by = ['rejected_on', 'primary_rule_code'],
        tags = ['audit', 'compliance', 'critical']
    )
}}

with rejected_events as (

    select * from {{ ref('int_trade_event_adjudicated') }}
    where verdict in ('REJECTED', 'SUPERSEDED')

{% if is_incremental() %}
      -- Append-only, so the watermark IS load-bearing here: without it, every run would
      -- duplicate the entire history. Using the adjudication timestamp rather than the
      -- batch sequence because rows can be re-adjudicated by a full refresh upstream.
      --
      -- Note the column names differ either side of the comparison: this model stores the
      -- adjudication timestamp as `rejected_at`. Writing max(adjudicated_at) here would
      -- not fail loudly -- SQL resolves the unqualified name against the outer query
      -- instead, turning the subquery into a correlated aggregate that Snowflake rejects.
      and adjudicated_at > (select coalesce(max(rejected_at), '1900-01-01'::timestamp_ltz) from {{ this }})
{% endif %}

),

/*
    The single most important rule for triage.

    An event that fails four rules produces four codes, and an operator needs to be told
    which one to fix first. Precedence, highest first:
      structural  -- if we could not parse it, nothing else is meaningful
      completeness
      reference data
      temporal
      economic
      version/lifecycle
      limit (warnings)

    Ordering by code alone would put RJ001 first simply because it sorts first, which is
    arbitrary. Ordering by category is a deliberate operational choice.

    Expressed as a flatten-and-rank rather than a correlated subquery with ORDER BY /
    LIMIT 1: Snowflake cannot evaluate that subquery shape and fails to compile it. The
    set-based form is also cheaper, because the precedence is resolved once for all events
    instead of once per row.
*/
reject_rule_precedence as (

    select
        reason.rule_code,
        case reason.rule_category
            when 'STRUCTURAL' then 1
            when 'COMPLETENESS' then 2
            when 'REFERENCE' then 3
            when 'TEMPORAL' then 4
            when 'ECONOMIC' then 5
            when 'LIFECYCLE' then 6
            when 'VERSION' then 7
            else 8
        end as category_rank

    from {{ ref('ref_rejection_reason') }} as reason
    where reason.severity = 'REJECT'

),

primary_rule as (

    select
        rejected_events.event_sk,
        reject_rule_precedence.rule_code as primary_rule_code

    from rejected_events,
        lateral flatten(input => rejected_events.violated_rule_codes) as violated
    join reject_rule_precedence
        on reject_rule_precedence.rule_code = violated.value::varchar

    qualify row_number() over (
        partition by rejected_events.event_sk
        order by reject_rule_precedence.category_rank, reject_rule_precedence.rule_code
    ) = 1

),

with_reasons as (

    select
        rejected_events.*,

        -- Supersession outranks any rejection code, exactly as it does in the verdict
        -- itself. A superseded event can carry REJECT-severity codes too -- the losing
        -- side of a same-version race is often also stale (RJ001) or post-cancellation
        -- (RJ010) -- and taking the reject code as the headline reason would label the
        -- row SUPERSEDED while reporting it under RJ001. The reject-rate metric excludes
        -- supersessions, so the two numbers would then disagree about the same event.
        -- Every code the event violated is still on the row, in violated_rule_codes.
        iff(
            rejected_events.verdict = 'SUPERSEDED',
            'RJ009',
            primary_rule.primary_rule_code
        ) as primary_rule_code

    from rejected_events
    left join primary_rule
        on primary_rule.event_sk = rejected_events.event_sk

),

final as (

    select
        -- Grain -------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key(['with_reasons.event_sk']) }} as rejection_sk,
        with_reasons.event_sk,
        with_reasons.batch_id,
        with_reasons.batch_seq,

        -- Disposition -------------------------------------------------------
        with_reasons.verdict as disposition,
        with_reasons.primary_rule_code,
        reason.rule_name as primary_rule_name,
        reason.rule_category as primary_rule_category,
        reason.severity as primary_rule_severity,
        reason.requirement_ref as primary_requirement_ref,
        reason.description as rejection_description,
        reason.remediation as remediation_guidance,

        -- All codes, so a report can show every problem at once rather than making the
        -- upstream team resubmit once per defect.
        with_reasons.violated_rule_codes,
        with_reasons.violated_rule_count,
        array_size(with_reasons.violated_rule_codes) > 1 as has_multiple_violations,

        -- Field-level diagnostics ------------------------------------------
        with_reasons.missing_mandatory_fields,
        with_reasons.cast_failed_fields,

        -- What the event claimed to be ---------------------------------------
        with_reasons.trade_id,
        with_reasons.trade_version,
        with_reasons.action,
        with_reasons.uti,
        with_reasons.product_type,
        with_reasons.buy_sell,
        with_reasons.notional_amount,
        with_reasons.notional_currency,
        with_reasons.settlement_currency,
        with_reasons.trade_date,
        with_reasons.settlement_date,
        with_reasons.maturity_date,
        with_reasons.counterparty_id,
        with_reasons.counterparty_name,
        with_reasons.book_id,
        with_reasons.desk,
        with_reasons.trader_id,
        with_reasons.legal_entity,

        -- Version arbitration context. Without these two columns an RJ001 rejection is
        -- unexplainable: "stale version" means nothing unless you can see both the
        -- version sent and the version stored.
        with_reasons.stored_version,
        with_reasons.effective_prior_version,
        with_reasons.intra_run_rank,
        with_reasons.prior_is_cancelled,

        -- Lineage: enough to find the exact line of the exact file ------------
        with_reasons.source_system,
        with_reasons.event_timestamp,
        with_reasons.source_file_name,
        with_reasons.source_file_row_number,
        with_reasons.load_method,
        with_reasons.load_ts,

        -- The evidence -------------------------------------------------------
        with_reasons.raw_payload,

        -- Audit lineage ------------------------------------------------------
        with_reasons.dbt_invocation_id,
        with_reasons.adjudicated_at as rejected_at,
        with_reasons.adjudicated_at::date as rejected_on,
        current_timestamp() as dbt_created_at

    from with_reasons
    left join {{ ref('ref_rejection_reason') }} as reason
        on with_reasons.primary_rule_code = reason.rule_code

)

select * from final
