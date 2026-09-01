/*
    One row per (event, rule that fired). The rule-hit log.

    FCT_TRADE_REJECTED answers "which events were rejected". This answers "which rules
    are firing, how often, and against which upstream system" -- and that is the question
    that actually improves data quality over time.

    Concretely, this is the table behind:
      * the reject-rate spike alert's "top rules fired" breakdown,
      * "MUREX accounts for 80% of RJ006 currency failures" -- a conversation with one
        upstream team rather than a generic quality complaint,
      * "RJ012 has fired 4,000 times and nobody has ever fixed it" -- a rule that is
        either wrong or being ignored, both of which are worth knowing.

    Flattening the array into rows is what makes all of that a GROUP BY instead of array
    gymnastics. WARN and SUPERSEDE hits are included, not just rejections, because a
    limit breach that never blocks anything is still something risk needs counted.
*/

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        cluster_by = ['evaluated_on', 'rule_code'],
        tags = ['audit', 'compliance']
    )
}}

with adjudicated as (

    select * from {{ ref('int_trade_event_adjudicated') }}
    where array_size(violated_rule_codes) > 0

{% if is_incremental() %}
      and adjudicated_at > (select coalesce(max(evaluated_at), '1900-01-01'::timestamp_ltz) from {{ this }})
{% endif %}

),

flattened as (

    /*
        LATERAL FLATTEN turns the code array into one row per code. Snowflake's flatten
        is the right tool rather than a join against the reason seed on ARRAY_CONTAINS:
        flatten reads the array once per row, whereas the join would evaluate a
        containment predicate for every (event, reason) pair -- 19x the work for the
        same answer.
    */
    select
        adjudicated.event_sk,
        adjudicated.batch_id,
        adjudicated.trade_id,
        adjudicated.trade_version,
        adjudicated.action,
        adjudicated.verdict,
        adjudicated.source_system,
        adjudicated.book_id,
        adjudicated.desk,
        adjudicated.counterparty_id,
        adjudicated.product_type,
        adjudicated.notional_amount,
        adjudicated.notional_currency,
        adjudicated.trade_date,
        adjudicated.maturity_date,
        adjudicated.source_file_name,
        adjudicated.source_file_row_number,
        adjudicated.dbt_invocation_id,
        adjudicated.adjudicated_at,

        rule_hit.value::varchar as rule_code,
        rule_hit.index as rule_hit_index,

        -- Which phase decided it: FIELD rules are upstream data problems, STATE rules
        -- are sequencing or lifecycle problems. Different teams, different fixes.
        case
            when array_contains(rule_hit.value, adjudicated.field_rule_codes) then 'FIELD'
            else 'STATE'
        end as rule_phase

    from adjudicated,
        lateral flatten(input => adjudicated.violated_rule_codes) as rule_hit

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['flattened.event_sk', 'flattened.rule_code']) }} as rule_result_sk,

        flattened.event_sk,
        flattened.batch_id,
        flattened.rule_code,
        reason.rule_name,
        reason.rule_category,
        reason.severity as rule_severity,
        reason.requirement_ref,
        reason.description as rule_description,
        reason.remediation,
        flattened.rule_phase,

        -- Whether this particular hit is what caused the event to be refused, as opposed
        -- to a warning recorded alongside an accepted trade.
        reason.severity = 'REJECT' as is_blocking,

        flattened.verdict as event_verdict,

        -- Dimensions for slicing. Denormalised on purpose: this table is queried
        -- interactively by operations, and making them join four dimensions to answer
        -- "which desk generates the most rejections" guarantees they will not.
        flattened.trade_id,
        flattened.trade_version,
        flattened.action,
        flattened.source_system,
        flattened.book_id,
        flattened.desk,
        flattened.counterparty_id,
        flattened.product_type,
        flattened.notional_amount,
        flattened.notional_currency,
        flattened.trade_date,
        flattened.maturity_date,
        flattened.source_file_name,
        flattened.source_file_row_number,

        flattened.dbt_invocation_id,
        flattened.adjudicated_at as evaluated_at,
        flattened.adjudicated_at::date as evaluated_on,
        current_timestamp() as dbt_created_at

    from flattened
    left join {{ ref('ref_rejection_reason') }} as reason
        on flattened.rule_code = reason.rule_code

)

select * from final
