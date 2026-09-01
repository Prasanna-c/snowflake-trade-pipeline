/*
    The rule book is defined in two places by necessity, and this test is what stops that
    becoming a liability.

      macros/rules/trade_validation_rules.sql  -- condition and severity (executable)
      seeds/ref_rejection_reason.csv           -- description and remediation (human)

    They cannot be merged: a Jinja list cannot be joined to in SQL, and a seed cannot hold
    an executable predicate. So instead they are held to describing the same set of codes,
    with the same severities.

    The failure this prevents is specific and nasty. Add a rule to the macro, forget the
    seed, and the rule fires correctly -- but FCT_TRADE_REJECTED left-joins the seed for its
    description, so the rejection appears with a null reason and null remediation. The
    pipeline is working and the compliance report is useless, which is the worst combination.

    Configured as an error rather than a warning: a rule code with no description is not
    something to fix later.
*/

{{ config(severity = 'error', tags = ['metadata', 'critical']) }}

with macro_rules as (

    {#- Render the macro's rule list as a SQL relation so it can be compared. -#}
    {%- set rules = trade_validation_rules() -%}
    select
        column1 as rule_code,
        column2 as severity,
        column3 as rule_name
    from values
    {%- for rule in rules %}
        (
            '{{ rule['code'] }}',
            '{{ rule['severity'] }}',
            '{{ rule['name'] | replace("'", "''") }}'
        ){{ "," if not loop.last }}
    {%- endfor %}

),

seed_rules as (

    select
        rule_code,
        severity,
        rule_name
    from {{ ref('ref_rejection_reason') }}

),

-- Declared in the macro but absent from the seed: rejections would have no description.
missing_from_seed as (

    select
        macro_rules.rule_code,
        'DECLARED_IN_MACRO_BUT_MISSING_FROM_SEED' as discrepancy,
        macro_rules.severity as macro_severity,
        cast(null as varchar) as seed_severity
    from macro_rules
    left join seed_rules on macro_rules.rule_code = seed_rules.rule_code
    where seed_rules.rule_code is null

),

-- In the seed but never evaluated: a rule that documentation promises and code never
-- applies, which is worse than an undocumented rule.
missing_from_macro as (

    select
        seed_rules.rule_code,
        'DOCUMENTED_IN_SEED_BUT_NEVER_EVALUATED' as discrepancy,
        cast(null as varchar) as macro_severity,
        seed_rules.severity as seed_severity
    from seed_rules
    left join macro_rules on seed_rules.rule_code = macro_rules.rule_code
    where macro_rules.rule_code is null

),

-- Severity disagreement: the seed says WARN, the macro rejects. The trade is refused
-- while the report insists it was only a warning.
severity_mismatch as (

    select
        macro_rules.rule_code,
        'SEVERITY_DISAGREEMENT' as discrepancy,
        macro_rules.severity as macro_severity,
        seed_rules.severity as seed_severity
    from macro_rules
    inner join seed_rules on macro_rules.rule_code = seed_rules.rule_code
    where macro_rules.severity <> seed_rules.severity

)

select * from missing_from_seed
union all
select * from missing_from_macro
union all
select * from severity_mismatch
