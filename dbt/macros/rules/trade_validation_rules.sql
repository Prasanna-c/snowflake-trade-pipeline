{#-
    ============================================================================
    THE RULE BOOK.

    Every business rule in the platform is declared exactly once, here. The adjudication
    model generates its SQL from this list, the audit tables are keyed on these codes, and a
    singular test asserts that this list and the ref_rejection_reason seed describe the same
    set of codes with the same severities.

    WHY a declarative list instead of hand-written CASE expressions in the model:

      * Adding a rule is a four-line change in one file. A reviewer sees the rule, not 200
        lines of restructured SQL, so review actually catches logic errors.
      * The rule set becomes queryable metadata. "Which rules are REJECT severity, and which
        requirement does each satisfy?" is answered from the seed, not by reading SQL.
      * Rules cannot drift out of the audit log. The same list generates both the evaluation
        and the log, so a rule can never fire without being recorded.

    PHASES -- the ordering is load-bearing, not cosmetic:

      FIELD  Rules decidable from one event plus reference data. Evaluated first.
      STATE  Rules comparing the event against the stored state of the trade. Evaluated only
             for events that passed the FIELD phase.

    Why FIELD must precede STATE: version arbitration compares against the last *accepted*
    version. If a malformed version 5 were allowed to set the high-water mark, a subsequent
    perfectly valid version 3 would be rejected as stale on the authority of an event we
    threw away. Filtering to field-valid events before computing the mark prevents that, and
    it is the subtlest piece of logic in the project.

    SEVERITY:
      REJECT     Event does not enter the golden record. Logged to AUDIT.
      WARN       Event is accepted and flagged. Reviewed, not blocked.
      SUPERSEDE  Event was replaced by a later arrival of the same version.

    Conditions are SQL fragments evaluated against these aliases:
      t    the typed trade event, plus arbitration columns
      cp   ref_counterparty            (left joined on t.counterparty_id)
      cur  ref_currency  as notional   (left joined on t.notional_currency)
      scur ref_currency  as settlement (left joined on t.settlement_currency)
      p    ref_product                 (left joined on t.product_type)
      bk   ref_book                    (left joined on t.book_id)

    A condition must be TRUE only when the rule is VIOLATED, and must be NULL-safe: a null
    input should trip the completeness rule (RJ004), not silently pass every other rule
    because `null > 5` is null rather than true.

    The list is built with append() rather than a single literal, because Jinja does not
    permit comments inside a tag -- and a rule book whose reasoning cannot be written next to
    the rule is a rule book nobody maintains correctly.
    ============================================================================
-#}

{% macro trade_validation_rules() %}

    {%- set rules = [] -%}

    {#- ---------------------------------------------------------------------
        BUSINESS RULE 1. Reject a version lower than the one already accepted.
        Evaluated against effective_prior_version, which is the greater of the stored
        version and the highest version accepted earlier in the same run.
    ---------------------------------------------------------------------- -#}
    {%- do rules.append({
        'code': 'RJ001',
        'name': 'Stale version',
        'severity': 'REJECT',
        'phase': 'STATE',
        'requirement': 'R1',
        'condition': 't.trade_version < t.effective_prior_version'
    }) -%}

    {#- Economically impossible: a trade cannot mature before it was struck. -#}
    {%- do rules.append({
        'code': 'RJ002',
        'name': 'Maturity before trade date',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.maturity_date is not null and t.trade_date is not null and t.maturity_date < t.trade_date'
    }) -%}

    {#- ---------------------------------------------------------------------
        BUSINESS RULE 3. Reject a trade whose maturity date has already passed.

        CANCEL is deliberately exempt. Cancelling a trade that has already matured is a
        legitimate correction, and refusing it would leave a wrongly-booked trade
        permanently in the book with no way to withdraw it.
    ---------------------------------------------------------------------- -#}
    {%- do rules.append({
        'code': 'RJ003',
        'name': 'Maturity in the past',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'R3',
        'condition': "t.maturity_date is not null and t.maturity_date < to_date('" ~ var('business_date') ~ "') and coalesce(t.action, '') <> 'CANCEL'"
    }) -%}

    {#- Completeness. Checked as one rule rather than ten so that a report says "these
        fields are missing" once, rather than emitting ten near-identical codes. The
        offending field names are in missing_mandatory_fields. -#}
    {%- do rules.append({
        'code': 'RJ004',
        'name': 'Missing mandatory field',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.trade_id is null
                      or t.trade_version is null
                      or t.action is null
                      or t.counterparty_id is null
                      or t.book_id is null
                      or t.product_type is null
                      or t.buy_sell is null
                      or t.notional_currency is null
                      or t.notional_amount is null
                      or t.trade_date is null'
    }) -%}

    {%- do rules.append({
        'code': 'RJ005',
        'name': 'Unknown counterparty',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.counterparty_id is not null and t.counterparty_ref_id is null'
    }) -%}

    {%- do rules.append({
        'code': 'RJ006',
        'name': 'Invalid currency',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.notional_currency is not null and t.notional_currency_ref_code is null'
    }) -%}

    {#- Direction is carried by buy_sell. A negative notional is always an upstream mapping
        defect, and accepting it would double-count the sign. -#}
    {%- do rules.append({
        'code': 'RJ007',
        'name': 'Non-positive notional',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.notional_amount is not null and t.notional_amount <= 0'
    }) -%}

    {#- ---------------------------------------------------------------------
        Type coercion failure, as distinct from a missing field.

        The typed model uses try_to_* casts, so a value that was PRESENT in the payload but
        became NULL after casting is a structural defect. The distinction matters
        operationally: RJ004 tells the upstream team "you did not send it", RJ008 tells them
        "you sent it in a format we cannot read". Conflating the two sends them after the
        wrong bug.
    ---------------------------------------------------------------------- -#}
    {%- do rules.append({
        'code': 'RJ008',
        'name': 'Malformed payload',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.cast_failure_count > 0'
    }) -%}

    {#- ---------------------------------------------------------------------
        BUSINESS RULE 2, the audit half. The winner of a same-version race is accepted and
        overwrites the stored trade; the losers are recorded as SUPERSEDED so the
        replacement is evidenced rather than invisible.
    ---------------------------------------------------------------------- -#}
    {%- do rules.append({
        'code': 'RJ009',
        'name': 'Superseded within run',
        'severity': 'SUPERSEDE',
        'phase': 'STATE',
        'requirement': 'R2',
        'condition': 't.intra_run_rank > 1'
    }) -%}

    {#- Cancellation is terminal. Reinstating requires a new trade_id. -#}
    {%- do rules.append({
        'code': 'RJ010',
        'name': 'Amendment after cancellation',
        'severity': 'REJECT',
        'phase': 'STATE',
        'requirement': 'OWN',
        'condition': "t.prior_is_cancelled and coalesce(t.action, '') in ('NEW', 'AMEND')"
    }) -%}

    {%- do rules.append({
        'code': 'RJ011',
        'name': 'Unknown book',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.book_id is not null and t.book_ref_id is null'
    }) -%}

    {%- do rules.append({
        'code': 'RJ012',
        'name': 'Settlement before trade date',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.settlement_date is not null and t.trade_date is not null and t.settlement_date < t.trade_date'
    }) -%}

    {#- ref_product is the platform's authorisation boundary, not merely a lookup: a product
        absent from it is one we are not permitted to process. -#}
    {%- do rules.append({
        'code': 'RJ013',
        'name': 'Unsupported product',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.product_type is not null and t.product_ref_type is null'
    }) -%}

    {#- Almost always a timezone defect upstream rather than a genuine future booking. -#}
    {%- do rules.append({
        'code': 'RJ014',
        'name': 'Trade date in the future',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': "t.trade_date is not null and t.trade_date > to_date('" ~ var('business_date') ~ "')"
    }) -%}

    {%- do rules.append({
        'code': 'RJ015',
        'name': 'Invalid direction',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': "t.buy_sell is not null and t.buy_sell not in ('BUY', 'SELL')"
    }) -%}

    {#- The counterparty exists but is defaulted or in administration. A credit control
        rather than a data quality one, which is why it is separate from RJ005: the
        remediation is escalation, not resubmission. -#}
    {%- do rules.append({
        'code': 'RJ016',
        'name': 'Inactive counterparty',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.counterparty_ref_id is not null and t.counterparty_is_active = false'
    }) -%}

    {#- A physically-settled product cannot settle in a non-deliverable currency; it has to
        be booked as an NDF instead. -#}
    {%- do rules.append({
        'code': 'RJ017',
        'name': 'Non-deliverable currency on physical settlement',
        'severity': 'REJECT',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.is_physically_settled = true and t.settlement_currency_ref_code is not null and t.settlement_currency_is_deliverable = false'
    }) -%}

    {#- ---------------------------------------------------------------------
        WARN, not REJECT. A limit breach is a risk conversation, and rejecting the trade
        would hide it -- the desk would simply resubmit as several smaller tickets and the
        position would never be seen. We want the breach recorded against the real trade.
    ---------------------------------------------------------------------- -#}
    {%- do rules.append({
        'code': 'RJ018',
        'name': 'Desk notional limit breached',
        'severity': 'WARN',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.notional_limit is not null and t.notional_amount is not null and t.notional_amount > t.notional_limit'
    }) -%}

    {#- Same UTI against two trade_ids suggests a double booking. Regulators care about this
        more than we do, hence WARN -- detected and reported rather than blocked. -#}
    {%- do rules.append({
        'code': 'RJ019',
        'name': 'Duplicate trade identifier',
        'severity': 'WARN',
        'phase': 'FIELD',
        'requirement': 'OWN',
        'condition': 't.uti_distinct_trade_count > 1'
    }) -%}

    {{ return(rules) }}

{% endmacro %}


{#- ------------------------------------------------------------------------ -#}
{#- Derived views over the rule book. Everything below is generated, never edited. -#}
{#- ------------------------------------------------------------------------ -#}

{% macro rules_in_phase(phase) %}
    {%- set matching = [] -%}
    {%- for rule in trade_validation_rules() -%}
        {%- if rule['phase'] == phase -%}
            {%- do matching.append(rule) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(matching) }}
{% endmacro %}


{% macro rule_codes_with_severity(severity) %}
    {%- set codes = [] -%}
    {%- for rule in trade_validation_rules() -%}
        {%- if rule['severity'] == severity -%}
            {%- do codes.append(rule['code']) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(codes) }}
{% endmacro %}


{#-
    Render a SQL array literal of rule codes, e.g. array_construct('RJ001','RJ002').
    Lets membership be tested without hard-coding the list at the call site.
-#}
{% macro rule_code_array(severity) %}
    {%- set codes = rule_codes_with_severity(severity) -%}
    {%- if codes | length == 0 -%}
        array_construct()
    {%- else -%}
        array_construct({% for code in codes %}'{{ code }}'{{ ", " if not loop.last }}{% endfor %})
    {%- endif -%}
{% endmacro %}


{#-
    Generate the violated-rule-code array for one phase.

    array_construct_compact drops the NULLs produced by non-matching CASE branches, so the
    result is exactly the codes that fired, with no sentinel values to filter later.

    Every rule is evaluated rather than short-circuiting on the first failure. That is a
    deliberate operational choice: a trade capture team told all four of its problems at once
    fixes them in one pass, instead of resubmitting four times and learning about one new
    rejection each time.
-#}
{% macro evaluate_rules(phase, alias='t') %}
    {%- set rules = rules_in_phase(phase) -%}
    array_construct_compact(
        {%- for rule in rules %}
        {#- Word-boundary substitution, not a plain replace: a bare `replace('t.', ...)`
            also rewrites the tail of any identifier ending in "t", so a future condition
            mentioning `amount.something` would silently compile to nonsense. Every
            condition must reference the `t.` alias only -- columns from the reference
            joins are projected into the model as `*_ref_*` for exactly this reason. -#}
        case when {{ modules.re.sub('\\bt\\.', alias ~ '.', rule['condition']) | trim }} then '{{ rule['code'] }}' end
        {{- "," if not loop.last }}
        {%- endfor %}
    )
{% endmacro %}


{#-
    Rule documentation rendered into the model's YAML description and into `dbt docs`, so the
    published documentation is true by construction rather than by someone remembering to
    update it.
-#}
{% macro rule_book_markdown() %}
    {%- set rules = trade_validation_rules() -%}
| Code | Rule | Severity | Phase | Requirement |
| ---- | ---- | -------- | ----- | ----------- |
    {%- for rule in rules %}
| {{ rule['code'] }} | {{ rule['name'] }} | {{ rule['severity'] }} | {{ rule['phase'] }} | {{ rule['requirement'] }} |
    {%- endfor %}
{% endmacro %}
