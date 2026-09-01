{#-
    ============================================================================
    dbt run auditing.

    AUDIT.DBT_RUN_RESULT records the outcome of every node in every invocation. It
    exists because of a specific gap: when dbt fails, Airflow knows, but Snowflake does
    not. Every Snowflake-side alert would stay green while the curated layer quietly
    went stale.

    Writing the result set back into Snowflake closes that gap. MONITORING.VW_PIPELINE_SLA
    reads this table to answer "when did dbt last succeed", and ALERT_TRANSFORM_BACKLOG
    fires off it -- so a dbt failure is detected even if the machine running Airflow
    has caught fire.

    It cannot be a model: it is populated by a hook from dbt's `results` object rather
    than by a SELECT, so on-run-start creates it and on-run-end appends to it.
    ============================================================================
-#}

{% macro create_audit_infrastructure() %}

    {%- if not execute -%}
        {{ return('select 1') }}
    {%- endif -%}

    {%- set audit_schema = generate_schema_name('audit', none) -%}
    {%- set fqn = target.database ~ '.' ~ audit_schema ~ '.dbt_run_result' -%}

    {%- set sql -%}
        create schema if not exists {{ target.database }}.{{ audit_schema }};

        create table if not exists {{ fqn }} (
            invocation_id       varchar(36)     not null,
            node_id             varchar(500)    not null,
            node_name           varchar(300),
            resource_type       varchar(50),
            node_status         varchar(50),
            execution_time_s    float,
            rows_affected       number(38, 0),
            failures            number(38, 0),
            message             varchar(5000),
            thread_id           varchar(50),
            dbt_target          varchar(50),
            dbt_version         varchar(50),
            invocation_args     variant,
            run_started_at      timestamp_ltz,
            run_completed_at    timestamp_ltz,
            run_status          varchar(20),
            logged_at           timestamp_ltz   not null default current_timestamp()
        )
        comment = 'One row per dbt node per invocation. Read by MONITORING.VW_PIPELINE_SLA.';
    {%- endset -%}

    {% do run_query(sql) %}
    {% do log("audit infrastructure ready: " ~ fqn, info=false) %}
    {{ return('select 1') }}

{% endmacro %}


{% macro log_dbt_results(results) %}

    {#- `results` is empty for `dbt parse`, `dbt compile`, `dbt docs generate` and for
        a run where selection matched nothing. Writing a zero-row insert would be
        harmless but noisy, and on a compile there is no warehouse session to spend. -#}
    {%- if not execute or results is none or results | length == 0 -%}
        {{ return('select 1') }}
    {%- endif -%}

    {%- set audit_schema = generate_schema_name('audit', none) -%}
    {%- set fqn = target.database ~ '.' ~ audit_schema ~ '.dbt_run_result' -%}

    {#- Roll the individual node statuses up into one verdict for the invocation, so
        the SLA view does not have to re-derive it on every read. -#}
    {%- set failure_statuses = ['error', 'fail', 'runtime error'] -%}
    {%- set has_failure = false -%}
    {%- for result in results -%}
        {%- if result.status | string | lower in failure_statuses -%}
            {%- set has_failure = true -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set run_status = 'failure' if has_failure else 'success' -%}

    {%- set rows = [] -%}
    {%- for result in results -%}
        {%- set message = (result.message | default('', true)) | string | replace("'", "''") | truncate(4900, true, '') -%}
        {%- set node_name = result.node.name | default('', true) | replace("'", "''") -%}
        {%- set row -%}
            (
                '{{ invocation_id }}',
                '{{ result.node.unique_id }}',
                '{{ node_name }}',
                '{{ result.node.resource_type }}',
                '{{ result.status }}',
                {{ result.execution_time | default(0, true) }},
                {{ (result.adapter_response.get('rows_affected') if result.adapter_response else none) | default('null', true) }},
                {{ result.failures | default('null', true) }},
                '{{ message }}',
                '{{ result.thread_id | default('', true) }}',
                '{{ target.name }}',
                '{{ dbt_version }}',
                to_variant(null),
                '{{ run_started_at }}',
                current_timestamp(),
                '{{ run_status }}'
            )
        {%- endset -%}
        {%- do rows.append(row | trim) -%}
    {%- endfor -%}

    {%- set sql -%}
        insert into {{ fqn }} (
            invocation_id, node_id, node_name, resource_type, node_status,
            execution_time_s, rows_affected, failures, message, thread_id,
            dbt_target, dbt_version, invocation_args,
            run_started_at, run_completed_at, run_status
        )
        select
            column1, column2, column3, column4, column5,
            column6, column7, column8, column9, column10,
            column11, column12, column13,
            column14, column15, column16
        from values
        {{ rows | join(',\n            ') }}
    {%- endset -%}

    {% do run_query(sql) %}
    {% do log(
        "logged " ~ results | length ~ " dbt node result(s) to " ~ fqn ~ " (run_status=" ~ run_status ~ ")",
        info=true
    ) %}
    {{ return('select 1') }}

{% endmacro %}
