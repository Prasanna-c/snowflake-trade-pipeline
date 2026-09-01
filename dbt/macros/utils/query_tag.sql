{#-
    ============================================================================
    Query tagging.

    Snowflake's QUERY_HISTORY records a query_tag against every statement. Setting it
    per model is what makes the whole monitoring layer possible: without it,
    ACCOUNT_USAGE shows a warehouse burning credits with no way to attribute the cost
    to a model, a dbt invocation, or even to this project as opposed to someone's
    ad-hoc query.

    The tag format is parsed by MONITORING.VW_DBT_QUERY_PERFORMANCE:
        project=trade-pipeline|env=<target>|model=<name>|materialization=<x>|invocation=<uuid>

    Why a pre-hook rather than the `query_tag` model config: the config sets the tag
    for the model's main statement only. A pre-hook sets it on the session, so it also
    covers the temp-table creation and the MERGE that an incremental materialization
    issues -- which is usually where the time actually goes.

    invocation_id ties every statement in one `dbt build` together, so an incident can
    be reconstructed as "this run, these models, this cost".

    NOTE ON NAMING: these are deliberately *not* called set_query_tag / unset_query_tag.
    dbt-snowflake ships macros with those exact names and calls them from inside its own
    materializations as unset_query_tag(original_query_tag). A project macro shadows the
    package one, so reusing the names makes every seed and table materialization fail with
    "takes not more than 0 argument(s)".
    ============================================================================
-#}

{% macro set_model_query_tag() %}
    {%- if execute -%}
        {%- set tag = [
            'project=trade-pipeline',
            'env=' ~ target.name,
            'model=' ~ this.identifier,
            'schema=' ~ this.schema,
            'materialization=' ~ (config.get('materialized') | default('view')),
            'invocation=' ~ invocation_id,
            'user=' ~ target.user
        ] | join('|') -%}
        alter session set query_tag = '{{ tag }}'
    {%- else -%}
        select 1
    {%- endif -%}
{% endmacro %}


{% macro unset_model_query_tag() %}
    {#- Unset rather than leave the last model's tag on the session. A leaked tag makes
        the next statement -- often an ad-hoc query in the same session -- look like it
        belonged to a model, which corrupts the cost attribution we just built. -#}
    alter session unset query_tag
{% endmacro %}
