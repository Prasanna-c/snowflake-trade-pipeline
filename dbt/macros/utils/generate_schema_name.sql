{#-
    ============================================================================
    Schema naming.

    dbt's default behaviour concatenates the profile schema and the model's custom
    schema, giving `dbt_local_core`. That is right for development and wrong for
    production, where the schema names are part of the platform's public contract --
    Terraform created CORE, the grants target CORE, and the BI tool connects to CORE.

    So:
      prod  -> the custom schema exactly:            CORE, STAGING, AUDIT, ...
      dev   -> prefixed with the developer's schema: DBT_LOCAL_CORE, ...
      ci    -> prefixed with the pull-request number: PR_412_CORE, ...

    The dev prefix is what lets two engineers build the same model concurrently
    without overwriting each other, and the CI prefix is what lets every pull request
    get a disposable, fully-isolated copy of the warehouse that is dropped afterwards.
    ============================================================================
-#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}


{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name in ('prod', 'production') -%}

        {#- Production uses the bare schema name. This is deliberate and is the reason
            an accidental `dbt build --target prod` from a laptop is dangerous, which
            is why the prod profile requires a key that only CI holds. -#}
        {{ custom_schema_name | trim | upper }}

    {%- else -%}

        {{ (default_schema ~ '_' ~ custom_schema_name) | trim | upper }}

    {%- endif -%}

{%- endmacro %}


{#-
    Alias generation. Kept explicit so that a model file rename does not silently
    rename the table a dashboard is pointed at -- a surprisingly common outage cause.
-#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim | upper }}
    {%- elif node.version -%}
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) | upper }}
    {%- else -%}
        {{ node.name | upper }}
    {%- endif -%}
{%- endmacro %}
