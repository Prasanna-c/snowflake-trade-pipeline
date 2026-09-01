"""
Every SQL statement the dashboard issues, in one file.

WHY NOT INLINE THE SQL IN THE PAGES
-----------------------------------
Because then a column rename in dbt is found by clicking through five pages. Collected here,
the answer to "what does the dashboard depend on" is `grep` on one file, and the CI job that
checks the dashboard's column references against the dbt manifest has one place to look.

WHY THE DASHBOARD READS MARTS AND VIEWS, NOT BASE TABLES
--------------------------------------------------------
Every statement below hits REPORTING, CORE, AUDIT or MONITORING. None reads RAW or
INTERMEDIATE. That is a boundary worth being strict about: the marts are the contract, they
are tested, and they are where the business definitions live. A dashboard that computes its
own reject rate from RAW will eventually disagree with the pipeline's reject rate, and then
two teams argue about which number is right instead of fixing the pipeline.

The one apparent exception is `PIPELINE_SLA`, which reads MONITORING -- deliberately, because
during an incident the marts may be exactly what is broken, and the monitoring layer is built
to answer questions without depending on them.
"""

from __future__ import annotations

import os

#: Schemas dbt owns. In every target except prod, dbt prefixes these with the target schema
#: -- DBT_LOCAL_CORE on a laptop, PR_412_CORE in CI -- so two builds cannot collide. See
#: dbt/macros/utils/generate_schema_name.sql; this must mirror it.
#:
#: RAW and MONITORING are absent deliberately: they are built by the Snowflake-native SQL
#: layer rather than by dbt, so they always carry their bare names in every environment.
_DBT_OWNED_SCHEMAS = frozenset(
    {"staging", "intermediate", "core", "reporting", "audit", "snapshots"}
)


def _resolve_schema(schema: str) -> str:
    if schema.lower() not in _DBT_OWNED_SCHEMAS:
        return schema.upper()
    if os.environ.get("SNOWFLAKE_ENV", "dev").strip().lower() in ("prod", "production"):
        return schema.upper()
    return f"{os.environ.get('DBT_SCHEMA', 'DBT_LOCAL').strip().upper()}_{schema.upper()}"


def _fq(database: str, schema: str, obj: str) -> str:
    return f"{database}.{_resolve_schema(schema)}.{obj.upper()}"


# ---------------------------------------------------------------------------
# Health header
# ---------------------------------------------------------------------------
def scorecard(database: str) -> str:
    """The single-row scorecard. Same row the Airflow publish gate reads."""
    return f"select * from {_fq(database, 'reporting', 'rpt_data_quality_scorecard')}"


def pipeline_sla(database: str) -> str:
    """Stage-by-stage RAG from the monitoring layer.

    Independent of dbt: reads load metadata and the stream queue directly, so it still
    answers when the transform layer is the thing that failed.
    """
    return f"select * from {_fq(database, 'monitoring', 'vw_pipeline_sla')}"


# ---------------------------------------------------------------------------
# Trade status
# ---------------------------------------------------------------------------
def daily_status(database: str, days: int) -> str:
    """Daily volumes and reject rate.

    The date filter is pushed into SQL rather than applied to a full extract in pandas.
    With 90 days of history that difference is invisible; with three years of it, the pandas
    version transfers a million rows to render thirty points. Filtering at the source is a
    habit worth keeping even when the data is small, because the data stops being small
    without anyone revisiting the dashboard.
    """
    return f"""
        select *
        from {_fq(database, "reporting", "agg_trade_status_daily")}
        where calendar_date >= dateadd('day', -{int(days)}, current_date())
        order by calendar_date
    """


def lifecycle_mix(database: str) -> str:
    return f"""
        select
            lifecycle_status,
            count(*) as trade_count,
            sum(notional_amount) as gross_notional
        from {_fq(database, "core", "fct_trade")}
        group by lifecycle_status
        order by trade_count desc
    """


def version_distribution(database: str) -> str:
    """How many trades sit at version 1, 2, 3...

    Amendment depth is a quiet but useful signal: a sudden jump in high-version trades
    usually means an upstream system is retransmitting, and business rule 1 is absorbing it
    silently because ascending versions are perfectly legal.
    """
    return f"""
        select
            least(current_version, 6) as version_bucket,
            iff(current_version >= 6, '6+', current_version::varchar) as version_label,
            count(*) as trade_count
        from {_fq(database, "core", "fct_trade")}
        group by 1, 2
        order by 1
    """


def book_exposure(database: str) -> str:
    return f"""
        select
            book_id,
            book_name,
            desk,
            live_trade_count,
            gross_live_notional,
            net_live_notional,
            notional_limit,
            limit_utilisation_pct,
            limit_status
        from {_fq(database, "core", "dim_book")}
        order by limit_utilisation_pct desc nulls last
    """


def counterparty_exposure(database: str, limit: int = 25) -> str:
    return f"""
        select
            counterparty_id,
            counterparty_name,
            country_code,
            credit_rating,
            is_active,
            live_trade_count,
            gross_live_notional,
            net_live_notional,
            rejected_event_count,
            has_live_trades_while_inactive
        from {_fq(database, "core", "dim_counterparty")}
        order by gross_live_notional desc nulls last
        limit {int(limit)}
    """


def expiring_soon(database: str) -> str:
    return f"""
        select *
        from {_fq(database, "reporting", "rpt_trade_expiring_soon")}
        order by
            case urgency
                when 'OVERDUE' then 0
                when 'TODAY' then 1
                when 'THIS_WEEK' then 2
                else 3
            end,
            maturity_date
        limit 500
    """


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------
def rejection_by_rule(database: str) -> str:
    """Rejection analysis rolled up to the rule.

    The mart is per (rule, source system) because concentration is the diagnostic signal.
    Summing back to the rule here rather than adding a second mart keeps one definition of
    a rule hit; the per-source detail is a separate query below, used for the drill-down.
    """
    return f"""
        select
            rule_code,
            any_value(rule_name) as rule_name,
            any_value(rule_category) as rule_category,
            any_value(rule_severity) as rule_severity,
            any_value(requirement_ref) as requirement_ref,
            any_value(remediation) as remediation,
            sum(hit_count) as hit_count,
            sum(distinct_trade_count) as distinct_trade_count,
            sum(affected_notional) as affected_notional,
            count(*) as sources_affected,
            max(is_concentrated) as is_concentrated,
            max(is_new_failure_mode) as is_new_failure_mode,
            max(is_chronic) as is_chronic,
            max(last_seen_at) as last_seen_at
        from {_fq(database, "reporting", "agg_rejection_analysis")}
        group by rule_code
        order by hit_count desc
    """


def rejection_by_source(database: str, rule_code: str | None = None) -> str:
    predicate = f"where rule_code = '{rule_code}'" if rule_code else ""
    return f"""
        select
            rule_code,
            rule_name,
            source_system,
            hit_count,
            distinct_trade_count,
            distinct_file_count,
            share_of_rule_pct,
            is_concentrated,
            first_seen_at,
            last_seen_at
        from {_fq(database, "reporting", "agg_rejection_analysis")}
        {predicate}
        order by hit_count desc
        limit 200
    """


def rejected_events(database: str, rule_code: str | None = None, limit: int = 200) -> str:
    """Individual rejected events, for the drill-down to an actual payload.

    This is the panel that makes the dashboard operationally useful rather than decorative.
    "Reject rate is 31%" prompts a question; being able to click through to the raw JSON of a
    rejected event answers it, and the raw payload is retained in the audit table precisely
    so this is possible.
    """
    predicate = (
        f"where array_contains('{rule_code}'::variant, violated_rule_codes)" if rule_code else ""
    )
    return f"""
        select
            rejected_at,
            trade_id,
            trade_version,
            disposition,
            primary_rule_code,
            primary_rule_name,
            violated_rule_codes,
            source_system,
            source_file_name,
            counterparty_id,
            notional_amount,
            notional_currency,
            raw_payload
        from {_fq(database, "audit", "fct_trade_rejected")}
        {predicate}
        order by rejected_at desc
        limit {int(limit)}
    """


def rejection_trend(database: str, days: int = 30) -> str:
    return f"""
        select
            evaluated_at::date as calendar_date,
            rule_code,
            rule_name,
            count(*) as hit_count
        from {_fq(database, "audit", "trade_rule_result")}
        where evaluated_at >= dateadd('day', -{int(days)}, current_date())
          and is_blocking
        group by 1, 2, 3
        order by 1
    """


def rule_catalogue(database: str) -> str:
    """The declared rules joined to whether they have ever fired.

    A rule that has never fired is either genuinely never violated or quietly broken, and a
    green test suite cannot tell those apart. Showing the two together is the cheapest
    available control on the rule engine's own correctness.
    """
    return f"""
        with fired as (
            select
                rule_code,
                count(*) as hit_count,
                max(evaluated_at) as last_fired_at
            from {_fq(database, "audit", "trade_rule_result")}
            group by rule_code
        )

        select
            reason.rule_code,
            reason.rule_name,
            reason.rule_category,
            reason.severity as rule_severity,
            reason.requirement_ref,
            reason.description,
            reason.remediation,
            coalesce(fired.hit_count, 0) as hit_count,
            fired.last_fired_at,
            fired.rule_code is null as never_fired
        from {_fq(database, "core", "ref_rejection_reason")} as reason
        left join fired on fired.rule_code = reason.rule_code
        order by never_fired desc, hit_count desc
    """


# ---------------------------------------------------------------------------
# Freshness, loads and cost
# ---------------------------------------------------------------------------
def file_arrival(database: str, limit: int = 100) -> str:
    return f"""
        select
            file_name,
            file_state,
            is_stalled,
            staged_at,
            first_row_loaded_at,
            stage_to_load_seconds,
            rows_in_file,
            size_bytes,
            load_method,
            expected_gap_minutes
        from {_fq(database, "monitoring", "vw_file_arrival")}
        order by coalesce(staged_at, first_row_loaded_at) desc nulls last
        limit {int(limit)}
    """


def batch_health(database: str, limit: int = 100) -> str:
    return f"""
        select *
        from {_fq(database, "monitoring", "vw_batch_health")}
        order by started_at desc
        limit {int(limit)}
    """


def stream_lag(database: str) -> str:
    return f"select * from {_fq(database, 'monitoring', 'vw_stream_lag')}"


def copy_errors(database: str, limit: int = 100) -> str:
    return f"""
        select
            logged_at,
            source_file_name,
            error_message,
            rejected_record
        from {_fq(database, "raw", "copy_error")}
        order by logged_at desc
        limit {int(limit)}
    """


def dbt_runs(database: str, limit: int = 50) -> str:
    """dbt run outcomes, persisted by the on-run-end hook.

    Reading dbt's success from Snowflake rather than from Airflow is what allows the platform
    to notice a stale curated layer when the orchestrator itself is down -- which is exactly
    the situation where nobody is watching the Airflow UI.

    The table is one row per node per invocation, so the rollup happens here. It is stored at
    node grain because that is the grain at which a failure is diagnosable ("which model
    broke"), and collapsing it at write time would throw that away to save a GROUP BY.
    """
    return f"""
        select
            invocation_id,
            any_value(dbt_target) as dbt_target,
            any_value(dbt_version) as dbt_version,
            any_value(run_status) as run_status,
            min(run_started_at) as run_started_at,
            max(run_completed_at) as run_completed_at,
            datediff('second', min(run_started_at), max(run_completed_at)) as duration_seconds,
            count_if(resource_type = 'model') as models_built,
            count_if(resource_type = 'test') as tests_run,
            count_if(resource_type = 'test' and lower(node_status) in ('fail', 'error'))
                as tests_failed,
            count_if(lower(node_status) in ('fail', 'error', 'runtime error')) as nodes_failed,
            round(sum(execution_time_s), 1) as node_seconds,
            sum(rows_affected) as rows_affected
        from {_fq(database, "audit", "dbt_run_result")}
        group by invocation_id
        order by run_started_at desc
        limit {int(limit)}
    """


def dbt_failed_nodes(database: str, limit: int = 100) -> str:
    """The individual nodes that failed, newest first. The drill-down from the run list."""
    return f"""
        select
            run_started_at,
            dbt_target,
            resource_type,
            node_name,
            node_status,
            failures,
            round(execution_time_s, 2) as execution_time_s,
            message
        from {_fq(database, "audit", "dbt_run_result")}
        where lower(node_status) in ('fail', 'error', 'runtime error', 'warn')
        order by run_started_at desc, node_name
        limit {int(limit)}
    """


def warehouse_credits(database: str, days: int = 14) -> str:
    return f"""
        select *
        from {_fq(database, "monitoring", "vw_warehouse_credits")}
        where usage_date >= dateadd('day', -{int(days)}, current_date())
        order by usage_date, warehouse_name
    """


def slowest_statements(database: str, days: int = 7, limit: int = 100) -> str:
    """The slowest tagged statements, with the view's own tuning diagnosis attached.

    This is the payoff for tagging every statement. Without the tag, ACCOUNT_USAGE can only
    say "the warehouse cost X"; with it, the answer is "adjudication accounted for 60% of X,
    the dashboard for 2%", which is the form a cost conversation can act on.

    Note this reads ACCOUNT_USAGE, so it lags by up to 45 minutes and will look empty on a
    brand-new trial account. That is a property of the source, not a bug, and the page says so
    rather than leaving someone to wonder.
    """
    return f"""
        select
            start_time,
            coalesce(model_name, 'non-dbt') as model_name,
            warehouse_name,
            warehouse_size,
            round(elapsed_seconds, 2) as elapsed_seconds,
            round(execution_seconds, 2) as execution_seconds,
            round(queued_overload_seconds, 2) as queued_overload_seconds,
            bytes_scanned,
            rows_produced,
            partition_scan_ratio,
            bytes_spilled_to_remote_storage,
            tuning_signal,
            execution_status,
            query_id
        from {_fq(database, "monitoring", "vw_dbt_query_performance")}
        where start_time >= dateadd('day', -{int(days)}, current_timestamp())
        order by elapsed_seconds desc
        limit {int(limit)}
    """


def model_build_cost(database: str, days: int = 7) -> str:
    """Total time per dbt model, which is the practical proxy for "what does this model cost".

    Credits are not attributable to a single statement on a shared warehouse, so elapsed
    warehouse-seconds is the honest measure available. It is good enough to answer the only
    question anyone asks of it: which model to optimise first.
    """
    return f"""
        select
            coalesce(model_name, 'non-dbt') as model_name,
            count(*) as statement_count,
            round(sum(elapsed_seconds), 1) as total_elapsed_seconds,
            round(avg(elapsed_seconds), 2) as avg_elapsed_seconds,
            round(max(elapsed_seconds), 2) as max_elapsed_seconds,
            sum(bytes_scanned) as bytes_scanned,
            count_if(bytes_spilled_to_remote_storage > 0) as statements_with_remote_spill,
            count_if(tuning_signal <> 'OK') as statements_with_tuning_signal
        from {_fq(database, "monitoring", "vw_dbt_query_performance")}
        where start_time >= dateadd('day', -{int(days)}, current_timestamp())
        group by 1
        order by total_elapsed_seconds desc
        limit 50
    """
