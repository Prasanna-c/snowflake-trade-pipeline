terraform {
  required_version = ">= 1.9.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

# =============================================================================
# Storage containers.
#
# One database per environment, with a schema per architectural layer. Schemas
# (not databases) are the layer boundary because:
#   - cross-schema joins inside one database need no extra grants or qualified
#     three-part names in dbt,
#   - Time Travel / retention is set per database and inherited, and
#   - cloning the whole environment for a "what-if" backfill is one command:
#     CREATE DATABASE TRADES_SANDBOX CLONE TRADES_PROD;
# =============================================================================

resource "snowflake_database" "this" {
  name    = upper("${var.name_prefix}_${var.env}")
  comment = "Trade lifecycle platform -- ${var.env}. Managed by Terraform."

  # Time Travel. 1 day on dev is enough to undo a mistake; 30 days (Enterprise)
  # on prod is the regulatory floor for being able to reproduce a rejected-trade
  # audit answer as of a past date.
  data_retention_time_in_days = var.data_retention_time_in_days

  # Prevents an accidental `DROP DATABASE` from an interactive session.
  drop_public_schema_on_creation = true
}

locals {
  # Layer schemas. Ordering here documents the flow of data.
  schemas = {
    RAW = {
      comment = "Landing zone. Immutable VARIANT payloads exactly as received, plus file lineage. Never edited, never deleted."
      # Raw is fully reproducible from the stage, so it needs no Time Travel
      # beyond the minimum -- this is a real storage saving at volume.
      transient = false
    }
    STAGING = {
      comment = "Typed, renamed, lightly-cleaned 1:1 views over RAW. No business logic."
      transient = true
    }
    INTERMEDIATE = {
      comment = "Business-rule engine: deduplication, version arbitration, validation verdicts."
      transient = true
    }
    CORE = {
      comment = "Conformed facts and dimensions. FCT_TRADE is the golden record."
      transient = false
    }
    REPORTING = {
      comment = "Aggregates and BI-facing views consumed by Streamlit / Tableau."
      transient = true
    }
    AUDIT = {
      comment = "Compliance evidence: rejected trades, rule-hit log, dbt run metadata. Append-only, retained longest."
      transient = false
    }
    MONITORING = {
      comment = "Operational observability views over SNOWFLAKE.ACCOUNT_USAGE and pipeline metadata."
      transient = false
    }
    SNAPSHOTS = {
      comment = "dbt snapshots -- SCD2 history of the golden record."
      transient = false
    }
  }
}

resource "snowflake_schema" "layer" {
  for_each = local.schemas

  database = snowflake_database.this.name
  name     = each.key
  comment  = each.value.comment

  # Transient schemas hold rebuildable derived data. They skip Fail-safe (the
  # 7-day non-configurable Snowflake safety net) which is ~pure storage saving
  # for anything we can regenerate from RAW with `dbt build --full-refresh`.
  is_transient = each.value.transient

  # Managed access: only the schema owner can grant on objects inside it, which
  # stops individual model owners from quietly widening access to trade data.
  with_managed_access = var.with_managed_access

  data_retention_time_in_days = each.value.transient ? 0 : var.data_retention_time_in_days
}

# -----------------------------------------------------------------------------
# Per-developer sandbox schemas. dbt's generate_schema_name macro writes here on
# the `dev` target, so two engineers never collide on the same table.
# -----------------------------------------------------------------------------
resource "snowflake_schema" "developer_sandbox" {
  for_each = var.env == "prod" ? toset([]) : toset(var.developer_sandboxes)

  database     = snowflake_database.this.name
  name         = upper("DBT_${each.value}")
  comment      = "Personal dbt sandbox for ${each.value}."
  is_transient = true

  data_retention_time_in_days = 0
}
