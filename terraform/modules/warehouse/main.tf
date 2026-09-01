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
# Compute: one warehouse per workload class.
#
# WHY separate warehouses instead of one shared warehouse:
#   1. Cost attribution -- WAREHOUSE_METERING_HISTORY is per-warehouse, so
#      splitting by workload is the only way to answer "what did ingestion cost
#      us vs. transformation vs. BI".
#   2. Blast radius -- a runaway BI dashboard cannot queue behind / starve the
#      transformation batch, because they draw on different compute.
#   3. Independent scaling -- loading is I/O bound and stays XSMALL; the dbt
#      transformation is the only thing that ever needs to grow.
#   4. Independent auto-suspend -- BI wants 300s (warm cache for interactive
#      users), batch wants 60s (release credits the moment the run ends).
# =============================================================================

resource "snowflake_resource_monitor" "workload" {
  for_each = { for k, v in var.warehouses : k => v if v.credit_quota != null }

  name         = upper("RM_${var.name_prefix}_${each.key}_${var.env}")
  credit_quota = each.value.credit_quota

  # frequency + start_timestamp must be set together.
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"

  # Defence in depth: warn early, throttle new queries at 90%, hard-stop at
  # 100%. On a 30-day trial this is what prevents a runaway task loop from
  # burning the entire credit allowance overnight.
  notify_triggers           = [50, 75, 90]
  suspend_trigger           = 90
  suspend_immediate_trigger = 100
}

resource "snowflake_warehouse" "this" {
  for_each = var.warehouses

  name           = upper("WH_${var.name_prefix}_${each.key}_${var.env}")
  warehouse_size = each.value.size
  warehouse_type = "STANDARD"
  comment        = each.value.comment

  # Suspend aggressively. Snowflake bills per-second with a 60s minimum, so
  # anything above 60s of idle is pure waste for batch workloads.
  auto_suspend        = each.value.auto_suspend
  auto_resume         = "true"
  initially_suspended = true

  # Multi-cluster is Enterprise+. On Standard/trial editions min=max=1 is the
  # only valid setting; the variable makes the production intent explicit
  # without breaking a trial account.
  min_cluster_count = each.value.min_cluster_count
  max_cluster_count = each.value.max_cluster_count
  scaling_policy    = each.value.scaling_policy

  # A query that runs longer than this is a bug, not a slow query. Failing fast
  # is cheaper than letting it burn credits until someone notices.
  statement_timeout_in_seconds        = each.value.statement_timeout_in_seconds
  statement_queued_timeout_in_seconds = each.value.statement_queued_timeout_in_seconds
  max_concurrency_level               = each.value.max_concurrency_level

  # Query acceleration offloads scan-heavy outliers to serverless compute so a
  # single large backfill does not force us to permanently size up.
  enable_query_acceleration           = each.value.enable_query_acceleration
  query_acceleration_max_scale_factor = each.value.enable_query_acceleration ? 8 : null

  resource_monitor = try(snowflake_resource_monitor.workload[each.key].name, null)
}
