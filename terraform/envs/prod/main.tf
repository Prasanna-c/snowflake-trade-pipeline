locals {
  env = "prod"
}

# =============================================================================
# Production differs from dev ONLY in configuration values, never in structure.
# The same modules are instantiated -- that is what makes "it worked in dev" a
# meaningful statement.
#
# Deltas vs. dev:
#   - warehouse sizes and multi-cluster (concurrency for BI, throughput for batch)
#   - 30-day Time Travel on non-transient schemas (regulatory reproducibility)
#   - managed-access schemas (only the owner may grant -- prevents access creep)
#   - no developer sandbox schemas
#   - higher credit quotas with earlier notification thresholds
# =============================================================================
module "warehouses" {
  source = "../../modules/warehouse"

  env = local.env

  warehouses = {
    load = {
      size                         = "SMALL"
      comment                      = "Trade file ingestion. Sized for parallel COPY across many files."
      auto_suspend                 = 60
      statement_timeout_in_seconds = 1800
      credit_quota                 = 200
    }
    transform = {
      size         = "LARGE"
      comment      = "dbt transformation batch."
      auto_suspend = 60
      # Multi-cluster on the batch warehouse absorbs the burst when several
      # micro-batches land at once instead of queueing them serially.
      min_cluster_count            = 1
      max_cluster_count            = 3
      scaling_policy               = "STANDARD"
      enable_query_acceleration    = true
      statement_timeout_in_seconds = 3600
      credit_quota                 = 1000
    }
    bi = {
      size         = "MEDIUM"
      comment      = "Interactive BI and Streamlit."
      auto_suspend = 300
      # ECONOMY scaling favours queueing over spinning up clusters, which suits
      # dashboards where a 5s wait is acceptable and credits are not.
      min_cluster_count            = 1
      max_cluster_count            = 3
      scaling_policy               = "ECONOMY"
      statement_timeout_in_seconds = 300
      max_concurrency_level        = 16
      credit_quota                 = 300
    }
  }
}

module "database" {
  source = "../../modules/database"

  env                         = local.env
  data_retention_time_in_days = 30
  with_managed_access         = true
  developer_sandboxes         = []
}

module "rbac" {
  source = "../../modules/rbac"

  env             = local.env
  database_name   = module.database.database_name
  warehouse_names = module.warehouses.warehouse_names

  service_users = {
    ingest = {
      comment           = "Trade file producer. Writes RAW only."
      functional_roles  = ["INGEST"]
      default_warehouse = "load"
      default_schema    = "RAW"
      rsa_public_key    = var.ingest_public_key
    }
    dbt = {
      comment           = "dbt Core / Airflow transformation runner."
      functional_roles  = ["TRANSFORM"]
      default_warehouse = "transform"
      default_schema    = "CORE"
      rsa_public_key    = var.dbt_public_key
    }
    bi = {
      comment                      = "Streamlit / Tableau reader."
      functional_roles             = ["ANALYST"]
      default_warehouse            = "bi"
      default_schema               = "REPORTING"
      rsa_public_key               = var.bi_public_key
      statement_timeout_in_seconds = 120
    }
  }

  depends_on = [module.database, module.warehouses]
}

module "governance" {
  source = "../../modules/governance"

  env           = local.env
  database_name = module.database.database_name

  alert_recipients = [var.alert_email]

  roles_allowed_to_notify = [
    module.rbac.functional_role_names["TRANSFORM"],
    module.rbac.functional_role_names["PLATFORM"],
  ]

  unmasked_roles = [
    module.rbac.functional_role_names["COMPLIANCE"],
    module.rbac.functional_role_names["PLATFORM"],
  ]

  roles_allowed_to_apply_policies = [
    module.rbac.functional_role_names["TRANSFORM"],
  ]

  depends_on = [module.rbac]
}
