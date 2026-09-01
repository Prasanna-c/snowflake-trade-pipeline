locals {
  env = "dev"
}

# =============================================================================
# Compute
#
# Dev sizing is deliberately minimal: XSMALL everywhere, single cluster, tight
# credit quotas. The whole point of a per-environment root module is that prod can
# differ in sizing without differing in structure.
# =============================================================================
module "warehouses" {
  source = "../../modules/warehouse"

  env = local.env

  warehouses = {
    load = {
      size    = "XSMALL"
      comment = "Trade file ingestion: PUT/COPY and Snowpipe backstop. I/O bound -- never needs to grow."
      # COPY on an XSMALL is bottlenecked by file count, not warehouse size.
      auto_suspend                 = 60
      statement_timeout_in_seconds = 1800
      credit_quota                 = 5
    }
    transform = {
      size    = "XSMALL"
      comment = "dbt transformation batch. The only warehouse that scales with trade volume."
      auto_suspend = 60
      # Long backfills are the outlier case; acceleration handles them without
      # permanently paying for a bigger warehouse.
      enable_query_acceleration    = false # Enterprise+ feature; leave off on trial
      statement_timeout_in_seconds = 3600
      credit_quota                 = 10
    }
    bi = {
      size    = "XSMALL"
      comment = "Streamlit / Tableau interactive queries."
      # Longer suspend keeps the result cache and warehouse warm between clicks.
      auto_suspend                 = 300
      statement_timeout_in_seconds = 300
      max_concurrency_level        = 8
      credit_quota                 = 5
    }
  }
}

# =============================================================================
# Storage
# =============================================================================
module "database" {
  source = "../../modules/database"

  env                         = local.env
  data_retention_time_in_days = 1
  with_managed_access         = false # dev: let developers grant freely
  developer_sandboxes         = var.developer_sandboxes
}

# =============================================================================
# Identity and access
# =============================================================================
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
      comment           = "Streamlit dashboard reader."
      functional_roles  = ["ANALYST"]
      default_warehouse = "bi"
      default_schema    = "REPORTING"
      rsa_public_key    = var.bi_public_key
      # A dashboard query that takes 2 minutes is a modelling bug.
      statement_timeout_in_seconds = 120
    }
  }

  depends_on = [module.database, module.warehouses]
}

# =============================================================================
# Governance
# =============================================================================
module "governance" {
  source = "../../modules/governance"

  env           = local.env
  database_name = module.database.database_name

  alert_recipients = [var.alert_email]

  roles_allowed_to_notify = [
    module.rbac.functional_role_names["TRANSFORM"],
    module.rbac.functional_role_names["PLATFORM"],
  ]

  # Only these personas see un-redacted counterparty names and exact notionals.
  unmasked_roles = [
    module.rbac.functional_role_names["TRANSFORM"],
    module.rbac.functional_role_names["COMPLIANCE"],
    module.rbac.functional_role_names["PLATFORM"],
    "ACCOUNTADMIN",
  ]

  roles_allowed_to_apply_policies = [
    module.rbac.functional_role_names["TRANSFORM"],
  ]

  depends_on = [module.rbac]
}
