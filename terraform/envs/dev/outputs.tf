output "database" {
  description = "Environment database name."
  value       = module.database.database_name
}

output "schemas" {
  description = "Layer => schema name."
  value       = module.database.schema_names
}

output "warehouses" {
  description = "Workload class => warehouse name."
  value       = module.warehouses.warehouse_names
}

output "functional_roles" {
  description = "Persona => functional role name."
  value       = module.rbac.functional_role_names
}

output "service_users" {
  description = "Logical service account => Snowflake user name."
  value       = module.rbac.service_user_names
}

output "notification_integration" {
  description = "Email notification integration for Snowflake ALERTs."
  value       = module.governance.notification_integration_name
}

output "masking_policies" {
  description = "Masking policies available to attach to CORE columns."
  value       = module.governance.masking_policy_names
}

# ---------------------------------------------------------------------------
# Convenience: paste this block into .env after `terraform apply`.
# ---------------------------------------------------------------------------
output "dotenv_snippet" {
  description = "Environment variables for .env, derived from what was actually created."
  value       = <<-EOT
    SNOWFLAKE_DATABASE=${module.database.database_name}
    SNOWFLAKE_USER=${module.rbac.service_user_names["dbt"]}
    SNOWFLAKE_ROLE=${module.rbac.functional_role_names["TRANSFORM"]}
    SNOWFLAKE_WAREHOUSE=${module.warehouses.warehouse_names["transform"]}
    SNOWFLAKE_LOAD_WAREHOUSE=${module.warehouses.warehouse_names["load"]}
    SNOWFLAKE_BI_WAREHOUSE=${module.warehouses.warehouse_names["bi"]}
    SNOWFLAKE_INGEST_USER=${module.rbac.service_user_names["ingest"]}
    SNOWFLAKE_INGEST_ROLE=${module.rbac.functional_role_names["INGEST"]}
    SNOWFLAKE_NOTIFICATION_INTEGRATION=${module.governance.notification_integration_name}
  EOT
}
