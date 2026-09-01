output "functional_role_names" {
  description = "Map of persona => functional role name."
  value       = { for k, r in snowflake_account_role.functional : k => r.name }
}

output "access_role_names" {
  description = "Map of schema_level => access role name."
  value       = { for k, r in snowflake_account_role.access : k => r.name }
}

output "service_user_names" {
  description = "Map of logical service account => Snowflake user name."
  value       = { for k, u in snowflake_service_user.svc : k => u.name }
}

output "connection_summary" {
  description = "Ready-to-paste connection settings for .env and dbt profiles.yml."
  value = {
    for k, u in snowflake_service_user.svc : k => {
      user      = u.name
      role      = snowflake_account_role.functional[var.service_users[k].functional_roles[0]].name
      warehouse = var.warehouse_names[var.service_users[k].default_warehouse]
      database  = var.database_name
    }
  }
}
