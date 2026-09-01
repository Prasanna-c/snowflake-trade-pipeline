output "database" {
  value = module.database.database_name
}

output "schemas" {
  value = module.database.schema_names
}

output "warehouses" {
  value = module.warehouses.warehouse_names
}

output "functional_roles" {
  value = module.rbac.functional_role_names
}

output "service_users" {
  value = module.rbac.service_user_names
}

output "notification_integration" {
  value = module.governance.notification_integration_name
}
