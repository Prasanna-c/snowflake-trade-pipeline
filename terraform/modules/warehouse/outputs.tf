output "warehouse_names" {
  description = "Map of workload class => warehouse name."
  value       = { for k, w in snowflake_warehouse.this : k => w.name }
}

output "resource_monitor_names" {
  description = "Map of workload class => resource monitor name."
  value       = { for k, m in snowflake_resource_monitor.workload : k => m.name }
}
