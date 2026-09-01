output "database_name" {
  description = "Name of the environment database."
  value       = snowflake_database.this.name
}

output "schema_names" {
  description = "Map of layer => schema name."
  value       = { for k, s in snowflake_schema.layer : k => s.name }
}

output "schema_fqns" {
  description = "Map of layer => fully qualified schema name, for use in grants."
  value       = { for k, s in snowflake_schema.layer : k => s.fully_qualified_name }
}
