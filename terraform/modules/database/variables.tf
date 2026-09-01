variable "env" {
  description = "Environment suffix (dev / prod)."
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be one of dev, test, prod."
  }
}

variable "name_prefix" {
  description = "Database-name prefix."
  type        = string
  default     = "TRADES"
}

variable "data_retention_time_in_days" {
  description = <<-EOT
    Time Travel window for non-transient schemas. Standard edition caps this at
    1 day; Enterprise allows up to 90. Keep dev at 1 to save storage.
  EOT
  type        = number
  default     = 1

  validation {
    condition     = var.data_retention_time_in_days >= 0 && var.data_retention_time_in_days <= 90
    error_message = "data_retention_time_in_days must be between 0 and 90."
  }
}

variable "with_managed_access" {
  description = "Enable managed-access schemas so only the schema owner can issue grants."
  type        = bool
  default     = true
}

variable "developer_sandboxes" {
  description = "Developer identifiers that each get a DBT_<name> sandbox schema (non-prod only)."
  type        = list(string)
  default     = []
}
