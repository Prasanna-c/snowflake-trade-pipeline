variable "env" {
  description = "Environment suffix (dev / prod)."
  type        = string
}

variable "name_prefix" {
  description = "Object-name prefix."
  type        = string
  default     = "TRADES"
}

variable "database_name" {
  description = "Database that hosts the tags and masking policies."
  type        = string
}

variable "alert_recipients" {
  description = <<-EOT
    Email addresses that Snowflake ALERTs may notify. Each must belong to a
    Snowflake user in this account AND be verified, otherwise SYSTEM$SEND_EMAIL
    fails at runtime with "Invalid recipient".
  EOT
  type        = list(string)

  validation {
    condition     = length(var.alert_recipients) > 0
    error_message = "At least one alert recipient is required."
  }

  validation {
    condition     = alltrue([for e in var.alert_recipients : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "alert_recipients must be valid email addresses."
  }
}

variable "roles_allowed_to_notify" {
  description = "Roles granted USAGE on the email notification integration."
  type        = list(string)
  default     = []
}

variable "unmasked_roles" {
  description = "Roles that see cleartext through the masking policies."
  type        = list(string)
}

variable "roles_allowed_to_apply_policies" {
  description = "Roles granted APPLY on the masking policies (dbt needs this to attach them)."
  type        = list(string)
  default     = []
}
