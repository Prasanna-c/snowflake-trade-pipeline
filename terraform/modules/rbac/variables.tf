variable "env" {
  description = "Environment suffix (dev / prod)."
  type        = string
}

variable "name_prefix" {
  description = "Role/user-name prefix."
  type        = string
  default     = "TRADES"
}

variable "database_name" {
  description = "Database the access roles are scoped to."
  type        = string
}

variable "warehouse_names" {
  description = "Map of workload class (load/transform/bi) => warehouse name."
  type        = map(string)

  validation {
    condition     = alltrue([for k in ["load", "transform", "bi"] : contains(keys(var.warehouse_names), k)])
    error_message = "warehouse_names must contain the keys load, transform and bi."
  }
}

variable "service_users" {
  description = <<-EOT
    Service accounts to create. The first entry of functional_roles becomes the
    user's default role. rsa_public_key is the base64 body of the public key with
    the PEM header/footer stripped -- generate it with `make keypair`.
  EOT
  type = map(object({
    comment                      = string
    functional_roles             = list(string)
    default_warehouse            = string
    default_schema               = optional(string, "CORE")
    rsa_public_key               = optional(string)
    statement_timeout_in_seconds = optional(number, 3600)
  }))
  default = {}
}
