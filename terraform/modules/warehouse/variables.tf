variable "env" {
  description = "Environment suffix (dev / prod)."
  type        = string
}

variable "name_prefix" {
  description = "Object-name prefix, e.g. TRADES."
  type        = string
  default     = "TRADES"
}

variable "warehouses" {
  description = <<-EOT
    Map of workload-class name => warehouse settings. The key becomes part of the
    warehouse name: WH_<prefix>_<key>_<env>.
  EOT
  type = map(object({
    size                                = string
    comment                             = string
    auto_suspend                        = optional(number, 60)
    min_cluster_count                   = optional(number, 1)
    max_cluster_count                   = optional(number, 1)
    scaling_policy                      = optional(string, "STANDARD")
    statement_timeout_in_seconds        = optional(number, 3600)
    statement_queued_timeout_in_seconds = optional(number, 600)
    max_concurrency_level               = optional(number, 8)
    enable_query_acceleration           = optional(bool, false)
    credit_quota                        = optional(number)
  }))

  validation {
    condition = alltrue([
      for w in values(var.warehouses) : contains(
        ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "X2LARGE", "X3LARGE", "X4LARGE"],
        upper(w.size)
      )
    ])
    error_message = "warehouse size must be a valid Snowflake T-shirt size."
  }

  validation {
    condition     = alltrue([for w in values(var.warehouses) : w.max_cluster_count >= w.min_cluster_count])
    error_message = "max_cluster_count must be >= min_cluster_count."
  }
}
