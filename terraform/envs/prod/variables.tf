variable "snowflake_organization_name" {
  type = string
}

variable "snowflake_account_name" {
  type = string
}

variable "snowflake_user" {
  type    = string
  default = "SVC_TRADES_TERRAFORM"
}

variable "snowflake_role" {
  type    = string
  default = "TRADES_TERRAFORM"
}

variable "snowflake_bootstrap_warehouse" {
  type    = string
  default = "WH_TRADES_TRANSFORM_PROD"
}

# In prod the key arrives from the CI secret store as a value, not a file on
# disk -- there is no persistent filesystem to put it on.
variable "snowflake_private_key" {
  description = "PKCS#8 private key contents, injected from the CI secret store."
  type        = string
  sensitive   = true
}

variable "snowflake_private_key_passphrase" {
  type      = string
  default   = null
  sensitive = true
}

variable "alert_email" {
  description = "Distribution list for production pipeline alerts."
  type        = string
}

variable "ingest_public_key" {
  type    = string
  default = null
}

variable "dbt_public_key" {
  type    = string
  default = null
}

variable "bi_public_key" {
  type    = string
  default = null
}
