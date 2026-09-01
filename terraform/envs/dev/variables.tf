# All of these are supplied via TF_VAR_* environment variables (see .env.example)
# or terraform.tfvars. Nothing sensitive is defaulted.

variable "snowflake_organization_name" {
  description = "Snowflake organization name (Snowsight > Account > lower-left account selector)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name within the organization."
  type        = string
}

variable "snowflake_user" {
  description = "Terraform service user."
  type        = string
  default     = "SVC_TRADES_TERRAFORM"
}

variable "snowflake_role" {
  description = "Role Terraform assumes. Purpose-built, not ACCOUNTADMIN."
  type        = string
  default     = "TRADES_TERRAFORM"
}

variable "snowflake_bootstrap_warehouse" {
  description = "Warehouse used for Terraform's own metadata queries. Must exist before the first apply."
  type        = string
  default     = "COMPUTE_WH"
}

variable "snowflake_private_key_path" {
  description = "Absolute path to the Terraform user's PKCS#8 private key."
  type        = string
}

variable "snowflake_private_key_passphrase" {
  description = "Passphrase for the private key. Empty if the key is unencrypted."
  type        = string
  default     = null
  sensitive   = true
}

variable "alert_email" {
  description = "Verified Snowflake user email that receives pipeline alerts."
  type        = string
}

variable "developer_sandboxes" {
  description = "Developer identifiers that each get a personal DBT_<name> schema."
  type        = list(string)
  default     = ["local"]
}

variable "ingest_public_key" {
  description = "PEM body (no header/footer) of the ingest service user's RSA public key."
  type        = string
  default     = null
}

variable "dbt_public_key" {
  description = "PEM body (no header/footer) of the dbt service user's RSA public key."
  type        = string
  default     = null
}

variable "bi_public_key" {
  description = "PEM body (no header/footer) of the BI service user's RSA public key."
  type        = string
  default     = null
}
