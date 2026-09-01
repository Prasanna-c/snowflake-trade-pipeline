# Copy to terraform.tfvars (git-ignored) or export the equivalent TF_VAR_* vars.
#
#   cp example.tfvars terraform.tfvars
#
# Find organization_name / account_name in Snowsight: bottom-left account menu >
# hover your account > "Account Identifier" shows ORG-ACCOUNT.

snowflake_organization_name = "MYORG"
snowflake_account_name      = "MYACCOUNT"

snowflake_user = "SVC_TRADES_TERRAFORM"
snowflake_role = "TRADES_TERRAFORM"

# Warehouse Terraform itself uses. COMPUTE_WH exists by default on a new trial.
snowflake_bootstrap_warehouse = "COMPUTE_WH"

snowflake_private_key_path = "/Users/me/snowflake-trade-pipeline/.secrets/tf_rsa_key.p8"

# Must be a VERIFIED email on a Snowflake user in this account, or alerts silently fail.
alert_email = "me@example.com"

developer_sandboxes = ["local"]

# Public keys for the service accounts Terraform creates. Generate with
# `make keypair`, then paste the single-line body (no BEGIN/END lines).
#   ingest_public_key = "MIIBIjANBgkq..."
#   dbt_public_key    = "MIIBIjANBgkq..."
#   bi_public_key     = "MIIBIjANBgkq..."
