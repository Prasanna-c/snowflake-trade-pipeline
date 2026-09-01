terraform {
  required_version = ">= 1.9.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }

  # Prod state MUST be remote and locked. Two engineers (or CI and an engineer)
  # applying concurrently against Snowflake grants will corrupt the access model
  # in ways that are tedious to unpick.
  #
  # Configure with `terraform init -backend-config=backend.hcl` so the bucket
  # name is not hard-coded in the repo.
  backend "s3" {
    key     = "snowflake/prod/terraform.tfstate"
    encrypt = true
  }
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = var.snowflake_role
  warehouse         = var.snowflake_bootstrap_warehouse

  authenticator          = "SNOWFLAKE_JWT"
  private_key            = var.snowflake_private_key
  private_key_passphrase = var.snowflake_private_key_passphrase

  params = {
    query_tag = "terraform|env=prod|project=trade-pipeline"
  }

  preview_features_enabled = [
    "snowflake_email_notification_integration_resource",
  ]
}
