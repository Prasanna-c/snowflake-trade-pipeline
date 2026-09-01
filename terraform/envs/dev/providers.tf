terraform {
  required_version = ">= 1.9.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }

  # ---------------------------------------------------------------------------
  # State backend.
  #
  # Local state is fine for a single-operator dev environment and keeps this
  # runnable on a laptop with zero cloud setup. For anything shared, switch to a
  # remote backend so that state is locked (two concurrent applies against
  # Snowflake will otherwise race on grants) and versioned.
  #
  # Uncomment and run `terraform init -migrate-state`:
  #
  # backend "s3" {
  #   bucket         = "db-trades-tfstate"
  #   key            = "snowflake/dev/terraform.tfstate"
  #   region         = "eu-central-1"
  #   dynamodb_table = "db-trades-tflock"   # state locking
  #   encrypt        = true
  # }
  # ---------------------------------------------------------------------------
  backend "local" {
    path = "terraform.tfstate"
  }
}

# =============================================================================
# Provider.
#
# Authentication is key-pair (SNOWFLAKE_JWT). Rationale in
# docs/adr/0006-keypair-authentication.md -- in short: passwords cannot be used
# by Snowflake TYPE=SERVICE users, and MFA enforcement makes password auth
# unusable for automation regardless.
#
# The provider role is deliberately NOT ACCOUNTADMIN. It is a purpose-built role
# with only the account-level privileges Terraform actually needs, so a mistake
# in a plan cannot reach billing or security settings.
# See snowflake/00_bootstrap/01_terraform_role.sql.
# =============================================================================
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = var.snowflake_role
  warehouse         = var.snowflake_bootstrap_warehouse

  authenticator          = "SNOWFLAKE_JWT"
  private_key            = file(var.snowflake_private_key_path)
  private_key_passphrase = var.snowflake_private_key_passphrase

  # Every statement Terraform issues is tagged, so QUERY_HISTORY can attribute
  # account changes to IaC vs. a human in Snowsight.
  params = {
    query_tag = "terraform|env=dev|project=trade-pipeline"
  }

  # Resources still in provider preview must be opted into explicitly.
  preview_features_enabled = [
    "snowflake_email_notification_integration_resource",
  ]
}
