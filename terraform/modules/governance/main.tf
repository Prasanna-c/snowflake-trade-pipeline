terraform {
  required_version = ">= 1.9.0"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

# =============================================================================
# Governance plane: alert delivery, data classification, column masking.
#
# In a bank, "who can see the notional and the counterparty" is a control that
# has to be provable, not a convention. Masking policies enforce it in the engine
# itself, so it holds no matter which tool connects -- dbt, Streamlit, Tableau or
# an analyst in Snowsight.
# =============================================================================

# -----------------------------------------------------------------------------
# Email notification integration -- the delivery channel for Snowflake ALERTs
# and for the pipeline's own SYSTEM$SEND_EMAIL calls.
#
# NOTE: recipients must be *verified* Snowflake users in the account. Snowflake
# will not deliver to an arbitrary address. Verify via Snowsight > Profile.
# -----------------------------------------------------------------------------
resource "snowflake_email_notification_integration" "alerts" {
  name    = "NI_${var.name_prefix}_EMAIL_${upper(var.env)}"
  enabled = true
  comment = "Email channel for trade pipeline alerts (${var.env})."

  allowed_recipients = var.alert_recipients
}

resource "snowflake_grant_privileges_to_account_role" "notification_usage" {
  for_each = toset(var.roles_allowed_to_notify)

  account_role_name = each.value
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_email_notification_integration.alerts.name
  }
}

# -----------------------------------------------------------------------------
# Object tagging -- machine-readable data classification.
#
# Tags let us answer "show me every column in the account that holds a
# counterparty identifier" from ACCOUNT_USAGE.TAG_REFERENCES, which is exactly
# the question an auditor asks. They also drive tag-based masking below.
# -----------------------------------------------------------------------------
resource "snowflake_tag" "classification" {
  name     = "DATA_CLASSIFICATION"
  database = var.database_name
  schema   = "CORE"
  comment  = "Sensitivity class of a column."

  allowed_values = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]
}

resource "snowflake_tag" "data_domain" {
  name     = "DATA_DOMAIN"
  database = var.database_name
  schema   = "CORE"
  comment  = "Business domain that owns the object."

  allowed_values = ["TRADE", "COUNTERPARTY", "REFERENCE", "OPERATIONS"]
}

# -----------------------------------------------------------------------------
# Masking policies.
#
# The policy body runs with the *querying* role in CURRENT_ROLE(), so one policy
# serves every consumer: privileged personas see cleartext, everyone else sees a
# redacted value. Applied to columns by dbt post-hooks (see macros/utils/
# apply_masking_policies.sql) so the policy lives with the infrastructure but the
# *application* travels with the model that owns the column.
# -----------------------------------------------------------------------------
resource "snowflake_masking_policy" "counterparty_name" {
  name     = "MP_COUNTERPARTY_NAME"
  database = var.database_name
  schema   = "CORE"

  argument {
    name = "val"
    type = "VARCHAR"
  }
  return_data_type = "VARCHAR"

  body = <<-SQL
    case
      when current_role() in (${join(", ", formatlist("'%s'", var.unmasked_roles))})
        then val
      when val is null then null
      else left(val, 2) || repeat('*', greatest(length(val) - 2, 0))
    end
  SQL

  comment               = "Redacts counterparty legal name for non-privileged roles."
  exempt_other_policies = false
}

resource "snowflake_masking_policy" "notional" {
  name     = "MP_NOTIONAL"
  database = var.database_name
  schema   = "CORE"

  argument {
    name = "val"
    type = "NUMBER(38,4)"
  }
  return_data_type = "NUMBER(38,4)"

  # Rounding rather than nulling keeps aggregate reporting usable for roles that
  # are not cleared to see individual trade sizes.
  body = <<-SQL
    case
      when current_role() in (${join(", ", formatlist("'%s'", var.unmasked_roles))})
        then val
      else round(val, -6)
    end
  SQL

  comment = "Rounds trade notional to the nearest million for non-privileged roles."
}

resource "snowflake_grant_privileges_to_account_role" "masking_policy_apply" {
  for_each = toset(var.roles_allowed_to_apply_policies)

  account_role_name = each.value
  privileges        = ["APPLY"]

  on_schema_object {
    object_type = "MASKING POLICY"
    object_name = snowflake_masking_policy.counterparty_name.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "masking_policy_apply_notional" {
  for_each = toset(var.roles_allowed_to_apply_policies)

  account_role_name = each.value
  privileges        = ["APPLY"]

  on_schema_object {
    object_type = "MASKING POLICY"
    object_name = snowflake_masking_policy.notional.fully_qualified_name
  }
}
