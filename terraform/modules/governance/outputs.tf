output "notification_integration_name" {
  description = "Email notification integration used by Snowflake ALERTs."
  value       = snowflake_email_notification_integration.alerts.name
}

output "masking_policy_names" {
  description = "Fully qualified masking policy names."
  value = {
    counterparty_name = snowflake_masking_policy.counterparty_name.fully_qualified_name
    notional          = snowflake_masking_policy.notional.fully_qualified_name
  }
}

output "tag_names" {
  description = "Fully qualified tag names."
  value = {
    data_classification = snowflake_tag.classification.fully_qualified_name
    data_domain         = snowflake_tag.data_domain.fully_qualified_name
  }
}
