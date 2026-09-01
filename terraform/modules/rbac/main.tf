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
# Two-tier RBAC (the Snowflake-recommended model).
#
#   ACCESS ROLES  (AR_*)  -- own privileges on exactly one schema at one level.
#                            "SELECT on everything in CORE" is an access role.
#   FUNCTIONAL ROLES (FR_*) -- own no privileges directly. They are bundles of
#                            access roles that describe a *persona*.
#   USERS         -- are only ever granted functional roles.
#
# WHY the extra layer? Without it, every new persona means re-deriving dozens of
# object grants, and revoking access means hunting them down again. With it,
# "give the new quant read access to trades but not to the reject payloads" is
# one GRANT of one access role. It also makes the access model auditable: you can
# answer "who can read rejected trades?" with a single SHOW GRANTS OF ROLE.
#
# Every table grant is issued twice -- once for existing objects (`all`) and once
# for `future` objects. Without the future grant, tomorrow's dbt model is
# invisible to readers until someone remembers to re-run a grant script.
# =============================================================================

locals {
  suffix = upper(var.env)

  # -------------------------------------------------------------------------
  # Access-role matrix: which schema, at which privilege level.
  # -------------------------------------------------------------------------
  read_schemas = ["RAW", "STAGING", "INTERMEDIATE", "CORE", "REPORTING", "AUDIT", "MONITORING", "SNAPSHOTS"]
  # MONITORING is writable because the Snowflake-native layer builds its views, alert
  # procedures and alerts there -- see snowflake/30_monitoring and snowflake/40_alerts.
  write_schemas = ["RAW", "STAGING", "INTERMEDIATE", "CORE", "REPORTING", "AUDIT", "MONITORING", "SNAPSHOTS"]

  access_roles = merge(
    { for s in local.read_schemas : "${s}_R" => {
      schema  = s
      level   = "R"
      comment = "Read-only on ${var.database_name}.${s}"
    } },
    { for s in local.write_schemas : "${s}_RW" => {
      schema  = s
      level   = "RW"
      comment = "Read-write + DDL on ${var.database_name}.${s}"
    } },
  )

  # -------------------------------------------------------------------------
  # Functional roles: the personas that actually get granted to users.
  # -------------------------------------------------------------------------
  functional_roles = {
    INGEST = {
      comment = "Trade file loader. Writes RAW only -- cannot read or alter curated trade data."
      access  = ["RAW_RW", "MONITORING_R"]
      warehouses = ["load"]
    }
    TRANSFORM = {
      comment = "dbt service persona. Full DDL across RAW and the modelling layers."
      # RAW is RW rather than R because this persona owns the Snowflake-native
      # ingestion layer: the file format, stage, stream, tasks and the drain
      # procedure, which inserts into RAW.LOAD_BATCH and RAW.TRADE_EVENT_QUEUE
      # and deletes from the queue when pruning.
      access = [
        "RAW_RW", "STAGING_RW", "INTERMEDIATE_RW", "CORE_RW",
        "REPORTING_RW", "AUDIT_RW", "SNAPSHOTS_RW", "MONITORING_RW",
      ]
      warehouses = ["transform"]
    }
    ANALYST = {
      comment = "BI / dashboard persona. Read-only on curated + reporting layers."
      access     = ["CORE_R", "REPORTING_R", "SNAPSHOTS_R"]
      warehouses = ["bi"]
    }
    COMPLIANCE = {
      comment = "Audit persona. Can read rejected trades and the full rule-hit log."
      access     = ["AUDIT_R", "CORE_R", "REPORTING_R"]
      warehouses = ["bi"]
    }
    PLATFORM = {
      comment = "Platform engineer. Operates warehouses and tasks, reads everything."
      access     = [for k in keys(local.access_roles) : k if endswith(k, "_R")]
      warehouses = ["load", "transform", "bi"]
    }
  }

  # Flatten functional role -> access role edges for for_each.
  role_hierarchy = merge([
    for fr_key, fr in local.functional_roles : {
      for ar_key in fr.access : "${fr_key}__${ar_key}" => {
        functional = fr_key
        access     = ar_key
      }
    }
  ]...)

  # Flatten functional role -> warehouse edges.
  role_warehouses = merge([
    for fr_key, fr in local.functional_roles : {
      for wh_key in fr.warehouses : "${fr_key}__${wh_key}" => {
        functional = fr_key
        warehouse  = var.warehouse_names[wh_key]
      }
    }
  ]...)

  # Flatten user -> functional role edges.
  user_roles = merge([
    for u_key, u in var.service_users : {
      for fr_key in u.functional_roles : "${u_key}__${fr_key}" => {
        user       = u_key
        functional = fr_key
      }
    }
  ]...)
}

# -----------------------------------------------------------------------------
# Roles
# -----------------------------------------------------------------------------
resource "snowflake_account_role" "access" {
  for_each = local.access_roles

  name    = "AR_${var.name_prefix}_${each.key}_${local.suffix}"
  comment = each.value.comment
}

resource "snowflake_account_role" "functional" {
  for_each = local.functional_roles

  name    = "FR_${var.name_prefix}_${each.key}_${local.suffix}"
  comment = each.value.comment
}

# Functional roles roll up to SYSADMIN so that account administrators inherit
# visibility of everything the platform creates -- otherwise objects owned by
# these roles are invisible to SYSADMIN, which is a classic Snowflake footgun.
resource "snowflake_grant_account_role" "functional_to_sysadmin" {
  for_each = local.functional_roles

  role_name        = snowflake_account_role.functional[each.key].name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "access_to_functional" {
  for_each = local.role_hierarchy

  role_name        = snowflake_account_role.access[each.value.access].name
  parent_role_name = snowflake_account_role.functional[each.value.functional].name
}

# -----------------------------------------------------------------------------
# Database-level privileges
# -----------------------------------------------------------------------------
resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each = snowflake_account_role.access

  account_role_name = each.value.name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = var.database_name
  }
}

# dbt needs CREATE SCHEMA to materialise custom schemas and, in dev, personal
# sandbox schemas. Scoped to the TRANSFORM persona only.
resource "snowflake_grant_privileges_to_account_role" "database_create_schema" {
  account_role_name = snowflake_account_role.functional["TRANSFORM"].name
  privileges        = ["CREATE SCHEMA", "USAGE", "MONITOR"]

  on_account_object {
    object_type = "DATABASE"
    object_name = var.database_name
  }
}

# -----------------------------------------------------------------------------
# Schema-level privileges
# -----------------------------------------------------------------------------
resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each = local.access_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = ["USAGE"]

  on_schema {
    schema_name = "\"${var.database_name}\".\"${each.value.schema}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_ddl" {
  for_each = { for k, v in local.access_roles : k => v if v.level == "RW" }

  account_role_name = snowflake_account_role.access[each.key].name
  privileges = [
    "USAGE",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE MATERIALIZED VIEW",
    "CREATE DYNAMIC TABLE",
    "CREATE STAGE",
    "CREATE FILE FORMAT",
    "CREATE STREAM",
    "CREATE TASK",
    "CREATE SEQUENCE",
    "CREATE FUNCTION",
    "CREATE PROCEDURE",
    "CREATE PIPE",
    # The eight Snowflake ALERTs in snowflake/40_alerts live in MONITORING.
    "CREATE ALERT",
  ]

  on_schema {
    schema_name = "\"${var.database_name}\".\"${each.value.schema}\""
  }
}

# -----------------------------------------------------------------------------
# Object-level privileges -- existing objects
# -----------------------------------------------------------------------------
locals {
  # Read grants apply to both tables and views; write grants only to tables
  # (a view is written by replacing its definition, which is a schema DDL right).
  read_object_types  = ["TABLES", "VIEWS", "MATERIALIZED VIEWS", "DYNAMIC TABLES", "EXTERNAL TABLES"]
  write_object_types = ["TABLES"]

  read_grants = merge([
    for k, v in local.access_roles : {
      for ot in local.read_object_types : "${k}__${replace(ot, " ", "_")}" => {
        access_role = k
        schema      = v.schema
        object_type = ot
      }
    } if v.level == "R"
  ]...)

  write_grants = merge([
    for k, v in local.access_roles : {
      for ot in local.write_object_types : "${k}__${replace(ot, " ", "_")}" => {
        access_role = k
        schema      = v.schema
        object_type = ot
      }
    } if v.level == "RW"
  ]...)
}

resource "snowflake_grant_privileges_to_account_role" "read_all" {
  for_each = local.read_grants

  account_role_name = snowflake_account_role.access[each.value.access_role].name
  privileges        = ["SELECT"]

  on_schema_object {
    all {
      object_type_plural = each.value.object_type
      in_schema          = "\"${var.database_name}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "read_future" {
  for_each = local.read_grants

  account_role_name = snowflake_account_role.access[each.value.access_role].name
  privileges        = ["SELECT"]

  on_schema_object {
    future {
      object_type_plural = each.value.object_type
      in_schema          = "\"${var.database_name}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "write_all" {
  for_each = local.write_grants

  account_role_name = snowflake_account_role.access[each.value.access_role].name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES"]

  on_schema_object {
    all {
      object_type_plural = each.value.object_type
      in_schema          = "\"${var.database_name}\".\"${each.value.schema}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "write_future" {
  for_each = local.write_grants

  account_role_name = snowflake_account_role.access[each.value.access_role].name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES"]

  on_schema_object {
    future {
      object_type_plural = each.value.object_type
      in_schema          = "\"${var.database_name}\".\"${each.value.schema}\""
    }
  }
}

# Read personas must also be able to read views built on tables they cannot see
# directly, and the ingest persona needs the stage. Stage READ/WRITE is granted
# to the RAW_RW access role only.
resource "snowflake_grant_privileges_to_account_role" "stage_future" {
  account_role_name = snowflake_account_role.access["RAW_RW"].name
  privileges        = ["READ", "WRITE", "USAGE"]

  on_schema_object {
    future {
      object_type_plural = "STAGES"
      in_schema          = "\"${var.database_name}\".\"RAW\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "file_format_future" {
  for_each = toset(["RAW_RW", "RAW_R"])

  account_role_name = snowflake_account_role.access[each.value].name
  privileges        = ["USAGE"]

  on_schema_object {
    future {
      object_type_plural = "FILE FORMATS"
      in_schema          = "\"${var.database_name}\".\"RAW\""
    }
  }
}

# -----------------------------------------------------------------------------
# Warehouse privileges
#   USAGE   -> may run queries on it
#   OPERATE -> may resume/suspend it (Airflow needs this to warm compute)
#   MONITOR -> may see queries and credit usage on it
# -----------------------------------------------------------------------------
resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  for_each = local.role_warehouses

  account_role_name = snowflake_account_role.functional[each.value.functional].name
  privileges        = ["USAGE", "OPERATE", "MONITOR"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = each.value.warehouse
  }
}

# Task and alert execution are account-level privileges that cannot be scoped to a
# schema. Only the TRANSFORM and PLATFORM personas may own or run the stream-drain /
# expiry tasks and the monitoring alerts.
resource "snowflake_grant_privileges_to_account_role" "execute_task" {
  for_each = toset(["TRANSFORM", "PLATFORM"])

  account_role_name = snowflake_account_role.functional[each.value].name
  privileges        = ["EXECUTE TASK", "EXECUTE MANAGED TASK", "EXECUTE ALERT"]
  on_account        = true
}

# Reading SNOWFLAKE.ACCOUNT_USAGE requires an imported privilege on the shared
# SNOWFLAKE database. This is what powers every monitoring view in the project.
resource "snowflake_grant_privileges_to_account_role" "account_usage" {
  for_each = toset(["PLATFORM", "TRANSFORM"])

  account_role_name = snowflake_account_role.functional[each.value].name
  privileges        = ["IMPORTED PRIVILEGES"]

  on_account_object {
    object_type = "DATABASE"
    object_name = "SNOWFLAKE"
  }
}

# -----------------------------------------------------------------------------
# Service users
#
# `snowflake_service_user` (not `snowflake_user`) is deliberate: Snowflake's
# TYPE=SERVICE users cannot log in with a password at all and are exempt from the
# MFA enforcement rolled out for human users. Key-pair only, which is what we
# want for anything running unattended.
# -----------------------------------------------------------------------------
resource "snowflake_service_user" "svc" {
  for_each = var.service_users

  name         = "SVC_${var.name_prefix}_${upper(each.key)}_${local.suffix}"
  login_name   = "SVC_${var.name_prefix}_${upper(each.key)}_${local.suffix}"
  display_name = "SVC ${var.name_prefix} ${upper(each.key)} ${local.suffix}"
  comment      = each.value.comment

  rsa_public_key = each.value.rsa_public_key

  default_warehouse = var.warehouse_names[each.value.default_warehouse]
  default_role      = snowflake_account_role.functional[each.value.functional_roles[0]].name
  default_namespace = "${var.database_name}.${each.value.default_schema}"

  # Force every statement to run under an explicitly-chosen role. With
  # secondary roles set to ALL, a service account silently inherits the union of
  # everything granted to it, which defeats least privilege.
  default_secondary_roles_option = "NONE"

  # Fail fast rather than hanging a scheduler slot.
  statement_timeout_in_seconds = each.value.statement_timeout_in_seconds
}

resource "snowflake_grant_account_role" "user_functional" {
  for_each = local.user_roles

  role_name = snowflake_account_role.functional[each.value.functional].name
  user_name = snowflake_service_user.svc[each.value.user].name
}
