-- =============================================================================
-- ONE-TIME MANUAL BOOTSTRAP -- run this once, by hand, as ACCOUNTADMIN in Snowsight.
--
-- This is the only script in the repository that is not automated, and that is
-- deliberate: it creates the identity that automation subsequently uses. You
-- cannot bootstrap a chain of trust from inside the chain.
--
-- It creates a purpose-built TRADES_TERRAFORM role rather than handing Terraform
-- ACCOUNTADMIN. If a bad plan is ever applied, the damage is bounded to the
-- objects this project owns -- it cannot touch billing, network policies,
-- replication or other teams' databases.
--
-- USAGE
--   1. Run `make keypair` locally, then copy the public key body.
--   2. Paste it into the SET statement below.
--   3. Execute the whole script in a Snowsight worksheet as ACCOUNTADMIN.
-- =============================================================================

use role accountadmin;

-- ---------------------------------------------------------------------------
-- Paste the single-line body of .secrets/tf_rsa_key.pub here (no BEGIN/END lines).
-- ---------------------------------------------------------------------------
set tf_public_key = 'PASTE_TERRAFORM_PUBLIC_KEY_BODY_HERE';

-- ---------------------------------------------------------------------------
-- The role Terraform assumes.
-- ---------------------------------------------------------------------------
create role if not exists trades_terraform
  comment = 'IaC role for the trade lifecycle platform. Managed by hand; owns everything Terraform creates.';

-- Terraform needs to create the object types this project uses -- and nothing else.
grant create database          on account to role trades_terraform;
grant create warehouse         on account to role trades_terraform;
grant create role              on account to role trades_terraform;
grant create user              on account to role trades_terraform;
grant create integration       on account to role trades_terraform;

-- Resource monitors are an ACCOUNTADMIN-only object type in Snowflake; the
-- privilege cannot be delegated, so we grant the role itself.
grant create resource monitor  on account to role trades_terraform;

-- Required so Terraform can GRANT the privileges it manages onward to the
-- access roles it creates. Without MANAGE GRANTS every grant resource fails.
grant manage grants            on account to role trades_terraform;

-- Read ACCOUNT_USAGE so `terraform plan` can detect drift in objects it manages.
grant imported privileges on database snowflake to role trades_terraform;

-- Roll up to SYSADMIN so account administrators retain visibility of everything
-- Terraform builds. Skipping this is the most common cause of "SYSADMIN cannot
-- see the table dbt just created".
grant role trades_terraform to role sysadmin;

-- ---------------------------------------------------------------------------
-- The Terraform service user. TYPE = SERVICE means:
--   * it has no password and cannot be used to log into the UI,
--   * it is exempt from the MFA enrolment Snowflake now enforces on human users,
--   * key-pair (or OAuth) is the only way in.
-- ---------------------------------------------------------------------------
create user if not exists svc_trades_terraform
  type = service
  default_role = trades_terraform
  default_warehouse = compute_wh
  comment = 'Terraform IaC runner for the trade lifecycle platform.';

alter user svc_trades_terraform set rsa_public_key = $tf_public_key;

-- Force every statement to declare its role explicitly.
alter user svc_trades_terraform set default_secondary_roles = ();

grant role trades_terraform to user svc_trades_terraform;

-- ---------------------------------------------------------------------------
-- Terraform's own bootstrap warehouse. It only runs metadata queries, so XSMALL
-- with a 60-second suspend costs effectively nothing.
-- ---------------------------------------------------------------------------
create warehouse if not exists compute_wh
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'Bootstrap / metadata warehouse.';

grant usage, operate on warehouse compute_wh to role trades_terraform;

-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
show grants to role trades_terraform;

select
    'Bootstrap complete. Next: cd terraform/envs/dev && terraform init && terraform apply'
        as next_step;
