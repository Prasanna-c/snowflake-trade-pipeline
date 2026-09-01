# ADR 0003: Two-tier RBAC — access roles and functional roles

**Status:** Accepted
**Date:** 2026-08
**Implementation:** `terraform/modules/rbac/main.tf`
**Referenced by:** [`setup.md`](../setup.md#on-roles-and-service-users),
[`overview.md`](../overview.md#two-tier-rbac)

---

## Context

Several identities need different access: a file loader that writes only to `RAW`, dbt which needs
DDL across the modelling layers, a dashboard that reads curated data only, a compliance function that
must read rejected trades and their payloads, and a platform engineer who operates warehouses and
tasks.

Snowflake's role model is hierarchical — roles can be granted to roles — so the *shape* of the
hierarchy is a design decision rather than a given.

## Decision

**Two tiers, with a strict rule about each.**

**Access roles** own privileges on exactly one schema at one level. Fifteen of them:
`AR_TRADES_<SCHEMA>_R_<ENV>` for eight schemas, `AR_TRADES_<SCHEMA>_RW_<ENV>` for the seven that are
written. So `AR_TRADES_CORE_R_DEV` means "read everything in `CORE`", and nothing else.

**Functional roles** own **no privileges directly**. They are bundles of access roles describing a
persona, plus warehouse usage. Five of them:

| Functional role | Persona | Access | Warehouse |
| --- | --- | --- | --- |
| `FR_TRADES_INGEST_*` | Trade file producer | `RAW_RW`, `MONITORING_R` | load |
| `FR_TRADES_TRANSFORM_*` | dbt / Airflow | `RAW_RW` + RW on all modelling layers | transform |
| `FR_TRADES_ANALYST_*` | Dashboard, BI | `CORE_R`, `REPORTING_R`, `SNAPSHOTS_R` | bi |
| `FR_TRADES_COMPLIANCE_*` | Audit | `AUDIT_R`, `CORE_R`, `REPORTING_R` | bi |
| `FR_TRADES_PLATFORM_*` | Platform engineer | every `_R` role | all three |

**Users are only ever granted functional roles.** Three service users, each with its own key pair and
exactly one persona: `SVC_TRADES_INGEST_*` → INGEST, `SVC_TRADES_DBT_*` → TRANSFORM,
`SVC_TRADES_BI_*` → ANALYST.

```
SVC_TRADES_BI_DEV ──> FR_TRADES_ANALYST_DEV ──> AR_TRADES_CORE_R_DEV      ──> privileges
                                            ├─> AR_TRADES_REPORTING_R_DEV ──> privileges
                                            └─> AR_TRADES_SNAPSHOTS_R_DEV ──> privileges
```

Every table grant is issued **twice**: once for existing objects and once as a `FUTURE` grant.

## Alternatives considered

### Direct grants to users

What most projects do first, and it fails predictably. Adding a schema means finding every user who
should see it and granting each one. Answering "what can the dashboard read" means comparing grant
lists that differ per user, because someone was granted something extra during an incident and it was
never removed.

The failure is not that it does not work initially — it is that privileges accumulate and become
unauditable, which is exactly what a regulated environment cannot tolerate.

### One role per persona, with privileges granted straight to it

Better than per-user, and still wrong: the same object privileges are duplicated across every role
needing them, so a new schema means editing all five roles, and they drift. Here, `CORE_R` is granted
to four functional roles and defined once.

### A three-tier model with database roles

Snowflake's guidance sometimes adds a database-role tier beneath access roles. Considered and rejected
as over-engineering for a single database — it would add a layer with no discriminating power. It
becomes worthwhile with many databases sharing access patterns, and this model extends to it without
restructuring.

### One service identity for everything

Simplest, and the tempting shortcut on a trial account. Rejected because the personas have genuinely
different blast radii: the file producer writes `RAW` and **cannot read or alter curated trade data**,
which means a compromised or buggy producer cannot corrupt the golden record. Collapsing them would
discard that containment for the sake of one fewer key to manage.

### `ACCOUNTADMIN` for everything

What happens by default on a trial. Rejected for the obvious reason and for a less obvious one: if the
pipeline runs as `ACCOUNTADMIN`, the RBAC model is never exercised, so nobody knows whether it works.
An untested model is documentation, not a control.

## Consequences

### Good

- **Adding a schema is one or two new access roles**, granted to whichever personas need them. No user
  is touched and no existing privilege changes.
- **Permissions are reviewable.** "What can the dashboard read" is answered by reading one functional
  role's definition in HCL, in git, in a pull request — or in Snowflake with a single
  `SHOW GRANTS OF ROLE`. That property is the entire point.
- **Least privilege is real, not aspirational.** `FR_TRADES_INGEST` genuinely cannot read `CORE`, and
  `FR_TRADES_ANALYST` genuinely cannot read `AUDIT` — so the dashboard cannot show a raw payload even
  by accident. Easy to verify by assuming the role and trying.
- **Compliance is separated from engineering.** `FR_TRADES_COMPLIANCE` reads `AUDIT` — the rejected
  payloads and the full rule-hit log — without any write privilege anywhere. That separation is
  awkward to express with direct grants and falls out naturally here.
- **Future grants remove a whole class of problem.** `GRANT SELECT ON FUTURE TABLES IN SCHEMA` means a
  new dbt model is readable by the right roles the moment it is created. Without it, tomorrow's model
  is invisible to readers until someone remembers a grant script, and the symptom is "the dashboard
  cannot see the new table".
- **Per-persona keys.** Each service user has its own key pair, so rotating or revoking one consumer's
  credential does not disturb the others, and the audit log attributes activity to a specific
  consumer.

### Bad

- **More objects** — fifteen access roles and five functional roles per environment, so forty in
  total across dev and prod. On a first read the indirection looks like ceremony. The naming
  convention (`AR_`/`FR_` prefix, schema and level in the name, environment suffix) is what keeps it
  navigable, and it is worth being strict about.
- **One more hop when debugging a permission error.** "Analyst cannot read `FCT_TRADE`" means checking
  whether the functional role has the access role *and* whether the access role has the privilege.
- **Three keys to manage instead of one**, and three more values in `example.tfvars`. The setup guide
  is longer as a direct result.
- **Discipline is required.** The model is worth something only if nothing is ever granted directly to
  a user, including at 2am during an incident. A single expedient direct grant is how the model
  quietly stops being true — and Terraform will not show it in a plan, because Terraform does not
  manage the user's ad-hoc grants.

### Neutral

- Roles are per-environment, so dev access never implies prod access. That doubles the object count
  and is not optional in a regulated context.
- Functional roles are granted to `SYSADMIN`, so the standard Snowflake hierarchy is preserved and an
  administrator can assume any persona for debugging.

## Notes

Masking policies are where this model earns its keep beyond tidiness.

Policies attach to **tags**, not to columns, and the tags classify the data. So classifying a new
column applies the correct masking automatically, and a column cannot be sensitive-but-unmasked
because someone completed two of three steps.

The policy body then reads the current role, and the allow-list is exactly three personas plus
`ACCOUNTADMIN` — `TRANSFORM`, `COMPLIANCE` and `PLATFORM`. So `FR_TRADES_ANALYST` sees curated trade
data with counterparty names and exact notionals redacted, while `FR_TRADES_COMPLIANCE` sees them
unmasked because investigating a rejection requires it.

This is precisely why functional roles hold no object privileges. With two mechanisms deciding what a
user sees — direct grants *and* masking policies — reasoning about the combination would be genuinely
hard. Keeping the tiers separate means access is decided in one place and *visibility within*
accessible data in another, and each is readable on its own.

Note the consequence: the policy names roles explicitly, so **adding a functional role requires
revisiting `unmasked_roles`**. That is a real maintenance cost and a deliberate choice. Failing
closed — a new role sees masked data until someone decides otherwise — is the correct default for
counterparty identifiers.
