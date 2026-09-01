# ADR 0002: Terraform owns objects with a lifecycle; SQL scripts own objects that are code

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`setup.md`](../setup.md#what-gets-created-and-by-which-tool)

---

## Context

Snowflake objects have to be created before anything runs: warehouses, database, schemas, roles,
grants, resource monitors, tags, masking policies, file formats, a stage, a pipe, a stream, tasks,
monitoring views, alerts and stored procedures.

Terraform has a capable Snowflake provider and could create all of them. So could a directory of SQL
scripts. Splitting between the two invites the criticism that provisioning lives in two places, so the
line has to be principled rather than arbitrary.

## Decision

**Terraform owns objects with a lifecycle. SQL scripts own objects that are code.**

**Terraform** (`terraform/modules/{warehouse,database,rbac,governance}`): warehouses, resource
monitors, database, schemas, access roles, functional roles, grants, tags, masking policies.

**SQL scripts** (`snowflake/`, deployed by `scripts/deploy_snowflake_sql.py`): file formats, the
internal stage, the Snowpipe, the stream, tasks, stored procedures, monitoring views, alerts.

**dbt** owns everything derived: staging views, intermediate models, marts, snapshots, tests.

The test for which side something falls on: **would you want Terraform to tell you this drifted?** A
warehouse resized by hand at 2am is exactly what a `terraform plan` should surface. A view definition
edited by hand should be caught by the fact that redeploying overwrites it.

## Alternatives considered

### Everything in Terraform

The purist position, and it has a real argument: one tool, one state file, one `plan` showing
everything.

Rejected because a large amount of what needs creating **is SQL**, and Terraform can only carry SQL as
an opaque string. A monitoring view with a nine-branch CASE expression becomes a heredoc that
Terraform cannot parse, cannot format, cannot lint and cannot diff meaningfully — a change shows as a
wall of red and green text with no structure. It would also be invisible to `sqlfluff`, which is what
keeps the rest of the project's SQL consistent.

The `snowflake_procedure` and `snowflake_view` resources exist, but using them means giving up every
SQL tool in exchange for a state entry.

### Everything in SQL scripts

Also coherent, and simpler: no Terraform, no state file, no provider version to manage.

Rejected because it loses the two things Terraform is actually for. **Drift detection** — `terraform
plan` on a warehouse or a grant tells you what changed outside the pipeline, which is precisely where
manual changes happen and precisely what an auditor asks about. And **destroy** — `make tf-destroy`
tears the environment down completely, which matters on a trial account and is a genuinely hard script
to write correctly by hand, because order and dependencies matter.

Roles and grants are the strongest case. "Who can read what" is a question that must be answerable
from source, and it is answerable from an HCL module in a way it is not from a pile of `GRANT`
statements accumulated over time.

### Terraform for objects, dbt hooks for views and procedures

Rejected as a layering violation. dbt's job is to build models from data. Making it responsible for
creating the stage that its own source table is loaded from means dbt cannot run until dbt has run.

## Consequences

### Good

- **Each object is managed by the tool suited to it.** SQL is reviewed as SQL, linted by `sqlfluff`,
  and readable in a pull request. Infrastructure is declarative with drift detection.
- **Both halves are idempotent.** Terraform by construction; the SQL scripts because every statement is
  `CREATE OR REPLACE`. Redeploying is a no-op, so `make bootstrap` is safe to run repeatedly, which
  matters more than it sounds during a first setup that goes wrong halfway.
- **`terraform plan` is meaningful**, because it covers only objects where drift is interesting. A plan
  that showed twelve view definitions every run would be scrolled past, and the one real change would
  be missed.
- **`make tf-destroy` genuinely works**, which is what makes a trial account safe to experiment on.
- **Ordering is explicit.** The `snowflake/` directories are numbered — `10_ingestion`,
  `20_streams_tasks`, `30_monitoring`, `40_alerts` — because the stage must exist before the pipe, the
  tables before the stream, and the views before the alerts that read them. A dependency graph
  expressed as a sort order, which is enough for a fifteen-file deployment and needs no explanation.

### Bad

- **Two tools, so two failure modes.** "Provisioning failed" needs qualifying with which half.
  `make doctor` checks for the outcome — do the RAW tables exist — rather than for the mechanism,
  which is the right level to check at.
- **The boundary needs a decision for each new object.** It is defensible but not mechanical, and a
  reviewer can disagree in good faith. The `would you want to see it drift` test resolves most cases.
- **State file management.** Local backend here, which is fine for one developer and wrong for a team.
  Production wants remote state with locking, and it is called out in the Terraform README.
- **`CREATE OR REPLACE` on a view briefly invalidates dependents.** Harmless here because deployment is
  a deliberate operation, not a hot path.

### Neutral

- Terraform needs `ACCOUNTADMIN` for the first apply, since roles, warehouses and resource monitors
  require it. After provisioning, everything runs as the functional role Terraform created. That is
  documented in the setup guide, because continuing to run as `ACCOUNTADMIN` would mean the RBAC model
  is never actually exercised.

## Notes

The one deliberate exception to `CREATE OR REPLACE` is the stream:

```sql
CREATE STREAM IF NOT EXISTS raw.trade_event_stream ...
```

A stream's offset is part of its **state**, not its definition. `CREATE OR REPLACE` resets it, and
combined with `SHOW_INITIAL_ROWS = TRUE` the next drain would re-read every row in `TRADE_EVENT`.

So the general rule — replace everything, redeployment is a no-op — has exactly one exception, and it
is the one object where the rule would cause data to be reprocessed. It is commented at length in the
script, because it looks like an inconsistency and someone will eventually try to "fix" it.
Recreating the stream deliberately is a runbook procedure with a documented backfill step, not a
deployment side effect.
