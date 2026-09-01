# Architecture overview

What this platform does, how the pieces fit together, and why each choice was made.

For installation see [`setup.md`](setup.md). For the business rules see
[`validation-logic.md`](validation-logic.md). For operations see [`runbook.md`](runbook.md).

---

## Contents

- [The problem](#the-problem)
- [The shape of the solution](#the-shape-of-the-solution)
- [Data flow, end to end](#data-flow-end-to-end)
- [The layers](#the-layers)
- [Component choices](#component-choices)
- [The data model](#the-data-model)
- [Cross-cutting properties](#cross-cutting-properties)
- [Security and governance](#security-and-governance)
- [Environments and promotion](#environments-and-promotion)
- [Repository layout](#repository-layout)
- [What was deliberately left out](#what-was-deliberately-left-out)

---

## The problem

Trade files arrive periodically from upstream capture systems. Each file contains trade events —
new bookings, amendments, cancellations — and each event must be validated against business rules
before it is allowed to affect the firm's record of its positions.

Four rules were specified:

1. Reject a version lower than one already accepted.
2. On a same-version resend, the later arrival wins.
3. Reject a trade whose maturity date has already passed.
4. A trade reaching maturity becomes EXPIRED.

Beneath that sits the harder requirement, which is not stated as a rule: **the platform must be
able to prove what it did.** Every rejection must be explainable, every accepted trade traceable to
the message that produced it, and every version of every trade recoverable. In a regulated context
"the pipeline ran successfully" is not an answer to "why was this trade not in yesterday's
position".

That requirement, more than the four rules, is what shapes the architecture below.

---

## The shape of the solution

Five components, each doing one thing:

| Component | Technology | Responsibility |
| --- | --- | --- |
| **Simulator** | Python (`trade_sim`) | Generate realistic trade events, inject known faults, record ground truth |
| **Platform** | Snowflake | Store, capture change, monitor, alert |
| **Transformation** | dbt | Type, adjudicate, model, test, document |
| **Orchestration** | Airflow | Schedule, sequence, gate, alert on execution |
| **Presentation** | Streamlit | Operational visibility |

Provisioned by Terraform, deployed by GitHub Actions, driven by a Makefile.

### The single most important design decision

**Transformation happens in SQL inside Snowflake, not in a separate compute engine.**

The data is already in Snowflake. Moving it out to Spark or a Python process to apply a business
rule, then moving the result back, adds network transfer, a second cluster to operate, a second
place for logic to live, and a second thing to secure — in exchange for nothing, because the
transformation is relational. Joins against reference data, window functions for version
arbitration, aggregations for reporting: all of it is what a columnar warehouse is built for.

Everything else follows from that. dbt is the natural way to manage SQL transformations, which
gives testing, lineage and documentation. Snowflake Streams give change capture without a queue.
Snowflake Alerts give monitoring that survives the orchestrator dying. Reasoning in full in
[`adr/0001-snowflake-native-elt.md`](adr/0001-snowflake-native-elt.md).

### Where Python is still the right answer

Python is used where SQL is genuinely the wrong tool: generating data, orchestrating a sequence,
and rendering a UI. Nothing that decides whether a trade is valid is written in Python. That
boundary is intentional and easy to state: **business logic lives in SQL, in dbt, under test.**

---

## Data flow, end to end

```
  Upstream capture systems
            |                    (simulated by trade_sim)
            v
    NDJSON files                 one line per event, plus a manifest of expected verdicts
            |
            |  PUT
            v
  @RAW.TRADE_LANDING             internal stage
            |
            |  COPY INTO (ON_ERROR = CONTINUE)      -->  RAW.COPY_ERROR   unparseable lines
            v
  RAW.TRADE_EVENT                immutable, insert-only, VARIANT payload
            |                    14-day Time Travel
            |  Stream (APPEND_ONLY)
            v
  RAW.TRADE_EVENT_STREAM         change capture, offset-based
            |
            |  SP_DRAIN_... (one transaction)
            v
  RAW.TRADE_EVENT_QUEUE          the boundary dbt reads
            |
   =========|=================== dbt from here down ===========================
            v
  STG_TRADE_EVENT                view: rename, no logic
            v
  INT_TRADE_EVENT_TYPED          try_to_* casts + cast_failure_count
            v
  INT_TRADE_EVENT_ADJUDICATED    FIELD rules -> arbitration -> STATE rules -> verdict
            |
      +-----+---------------------------+------------------------------+
      v                                 v                              v
  CORE.FCT_TRADE                 AUDIT.FCT_TRADE_REJECTED     AUDIT.TRADE_RULE_RESULT
  CORE.FCT_TRADE_VERSION         (with raw_payload)            (every rule hit, incl. WARN)
      |
      v
  SNAPSHOTS.SNP_TRADE            SCD2 history
      |
      v
  REPORTING.*                    aggregates, scorecard, expiring-soon
```

Two things about this diagram are worth stating explicitly.

**The queue table is the contract.** dbt never reads `RAW.TRADE_EVENT` or the stream directly. It
reads `RAW.TRADE_EVENT_QUEUE`, which means the ingestion mechanism can change — Snowpipe, batch
COPY, an external table, a Kafka connector — without dbt changing at all. It is also what makes
dbt's source freshness check meaningful: freshness on the queue answers "has new work arrived for
me", which is the question that matters.

**Adjudication fans out to three destinations, and every event reaches exactly one of the first
three paths.** That invariant is asserted by `assert_no_event_is_silently_dropped`, and it is the
structural reason the platform can prove it did not lose anything.

---

## The layers

### RAW — immutable landing

`RAW.TRADE_EVENT` stores the payload as `VARIANT`, insert-only, never updated or deleted. Also
`RAW.LOAD_BATCH` (one row per load attempt), `RAW.COPY_ERROR` (lines that never parsed),
`RAW.TRADE_EVENT_QUEUE` (the drained delta).

**Why `VARIANT` rather than typed columns at landing.** A typed landing table has to reject a row it
cannot cast, and the row is then gone — the one piece of evidence needed to work out what the
upstream did wrong. Landing the payload as-is means an upstream schema change is a *transformation*
problem discovered in dbt, with the original bytes still available, rather than an *ingestion*
failure that discards the evidence. Cost: a cast layer is needed downstream. Benefit: the platform
never loses a message because it did not understand it.

**Why insert-only.** This is the audit foundation. Because RAW is append-only, every table
downstream is reconstructible from it, so a bug in adjudication is recoverable by fixing the model
and rebuilding rather than by going back to the source system.

Time Travel is 14 days here and 1 day on the rebuildable intermediate schema, because retention
costs storage and there is no reason to pay it twice for derived data.

### Change capture — Streams and Tasks

A stream on `RAW.TRADE_EVENT` with `APPEND_ONLY = TRUE`, drained by
`SP_DRAIN_TRADE_EVENT_STREAM` into the queue table.

**Why a stream rather than a high-water-mark query.** `where load_ts > (select max(load_ts) ...)`
is the obvious alternative and it is subtly wrong: two sessions inserting concurrently can commit
out of timestamp order, so a row with an earlier timestamp can become visible after the mark has
passed it, and it is then never picked up again. Silent loss, load-dependent, essentially
undebuggable. A stream tracks a transactional offset and has no such race.

**Why the drain is one transaction.** The insert that consumes the stream, the batch record, and
the offset advance all commit together. So a failed drain leaves the offset exactly where it was —
nothing half-consumed, nothing lost, and re-running is always safe. That single property is what
makes most of the runbook say "retry it".

The trade-off is the one real sharp edge in the design: a stream goes stale if not consumed within
the source table's Time Travel retention, and past that point the delta is unrecoverable from the
stream. It is monitored by `VW_STREAM_LAG`, and the recovery procedure is
[in the runbook](runbook.md#recreating-the-stream). Full reasoning in
[`adr/0004-streams-and-tasks-for-cdc.md`](adr/0004-streams-and-tasks-for-cdc.md).

### Transformation — dbt

Four layers, with a rule about each:

- **staging** — views. Rename and re-shape only. No business logic, ever, so the mapping from
  source field to model column is a single readable file.
- **intermediate** — the typing model and the adjudication model. All business logic lives here.
- **marts** — `core` (facts and dimensions) and `reporting` (aggregates). Read by humans and BI.
- **audit** — rejections and rule results. A first-class output, not a debug artifact.

The adjudication model is the heart of the platform and is described in
[`validation-logic.md`](validation-logic.md). The one structural point worth repeating here: the
rules are **declared as data** in a single macro, and the SQL, the audit rows and the published
documentation are all generated from that declaration. Adding a rule is a four-line change in one
file, and a rule cannot fire without being logged, because the same list generates both.
[`adr/0005-rules-as-declarative-macro.md`](adr/0005-rules-as-declarative-macro.md).

### Orchestration — Airflow

One DAG, twenty tasks, three groups: ingest, transform, verify. Hourly,
`max_active_runs=1`, `catchup=False`.

Both of those settings are correctness requirements rather than resource tuning. Concurrent runs
would race on the version high-water mark. Backfilling would re-adjudicate today's data against a
historical business date, which is meaningless — the rules evaluate against current state.
Historical reprocessing is [a deliberate separate operation](runbook.md#backfill-and-replay).

Snowflake Tasks also exist, handling the minute-by-minute drain and the housekeeping. The division:
**Snowflake Tasks do what must happen continuously and independently; Airflow does what needs
sequencing, gating and cross-system coordination.**
[`adr/0008-orchestration-choice.md`](adr/0008-orchestration-choice.md).

### Presentation — Streamlit

Four pages over the marts and monitoring views. It reads **only** marts and monitoring — never RAW,
never an intermediate model — and a test enforces it, so the dashboard cannot quietly become a
second definition of the truth that disagrees with the tested one.
[`adr/0010-dashboard-choice.md`](adr/0010-dashboard-choice.md).

---

## Component choices

The alternatives that were seriously considered, and why each was not chosen. These are the
questions an interviewer asks.

### Why not Spark or EMR for transformation

The transformation is relational, and the data is already in the warehouse. Spark would add a
cluster to size, tune and secure, a second language for business logic, and network transfer in both
directions — to do joins and window functions less well than Snowflake does them natively. Spark
earns its keep on unstructured data, on ML feature pipelines, and where compute must live outside
the warehouse for cost or locality reasons. None of those apply.

### Why not Kafka for ingestion

The upstream produces files, periodically. Kafka is the right answer when the source is a stream of
events and latency matters in seconds. Introducing it here would mean operating a broker, plus
Kafka Connect, plus schema registry, to move files that arrive hourly — and the pipeline would
still be a batch pipeline, because the business rules need the whole batch to arbitrate versions.

The design does not preclude it: the queue table is the contract, so a Kafka connector could replace
the file path without dbt changing. Discussed in [`scalability.md`](scalability.md).

### Why not Snowflake Dynamic Tables instead of dbt

Genuinely tempting, and this is the closest call in the project. Dynamic Tables give declarative
incremental refresh with no orchestrator. They were not chosen because they do not provide the
things this platform needs most: unit testing of business rules against mock data, a versioned
declarative rule catalogue, generated documentation, or `--store-failures` for investigating a
failing assertion. A platform whose main requirement is provable correctness needs a testing
framework more than it needs automatic refresh.

### Why not Snowflake Tasks for the whole pipeline

Tasks can express a DAG, and doing so would remove Docker and Airflow from the local footprint
entirely. Rejected because task trees give no cross-system coordination, no file-arrival sensing
outside Snowflake, weak retry semantics for the file-transfer step, and — decisively — no place to
put a data quality gate that can *stop* the pipeline and explain why. The gates are the part of the
DAG doing the most work.

### Why dbt-core rather than dbt Cloud

Cost, and the requirement that everything run on a laptop. dbt-core also keeps the whole project in
one repository under one CI pipeline. dbt Cloud's scheduler and IDE would be a reasonable choice for
a team; they would not change the models.

### Why Streamlit rather than Tableau or Power BI

It is Python, in the same repository, under the same tests, installable with `pip`. A BI tool would
be the right answer for business users; the audience for this dashboard is whoever is on call, and
the requirement is that it ships with the code.

---

## The data model

### Core

**`FCT_TRADE`** — the golden record. One row per trade, current version, current lifecycle status.
Incremental, merged on `trade_id`. This is what "what is our position" reads.

**`FCT_TRADE_VERSION`** — the version ledger. Every accepted version of every trade, one row each.
Incremental, merged on a surrogate key of trade and version. This is what "what did we know, and
when" reads.

**Why both.** A single table cannot answer both questions. Keeping only the current version makes
history unavailable; keeping only history makes every position query a window function over the
full ledger. `FCT_TRADE` is exactly the maximum-version projection of `FCT_TRADE_VERSION`, and
`assert_fct_trade_matches_version_ledger` asserts it — so the redundancy is checked rather than
trusted.

**Dimensions** — `DIM_COUNTERPARTY`, `DIM_BOOK`, `DIM_PRODUCT`, each enriched with observed activity
from the fact tables, so a dimension row carries both its reference attributes and how much it is
actually used.

### Audit

**`FCT_TRADE_REJECTED`** — every refused and superseded event, **with `raw_payload` as it arrived**.
Retaining the payload is what makes a rejection investigable; without it, "RJ008 fired" is a dead
end.

**`TRADE_RULE_RESULT`** — one row per (event, rule that fired), including WARN hits on trades that
were accepted. This is what makes "which rule is causing this" answerable, and what
`rules_never_fired` is computed from.

### Snapshots

**`SNP_TRADE`** — SCD2 over `FCT_TRADE`.

**Why, given the version ledger already exists.** Some state transitions have no event. The most
important is expiry: a trade becomes EXPIRED because a date passed, not because a message arrived,
so it appears nowhere in the version ledger. The snapshot is what makes "what did this trade look
like on 3 March" answerable across those transitions.

`assert_snapshot_covers_material_columns` fails if a column is added to `FCT_TRADE` and not to the
snapshot's `check_cols`, because an audit trail that silently stops covering a column is worse than
one that is obviously incomplete.

### Reporting

Daily status aggregates, rejection analysis by rule and source, trades expiring soon, and the
one-row data quality scorecard that the publish gate and the dashboard both read.

---

## Cross-cutting properties

Four properties hold throughout, and each is enforced somewhere rather than merely intended.

### Idempotency

Every step can be re-run safely.

| Step | Mechanism |
| --- | --- |
| COPY | Snowflake's load history skips already-ingested files |
| Drain | Single transaction; offset advances only on success |
| dbt incremental models | Merge on a surrogate key, never append |
| Snowflake SQL deployment | `CREATE OR REPLACE`, except the stream |
| Terraform | Declarative by construction |

This is why "retry it" is the first line of most runbook procedures, and why you never need to
establish what a failed attempt managed to complete before retrying.

### Auditability

Nothing is deleted. Rejections keep their payload. Rule hits are recorded individually. RAW is
immutable. Snapshots historise state changes that have no event.
[`adr/0007-append-only-audit.md`](adr/0007-append-only-audit.md).

### Testability

Business rules are provable with no warehouse and no credentials, in seconds, via dbt unit tests
against mock rows. That is what makes the rules maintainable — a rule you cannot cheaply test is a
rule nobody will change confidently.

Above that: 162 generic data tests, six singular tests asserting cross-model invariants, 88 Python tests,
DAG static analysis, and repository self-consistency checks.

### Provable correctness

The simulator records, for every event it writes, the verdict that event *must* receive. `reconcile`
compares that ground truth against what the pipeline actually decided.

This is qualitatively different from every other check, because it can detect an event that is
**absent**. Tests validate rows that are present; only reconciliation notices a row that should
exist and does not. It is the difference between "the DAG went green" and "the pipeline was
correct".

---

## Security and governance

### Two-tier RBAC

Access roles hold privileges on exactly one schema at one level — fifteen of them, named
`AR_TRADES_<SCHEMA>_<R|RW>_<ENV>`. Functional roles hold no privileges directly and describe
personas: `INGEST`, `TRANSFORM`, `ANALYST`, `COMPLIANCE`, `PLATFORM`. Users get functional roles only.

Three service users, each with its own key pair and exactly one persona: the loader writes `RAW` and
**cannot read or alter curated data**, dbt has DDL across the modelling layers, the dashboard reads
curated data masked. That is a real containment boundary, not just tidiness — a buggy loader cannot
corrupt the golden record.

**Why two tiers rather than granting directly.** Direct grants mean adding a schema requires touching
every role that should see it, and answering "what can the dashboard read" means comparing grant lists
that differ per user because someone was granted something extra during an incident. With two tiers, a
new schema is one new access role, and permissions are reviewable in a pull request.
[`adr/0003-two-tier-rbac.md`](adr/0003-two-tier-rbac.md).

### Masking

Object tags classify columns; masking policies attach to the tags rather than to the columns. So
classifying a new column applies the right masking automatically, and a column cannot be sensitive but
unmasked because someone forgot the second step. Counterparty names and exact notionals are redacted
from the `ANALYST` persona and visible to `TRANSFORM`, `COMPLIANCE` and `PLATFORM` — compliance needs
them unmasked because investigating a rejection requires it.

### Authentication

Key pair, not passwords. Non-interactive, rotatable without downtime, and nothing reusable crosses
the network. [`adr/0006-keypair-authentication.md`](adr/0006-keypair-authentication.md).

### Cost as a control

Three warehouses, one per workload, each with its own resource monitor. An account-level monitor
would suspend everything when one workload misbehaves, so a runaway BI query would stop ingestion.
Per-warehouse monitors make the blast radius equal to the workload that caused it — which is also
the reason the warehouses are separate at all.

---

## Environments and promotion

Two environments, `dev` and `prod`, differing only in Terraform variables: warehouse size, credit
quota, Time Travel retention. Same modules, same SQL, same dbt code.

dbt's `generate_schema_name` macro routes by target, and a pull request builds into an isolated
`PR_<number>_*` schema that `pr-cleanup.yml` drops when the PR closes — so a reviewer can query
what a PR actually produced, and the account does not silently fill with abandoned schemas.

**Promotion:** CI runs the offline tier on every push, and the warehouse tier where credentials are
available. CD runs `terraform plan`, waits for **manual approval**, then applies, deploys the SQL
layer, runs `dbt build`, and checks platform health before finishing.

The approval gate sits between plan and apply because a Terraform plan against a warehouse can
contain a destructive change — a warehouse replacement, a role drop — and nobody should discover
that from the apply log.

---

## Repository layout

```
ingestion/       trade_sim: generator, loader, reconciler, CLI, tests
dbt/             models, macros (the rule book), seeds, snapshots, tests
airflow/         DAG, callbacks, Dockerfile, docker-compose
snowflake/       10_ingestion, 20_streams_tasks, 30_monitoring, 40_alerts
terraform/       modules (warehouse, database, rbac, governance) + dev/prod
dashboard/       Streamlit app, pages, query library, render tests
scripts/         doctor, deploy_snowflake_sql, run_sql, validate_dags, selfcheck
.github/         ci.yml, cd.yml, pr-cleanup.yml
docs/            this directory, plus adr/ and diagrams/
Makefile         every operation, one entrypoint
```

The `snowflake/` directories are numbered because deployment order matters: the stage must exist
before the pipe, the tables before the stream, the views before the alerts that read them.
`deploy_snowflake_sql.py` walks them in order.

---

## What was deliberately left out

Stating the boundaries is part of the design.

**Real-time streaming.** The rules need the whole batch to arbitrate versions, so a per-event
streaming design would have to buffer anyway. Hourly matches the upstream.

**A separate reference data platform.** Counterparties, currencies, products and books are dbt
seeds. Correct for a case study, wrong for production, where they are someone else's system of
record and should be ingested rather than committed.

**Trade enrichment and valuation.** No pricing, no PV, no risk. That is a different system with
different requirements, and the platform's job is to produce a trustworthy trade record for it to
consume.

**Distribution drift detection.** Reject rate is monitored; the shape of the data is not. It needs a
baseline of normal that a fresh platform does not have, and alarms without a baseline are noise. See
[monitoring.md](monitoring.md#what-is-not-monitored-and-why).

**External uptime monitoring.** Every monitor here runs inside Snowflake, so Snowflake being down
takes the monitoring with it. That is the accepted single point of failure of a warehouse-native
design, and the honest mitigation is an external check, which is out of scope for a local
deployment.
