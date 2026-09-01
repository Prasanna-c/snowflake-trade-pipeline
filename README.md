# Trade lifecycle validation pipeline

A production-grade data platform that ingests trade events, validates them against business rules,
and maintains an auditable golden record of every trade — built Snowflake-native with dbt, Airflow
and Terraform.

**It does not just process trades. It proves it processed them correctly.** The simulator records,
for every event it generates, the verdict that event must receive. After each run, reconciliation
compares that ground truth against what the pipeline actually decided — so the pipeline can detect
an event that is *absent*, not merely one that is wrong.

---

## What it does

Files of trade events arrive. Each event is a new booking, an amendment, or a cancellation. The
platform:

- **lands** them immutably, retaining the original bytes even when they cannot be parsed;
- **validates** them against nineteen business rules across two evaluation phases;
- **arbitrates** versions, so the correct version of a trade wins deterministically;
- **maintains** a golden record, a full version ledger, and SCD2 history;
- **explains** every rejection, keeping the payload, the rules that fired, and the source file;
- **monitors** itself in a way that survives the orchestrator being down;
- **stops** rather than publishing data it cannot vouch for.

### The four required rules

| Requirement | Rule | Implementation |
| --- | --- | --- |
| R1 | Reject a version lower than one already accepted | `RJ001` |
| R2 | On a same-version resend, the later arrival wins | `RJ009` + arbitration |
| R3 | Reject a trade whose maturity has passed | `RJ003` |
| R4 | A trade reaching maturity becomes EXPIRED | Expiry sweep in `fct_trade` |

Plus sixteen further rules — completeness, referential integrity, type coercion, direction,
settlement, credit status, desk limits — each tagged so what was asked for is separable from what a
real platform needs. [Full catalogue and reasoning](docs/validation-logic.md).

---

## Architecture

```
Upstream files ──► @stage ──► RAW.TRADE_EVENT ──► Stream ──► QUEUE ──► dbt ──► marts
                                    │                                    │
                                    ▼                                    ▼
                             RAW.COPY_ERROR                    AUDIT.FCT_TRADE_REJECTED
                            (unparseable lines)                 (with raw_payload)
```

| Layer | Technology | Owns |
| --- | --- | --- |
| Simulation | Python (`trade_sim`) | Event generation, fault injection, ground truth, reconciliation |
| Storage | Snowflake | Immutable landing, change capture, monitoring, alerting |
| Transformation | dbt | Typing, adjudication, marts, snapshots, tests, docs |
| Orchestration | Airflow + Snowflake Tasks | Sequencing and gating; continuous drain |
| Presentation | Streamlit | Trade status, rejection analysis, pipeline health |
| Provisioning | Terraform | Warehouses, database, two-tier RBAC, masking, cost controls |
| Delivery | GitHub Actions | Two-tier CI, CD with a plan/approve/apply gate |

**The central decision:** transformation happens in SQL inside Snowflake, not in a separate compute
engine. The data is already there and the work is relational — joins, window functions, aggregations.
Everything else follows from that. [ADR 0001](docs/adr/0001-snowflake-native-elt.md)

**Business logic lives only in dbt.** Nothing that decides whether a trade is valid is written in
Python.

Diagrams: [architecture](docs/diagrams/01-architecture.puml),
[data flow](docs/diagrams/02-data-flow.puml),
[adjudication](docs/diagrams/03-adjudication-sequence.puml),
[gates](docs/diagrams/04-orchestration-and-gates.puml). Render with `make diagrams`.

---

## Quick start

You need Python 3.11 or 3.12, Terraform 1.5+, Docker Desktop with 4 GB, and a free
[Snowflake trial](https://signup.snowflake.com) (Enterprise edition).

```bash
make install                              # venv, dependencies, pre-commit hooks
cp .env.example .env                      # then set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER
cp dbt/profiles.yml.example dbt/profiles.yml
make keypair                              # then register the public key in Snowflake
make doctor                               # must be green before continuing
make bootstrap                            # provision everything, ~3 minutes
make demo                                 # load → drain → transform → test → reconcile
make status                               # four RAG columns, all GREEN
```

`make quickstart` prints this as a checklist. `make help` lists every target.
Full walkthrough with reasoning: [`docs/setup.md`](docs/setup.md).

### Try it without Snowflake

A large amount of the project runs with no credentials at all — this is what CI's offline tier runs:

```bash
make ci-local          # lint, pytest, dbt parse, DAG validation, selfcheck
make dbt-unit-test     # every business rule, proved against mock rows, in seconds
make generate          # write trade files to ./data
```

---

## Documentation

| Document | Read it for |
| --- | --- |
| [`overview.md`](docs/overview.md) | Architecture, component choices, the data model |
| [`setup.md`](docs/setup.md) | Installation, step by step, with the reasoning |
| [`validation-logic.md`](docs/validation-logic.md) | Every rule, and why each is written that way |
| [`runbook.md`](docs/runbook.md) | **What to do when something breaks** |
| [`monitoring.md`](docs/monitoring.md) | Every monitor, alert and threshold, and why that number |
| [`scalability.md`](docs/scalability.md) | What breaks first at 100×, 1,000× and 10,000× |
| [`adr/`](docs/adr/README.md) | Ten decision records, including the alternatives rejected |
| [`interview-notes.md`](docs/interview-notes.md) | The design questions, answered |

Every alert this platform emits links to a runbook section, and `make selfcheck` fails if a link
points at a heading that does not exist — so the documentation cannot silently drift from the code
that cites it.

---

## Design highlights

The things worth reading the code for.

**Rules declared as data.** All nineteen live in one macro as a list of dictionaries. The evaluation
SQL, the audit rows and the published documentation are generated from that single declaration, so
**a rule cannot fire without being logged**. Adding a rule is four lines in one file.
[ADR 0005](docs/adr/0005-rules-as-declarative-macro.md)

**Phase ordering that prevents a poisoned history.** Rules split into FIELD and STATE, with version
arbitration between them over *field-valid events only*. Without that filter, a malformed version 5
would set the high-water mark and a perfectly valid version 3 would then be rejected as stale — on
the authority of an event already refused. One bad message would poison a trade's entire subsequent
history, and every resulting rejection would look correct. There is a unit test for exactly this.

**Change capture that cannot lose a row.** A Snowflake Stream tracks a transactional offset, not a
timestamp. The obvious alternative — `where load_ts > max(load_ts)` — has a race: two sessions
committing out of timestamp order means a row can become visible after the mark has passed it, lost
silently and unreproducibly. [ADR 0004](docs/adr/0004-streams-and-tasks-for-cdc.md)

**Idempotency throughout.** `COPY` skips files already ingested. The drain is one transaction, so a
failure leaves the offset exactly where it was. Every incremental model merges on a surrogate key.
This is why most of the runbook says "retry it", and why you never need to establish what a failed
attempt completed.

**Gates that stop the pipeline, positioned deliberately.** The reject-rate gate sits *after*
adjudication — so the rate is measurable and every rejection is already in the audit log — and
*before* the marts, so a suspect batch is stopped before anyone trades on it. Earlier there is
nothing to measure; later the damage is done.

**Monitoring that survives the orchestrator.** dbt writes its own outcomes to a Snowflake table from
an `on-run-end` hook, and Snowflake Alerts read it on Snowflake's own schedule. A stale curated layer
is detected even if the machine running Airflow is switched off — which is exactly when nobody is
watching the Airflow UI.

**Nothing is deleted.** Rejections keep their payload; unparseable lines keep their raw bytes; every
rule hit is recorded individually, including warnings on accepted trades. So "why was this trade not
in yesterday's position" is answerable. [ADR 0007](docs/adr/0007-append-only-audit.md)

**A staleness detector derived from the data.** Because the expiry sweep runs on every build, a
matured trade still marked LIVE proves no build has completed since it matured — regardless of what
Airflow reports. It is the first RED condition in the health scorecard.

---

## Testing

| Layer | Count | Needs a warehouse? |
| --- | --- | --- |
| dbt unit tests — business rules against mock rows | 12 | No |
| dbt data tests (generic) | 162 | Yes |
| Singular tests — cross-model invariants | 6 | Yes |
| Python tests — simulator, dashboard render, alerting, DAG helpers | 113 | No |
| DAG static analysis, incl. task reachability | — | No |
| Repository self-consistency (`selfcheck`) | — | No |
| Reconciliation against generated ground truth | — | Yes |

The singular tests are the interesting ones. `assert_no_event_is_silently_dropped` asserts that every
event entering the queue reaches exactly one destination — a violation is silent data loss.
`assert_sla_thresholds_agree` exists because thresholds are deliberately duplicated across three
layers so each works when the others are down, and this is what stops duplication becoming
divergence.

### What runs in CI, and what does not

CI is in two tiers. Everything above that says "No" to a warehouse runs on every push and pull
request, needs no credentials, and finishes in about two minutes. The jobs that need Snowflake —
`dbt build` into a per-PR schema, and the whole of CD — are switched off unless the repository says
otherwise:

| Repository variable | Set it to `true` to enable |
| --- | --- |
| `SNOWFLAKE_CI_ENABLED` | `dbt build` on pull requests, in an isolated `PR_<number>` schema |
| `SNOWFLAKE_CD_ENABLED` | The CD workflow: Terraform plan/apply, SQL deploy, production dbt |

They are repository *variables* rather than secrets because GitHub does not expose secrets to a
job's `if:` condition. Both default to off, so a clone with no Snowflake account behind it gets a
green tier-1 run rather than a wall of red that means nothing. A red badge that everyone knows to
ignore is worse than no badge.

---

## Repository layout

```
ingestion/     trade_sim: generator, loader, reconciler, CLI
dbt/           models, macros (the rule book), seeds, snapshots, tests
airflow/       DAG, callbacks, Dockerfile, docker-compose
snowflake/     10_ingestion, 20_streams_tasks, 30_monitoring, 40_alerts
terraform/     modules (warehouse, database, rbac, governance) + dev/prod
dashboard/     Streamlit app, pages, query library
scripts/       doctor, deploy_snowflake_sql, run_sql, validate_dags, selfcheck
docs/          documentation, ADRs, diagrams
Makefile       every operation, one entrypoint
```

The `snowflake/` directories are numbered because deployment order matters: the stage before the
pipe, the tables before the stream, the views before the alerts that read them.

---

## Operating it

```bash
make status            # which stage is at fault
make doctor            # local setup and connectivity
make dashboard         # http://localhost:8501
make airflow-up        # http://localhost:8080, admin/admin
make reconcile         # prove the last run reached the right verdicts
python scripts/run_sql.py --preset health   # the triage view
```

When something breaks, [`docs/runbook.md`](docs/runbook.md) has a section per failure class, and every
alert names the one you need.

---

## Cost

Under one credit a day in normal use. XSMALL warehouses with 60-second auto-suspend, and the
minute-by-minute drain task is gated on `SYSTEM$STREAM_HAS_DATA` so it costs nothing when idle.

Resource monitors cap dev at 20 credits a month across three warehouses and suspend at 90% — tight on
purpose, because a runaway loop is the most likely way to lose a trial account.

Per-warehouse monitors rather than account-level, so a runaway BI query cannot take down ingestion.
