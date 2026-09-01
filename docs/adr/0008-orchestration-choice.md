# ADR 0008: Airflow for orchestration, Snowflake Tasks for continuous work

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`setup.md`](../setup.md#step-10-airflow),
[`overview.md`](../overview.md#orchestration--airflow)

---

## Context

Something has to decide when the pipeline runs, in what order, and what happens when a step fails.
The work divides into two kinds:

- **Continuous, independent, cheap** — draining the change stream, pruning the queue, archiving
  loaded stage files. This should happen whether or not anything else is running.
- **Sequenced, gated, cross-system** — wait for files, transfer them, load, transform, test, verify.
  This needs ordering, needs to be able to *stop*, and touches both the local filesystem and
  Snowflake.

The case study permitted Airflow, Cloud Composer, or Snowflake-native orchestration.

## Decision

**Both, split by which kind of work it is.**

**Snowflake Tasks** own the continuous work: `TASK_DRAIN_TRADE_EVENT_STREAM` (every minute, gated on
`SYSTEM$STREAM_HAS_DATA`), `TASK_PRUNE_TRADE_EVENT_QUEUE` (chained after the drain),
`TASK_ARCHIVE_LOADED_FILES` (daily).

**Airflow** owns the batch pipeline: one DAG, twenty tasks, three groups, hourly,
`max_active_runs=1`, `catchup=False`. dbt is invoked with `BashOperator`.

The dividing line, stated once: **Snowflake Tasks do what must happen continuously and independently;
Airflow does what needs sequencing, gating and cross-system coordination.**

## Alternatives considered

### Snowflake Tasks for the entire pipeline

Seriously considered, and it would have removed Docker and Airflow from the local footprint
altogether — a real benefit for a project someone has to install on a laptop. Task trees can express
a DAG, and `SYSTEM$STREAM_HAS_DATA` gives conditional execution.

Rejected for four reasons, in increasing order of weight:

1. **No cross-system reach.** The pipeline has to notice a file on a local filesystem and `PUT` it.
   A Snowflake Task cannot see outside Snowflake.
2. **Weak retry semantics for the transfer step.** `TASK_AUTO_RETRY_ATTEMPTS` is a fixed count with
   no backoff. Transient network errors on a file upload want exponential backoff.
3. **No usable alerting from a task tree.** A task can call a procedure that sends email, but
   assembling "which task, which try, what exception, here is the log" is machinery Airflow already
   has.
4. **Decisively: nowhere to put a gate that stops the pipeline and explains why.** The four data
   quality gates are among the most valuable parts of the DAG. A gate needs to evaluate a condition,
   decide blocking versus warning, notify with detail, and halt everything downstream while leaving
   what already ran intact. In a task tree that is a stored procedure raising an exception, and the
   operator's diagnosis is whatever fitted in the error string.

### Cloud Composer

Managed Airflow, and the right answer for a real production deployment — no scheduler to run, no
Postgres to back up. Rejected because it is not free and cannot run on a laptop, which are the two
constraints this project actually has. The DAG is plain Airflow and would move to Composer unchanged.

### Prefect or Dagster

Both are better designed than Airflow in specific ways — Dagster's asset model in particular maps
neatly onto dbt models, and its dbt integration is the best available. Rejected on a single practical
ground: Airflow is what is deployed at the organisation this was written for, and an orchestrator
nobody operates is a liability regardless of its design. Choosing the tool the team runs is the right
call even when it is not the best tool.

### `astronomer-cosmos` instead of `BashOperator` for dbt

Cosmos parses the dbt manifest and renders each model as its own Airflow task, which gives per-model
retries and a beautiful graph. Genuinely better for a large project.

Rejected here because it inverts the ownership of the run. Cosmos decides model order from the
manifest; with `BashOperator` the DAG expresses the order deliberately — and the order is where the
design is. `gate_reject_rate` must sit *after* adjudication and *before* the marts, and
`dbt_snapshot` must run *after* `dbt_test`. Those placements are load-bearing correctness decisions,
not conveniences, and they are much clearer as explicit tasks than as configuration steering a
renderer. Cosmos also adds a dependency that must stay compatible with both Airflow and dbt, and
this project already pins both tightly.

The cost of `BashOperator` is real and worth stating: no per-model retry, and dbt failures surface as
"the layer failed" rather than "this model failed". That is mitigated by splitting the run into four
tasks by layer, which localises a failure to a layer while keeping the gates explicit.

## Consequences

### Good

- **The drain runs every minute for effectively no cost.** Gated on `SYSTEM$STREAM_HAS_DATA`, the
  task evaluates its condition and skips without starting compute. So change capture keeps up with
  arrivals independently of the hourly batch — and the queue is already drained when Airflow's
  transform stage begins.
- **Change capture survives Airflow being down.** The most operationally valuable consequence: if the
  laptop running Airflow is switched off, the stream still drains, so the stream does not approach
  staleness and the delta is not lost. Had the drain lived only in the DAG, an Airflow outage longer
  than Time Travel retention would be unrecoverable data loss.
- **Gates have somewhere to live**, with the notification detail an operator needs and a runbook
  anchor per failure class.
- **`max_active_runs=1` is enforceable.** It is a correctness requirement — concurrent runs would
  race on the version high-water mark — and Airflow expresses it in one line.
- The DAG is portable to Composer or MWAA with no change.

### Bad

- **Two orchestrators means two places to look.** Mitigated by `VW_TASK_HISTORY` and
  `ALERT_TASK_FAILURE`, so a failing Snowflake Task pages someone rather than waiting to be noticed —
  but the honest cost is that "why is nothing happening" has two possible answers.
- **A recreated task tree comes back suspended.** A Snowflake default that catches everyone once, and
  the reason `--preset tasks` exists and the runbook mentions `ALTER TASK ... RESUME` explicitly.
- **Docker is required locally**, which is the largest single item in the setup guide and the source of
  the most common installation problem (Docker Desktop defaulting to 2 GB, which OOM-kills the
  scheduler in a way that looks like a DAG bug).
- Airflow's own SLA mechanism is the only thing that can report "this sensor has been waiting too
  long", and it is a feature Airflow has repeatedly reworked. It is used for exactly three tasks so
  the exposure is small.

### Neutral

- `catchup=False` is not an orchestration convenience but a correctness decision: the rules evaluate
  against current state, so replaying a historical interval would re-adjudicate today's data with a
  historical business date. Historical reprocessing is a deliberate separate operation, documented in
  the runbook.

## Notes

One wiring detail cost real debugging time and is worth recording, because it is invisible on
inspection.

Chaining `@task_group`s with `>>` from outside attaches to the group's **last** task, not its first.
So `ingest_group() >> transform_group()` produces a graph that *looks* sequential in the UI while the
two groups actually start in parallel — and with a fast machine it may even appear to work.

The fix is that each group function takes an `upstream` argument and chains it to its own first task
explicitly. The guard is a reachability check in `scripts/validate_dags.py` that asserts every task
is downstream of `start`, which turns this class of mistake into a CI failure with a message naming
the likely cause. A graph that is wrong in a way you cannot see is exactly what static analysis is
for.
