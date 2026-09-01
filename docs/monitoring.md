# Monitoring and alerting

Every monitor, every alert, every threshold, and the reasoning behind each number.

If you have been paged, go to the [runbook](runbook.md) instead — this document explains how the
detection works, not what to do about it.

---

## Contents

- [Design principles](#design-principles)
- [The three layers](#the-three-layers)
- [Layer 1: Snowflake monitoring views](#layer-1-snowflake-monitoring-views)
- [Layer 2: Snowflake alerts](#layer-2-snowflake-alerts)
- [Layer 3: Airflow gates and callbacks](#layer-3-airflow-gates-and-callbacks)
- [The dbt scorecard](#the-dbt-scorecard)
- [Cost monitoring](#cost-monitoring)
- [Threshold reference](#threshold-reference)
- [Why thresholds are duplicated](#why-thresholds-are-duplicated)
- [What is not monitored, and why](#what-is-not-monitored-and-why)

---

## Design principles

Five decisions shape everything below. They are worth stating because most of the specific
thresholds follow from them.

### 1. Detect absence, not just failure

The failure mode that matters most in a batch pipeline is that nothing happened. No file arrived,
no task ran, no build completed. None of these raises an error, because there is no execution in
which to raise one. Errors monitor themselves; absence has to be monitored deliberately, by
comparing observed state against expected cadence.

That is why `VW_FILE_ARRIVAL` compares the stage directory against loaded rows rather than watching
for load errors, and why the single most informative alarm on the platform —
`overdue_expiry_trades > 0` — is a *data* condition rather than an execution one.

### 2. Monitoring must survive the failure it monitors

An observability layer that lives inside the orchestrator is blind exactly when the orchestrator
dies, which is when you most need it. So the primary health signal is a Snowflake view over
Snowflake tables, with Snowflake alerts evaluating it on a Snowflake schedule. Airflow's callbacks
are a convenience on top, not the foundation.

Concretely: dbt writes its own outcomes to `AUDIT.DBT_RUN_RESULT` from an `on-run-end` hook, and
`ALERT_TRANSFORM_BACKLOG` reads that table. If the machine running Airflow is switched off, a stale
curated layer is still detected and still emails someone.

### 3. Real-time views must not depend on lagging sources

`SNOWFLAKE.ACCOUNT_USAGE` lags by up to 45 minutes. That is fine for cost analysis and useless
during an incident. Every view in this project is therefore explicitly one or the other, and the
incident-path views read only project tables and `INFORMATION_SCHEMA`:

| Purpose | Source | Latency |
| --- | --- | --- |
| Incident triage | Project tables, `INFORMATION_SCHEMA`, stage directory | Real-time |
| Cost and tuning | `ACCOUNT_USAGE` | Up to 45 min |

Mixing the two in one view would silently make the fast half as slow as the slow half.

### 4. Health must not depend on the thing most likely to be broken

`MONITORING.VW_PIPELINE_SLA` reads RAW tables, the load batch table, the stream queue and
`FCT_TRADE` — never a reporting mart. During an incident the marts are frequently the broken thing,
and a health view that fails to compile when the pipeline is unhealthy answers nothing.

### 5. An alert must carry its own diagnosis

Four separate RAG columns rather than one overall light, because "the platform is red" starts a
hunt whereas "capture is red, everything else green" points at the stream drain. Every Airflow
alert email carries the DAG, task, try number, the exception, a direct log link, and the runbook
anchor for that specific failure class. The aim is that the email alone is enough to decide whether
to act now or in the morning.

---

## The three layers

| Layer | Runs where | Detects | Survives Airflow down? |
| --- | --- | --- | --- |
| Snowflake views + alerts | Snowflake, on its own schedule | Absence, staleness, stalls, cost | Yes |
| dbt tests + scorecard | Inside the dbt run | Correctness and internal consistency | N/A |
| Airflow gates + callbacks | Airflow | Per-run quality, execution failures, latency | No |

The overlap is intentional. See [Why thresholds are duplicated](#why-thresholds-are-duplicated).

---

## Layer 1: Snowflake monitoring views

Ten views in `MONITORING`, defined in `snowflake/30_monitoring/` — four real-time, six lagging.

### Real-time — safe to use during an incident

#### `VW_PIPELINE_SLA` — the triage view

One row, four independent RAG columns. This is the first thing to read in any incident, and
`python scripts/run_sql.py --preset health` prints it.

| Column | AMBER | RED | Runbook |
| --- | --- | --- | --- |
| `ingestion_status` | No load for **> 45 min** | No load for **> 90 min**, or none ever | [File arrival delay](runbook.md#file-arrival-delay) |
| `capture_status` | No drain for **> 15 min** | Any stuck batch, or any failed drain in 24h | [Stream drain failures](runbook.md#stream-drain-failures) |
| `transform_status` | Backlog older than **60 min** | No dbt success for **> 180 min** | [dbt failures](runbook.md#dbt-failures) |
| `correctness_status` | — | `trades_overdue_for_expiry > 0` | [Expiry sweep failure](runbook.md#expiry-sweep-failure) |

`correctness_status` has no AMBER, deliberately. A matured trade still marked LIVE is either
correct or it is not; there is no degraded middle state, and offering one would invite ignoring it.

**Where the numbers come from.** The upstream feed is hourly. 45 minutes is under one cycle — early
enough to be actionable, late enough not to fire on ordinary jitter. 90 minutes is a full missed
cycle, which is unambiguous. Transform's 180 minutes is three missed hourly builds: a single failed
build is recoverable by the next scheduled run without waking anyone, and paging on the first one
trains people to ignore the alert.

A stuck batch is RED with no grace period because a `RUNNING` row older than 15 minutes is the
signature of a session that died mid-load — a failure that otherwise leaves no error anywhere at
all.

#### `VW_FILE_ARRIVAL`

Joins the stage directory listing against `RAW.TRADE_EVENT` to classify every file:

| `file_state` | Meaning |
| --- | --- |
| `STAGED_NOT_LOADED` | On the stage, nothing has ingested it |
| `LOADED_AND_ARCHIVED` | Loaded, then removed from the stage. Normal |

`is_stalled` is true when a file has been staged and unloaded for over **15 minutes**.

`expected_gap_minutes` is the **median observed gap over the last 7 days**, derived rather than
configured. A hard-coded "expect a file every 60 minutes" becomes wrong the moment the upstream
changes cadence, and a stale hard-coded expectation is worse than none because it produces
confident false alarms. Deriving it means the monitor adapts to a move from hourly to
every-fifteen-minutes without anyone editing a threshold.

#### `VW_BATCH_HEALTH`

Per-batch outcomes from `RAW.LOAD_BATCH`. `is_stuck` is `RUNNING` for over **15 minutes**. Duration
is compared against a trailing average over the preceding **20** batches, so a batch taking four
times as long as normal is visible before it fails — a rolling baseline rather than an absolute
limit, because absolute limits are wrong at both ends as volume changes.

#### `VW_STREAM_LAG`

Lag on `RAW.TRADE_EVENT_STREAM` against `staleness_limit_minutes` = **20160** (14 days, the source
table's Time Travel retention).

This is the one failure a retry cannot fix. Past the staleness limit the delta is unrecoverable
from the stream and the gap must be backfilled by hand — see
[Recreating the stream](runbook.md#recreating-the-stream). Selecting from a stream does not advance
its offset, so this view is free to query as often as you like.

### Lagging — `ACCOUNT_USAGE`, for cost and tuning only

| View | Reads | Purpose |
| --- | --- | --- |
| `VW_WAREHOUSE_CREDITS` | `warehouse_metering_history`, 90d | Credits by warehouse, week-on-week delta |
| `VW_SERVERLESS_CREDITS` | Serverless task, pipe, clustering history, 90d | The costs that do not appear against a warehouse |
| `VW_DBT_QUERY_PERFORMANCE` | `query_history`, 30d, filtered by query tag | Per-model cost and tuning signals |
| `VW_TASK_HISTORY` | `task_history`, 30d | Task outcomes, with `SKIPPED` classified as `IDLE_NO_DATA` |
| `VW_COPY_HISTORY` | `copy_history`, 30d | Load outcomes, including partial loads |
| `VW_STORAGE_GROWTH` | `table_storage_metrics` | Storage by table, including Time Travel and Fail-safe |

Two of these deserve expansion.

**`VW_COPY_HISTORY` and the partial-load problem.** `COPY` runs with `ON_ERROR = CONTINUE` so one
malformed line cannot block a whole file. The consequence is that a file which loaded 900 of 1000
rows reports **success**. This view derives `load_outcome = 'PARTIAL_LOAD'` whenever
`row_parsed > row_count`, which is the only way that loss becomes visible. Resilient ingestion
without this view is silent data loss with extra steps.

**`VW_DBT_QUERY_PERFORMANCE` and `tuning_signal`.** Attribution to a model is possible only because
every dbt statement carries `project=trade-pipeline` and its model name in the `QUERY_TAG`, set by
a pre-hook. Without the tag, `ACCOUNT_USAGE` can tell you what the warehouse cost but not what
caused it.

`tuning_signal` translates raw counters into an instruction, because
`bytes_spilled_to_remote_storage = 4200000000` is a number, not an action:

| Signal | Condition | Action |
| --- | --- | --- |
| `REMOTE_SPILL_SIZE_UP_WAREHOUSE` | Any remote spill | Query exceeded memory. Size up |
| `QUEUEING_ADD_CLUSTER` | Queued overload **> 30000 ms** | Concurrency, not query cost. Add a cluster |
| `FULL_SCAN_REVIEW_PRUNING` | **> 1000** partitions and **> 80%** scanned | Pruning is not working. Review clustering |
| `COMPILE_BOUND_SIMPLIFY_SQL` | Compile **> execute** and **> 5000 ms** | The SQL is too complex, not the data too large |
| `OK` | — | — |

The distinction between the first two is the one people get wrong most often, and they have
opposite remedies: spilling needs a bigger warehouse, queueing needs more clusters of the same
size. Sizing up to fix queueing costs more and fixes nothing.

---

## Layer 2: Snowflake alerts

Eight alerts in `MONITORING`, defined in `snowflake/40_alerts/02_alerts.sql`. All deliver email via
`SP_NOTIFY`, which wraps `SYSTEM$SEND_EMAIL` against the notification integration.

| Alert | Schedule | Fires when | Severity |
| --- | --- | --- | --- |
| `ALERT_TASK_FAILURE` | 5 min | Any task `FAILED` in the last 30 min | P2 |
| `ALERT_STUCK_BATCH` | 10 min | Any batch `RUNNING` **> 15 min** | P1 |
| `ALERT_INGESTION_STALL` | 15 min | `ingestion_status = 'RED'`, **business hours only** | P2 |
| `ALERT_TRANSFORM_BACKLOG` | 15 min | `transform_status` RED or AMBER **and** rows are waiting | P2 |
| `ALERT_REJECT_RATE_SPIKE` | 30 min | **≥ 100** events in 1h **and** reject rate **> 25%** | P2 |
| `ALERT_PARTIAL_LOAD` | 30 min | Any `RAW.COPY_ERROR` row in the last hour | P3 |
| `ALERT_EXPIRY_OVERDUE` | Daily 06:00 UTC | Any matured trade not EXPIRED or CANCELLED | P1 |
| `ALERT_CREDIT_BURN` | Daily 07:00 UTC | Today's credits exceed the daily budget (default **5**) | P3 |

### Notes on specific alerts

**`ALERT_INGESTION_STALL` is the only one with a business-hours guard** —
`dayofweekiso() <= 5` and hour between **6 and 22 UTC**. Missing data is expected outside market
hours, and an alert that fires every single weekend gets filtered to a folder nobody reads, taking
the weekday alerts with it. No other alert has this guard because every other condition indicates
something genuinely wrong regardless of the hour: a stuck batch at 3am on Sunday is still a stuck
batch.

**`ALERT_REJECT_RATE_SPIKE` requires ≥ 100 events before evaluating.** Without a minimum volume, 1
rejection out of 2 events is a 50% reject rate and pages someone at the start of every quiet
period. Rate-based alerts need a volume floor or they fire hardest when the least is happening.

**`ALERT_TRANSFORM_BACKLOG` requires both a status *and* rows waiting.** A stale transform with an
empty queue means there was nothing to transform, which is not a fault. Alerting on staleness alone
would fire through every quiet weekend.

**`ALERT_EXPIRY_OVERDUE` is daily, not every 15 minutes**, because the condition can only change
once a day — at the date boundary. 06:00 UTC puts it in front of European market open.

**`ALERT_STUCK_BATCH` is P1 despite sounding minor.** It means a session died mid-load, which is
the failure class that leaves no error behind anywhere.

---

## Layer 3: Airflow gates and callbacks

The DAG runs hourly (`0 * * * *`), `max_active_runs=1`, `catchup=False`, with a 2-hour
`dagrun_timeout`.

`max_active_runs=1` is a correctness requirement, not a resource one. Two concurrent runs merging
into `FCT_TRADE` would race on the version high-water mark, and
`assert_fct_trade_matches_version_ledger` is the test that catches it after the fact.

`catchup=False` because backfilling adjudication is meaningless: the rules evaluate against
*current* state, so replaying last Tuesday's run would re-adjudicate today's data with last
Tuesday's business date. Historical reprocessing is a deliberate separate operation — see
[Backfill and replay](runbook.md#backfill-and-replay).

### The four gates

Where each gate sits in the DAG is the design. Each can be relaxed for one run by an environment
variable, which is what makes an operator's override explicit and temporary rather than a code
change.

| Gate | Position | Blocks on | Env override |
| --- | --- | --- | --- |
| `gate_load_integrity` | After load, before transform | Any batch `RUNNING` **> 1 hour**; parse error rate **> 5%** | `DQ_MAX_PARSE_ERROR_RATE` |
| `check_source_freshness` | Before any model builds | dbt freshness `error` | — |
| `gate_reject_rate` | After adjudication, **before the marts** | **≥ 50** events **and** reject rate **> 25%** over 2h | `DQ_MAX_REJECT_RATE`, `DQ_MIN_EVENTS_FOR_GATE` |
| `gate_publish_readiness` | After the marts | `overdue_expiry_trades > 0`; scorecard `RED` | — |

**`gate_reject_rate`'s position is the most deliberate decision in the DAG.** Adjudication has
already run, so the rate is measurable and every rejected trade is already in the audit log for
investigation — but `FCT_TRADE` has not yet been touched, so a suspect batch can still be stopped
before anyone trades on it. Earlier there would be nothing to measure. Later the damage would be
done and the gate could only report it.

**Every gate warns before it blocks.** `gate_reject_rate` warns at **60% of its blocking
threshold** (15% at the default), sending a notification while letting the run proceed. A control
with only one setting is either too tight and gets disabled, or too loose and never fires.
`gate_load_integrity` similarly warns on any errored rows while only blocking above 5%.

**`check_source_freshness` exists as a separate task from `dbt_source_freshness`** because
`dbt source freshness` exits non-zero on a *warning* as well as an error. The dbt task therefore
appends `|| true` and this Python task parses `target/sources.json` and decides. Swallowing the
exit code without then inspecting the artifact would be the bug; inspecting it is the point.

### SLAs

| Task | SLA | A miss means |
| --- | --- | --- |
| `ingest.wait_for_files` | 45 min | Files are late. The sensor keeps waiting |
| `transform.dbt_run_adjudication` | 20 min | Adjudication is slowing |
| `transform.dbt_run_marts` | 25 min | Mart builds are slowing |

Only three tasks have SLAs. An SLA on every task produces noise that hides the three that matter.

The sensor's SLA is the important one conceptually: it fires **while the task is still running and
still correct**. There is no failure to catch, because waiting for a file that has not arrived is
exactly what the sensor is for. The SLA is the only mechanism that tells a human the wait has
become abnormal.

### Retries

Default `retries=3` with exponential backoff from 2 minutes, capped at 20. The overrides matter
more than the default:

| Task | Retries | Why |
| --- | --- | --- |
| `wait_for_files` | **0** | A sensor already retries by definition. Retrying it wraps a poll loop in a poll loop |
| `load_files`, `drain_stream` | 3 | Transient network and warehouse errors are common and genuinely transient |
| `dbt_test` | **1** | A failing test is a fact about the data. Retrying it is hoping the data changed |
| dbt model tasks | 2 | A retry can clear a warehouse timeout |
| `reconcile` | 1 | Same reasoning as `dbt_test` |

Retrying only helps where the failure is transient. Retrying a deterministic failure three times
turns a 30-second red task into a two-minute red task and delays the alert.

### Callbacks

| Callback | Delivers to |
| --- | --- |
| `on_failure` | Email, plus Slack **on the final attempt only** |
| `on_retry` | Log only |
| `on_sla_miss` | Email and Slack |
| `notify_dq_gate_breach` | Email and Slack, called directly by the gates |

Slack only on the final attempt is the point of separating `on_retry` from `on_failure`. A task
that fails twice and succeeds on the third attempt is a healthy pipeline absorbing a transient
error; posting all three attempts to a channel teaches people that the channel is noise.

`on_retry` logs and does not notify, so the history is available when investigating without being
pushed at anyone.

**Notification failures are swallowed deliberately.** If SMTP is misconfigured, a raising callback
would replace the original task failure with an SMTP traceback — the real error disappears and the
engineer debugs the mail server instead of the pipeline. Delivery problems are logged and
suppressed. The task's own status is the source of truth; alerting is a convenience on top and must
never be able to make things worse.

---

## The dbt scorecard

`REPORTING.RPT_DATA_QUALITY_SCORECARD` is a single row, rebuilt on every dbt run.

Two consumers read it and no other definition of curated-layer health exists:
`gate_publish_readiness` and the dashboard header. One definition means an on-call engineer never
faces a dashboard saying GREEN and a page saying RED and has to decide which to believe.

The Snowflake alerts deliberately do **not** read the scorecard, since it is a mart and would be
unavailable in exactly the incidents worth alerting on. They read `VW_PIPELINE_SLA` instead.

The two are not interchangeable and their headline verdicts are not comparable: this rollup reads
curated-layer facts, while `VW_PIPELINE_SLA` reads RAW-layer facts and reports four per-domain
statuses. Stale ingestion with a perfectly healthy curated layer is a normal state, not a
contradiction. The one condition both layers implement is the expiry canary, and
`assert_sla_thresholds_agree` asserts that those two independent implementations agree — and that
the canary is not masked by the first-match-wins ordering below.

### `overall_status`, in order — first match wins

| # | Condition | Verdict |
| --- | --- | --- |
| 1 | `overdue_expiry_trades > 0` | RED |
| 2 | `pending_events > 100000` | RED |
| 3 | `minutes_since_last_adjudication > 180` | RED |
| 4 | `reject_rate_24h_pct > 25` | RED |
| 5 | `pending_events > 10000` | AMBER |
| 6 | `minutes_since_last_adjudication > 90` | AMBER |
| 7 | `reject_rate_24h_pct > 15` | AMBER |
| 8 | `parse_errors_last_24h > 0` | AMBER |
| 9 | — | GREEN |

The ordering is the diagnosis. Because the first match wins, the RED conditions are listed in order
of how directly they indicate a broken pipeline, so the verdict identifies which problem to look at
rather than merely that there is one. `overdue_expiry_trades` is first for the reason in
[the validation logic](validation-logic.md#the-four-required-rules): it is the only condition that
positively proves no build has completed.

**Any parse error at all is AMBER**, not just a high rate. `ON_ERROR = CONTINUE` means a parse
error is data the platform received and did not store. That should never be routine, and a rate
threshold would make a low, constant level of loss invisible.

**`rules_never_fired` is reported but excluded from the rollup.** A declared rule with no recorded
hit is either genuinely never violated or silently broken, and a passing test suite looks identical
in both cases — but many rules legitimately never fire, so it cannot be an alarm. It is a prompt to
investigate, surfaced on the dashboard's rule catalogue.

---

## Cost monitoring

Cost is a reliability concern here, not a finance one: a Snowflake trial has finite credits, and an
exhausted trial is an outage.

### Resource monitors

Six monitors, `RM_TRADES_{workload}_{env}`, one per warehouse, MONTHLY quota. All share the same
triggers: **notify at 50/75/90%, suspend at 90%, suspend immediately at 100%**.

| Warehouse | dev quota | prod quota |
| --- | --- | --- |
| `WH_TRADES_LOAD` | 5 | 200 |
| `WH_TRADES_TRANSFORM` | 10 | 1000 |
| `WH_TRADES_BI` | 5 | 300 |

**One monitor per warehouse rather than one for the account.** An account-level monitor suspends
everything when any one workload misbehaves, so a runaway ad-hoc query in the BI warehouse takes
down ingestion. Per-warehouse monitors make the blast radius equal to the workload that caused it —
which is also the reason the three warehouses are separate in the first place.

**`suspend_trigger` at 90 and `suspend_immediate` at 100.** The 90% suspend lets running queries
finish and refuses new ones; 100% kills in flight. A single immediate suspend at 100% would abort a
long transform mid-write with no warning.

Dev quotas are deliberately tiny. A runaway loop in development on a trial account is the most
likely way to lose the environment, and 5 credits caps the damage at a few pounds.

### Attributing cost

Three views cover the three ways Snowflake bills, and missing any one gives a wrong total:

- `VW_WAREHOUSE_CREDITS` — warehouse compute, with week-on-week deltas.
- `VW_SERVERLESS_CREDITS` — Snowpipe, serverless tasks and automatic clustering, none of which
  appear against a warehouse. This is the line item that surprises people, since Snowpipe is billed
  per file and a chatty producer sending many small files can cost more than the data warrants.
- `VW_STORAGE_GROWTH` — storage including Time Travel and Fail-safe, which is why retention is 14
  days on RAW and 1 day on the rebuildable intermediate schema.

`ALERT_CREDIT_BURN` runs daily at 07:00 UTC against `SNOWFLAKE_DAILY_CREDIT_BUDGET` (default 5).
Daily rather than hourly: credit data lags in `ACCOUNT_USAGE` anyway, so a faster schedule would
only re-report the same figure.

---

## Threshold reference

Every threshold in one table, for changing one without hunting.

| Threshold | Value | Set in |
| --- | --- | --- |
| Ingestion AMBER / RED | 45 / 90 min | `VW_PIPELINE_SLA` |
| Transform RED | 180 min | `VW_PIPELINE_SLA` |
| Transform backlog AMBER | 60 min | `VW_PIPELINE_SLA` |
| Drain lag AMBER | 15 min | `VW_PIPELINE_SLA` |
| Stuck batch (alert, view) | 15 min | `VW_BATCH_HEALTH` |
| Stuck batch (Airflow gate) | 60 min | `trade_pipeline.py` |
| Stalled staged file | 15 min | `VW_FILE_ARRIVAL` |
| Stream staleness limit | 20160 min (14 d) | `VW_STREAM_LAG` |
| Max reject rate (gate) | 0.25 | `DQ_MAX_REJECT_RATE` |
| Reject-rate warning | 0.15 (60% of blocking) | `trade_pipeline.py` |
| Min events for reject gate | 50 | `DQ_MIN_EVENTS_FOR_GATE` |
| Max parse error rate | 0.05 | `DQ_MAX_PARSE_ERROR_RATE` |
| File delay / sensor timeout | 90 min | `DQ_MAX_FILE_DELAY_MINUTES` |
| Reject spike (Snowflake alert) | 25% over ≥ 100 events in 1h | `ALERT_REJECT_RATE_SPIKE` |
| Scorecard reject AMBER / RED | 15% / 25% | `rpt_data_quality_scorecard.sql` |
| Scorecard backlog AMBER / RED | 10000 / 100000 | `rpt_data_quality_scorecard.sql` |
| Scorecard staleness AMBER / RED | 90 / 180 min | `rpt_data_quality_scorecard.sql` |
| Daily credit budget | 5 | `SNOWFLAKE_DAILY_CREDIT_BUDGET` |
| Queue retention | 7 days | `TASK_PRUNE_TRADE_EVENT_QUEUE` |
| Loaded-file archival | 3 days | `TASK_ARCHIVE_LOADED_FILES` |

### Snowflake tasks

| Task | Schedule | Does |
| --- | --- | --- |
| `TASK_DRAIN_TRADE_EVENT_STREAM` | 1 min, gated on `SYSTEM$STREAM_HAS_DATA` | Drains the stream into the queue |
| `TASK_PRUNE_TRADE_EVENT_QUEUE` | After the drain | Retains 7 days of queue rows |
| `TASK_ARCHIVE_LOADED_FILES` | Daily 02:30 UTC | Removes stage files loaded over 3 days ago |

The drain task is gated on `SYSTEM$STREAM_HAS_DATA`, so a 1-minute schedule costs nothing when
there is nothing to do — the task evaluates its condition and skips without starting compute. This
is why `VW_TASK_HISTORY` classifies `SKIPPED` as `IDLE_NO_DATA` rather than as a problem: skipping
is the normal state.

`TASK_PRUNE_TRADE_EVENT_QUEUE` runs *after* the drain rather than on its own schedule, so pruning
can never run concurrently with the insert that fills the queue.

---

## Why thresholds are duplicated

The 90-minute ingestion SLA appears in `VW_PIPELINE_SLA`, in the dbt source freshness
configuration, and in the Airflow sensor timeout. The reject-rate threshold appears in the Airflow
gate, the Snowflake alert, and the dbt scorecard.

That duplication is deliberate, and it is a real trade-off rather than an oversight.

**Why duplicate.** Each layer must work when the others are down. If Airflow held the only
definition of "too stale", a dead scheduler would mean nothing is stale ever again — the monitor and
the monitored would fail together. Three independent detectors mean three independent chances to
notice, and when two of them agree the finding is corroborated by systems that share no code.

**What it costs.** They can drift. A threshold changed in one place and not the others produces the
worst possible outcome: a dashboard and a page that disagree, and an engineer who no longer trusts
either.

**How that cost is paid down.** `assert_sla_thresholds_agree` compares the dbt scorecard's
thresholds against `VW_PIPELINE_SLA`'s and fails if they diverge. The duplication survives; the
divergence does not. A test is a cheaper way to keep three copies honest than a shared
configuration layer would be, and it does not reintroduce the single point of failure that the
duplication exists to avoid.

---

## What is not monitored, and why

Knowing the gaps is part of operating the platform, and every one of these is a decision rather
than an omission.

**Row-level lineage.** No record of which specific input row produced which output row beyond
`EVENT_SK`. At this volume the surrogate key is sufficient to trace an event through every layer;
full column-level lineage would be a large amount of machinery for a question nobody has asked.

**Distribution drift.** Reject *rate* is monitored; the shape of the data is not. A batch where
every notional is exactly 1,000,000 would pass every check. Detecting that needs statistical
profiling — `dbt-expectations` is already a dependency and would be where to start — but it needs a
baseline of normal, which a fresh platform does not have. Adding it before there is history to
compare against would produce alarms with no meaning.

**Business reasonableness.** The platform validates that a trade is *well-formed*, not that it is
*sensible*. A 500-billion notional in a valid currency for an active counterparty passes
everything. `RJ018` catches it only if the desk has a limit configured. Genuine reasonableness
checks need a risk model, which is a different system.

**End-to-end latency per trade.** Stage latency is measured; a single trade's journey is not
timestamped through every hop. Adding per-event timing to the hot path costs more than the question
is currently worth.

**Snowflake's own availability.** If Snowflake is down, every one of these monitors is down with
it, since they all run inside Snowflake. This is the accepted single point of failure of the
architecture — the trade-off taken by choosing a Snowflake-native design, and the reason
`AUDIT.DBT_RUN_RESULT` exists as an in-warehouse record rather than relying solely on Airflow's
metadata database. External uptime monitoring would be the mitigation, and it is out of scope for a
local deployment.
