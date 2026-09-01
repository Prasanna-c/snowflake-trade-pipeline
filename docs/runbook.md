# Runbook

Operational procedures for the trade lifecycle pipeline. Every failure alert this platform
emits links to a section here, and `scripts/selfcheck.py` fails CI if a link points at a
heading that does not exist — so this document cannot silently drift from the code that cites it.

**Read this first if you have been paged.** Go to [Triage](#triage), spend one minute there, then
jump to the section it sends you to.

---

## Contents

- [Triage](#triage)
- [Recovery principles](#recovery-principles)
- [Preflight failures](#preflight-failures)
- [File arrival delay](#file-arrival-delay)
- [Load failures](#load-failures)
- [Stream drain failures](#stream-drain-failures)
- [Source freshness failure](#source-freshness-failure)
- [Data quality gate tripped](#data-quality-gate-tripped)
- [A dbt command on the host fails with PermissionError](#a-dbt-command-on-the-host-fails-with-permissionerror)
- [dbt failures](#dbt-failures)
- [dbt test failures](#dbt-test-failures)
- [Snapshot failures](#snapshot-failures)
- [Reconciliation mismatch](#reconciliation-mismatch)
- [Expiry sweep failure](#expiry-sweep-failure)
- [Platform health RED](#platform-health-red)
- [SLA miss](#sla-miss)
- [Recreating the stream](#recreating-the-stream)
- [Backfill and replay](#backfill-and-replay)
- [The DAG is listed but cannot be triggered](#the-dag-is-listed-but-cannot-be-triggered)
- [Escalation](#escalation)

---

## Triage

One command tells you which stage is at fault:

```bash
make sql Q="select * from \$SNOWFLAKE_DATABASE.monitoring.vw_pipeline_sla"
# or, equivalently and shorter:
python scripts/run_sql.py --preset health
```

The view returns four independent RAG columns rather than one overall light, which is the whole
point of it: "the platform is red" starts a hunt, whereas "capture is red and everything else is
green" points at the stream drain immediately.

| Column | RED means | Go to |
| --- | --- | --- |
| `ingestion_status` | No file has loaded for over 90 minutes | [File arrival delay](#file-arrival-delay) |
| `capture_status` | A batch is stuck, or a drain failed in the last 24h | [Stream drain failures](#stream-drain-failures) |
| `transform_status` | dbt has not succeeded for over 180 minutes | [dbt failures](#dbt-failures) |
| `correctness_status` | A matured trade is still marked LIVE | [Expiry sweep failure](#expiry-sweep-failure) |

`VW_PIPELINE_SLA` reads project tables and the stream queue only — never a dbt mart. That is
deliberate: during an incident the marts are frequently the thing that is broken, and a health
view that depends on them answers nothing exactly when you need it. It is also real-time, unlike
anything reading `ACCOUNT_USAGE`.

If the view itself will not read, the Snowflake layer is not deployed or your role cannot see it.
Run `make doctor`.

### Was it the run, or the data?

`VW_PIPELINE_SLA` describes the state of the *data*. It cannot tell you whether the orchestrator
ran at all, and the two failure modes look identical from the warehouse: a DAG that failed at
`load_files` and a DAG that was never unpaused both present as `ingestion_status` RED with no
error anywhere. So ask the scheduler as well:

```bash
make airflow-status     # verdict on the latest run, task by task; exits non-zero if it failed
```

It exits 0 for success, 1 for a failed run, and 2 when there is no verdict yet — queued, still
running, or never triggered — because "not finished" and "finished badly" call for different
responses. It reports skipped tasks explicitly rather than folding them into a green rollup,
since a skipped `simulate_arrivals` is correct when `PIPELINE_SIMULATE_ARRIVALS=false` and a hole
in the run when it is not.

### The dashboard as an alternative

`make dashboard` opens the same four badges plus the drill-downs, at `http://localhost:8501`. The
**Pipeline health** page is laid out in pipeline order — file arrival, load batches, change
capture, transform, parse errors, cost — so reading top to bottom is itself the triage.

---

## Recovery principles

Four properties hold across this pipeline. Knowing them is what makes most of the procedures
below safe to run without thinking hard, and it is worth reading once before you need it.

**1. Every step is idempotent, so re-running is always the first thing to try.**

- `RAW.TRADE_EVENT` is insert-only and never updated.
- `COPY INTO` consults Snowflake's load history and skips files it has already ingested, so a
  repeated COPY cannot double-load.
- The stream drain is a single transaction: either the rows land in the queue *and* the batch is
  recorded *and* the stream offset advances, or none of it happens.
- Every incremental dbt model merges on a surrogate key rather than appending.

So the answer to "did my retry make it worse?" is no, and you do not need to establish what the
previous attempt managed to complete before you retry it.

**2. Nothing is deleted, so there is always evidence.**

Rejected events keep their original bytes in `AUDIT.FCT_TRADE_REJECTED.raw_payload`. Lines that
failed to parse — which never reached the rule engine at all — are in `RAW.COPY_ERROR` with the
raw line. `AUDIT.TRADE_RULE_RESULT` records every rule hit including non-blocking warnings. You
can reconstruct what happened without going back to the source system.

**3. A blocked pipeline is the designed behaviour, not a fault to work around.**

When a data quality gate trips, the marts are deliberately left un-refreshed. The golden record
still holds the last known-good state. Resist the urge to force the run through: publishing
suspect data to `FCT_TRADE` is much harder to undo than a delayed refresh, because downstream
consumers will have read it.

**4. Snowflake monitoring keeps working when Airflow is down.**

dbt writes its own run outcomes to `AUDIT.DBT_RUN_RESULT` from an `on-run-end` hook, and the
Snowflake alerts read that table. So a stale curated layer is detected even if the machine
running Airflow has died — which is precisely the situation in which nobody is watching the
Airflow UI.

---

## Preflight failures

**Alert:** `preflight` task failed.
**Meaning:** The pipeline could not confirm the platform is deployable-into. Nothing has run yet,
so nothing is in an odd state.

This task exists so that a missing deployment fails in seconds with a clear message, rather than
twenty minutes later inside dbt with a "table does not exist" error that reads like a modelling
bug.

### Diagnose

```bash
make doctor
```

### Common causes

**"The Snowflake ingestion layer is not deployed. Missing objects: ..."**

The RAW tables do not exist. Deploy them:

```bash
make deploy-sql-plan   # see what would be applied, connects to nothing
make deploy-sql
```

**Connection failure.** `make doctor` distinguishes the cases in order — DNS, then credentials,
then role, then warehouse — because they produce nearly identical driver errors. The most common
by a distance is a public key that was generated but never registered:

```sql
ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of .secrets/rsa_key.pub, newlines stripped>';
```

**"No active warehouse selected."** The role can *see* the warehouse but has no `USAGE` on it.
Visibility is not usage. `make tf-apply`, or grant it directly.

### Resolve

Fix the cause, then clear the task in Airflow. No cleanup is needed — preflight has no side
effects.

---

## File arrival delay

**Alert:** `wait_for_files` SLA miss, or `ALERT_INGESTION_STALL`, or `ingestion_status` is RED.
**Meaning:** No new data has arrived within the 90-minute SLA.

Absence produces no event, which is why this is monitored by comparing the stage against loaded
rows rather than by watching for errors. There is nothing to catch.

### Diagnose

```bash
python scripts/run_sql.py --preset arrivals
```

`file_state` separates three genuinely different problems:

| `file_state` | Meaning | Action |
| --- | --- | --- |
| `STAGED_NOT_LOADED` with `is_stalled` | The file is sitting on the stage and nothing picked it up | Snowpipe or COPY problem — see below |
| `LOADED_AND_ARCHIVED` | Loaded, then removed from the stage | Normal. Not your problem |
| No rows at all recently | The file never arrived | Upstream problem — see below |

`expected_gap_minutes` is the median observed gap over the last seven days, derived rather than
hard-coded, so the monitor keeps working when the upstream changes cadence from hourly to every
fifteen minutes.

### If the file never arrived

This is an upstream problem and the pipeline is behaving correctly by waiting. The sensor keeps
waiting because the file may still be coming; the SLA callback is the part that tells a human it
is late. Contact the source system owner. Do not clear the sensor — it will simply wait again.

If you need to confirm the pipeline itself is healthy while you wait, generate a batch locally:

```bash
make demo
```

### If the file is staged but not loading

Snowpipe has stalled or the COPY step is not running.

```bash
python scripts/run_sql.py "select system\$pipe_status('\$SNOWFLAKE_DATABASE.raw.pipe_trade_event')"
```

A `executionState` other than `RUNNING` needs a resume:

```sql
ALTER PIPE RAW.PIPE_TRADE_EVENT SET PIPE_EXECUTION_PAUSED = FALSE;
```

`pendingFileCount` climbing with `RUNNING` state usually means the file format no longer matches
the data. Check `RAW.COPY_ERROR` — see [Load failures](#load-failures).

To load immediately without waiting for Snowpipe:

```bash
python -m trade_sim.cli load --skip-generate
```

---

## Load failures

**Alert:** `load_files` failed, `gate_load_integrity` tripped, or `ALERT_PARTIAL_LOAD`.
**Meaning:** Rows were lost or refused during ingestion.

`COPY` runs with `ON_ERROR = CONTINUE` so one malformed line cannot block an entire file. That
resilience is also exactly what makes loss invisible, which is why every rejected line is
captured and the loss is measured deliberately rather than assumed to be zero.

### Diagnose

```bash
python scripts/run_sql.py --preset parse-errors
python scripts/run_sql.py --preset batches
```

### "Parse error rate N% exceeds the 5% threshold"

Inspect `RAW.COPY_ERROR.rejected_record`, which holds the raw bytes of each refused line. In
practice this is nearly always one of:

- The upstream changed serialisation — a date format, a number rendered as a string, a new
  enclosing array.
- An encoding change, typically a UTF-8 BOM appearing on the first line.
- Truncation, where the producer wrote a partial file. The writer in this repo renames from a
  temporary file specifically to prevent this; an external producer may not.

The rejected rows are recoverable. Fix the upstream, and the corrected file will load normally —
Snowflake's load history is keyed on the file name and its checksum, so a re-sent corrected file
under the same name is *not* skipped.

### "N batch(es) have been RUNNING for over an hour"

A previous run died mid-load, leaving a `RAW.LOAD_BATCH` row that was never closed. That row is
the signature of a crashed session, and it is the only way to distinguish "still working" from
"died silently" — a failure that otherwise leaves no error anywhere.

The data itself is fine: the batch row is metadata. Close the stale rows:

```sql
UPDATE RAW.LOAD_BATCH
   SET batch_status = 'FAILED',
       completed_at = CURRENT_TIMESTAMP(),
       error_message = 'closed manually: session died, see runbook#load-failures'
 WHERE batch_status = 'RUNNING'
   AND started_at < DATEADD('hour', -1, CURRENT_TIMESTAMP());
```

Then clear the task. The COPY will skip whatever already loaded.

### Verifying nothing was lost

```bash
python scripts/run_sql.py "
  select * from \$SNOWFLAKE_DATABASE.monitoring.vw_copy_history
  where rows_parsed <> rows_loaded"
```

Any row here is a partial load: a file where Snowflake parsed more rows than it loaded. This view
exists because `ON_ERROR = CONTINUE` reports success on a partially loaded file.

---

## Stream drain failures

**Alert:** `drain_stream` failed, `capture_status` is RED, or `ALERT_STUCK_BATCH` /
`ALERT_TASK_FAILURE`.
**Meaning:** Change capture is not moving rows from `RAW.TRADE_EVENT` into the queue dbt reads.

### Diagnose

```bash
python scripts/run_sql.py --preset backlog
python scripts/run_sql.py --preset tasks
```

### The reassuring part

`SP_DRAIN_TRADE_EVENT_STREAM` is transactional. The insert that consumes the stream and the
update that closes the batch are one transaction, so a failed drain has left the stream offset
exactly where it was. Nothing was half-consumed, and nothing was lost. Re-running is safe and is
the correct first action:

```bash
make drain
```

Selecting from a stream does not advance its offset — only DML that consumes it does — so you can
inspect `VW_STREAM_LAG` as often as you like without side effects.

### If the scheduled task is failing rather than the Airflow step

```sql
SHOW TASKS IN SCHEMA RAW;
ALTER TASK RAW.TASK_DRAIN_TRADE_EVENT_STREAM RESUME;
```

A task tree that was recreated comes back **suspended**, which is a Snowflake default that
catches everyone at least once. `--preset tasks` reads
`INFORMATION_SCHEMA.TASK_HISTORY` (real-time, 7 days), not `ACCOUNT_USAGE` (45-minute lag), so it
is usable during an incident.

### If the stream has gone stale

A stream becomes stale once the source table's Time Travel retention elapses without the stream
being consumed. Past that point the delta is **unrecoverable from the stream** — this is the one
failure in this pipeline that a retry cannot fix.

```bash
python scripts/run_sql.py "select * from \$SNOWFLAKE_DATABASE.monitoring.vw_stream_lag"
```

If `lag_minutes` is approaching `staleness_limit_minutes`, drain now. If it has already passed,
go to [Recreating the stream](#recreating-the-stream).

---

## Source freshness failure

**Alert:** `check_source_freshness` failed.
**Meaning:** dbt's freshness check found `RAW.TRADE_EVENT_QUEUE` older than its SLA.

This runs *before* any model is built, because a stale source means the pipeline is being asked
to transform data that never arrived — and finding that out first is far cheaper than finding it
out after twenty minutes of modelling.

### Diagnose

```bash
make dbt-freshness
```

The freshness thresholds in `dbt/models/staging/_staging__sources.yml` deliberately match the
monitoring SLAs, so a freshness error and a RED `ingestion_status` are the same fact reported by
two independent systems. If they disagree, one of them is broken — and that is worth
investigating on its own.

### Resolve

A freshness **error** is almost always an upstream delay, so treat it as
[File arrival delay](#file-arrival-delay).

A freshness **warning** does not stop the pipeline. `dbt source freshness` exits non-zero on a
warning as well as an error, which is why the Airflow task appends `|| true` and a separate
Python task parses `target/sources.json` and decides. Swallowing the exit code *without* then
inspecting the artifact would be the bug; inspecting it is the point.

---

## Data quality gate tripped

**Alert:** `gate_reject_rate`, `gate_load_integrity` or `gate_publish_readiness` failed.
**Meaning:** The pipeline stopped on purpose. **The golden record has not been updated.**

This is the pipeline working. Three gates sit at three different points, and where each sits is
the design:

| Gate | Position | Question it answers |
| --- | --- | --- |
| `gate_load_integrity` | After load, before transform | Did ingestion lose rows? |
| `gate_reject_rate` | After adjudication, before the marts | Is this batch plausible? |
| `gate_publish_readiness` | After the marts | Is the platform fit to publish? |

`gate_reject_rate`'s position is the most deliberate. Adjudication has already happened, so the
rate is measurable and every rejected trade is already in the audit log for investigation — but
the golden record has not yet been touched, so a suspect batch can still be stopped before anyone
trades on it. Earlier there would be nothing to measure; later the damage would be done and the
gate could only report it.

### Diagnose

```bash
python scripts/run_sql.py --preset rejects
```

Then open the **Rejections** page of `make dashboard`. Three questions have to be answered before
you can act, and the page is built around them:

**1. Which rule is firing?** The rule leaderboard.

**2. Is it one upstream system or all of them?** The concentration analysis. `is_concentrated`
means over 80% of hits came from a single source system — a release broke that feed. Spread
evenly across sources means *our* reference data or the rule itself is wrong. Opposite responses,
and completely invisible from a total count.

**3. What did the message look like?** Select a row on the **Rejected events** tab to see
`raw_payload` as it arrived. This is why the payload is retained.

### Decide

**A genuine upstream data problem.** Leave the gate tripped. The golden record correctly holds
the last good state. Escalate to the source system owner. When corrected data arrives, the
pipeline processes it normally with no intervention.

**Expected behaviour that the threshold does not account for** — a migration replaying history,
a deliberate bulk amendment. Raise the threshold for this run only:

```bash
DQ_MAX_REJECT_RATE=0.60 make dbt-build-incremental
```

Then put the threshold back. A permanently raised gate is a gate that has stopped working.

**A bug in a rule.** The rule is rejecting valid trades. Fix the rule in
`dbt/macros/rules/trade_validation_rules.sql`, add a unit test that proves the case it got wrong
(`dbt/models/intermediate/_int_trade_event_adjudicated__unit_tests.yml`), update
`dbt/seeds/ref_rejection_reason.csv` if the description changed, and re-run. Previously rejected
events are **not** automatically reconsidered — see [Backfill and replay](#backfill-and-replay).

### Note on SUPERSEDED

`SUPERSEDED` is excluded from the reject-rate numerator. It means a same-version resend was
replaced by a later arrival, which is business rule 2 working correctly. Folding it in would make
the gate fire on healthy amendment traffic.

---

## A dbt command on the host fails with PermissionError

**Symptom:** any `make dbt-*` target stops before reading a model:

```
[Errno 13] Permission denied: '.../dbt/logs/dbt.log'
```

**Meaning:** something else wrote that file as a different user. The Airflow containers mount
`dbt/` from the host, so a DAG run used to leave `dbt/logs` and `dbt/target` owned by the
container's user, and the host account cannot then open them. Compose now points `DBT_LOG_PATH`
and `DBT_TARGET_PATH` at a named volume so the two writers no longer share these directories, but
a project that has already run against the older compose file still has the files on disk.

**Fix.** Both directories are regenerated on demand, so deleting them is safe and is the whole
repair:

```bash
sudo rm -rf dbt/logs dbt/target
make dbt-parse
```

Then confirm the containers will not recreate the problem. `AIRFLOW_UID` must be *your* user id, not
the image's 50000, or the same collision returns through `dbt_packages` and `data/`:

```bash
make airflow-uid                          # writes id -u into both env files
grep -n '^AIRFLOW_UID' .env airflow/.env  # confirm, and compare with id -u
```

`make airflow-up` runs that first, so a normal start cannot get it wrong; the check exists for a
Compose command driven by hand. Setting the variable does not retroactively fix files an earlier
start already reassigned, so take those back too:

```bash
sudo chown -R "$(id -u)" data dbt/dbt_packages
```

## dbt failures

**Alert:** `dbt_seed`, `dbt_run_staging`, `dbt_run_adjudication` or `dbt_run_marts` failed.
**Meaning:** A model did not build. The curated layer is stale but internally consistent — dbt
builds each model in its own transaction, so there is no half-built table.

The Airflow DAG splits dbt across several tasks rather than running one `dbt build`. One task
would be simpler and would also mean a failure told you only "dbt failed". Splitting by layer
means the Airflow graph itself localises the fault, and a retry resumes from the failed layer
instead of the beginning.

### Diagnose

Which layer failed narrows it immediately:

| Failed task | Meaning |
| --- | --- |
| `dbt_seed` | Reference data would not load. Usually a CSV type change |
| `dbt_run_staging` | Casting or typing. Usually an upstream schema change |
| `dbt_run_adjudication` | **The rules did not run.** The most serious of the four |
| `dbt_run_marts` | The rules ran; a report is stale |

```bash
python scripts/run_sql.py --preset dbt-runs
```

Reading dbt's outcome from Snowflake rather than from Airflow is what lets you diagnose this when
Airflow itself is the thing that is down.

### Common causes

**"Compilation Error: model not found"** — a `ref()` naming a model that does not exist. Catch
this before it reaches a warehouse:

```bash
make dbt-parse
```

**"Database Error: invalid identifier"** — a column referenced that the upstream model no longer
produces. Usually a rename applied in one place and not the other.

**A statement timeout on `int_trade_event_adjudicated`** — the adjudication model does the most
work. It has a 30-minute execution timeout and a 20-minute SLA. If it is legitimately growing
past that, see [`docs/scalability.md`](scalability.md); the first lever is warehouse size, and the
model is routed to the transform warehouse precisely so it can be sized independently.

**"Object does not exist"** on the first ever run — the RAW layer is not deployed. Run
`make deploy-sql`.

### Resolve

```bash
# Rebuild only the failed model and everything downstream of it.
cd dbt && dbt build --target dev --select int_trade_event_adjudicated+
```

The `+` suffix matters: rebuilding a model without its descendants leaves the marts inconsistent
with it, and the singular tests will then fail for a reason that looks unrelated.

Then clear the Airflow task. Incremental models merge, so a partial previous attempt cannot
produce duplicates.

---

## dbt test failures

**Alert:** `dbt_test` failed.
**Meaning:** The models built, but an invariant does not hold. This is a *correctness* alarm, not
an availability one, and it is more serious than a build failure: the data is present and wrong,
which is worse than absent.

Tests run *after* the marts and *in addition to* the gates, because they answer a different
question. The gates ask "is the incoming data plausible"; the tests ask "is what we built
internally consistent".

### Diagnose

Failing rows are persisted, not merely counted, because `--store-failures` is set:

```sql
SHOW TABLES IN SCHEMA DBT_TEST_FAILURES;
SELECT * FROM DBT_TEST_FAILURES.<test_name> LIMIT 100;
```

That is the difference between a debuggable failure and a rerun.

### The singular tests, and what each one failing means

These are cross-model invariants, and each was written because its violation is otherwise
invisible.

**`assert_no_event_is_silently_dropped`** — the most important test in the project. Every event
that entered the queue must reach exactly one of three destinations: accepted, rejected, or
superseded. A failure means **silent data loss**, which is the worst outcome in a regulated
pipeline and the hardest to notice. Treat as P1. Do not clear and re-run; find the event.

```sql
SELECT * FROM DBT_TEST_FAILURES.assert_no_event_is_silently_dropped;
```

**`adjudicated_one_accepted_event_per_trade_version`** — two accepted events share a
`(trade_id, trade_version)`. Business rule 2 allows a same-version resend, but only one arrival may
survive; the rest must be SUPERSEDED. Within a single build that is handled by `intra_run_rank`, so
a failure here almost always means the same version was accepted in *two different* builds.

On a demo environment the usual cause is that the generator's trade book was deleted while
Snowflake kept the matching history — the trade book restarts at `TRD-000000001` and replays
identities the warehouse has already accepted. `trade-sim reset-book` warns about this; deleting
`data/state/` by hand has the same effect without the warning.

Recovery is `make dbt-rebuild`, which reprocesses the whole queue in one run so the collision falls
inside a single build, where rule 2 resolves it. On a real feed, treat it as a rule 2 defect instead
and find the two builds:

```sql
SELECT trade_id, trade_version, COUNT(*), COUNT(DISTINCT batch_id), MIN(adjudicated_at), MAX(adjudicated_at)
FROM INTERMEDIATE.INT_TRADE_EVENT_ADJUDICATED
WHERE verdict = 'ACCEPTED' AND trade_id IS NOT NULL
GROUP BY 1, 2 HAVING COUNT(*) > 1;
```

**`assert_fct_trade_matches_version_ledger`** — the golden record has diverged from the version
ledger. `FCT_TRADE` must be exactly the maximum-version projection of `FCT_TRADE_VERSION`. A
failure usually means two writers ran concurrently, which is what `max_active_runs = 1` exists to
prevent. Check whether a manual `dbt build` overlapped a scheduled run.

**`assert_version_history_has_no_gaps`** — an accepted trade jumps from version 2 to version 4.
Sometimes legitimate (the upstream never sent 3), often data loss. Cross-check against
`AUDIT.FCT_TRADE_REJECTED` for the missing version: if it was rejected, the gap is explained.

**`assert_rule_catalogue_matches_macro`** — the executable rules and the human-readable seed have
drifted. Someone added a rule to one and not the other. `make selfcheck` catches this offline in
seconds; this test is the warehouse-side backstop.

**`assert_sla_thresholds_agree`** — the dbt scorecard and `MONITORING.VW_PIPELINE_SLA` no longer
agree on the expiry canary, the one condition both implement. Either the condition was changed in
one layer and not the other, or the scorecard's rollup was reordered so a softer condition now
masks it. Fix both layers; the failure row names which of the two it is. Note that the two
headline verdicts are *not* expected to match — they measure different layers.

**`assert_snapshot_covers_material_columns`** — `snp_trade`'s `check_cols` list no longer covers
every material column of `fct_trade`, so a change to the uncovered column would not be
historised. Silent incompleteness in an audit trail. Either add the column to the list in
`dbt/macros/snapshots/trade_snapshot_check_cols.sql`, or add it to the test's exclusion list with
the reason it does not need historising.

### `unique_int_trade_event_adjudicated_event_sk`

`EVENT_SK` is an IDENTITY column on `RAW.TRADE_EVENT`, so a duplicate is never a duplicate *event* —
it is the same physical row adjudicated twice. Before suspecting the queue, note that
`RAW.TRADE_EVENT_QUEUE.EVENT_SK` carries its own `unique` source test, and that source tests are
included in `make dbt-build-incremental` (only seeds are excluded). If that test passed in the same
run, delivery is not the problem.

Ask how many writes produced the copies:

```sql
SELECT event_sk, COUNT(1) AS copies, COUNT(DISTINCT adjudicated_at) AS writes,
       MIN(adjudicated_at) AS first_write, MAX(adjudicated_at) AS last_write
FROM <dev_schema>_INTERMEDIATE.INT_TRADE_EVENT_ADJUDICATED
GROUP BY event_sk HAVING COUNT(1) > 1
ORDER BY copies DESC LIMIT 5;
```

Two distinct `adjudicated_at` values seconds apart means **two dbt runs merged at once** — almost
always the hourly DAG overlapping a manual `make demo`. A merge deduplicates against committed rows
only, so both runs found the key absent and both inserted it. Confirm with the callers:

```sql
SELECT batch_type, batch_status, orchestrator_run_id, started_at, row_count
FROM RAW.LOAD_BATCH ORDER BY started_at DESC LIMIT 15;
```

A single `adjudicated_at` means one run produced both rows, which is a fan-out: check the reference
seeds for a duplicated key, since `dbt-build-incremental` excludes exactly those tests
(`dbt test --select resource_type:seed`).

Repair with `make dbt-rebuild`. The duplicates cannot be merged away, because the merge key is what
is duplicated. Prevent it with `make pause-writers` before any manual run — see
docs/known-limitations.md#one-environment-one-writer.

### Resolve

Fix the cause, not the test. If a test is genuinely wrong, change it in the same commit as a
comment explaining why — a threshold quietly relaxed to make CI green is how a control stops
being a control.

---

## Snapshot failures

**Alert:** `dbt_snapshot` failed.
**Meaning:** SCD2 history was not captured for this run.

The snapshot runs **after** the tests, deliberately. Snapshotting data that has just failed its
tests writes a bad state into immutable history, and SCD2 history cannot be tidied up afterwards
without destroying the audit trail that is its entire purpose.

### Why the snapshot exists at all, given `FCT_TRADE_VERSION`

The version ledger records every accepted *event*. But some state transitions have no
corresponding event — most importantly the expiry sweep, where a trade moves from LIVE to EXPIRED
because a date passed, not because anyone sent a message. `snp_trade` is what captures those, so
"what did this trade look like on 3 March" is answerable.

### Diagnose and resolve

```bash
make dbt-snapshot
```

**"Snapshot target has missing columns"** — a column was added to `fct_trade` after the snapshot
table was created. dbt does not automatically evolve a snapshot's schema. Add the column
explicitly:

```sql
ALTER TABLE SNAPSHOTS.SNP_TRADE ADD COLUMN <name> <type>;
```

Then add it to the list in `dbt/macros/snapshots/trade_snapshot_check_cols.sql`, or
`assert_snapshot_covers_material_columns` will fail on the next test run. The snapshot and that
test both read the macro, so the column only needs adding once.

**A missed run is recoverable but lossy.** Re-running the snapshot now captures *current* state.
Intermediate states between the missed run and now are gone, because the snapshot samples rather
than streams. If the gap matters, `FCT_TRADE_VERSION` still holds every accepted event, and
Time Travel on `FCT_TRADE` covers the retention window:

```sql
SELECT * FROM CORE.FCT_TRADE AT(TIMESTAMP => '2026-03-03 12:00:00'::timestamp_ltz);
```

---

## Reconciliation mismatch

**Alert:** `reconcile` failed.
**Meaning:** The pipeline reached a different verdict than the injected faults required, or events
are missing entirely.

This is the check that turns "the DAG went green" into "the pipeline was correct". Every other
check validates rows that are *present*; reconciliation is the only one that can detect an event
which is *absent*.

It works by comparing the generator's `BatchManifest` — which records, for every event it wrote,
the verdict that event must receive and the rule that must fire — against
`INTERMEDIATE.INT_TRADE_EVENT_ADJUDICATED`. Because the generator knows the ground truth, a
mismatch is unambiguous.

### Diagnose

```bash
make reconcile
```

Three discrepancy kinds, and they mean quite different things:

**`MISSING_EVENT`** — an event in the manifest has no verdict at all. **Silent data loss.** Treat
as P1. Work backwards: is it in `RAW.TRADE_EVENT`? If not, the load lost it — check
`RAW.COPY_ERROR`. If it is there but not in the queue, the drain lost it. If it is in the queue
but not adjudicated, the transform lost it.

**`WRONG_VERDICT`** — the event was adjudicated, but accepted when it should have been rejected or
the reverse. A rule logic bug. The manifest names the expected rule, so this points at a specific
rule. Reproduce it as a dbt unit test before changing anything: `make dbt-unit-test` runs against
mock data and needs no warehouse, so the loop is seconds rather than minutes.

**`MISSING_RULE_CODE`** — the verdict was right but the reason was not. Less urgent than a wrong
verdict, and still a real defect: the audit trail would attribute the rejection to the wrong rule,
and someone would investigate the wrong upstream system.

### Note on unmatchable faults

Some injected faults destroy the trade identifier itself — `unparseable_json` is the obvious case.
Those events cannot be matched by `trade_id` and the reconciler excludes them by design, checking
instead that they appear in `RAW.COPY_ERROR`. This is not a gap; it is the only correct way to
verify a fault whose whole nature is that the record became unidentifiable.

### Note on replayed trade universes

A mass failure — most events wrong, dominated by `expected ACCEPTED, got SUPERSEDED` and
`got REJECTED` on events with no injected fault — means one of two things, and the count of
distinct trade IDs tells you which. If the failing IDs restart from `TRD-000000001`, the warehouse
holds more than one simulated universe, as below. If they do not, the manifest is wrong rather than
the pipeline: the expectations depend on business-time ordering and on rule 2's race, both of which
`TestManifestAgreesWithArbitration` in `ingestion/tests/test_generator.py` asserts offline. Run
`make pytest` first — it is a great deal faster than a warehouse round trip, and it has caught every
instance of this so far. Generating from an empty trade book always starts at `TRD-000000001`, so two
runs from a deleted or missing `data/state/trade_book.json` mint the same identifiers. The pipeline
is then right and the manifest is right: the second arrival of `TRD-000000001 v1` genuinely is a
duplicate version, and business rules 1 and 2 genuinely do reject or supersede it.

A batch reference beginning `af` is an Airflow-generated batch, and two universes in one warehouse
usually means the hourly DAG generated its own while you were running `make demo`. The trade book is
locked now, so concurrent simulators extend one universe rather than forking it — but a book deleted
between the two runs still restarts at `TRD-000000001`, and the lock cannot know that. `make
pause-writers` is the guard; see docs/known-limitations.md#one-environment-one-writer.

Reconciliation scopes each manifest to the events loaded from that batch's own files, so the
batches do not contaminate each other's *comparison* — but they still share one trade book inside
the warehouse, so the later universe's verdicts cannot match its manifest. There is no repair,
only a clean slate:

```sql
-- In Snowsight, as the deployment role. Removes every loaded event.
TRUNCATE TABLE RAW.TRADE_EVENT;
TRUNCATE TABLE RAW.TRADE_EVENT_QUEUE;
TRUNCATE TABLE RAW.COPY_ERROR;
TRUNCATE TABLE RAW.LOAD_BATCH;
REMOVE @RAW.TRADE_LANDING;
```

```bash
# The generator's memory and its output have to go with it, or the next run replays
# identities the warehouse has just forgotten.
rm -rf data/landing data/manifests data/state
make dbt-rebuild   # reprocess from an empty queue, so no stale verdicts survive
make demo          # one universe, generated and reconciled in a single pass
```

Truncating `RAW.TRADE_EVENT` is safe for the stream: it is `APPEND_ONLY`, so it records the
insertions it has not yet shown you and ignores the removal. `REMOVE @RAW.TRADE_LANDING` matters
because `COPY` remembers the files it has already loaded for 64 days and would otherwise skip them.

`SNAPSHOTS.SNP_TRADE` survives this, because `--full-refresh` does not rebuild snapshots — keeping
history across a rebuild is the point of one. It holds no reference to the fact and its own tests are
self-contained, so a stale snapshot cannot fail the build; it simply carries versions of trades the
warehouse no longer has. Add `DROP TABLE IF EXISTS SNAPSHOTS.SNP_TRADE;` to the reset if you want the
account genuinely empty, for instance before a demo.

### If reconciliation is not applicable

With a real upstream feed there is no ground truth to compare against, and the task skips rather
than pretending otherwise. `PIPELINE_SIMULATE_ARRIVALS=false` disables it.

---

## Expiry sweep failure

**Alert:** `correctness_status` is RED, `gate_publish_readiness` failed with "matured trade(s) are
still marked LIVE", or `ALERT_EXPIRY_OVERDUE`.
**Meaning:** Business rule 4 is not being applied. **This is the canary for a stalled pipeline.**

### Why this is the most informative single alarm on the platform

Every other lifecycle transition is driven by an incoming event. Expiry is driven by *time*: a
trade becomes EXPIRED because a date passed, and the only thing that notices is the sweep inside
`fct_trade`, which unions newly accepted trades with existing trades whose maturity date has now
passed.

That sweep runs on every dbt build. So a matured trade still marked LIVE proves that **no dbt
build has completed since that trade matured** — regardless of what Airflow reports, regardless of
whether any alert fired, regardless of whether anything looks broken. It is a positive proof of
staleness derived from the data itself, which is much stronger than the absence of an error.

### Diagnose

```bash
python scripts/run_sql.py --preset expiry-overdue
```

`maturity_date` on the oldest row tells you how long the pipeline has been stalled.

### Resolve

Usually the sweep simply has not run:

```bash
make dbt-build-incremental
python scripts/run_sql.py --preset expiry-overdue   # must now return no rows
```

If it *has* run and rows remain, the sweep logic itself is broken — check that
`fct_trade`'s union branch for existing trades has not been dropped by a refactor, and that
`var('business_date')` is resolving to the date you expect. The
`fct_trade_no_matured_trade_is_still_live` data test asserts this invariant, so also ask why that
test passed.

---

## Platform health RED

**Alert:** `gate_publish_readiness` failed with "Platform health is RED".
**Meaning:** `REPORTING.RPT_DATA_QUALITY_SCORECARD` rolled up to RED.

The scorecard is one row, and two consumers read it: this gate and the dashboard header. That is
deliberate — one definition of "healthy" means you never face a dashboard saying GREEN and a page
saying RED and have to decide which to believe. The Snowflake alerts read `VW_PIPELINE_SLA` rather
than the scorecard, because the scorecard is a mart and would be missing in exactly the incidents
worth alerting on.

### Diagnose

```bash
python scripts/run_sql.py --preset scorecard
```

The RAG rollup is ordered, so the first matching condition is your problem:

| Condition | Verdict | Go to |
| --- | --- | --- |
| `overdue_expiry_trades > 0` | RED | [Expiry sweep failure](#expiry-sweep-failure) |
| `pending_events > 100000` | RED | [Stream drain failures](#stream-drain-failures) |
| Over 180 min since adjudication | RED | [dbt failures](#dbt-failures) |
| 24h reject rate over 25% | RED | [Data quality gate tripped](#data-quality-gate-tripped) |
| `pending_events > 10000` | AMBER | Transform is behind but coping |
| Over 90 min since adjudication | AMBER | Watch it |
| 24h reject rate over 15% | AMBER | Investigate during hours |
| Any parse errors in 24h | AMBER | [Load failures](#load-failures) |

The dashboard's landing page states these conclusions in words under **Findings**, rather than
leaving you to infer them from a grid of numbers. Expecting a tired engineer to notice that
`overdue_expiry_trades` reads 4 instead of 0 among five metrics is how incidents get missed.

### On `rules_never_fired`

Not part of the RAG rollup, and worth its own attention. A declared rule with no recorded hit is
either genuinely never violated or silently broken, and a passing test suite looks identical in
both cases. The **Rule catalogue** section of the Rejections page lists them.

---

## SLA miss

**Alert:** `sla_miss_callback` fired for a task.
**Meaning:** A task took longer than its SLA. It has **not** failed and may still succeed.

An SLA miss is a warning about latency, not an error. Distinguishing them matters: the sensor's
SLA fires while the sensor is still legitimately waiting.

| Task | SLA | Miss means |
| --- | --- | --- |
| `wait_for_files` | 45 min | Files are late. The sensor keeps waiting; see [File arrival delay](#file-arrival-delay) |
| `dbt_run_adjudication` | 20 min | Adjudication is slowing. Volume growth or a regression |
| `dbt_run_marts` | 25 min | Mart builds are slowing |

### Diagnose a slowing model

```bash
python scripts/run_sql.py "
  select model_name, count(*) as statements,
         round(sum(elapsed_seconds), 1) as total_seconds,
         count_if(tuning_signal <> 'OK') as flagged
  from \$SNOWFLAKE_DATABASE.monitoring.vw_dbt_query_performance
  where start_time >= dateadd('day', -7, current_timestamp())
  group by 1 order by total_seconds desc"
```

This is attributable to a model only because every dbt statement is tagged with its model name by
the `query_tag` pre-hook. Without the tag, `ACCOUNT_USAGE` can say what the warehouse cost but not
what caused it.

`tuning_signal` translates raw counters into an instruction, because
`bytes_spilled_to_remote_storage = 4.2e9` is not an instruction:

| Signal | Action |
| --- | --- |
| `REMOTE_SPILL_SIZE_UP_WAREHOUSE` | The query exceeded memory. Size up |
| `QUEUEING_ADD_CLUSTER` | Concurrency, not query cost. Add a cluster |
| `FULL_SCAN_REVIEW_PRUNING` | Pruning is not working. Review clustering |
| `COMPILE_BOUND_SIMPLIFY_SQL` | Compilation dominates. The SQL is too complex |

Note this reads `ACCOUNT_USAGE` and lags by up to 45 minutes, so it is a tuning tool rather than
an incident tool.

For growth beyond what a warehouse resize solves, see [`docs/scalability.md`](scalability.md).

---

## Recreating the stream

**When:** The stream has gone stale, or its definition must change.
**Severity:** This is a data-affecting operation. It is in the runbook rather than in the deploy
script for exactly that reason.

`snowflake/20_streams_tasks/01_stream_and_drain.sql` creates the stream with
`CREATE STREAM IF NOT EXISTS`, not `CREATE OR REPLACE` — the one deliberate exception to that
directory's otherwise uniform `CREATE OR REPLACE` style. A stream's offset is part of its *state*,
not its definition, and replacing it resets that offset. Combined with
`SHOW_INITIAL_ROWS = TRUE`, the next drain would re-read every row in `TRADE_EVENT`.

So a redeploy will never recreate the stream, and recreating it is a conscious act with a known
consequence.

### Procedure

**1. Establish what has already been adjudicated.** This is the watermark you will replay from.

```sql
SELECT MAX(load_ts) AS high_water_mark
  FROM RAW.TRADE_EVENT_QUEUE;
```

**2. Drain whatever the stream still holds,** if it is not stale. Skip if it is.

```bash
make drain
```

**3. Recreate.**

```sql
CREATE OR REPLACE STREAM RAW.TRADE_EVENT_STREAM
    ON TABLE RAW.TRADE_EVENT
    APPEND_ONLY = TRUE
    -- FALSE, not TRUE: the rows already in the table have been processed, and
    -- SHOW_INITIAL_ROWS would re-emit all of them.
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Recreated <date> per runbook#recreating-the-stream';
```

**4. Backfill the gap** between the watermark and now, which the new stream will not see:

```sql
INSERT INTO RAW.TRADE_EVENT_QUEUE (event_sk, raw_payload, source_file_name,
                                   source_file_row_number, load_method, load_ts, drained_at)
SELECT event_sk, raw_payload, source_file_name,
       source_file_row_number, load_method, load_ts, CURRENT_TIMESTAMP()
  FROM RAW.TRADE_EVENT
 WHERE load_ts > '<high_water_mark>'::timestamp_ltz
   AND event_sk NOT IN (SELECT event_sk FROM RAW.TRADE_EVENT_QUEUE);
```

The `NOT IN` guard makes this re-runnable. `EVENT_SK` is the merge key throughout the pipeline, so
even an accidental duplicate would deduplicate at adjudication — but not relying on that is
cheaper than explaining it afterwards.

**5. Verify, then resume normal processing.**

```bash
python scripts/run_sql.py --preset backlog
make dbt-build-incremental
make reconcile
```

---

## Backfill and replay

### Reprocessing events that were wrongly rejected

Fixing a rule does **not** retroactively reconsider events it previously rejected. Adjudication is
append-only by design: a verdict is a historical fact about what the platform decided at a point
in time, and silently changing it would destroy the audit trail.

To reprocess deliberately, re-queue the affected events from the immutable RAW layer:

```sql
-- Re-queue events rejected by a specific rule, for re-adjudication under the corrected rule.
INSERT INTO RAW.TRADE_EVENT_QUEUE (event_sk, raw_payload, source_file_name,
                                   source_file_row_number, load_method, load_ts, drained_at)
SELECT e.event_sk, e.raw_payload, e.source_file_name,
       e.source_file_row_number, e.load_method, e.load_ts, CURRENT_TIMESTAMP()
  FROM RAW.TRADE_EVENT AS e
 WHERE e.event_sk IN (
         SELECT event_sk FROM AUDIT.FCT_TRADE_REJECTED
          WHERE primary_rule_code = 'RJ00X'
            AND rejected_on >= '2026-03-01'
       );
```

Then `make dbt-build-incremental`. The original rejection rows remain in
`AUDIT.FCT_TRADE_REJECTED` — the audit trail shows both the original refusal and the later
acceptance, which is exactly what an auditor wants to see.

### Full rebuild

```bash
cd dbt && dbt build --target prod --full-refresh
```

This rewrites `FCT_TRADE_VERSION`, which is the version ledger. Understand before running it that
you are rebuilding the audit record, and that any event no longer present in RAW will not
reappear. `RAW` is the only true source; everything downstream is derived and therefore
reconstructible from it.

### Why `catchup` is off

Backfilling adjudication is meaningless. The rules evaluate against *current* state — the stored
high-water mark, today's date for maturity comparisons — so replaying last Tuesday's hourly run
would re-adjudicate today's data with last Tuesday's business date. Historical reprocessing is
this deliberate, separate operation instead.

---

## The DAG is listed but cannot be triggered

**Symptom.** The two halves of Airflow disagree about whether the DAG exists.

```
$ airflow dags list
dag_id         | fileloc                             | owners           | is_paused
trade_pipeline | /opt/airflow/dags/trade_pipeline.py | data-engineering | None

$ airflow dags trigger trade_pipeline
airflow.exceptions.DagNotFound: Dag id trade_pipeline not found in DagModel
```

**Reading it.** `dags list` parses the dags folder directly, so it proves the file is valid Python
and free of import errors. `trigger` reads the `dag` table, so it proves the scheduler never
registered what it parsed. `is_paused` being `None` rather than `True` says the same thing: there is
no row, and therefore no pause flag. `dags list-import-errors` printing "No data found" is
consistent — the file did not fail to import, it was never processed.

**Cause, almost always ownership.** The scheduler writes per-file parse logs under
`logs/dag_processor_manager/`. When that directory is not writable by the uid the scheduler runs as,
DAG file processing fails before serialisation, and the `dag` table stays empty. It presents as a
DAG that exists and does not exist at once, with nothing in the UI.

The directory is a bind mount from the host, and the init container that creates it runs as root
while the others run as `AIRFLOW_UID`. Initialisation therefore chowns it; if that step is skipped
or `AIRFLOW_UID` is unset, this is the result.

**Diagnose.**

```bash
cd airflow
ls -ln logs
docker compose logs airflow-scheduler | grep -i 'permission\|denied'
docker compose exec airflow-scheduler airflow dags report
```

Owner `0` in `ls -ln` against a scheduler running as 1000 is the confirmation. `dags report` lists
the files the processor has actually parsed, with durations — an absent or never-parsed file
corroborates it.

**Resolve.**

```bash
sudo chown -R "$(id -u):0" logs ../data
docker compose restart airflow-scheduler
sleep 60 && docker compose exec airflow-scheduler airflow dags list
```

`is_paused` should now read `True` instead of `None`, and `trigger` will work. On a laptop where
`AIRFLOW_UID` was never written to `airflow/.env`, fix that too, or the next
`docker compose down -v && up` recreates the problem:

```bash
grep AIRFLOW_UID .env || echo "AIRFLOW_UID=$(id -u)" >> .env
```

---

## Escalation

| Severity | Definition | Response |
| --- | --- | --- |
| **P1** | Data loss, or the golden record is wrong | Immediate. `assert_no_event_is_silently_dropped` or `MISSING_EVENT` are always P1 |
| **P2** | Pipeline stalled, data stale but correct | Within the hour during market hours |
| **P3** | Degraded — elevated rejects, cost anomaly, one stale report | Next business day |

**What to capture before escalating.** All four in one place, because reconstructing them later
during a handover wastes the most valuable minutes:

```bash
python scripts/run_sql.py --preset health      > incident-health.txt
python scripts/run_sql.py --preset backlog     > incident-backlog.txt
python scripts/run_sql.py --preset batches     > incident-batches.txt
python scripts/run_sql.py --preset dbt-runs    > incident-dbt.txt
```

Plus the Airflow log URL from the alert email, and the `query_id` of any failing statement — with
it, Snowflake support can see the exact query profile.

### Related documents

- [`docs/overview.md`](overview.md) — architecture and how the pieces fit together
- [`docs/validation-logic.md`](validation-logic.md) — every business rule and why it is written that way
- [`docs/scalability.md`](scalability.md) — what changes at 100× and 10,000× volume
- [`docs/monitoring.md`](monitoring.md) — every alert, its threshold, and why that threshold
- [`docs/setup.md`](setup.md) — first-time installation
