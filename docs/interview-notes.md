# Interview notes

The questions this design invites, and the answers. Ordered roughly by how likely each is to come up.

Everything here is a summary of reasoning stated in full elsewhere; each answer links to it. If a
question goes deeper than the answer below, the link is where the detail is.

---

## Contents

- [Opening: describe what you built](#opening-describe-what-you-built)
- [Technology choices](#technology-choices)
- [The business rules](#the-business-rules)
- [Data modelling](#data-modelling)
- [Reliability and operations](#reliability-and-operations)
- [Testing](#testing)
- [Security and governance](#security-and-governance)
- [Scale and cost](#scale-and-cost)
- [Questions to be honest about](#questions-to-be-honest-about)
- [What to demonstrate live](#what-to-demonstrate-live)

---

## Opening: describe what you built

A trade lifecycle validation platform. Files of trade events arrive, are validated against nineteen
business rules, and either enter the golden record or are rejected with a full audit trail.

The design is Snowflake-native ELT: land the payload unchanged, then transform in SQL with dbt.
Airflow orchestrates and gates, Terraform provisions, Streamlit provides operational visibility.

**The one thing I would lead with:** the platform does not just process trades, it *proves* it
processed them correctly. The simulator records, for every event it generates, the verdict that event
must receive. After each run, reconciliation compares that ground truth against what the pipeline
actually decided. That is the difference between "the DAG went green" and "the pipeline was correct",
and it is the only check that can detect an event which is **absent** rather than wrong.

---

## Technology choices

### Why not Spark?

The data is already in Snowflake and the transformation is relational — joins against reference data,
a window function for version arbitration, aggregations. Spark would mean reading out of the
warehouse, processing on a separate cluster, and writing back: paying network transfer twice, adding a
cluster to size and secure, and adding a second language for business logic, in order to do relational
work less well than a columnar warehouse does it natively.

Spark earns its keep on unstructured data, ML feature pipelines, and where compute must live outside
the warehouse. None applies here. [ADR 0001](adr/0001-snowflake-native-elt.md)

### Why dbt rather than stored procedures?

Not for the SQL — procedures could express the same logic. For the machinery around it: a dependency
graph, lineage, generated documentation, `--store-failures`, and unit tests that run business rules
against mock rows with no warehouse. For a platform whose main requirement is provable correctness,
the testing framework matters more than the SQL.

### Why not Dynamic Tables?

The closest call in the project, and worth conceding as a good question. They give declarative
incremental refresh with no orchestrator. They do not give unit testing against mock data, a versioned
rule catalogue, generated docs, or a way to inspect the rows that failed an assertion.

Where I would use them is the mart layer at high volume, keeping dbt for adjudication. The split is:
dbt where correctness must be proved, Dynamic Tables where freshness must be maintained.
[scalability.md](scalability.md#stage-4-10000--50m-per-batch)

### Why both Airflow and Snowflake Tasks? Isn't that two orchestrators?

It is, and the division is principled: **Snowflake Tasks do what must happen continuously and
independently; Airflow does what needs sequencing, gating and cross-system coordination.**

The concrete payoff: the drain task runs every minute, gated on `SYSTEM$STREAM_HAS_DATA` so it costs
nothing when idle. That means change capture keeps up **even when Airflow is down** — which matters
because a stream goes stale if unconsumed for longer than Time Travel retention, and a stale stream is
the one failure a retry cannot fix.

What Airflow gives that a task tree cannot: sensing files outside Snowflake, exponential backoff on
transfer, alerting with real detail, and — decisively — somewhere to put a gate that stops the pipeline
and explains why. [ADR 0008](adr/0008-orchestration-choice.md)

### Why not Kafka?

The upstream produces files, periodically. A broker would be operational cost for no benefit, and the
pipeline would still be batch, because version arbitration needs the whole batch.

The design does not preclude it: dbt reads `RAW.TRADE_EVENT_QUEUE` and nothing upstream of it, so a
Kafka connector could replace the file path without a single model changing.

---

## The business rules

### Walk me through the version rules

R1 rejects a version lower than one already accepted. R2 says on a same-version resend the later
arrival wins. Three computed columns carry it:

- `effective_prior_version` — the greater of the stored version and the highest version accepted
  *earlier in the same run*. The second half matters: a batch can contain versions 1, 2 and 3 of one
  trade, and comparing each only against stored state would accept all three with the outcome depending
  on merge order.
- `intra_run_rank` — ordered by arrival sequence, not by any payload timestamp. A payload timestamp is
  set by the sender and cannot be trusted to order two messages the sender may have emitted out of
  order.
- `prior_is_cancelled` — cancellation is terminal.

[validation-logic.md](validation-logic.md#version-arbitration-in-detail)

### What is the hardest piece of logic in the project?

The phase ordering, and this is the answer I would most want to be asked for.

Rules split into FIELD (decidable from one event plus reference data) and STATE (needs the stored
state). Version arbitration runs between them, over **field-valid events only**.

Without that filter: a malformed version 5 arrives — say a negative notional — and sets the high-water
mark to 5. A perfectly valid version 3 arriving next is rejected as stale, **on the authority of an
event we already refused**. One bad message poisons the trade's entire subsequent history, and every
resulting rejection looks individually correct. Nothing in the audit log points at the cause.

The general principle: a rejected event must have no influence on any later decision. There is a unit
test for exactly this case, because it is the failure most likely to be reintroduced by a well-meaning
refactor.

### Why is rule 4 not a rule?

Expiry is driven by time, not by an event. Implemented as a rule, a trade would only expire if
something happened to arrive and remind us, so a quiet book would keep matured trades marked LIVE
indefinitely. Instead `fct_trade` re-derives status on every build.

That gives a valuable side effect: because the sweep runs every build, **a matured trade still marked
LIVE proves no build has completed since it matured** — regardless of what Airflow reports. It is the
platform's most reliable staleness detector, which is why it is the first RED condition in the
scorecard.

### Why is a limit breach a warning rather than a rejection?

Because rejecting it would make things worse. The desk resubmits as three smaller tickets, each under
the limit, the breach appears nowhere, and the position is larger than the limit was meant to allow.
Accepting the trade with the breach recorded keeps the risk visible, which is the actual objective.

A control people can route around is not a control.

### Why nineteen rules when four were asked for?

Three carry a requirement tag — `RJ001` for R1, `RJ009` for R2, `RJ003` for R3 — and the fourth stated
requirement is the expiry sweep, which is not a rule. The other sixteen are tagged `OWN`. The tagging is
deliberate so a
reviewer can separate what was asked for from what a real platform needs. `RJ005` versus `RJ016` is a
good example of why the extras are not padding: unknown counterparty is a data problem fixed by
resubmission, inactive counterparty is a credit control fixed by escalation. Merging them would send
half the cases to the wrong team.

### How would you add a rule?

Four lines in one macro, one row in a seed, one unit test. Then `make dbt-unit-test && make selfcheck`
— neither needs a warehouse, so the loop is seconds. No model SQL changes, because the macro generates
the evaluation SQL, the audit rows and the documentation from the declaration.

That last property is the point: **a rule cannot fire without being logged**, because the same list
generates both. [ADR 0005](adr/0005-rules-as-declarative-macro.md)

---

## Data modelling

### Why both `FCT_TRADE` and `FCT_TRADE_VERSION`?

One table cannot answer both questions. Current-version-only makes history unavailable;
history-only makes every position query a window function over the full ledger.

`FCT_TRADE` is exactly the maximum-version projection of `FCT_TRADE_VERSION`, and
`assert_fct_trade_matches_version_ledger` asserts it. The redundancy is checked rather than trusted —
and when it fails, it usually means two writers ran concurrently, which is what `max_active_runs=1`
exists to prevent.

### You have a version ledger. Why also a snapshot?

Because some state transitions have no event. Expiry is the case that matters: a trade becomes EXPIRED
because a date passed, so it appears nowhere in the ledger. `SNP_TRADE` is what makes "what did this
trade look like on 3 March" answerable across time-driven as well as event-driven changes.

### Why land as `VARIANT` rather than typed columns?

A typed landing table must reject a row it cannot cast, and the row is then gone — the one piece of
evidence needed to work out what the upstream did wrong. Landing the payload as-is makes an upstream
schema change a *transformation* problem discovered in dbt, with the original bytes available, rather
than an *ingestion failure that discards the evidence*.

The cost is a cast layer downstream, and that layer is where `cast_failure_count` comes from — which is
what distinguishes "you did not send it" (`RJ004`) from "you sent it unreadably" (`RJ008`).

---

## Reliability and operations

### What happens if a file arrives late?

The sensor keeps waiting, because the file may still be coming, and its 45-minute SLA notifies a human
that the wait has become abnormal. Those are deliberately separate: waiting is correct behaviour, and
there is no failure to catch.

Absence produces no error, so it is detected by comparing the stage against loaded rows rather than by
watching for failures. `expected_gap_minutes` is the **median observed gap over seven days**, derived
rather than hard-coded, so the monitor keeps working when the upstream changes cadence.
[runbook](runbook.md#file-arrival-delay)

### What happens if data quality is bad?

The reject-rate gate trips and **the marts are deliberately left un-refreshed**. The golden record still
holds the last known-good state.

Where the gate sits is the design: after adjudication, so the rate is measurable and every rejected
trade is already in the audit log for investigation — but before the marts, so a suspect batch can be
stopped before anyone trades on it. Earlier there is nothing to measure; later the damage is done.

An operator can raise the threshold for one run with `DQ_MAX_REJECT_RATE=0.60`, which makes the override
explicit and temporary rather than a code change.

### Is a blocked pipeline not a problem in itself?

It is the designed behaviour. Publishing suspect data to the golden record is much harder to undo than a
delayed refresh, because downstream consumers will have read it. The instinct to force the run through is
the thing the gate exists to resist.

### What if a task fails halfway?

Retry it — and you do not need to establish what the previous attempt completed, because every step is
idempotent:

- `COPY` consults Snowflake's load history and skips files already ingested.
- The drain is **one transaction**: insert, batch record, and offset advance commit together, so a
  failure leaves the offset exactly where it was.
- Every incremental model merges on a surrogate key rather than appending.

That property is why most of the runbook says "retry it". [runbook](runbook.md#recovery-principles)

### How do you know nothing was silently lost?

Three independent mechanisms:

1. **`assert_no_event_is_silently_dropped`** — every event entering the queue must reach exactly one of
   three destinations. The most important test in the project.
2. **`VW_COPY_HISTORY`** derives `PARTIAL_LOAD` where `rows_parsed > rows_loaded`. Necessary because
   `ON_ERROR = CONTINUE` reports **success** on a partially loaded file — resilient ingestion is
   precisely what makes loss invisible.
3. **Reconciliation** against the generator's manifest, which is the only check that can notice an event
   that is absent.

### How is this monitored if Airflow dies?

That was a design constraint, not an afterthought. dbt writes its own outcomes to
`AUDIT.DBT_RUN_RESULT` from an `on-run-end` hook, and the Snowflake alerts read Snowflake tables on
Snowflake's own schedule. So a stale curated layer is detected and emailed even if the machine running
Airflow is switched off — which is exactly the situation in which nobody is watching the Airflow UI.

An observability layer that lives inside the orchestrator is blind precisely when you need it.
[monitoring.md](monitoring.md#design-principles)

### Why four RAG columns instead of one health light?

"The platform is red" starts a hunt. "Capture is red, everything else green" points at the stream drain.
The health view also reads only RAW tables, the batch table, the queue and `FCT_TRADE` — never a
reporting mart — because during an incident the marts are frequently the broken thing, and a health view
that fails when the pipeline is unhealthy answers nothing.

### Why are thresholds duplicated across three layers?

So each layer works when the others are down. If Airflow held the only definition of "too stale", a dead
scheduler would mean nothing is ever stale again — the monitor and the monitored failing together.

The cost is drift, and drift is the worst outcome: a dashboard and a page that disagree, and an engineer
who trusts neither. So `assert_sla_thresholds_agree` compares them and fails if they diverge. The
duplication survives; the divergence does not.
[monitoring.md](monitoring.md#why-thresholds-are-duplicated)

### What is the worst failure mode in the design?

A stream going stale. If the stream is not consumed within the source table's Time Travel retention, the
delta is **unrecoverable from the stream** — the one failure a retry cannot fix.

Mitigated three ways: the task drains every minute, `VW_STREAM_LAG` reports lag against the limit
explicitly, and the runbook has a recovery procedure that recreates the stream with
`SHOW_INITIAL_ROWS = FALSE` and backfills the gap from a recorded high-water mark.

It is also why the stream is the single exception to the deployment scripts' `CREATE OR REPLACE` style:
replacing a stream resets its offset, which would re-emit every row in the table.
[runbook](runbook.md#recreating-the-stream)

---

## Testing

### How do you test business rules without a warehouse?

dbt unit tests. They run the real model SQL against fixed input rows and assert the verdict — no
warehouse, no credentials, seconds. That is what makes the rules maintainable: a rule you cannot cheaply
test is a rule nobody will change confidently.

`make dbt-unit-test` is the loop I would use while writing a rule.

### What do the singular tests catch that data tests do not?

Cross-model invariants. Each was written because its violation is otherwise invisible:

- **`assert_no_event_is_silently_dropped`** — silent data loss. P1.
- **`assert_fct_trade_matches_version_ledger`** — the golden record diverged from the ledger, usually
  concurrent writers.
- **`assert_version_history_has_no_gaps`** — v2 jumps to v4. Sometimes legitimate, often loss.
- **`assert_rule_catalogue_matches_macro`** — the executable rules and the human-readable seed drifted.
- **`assert_sla_thresholds_agree`** — the duplicated thresholds diverged.
- **`assert_snapshot_covers_material_columns`** — a column was added to the fact and not the snapshot, so
  changes to it would silently not be historised.

### What is `selfcheck`?

A script that scans the repository for references to Makefile targets, file paths, documentation anchors
and dbt models, and fails if any resolves to nothing. It is what stops a runbook from telling an on-call
engineer to run a command that was renamed six months ago.

It caught two real problems while this was built: a renamed dbt build target still referenced by the
CLI's own output, and every runbook anchor before the runbook existed.

### What tests exist in total?

162 generic dbt data tests, 6 singular tests, 9 dbt unit tests, 88 Python tests (simulator plus dashboard render
tests via `AppTest`), DAG static analysis including a reachability check, and repository
self-consistency. `make ci-local` runs everything that needs no warehouse.

---

## Security and governance

### Explain the RBAC model

Two tiers. Fifteen access roles hold privileges on exactly one schema at one level. Five functional
roles hold **no object privileges at all** — only access roles plus warehouse usage — and describe
personas: `INGEST`, `TRANSFORM`, `ANALYST`, `COMPLIANCE`, `PLATFORM`. Users get functional roles only,
never a direct grant.

The reason: with direct grants, adding a schema means touching every user, and "what can the dashboard
read" means comparing grant lists that differ per user because someone was granted something extra
during an incident and it was never removed. With two tiers it is one new access role, and permissions
are reviewable in a pull request — or answerable in Snowflake with a single `SHOW GRANTS OF ROLE`.

**The containment is real, not cosmetic.** `INGEST` writes `RAW` and cannot read or alter curated trade
data, so a buggy or compromised loader cannot corrupt the golden record. `ANALYST` cannot read `AUDIT`,
so the dashboard cannot display a raw payload even by accident. `COMPLIANCE` reads the audit layer with
no write privilege anywhere — a separation that is awkward with direct grants and falls out naturally
here.

Future grants matter too: `GRANT SELECT ON FUTURE TABLES IN SCHEMA` means a new dbt model is readable by
the right roles the moment it exists, which removes the entire "the dashboard cannot see the new table"
class of problem. [ADR 0003](adr/0003-two-tier-rbac.md)

### How does masking work?

Policies attach to **tags**, not columns. So classifying a column as PII applies the correct masking
automatically, and a column cannot be sensitive-but-unmasked because someone did two of three steps.

This is also why functional roles hold no object privileges: with two mechanisms deciding what a user
sees, reasoning about the combination would be genuinely hard. Access is decided in one place, visibility
within accessible data in another.

### Why key pair rather than a password?

MFA makes passwords unusable for a service identity; a key rotates without downtime because Snowflake
accepts two public keys simultaneously; and nothing reusable crosses the network, since the key signs a
JWT locally.

The honest downside: an unencrypted key on disk means a laptop with that file can act as the service
user. Mitigated by mode 600, gitignore and Gitleaks in CI, and in production it belongs in a secrets
manager with the passphrase support the code already has.
[ADR 0006](adr/0006-keypair-authentication.md)

---

## Scale and cost

### What breaks first at 100× volume?

Nothing structural. Size the transform warehouse from XSMALL to MEDIUM, cluster
`FCT_TRADE_VERSION` on `(trade_date, trade_id)`, drop retention on the rebuildable intermediate schema,
and batch the producer's files into the 100–250 MB range. Two Terraform variables and two `ALTER TABLE`s.

Sizing up is often **cost-neutral** for a compute-bound query, since billing is per-second: a query that
takes half as long on a warehouse costing twice as much costs the same and finishes sooner.

### And at 1,000×?

The adjudication model, specifically. Version arbitration is a window function that must consider the
stored version of every trade it touches, so it joins the delta against `FCT_TRADE`. The fix is to narrow
that to a semi-join against the trades the batch actually mentions — correctness holds because
arbitration only ever compares against trades present in the batch.

That is the first change requiring a code review rather than a configuration change, and the unit tests
are what make it safe to attempt.

### When does the architecture actually change?

Around 10,000× — roughly 1.2bn events a day. File ingestion stops being the right shape, and Snowpipe
Streaming replaces it. **The point is that not one dbt model changes**, because dbt reads the queue table
and nothing upstream of it. That contract was designed for exactly this.

### Can you make it real-time?

To seconds, mostly yes: Snowpipe Streaming to RAW, the drain already runs every minute, Dynamic Tables on
the marts with a one-minute lag.

The hard limit is R2. "The later of two same-version arrivals wins" requires knowing whether a later one
exists, which is inherently a window over time. No infrastructure removes that. Sub-second adjudication
would require changing the *rule* — accepting optimistically and correcting on late arrival — which trades
correctness-on-first-read for latency. That is a business decision, not an engineering one.

### How do you control cost?

Three warehouses, one per workload, each with its own resource monitor: notify at 50/75/90, suspend at 90,
suspend immediately at 100. **Per-warehouse rather than account-level**, so a runaway BI query cannot take
down ingestion — which is also why the warehouses are separate at all.

Attribution comes from query tagging: every dbt statement carries its model name, so
`VW_DBT_QUERY_PERFORMANCE` says what *caused* the cost, not just what the warehouse spent.

The line item that surprises people is serverless — Snowpipe per-file charges, serverless tasks,
automatic clustering. None appears against a warehouse and none is capped by a warehouse monitor, which is
why `VW_SERVERLESS_CREDITS` is separate. At 10,000× it is the fastest-growing component.

### What does `tuning_signal` do?

Translates raw counters into an instruction. `bytes_spilled_to_remote_storage = 4200000000` is a number,
not an action. `REMOTE_SPILL_SIZE_UP_WAREHOUSE` is.

The distinction that matters most is spilling versus queueing — volume versus concurrency. They have
opposite remedies, and sizing up to fix queueing costs more and fixes nothing.

---

## Questions to be honest about

Prepared answers for the weaknesses. Conceding a real limitation is stronger than defending it.

### The reference data is dbt seeds

Correct for a case study, wrong for production. Counterparties, currencies, products and books are
someone else's system of record and should be ingested with their own freshness SLAs, not committed as
CSVs. The rules would not change — they join against the same relations.

### The three service users share one key locally

Terraform creates three service users with three genuinely different privilege sets, which is where the
security value is — but in a local setup all three are given the same public key, because `make keypair`
generates one pair. In production each would get its own key from a secrets manager. The Terraform
variables are already separate (`ingest_public_key`, `dbt_public_key`, `bi_public_key`) precisely so
that requires no code change.

Relatedly, the private key is unencrypted on disk locally. The code supports a passphrase; the local
setup does not use one, so a laptop with that file can act as the service user.

### Snowflake is a single point of failure, including for monitoring

Every monitor runs inside Snowflake, so Snowflake being down takes the observability with it. That is the
accepted trade-off of a warehouse-native design. The mitigation is external uptime monitoring, out of
scope for a local deployment. `AUDIT.DBT_RUN_RESULT` exists partly to reduce the dependency on Airflow's
metadata database, but it does not address this.

### Distribution drift is not detected

Reject rate is monitored; the shape of the data is not. A batch where every notional is exactly 1,000,000
passes every check. It needs a statistical baseline of normal that a fresh platform does not have, and
alarms without a baseline are noise. `dbt-expectations` is already a dependency and would be where to
start.

### The rule conditions are Jinja strings

No autocomplete, and a typo is a compile error rather than an editor error. `make dbt-parse` catches it in
seconds without a warehouse. The `t.` prefix convention is also load-bearing — `evaluate_rules` rewrites
it to the caller's alias, so a condition written without it silently fails to be re-aliased. That is the
sharpest edge in the design and it is enforced by nothing but consistency.

### Business reasonableness is not validated

The platform validates that a trade is *well-formed*, not that it is *sensible*. A 500-billion notional in
a valid currency for an active counterparty passes everything unless the desk has a limit configured.
Genuine reasonableness checks need a risk model, which is a different system.

### `WARN` trades enter the golden record

A limit-breaching trade is accepted. That is deliberate — see the reasoning above — but it does mean the
golden record contains trades a risk function may not have approved. The mitigation is that the breach is
recorded against the real trade and reported, rather than being pushed into three invisible tickets.

---

## What to demonstrate live

In this order, about ten minutes.

**1. `make demo`** — generate, load, drain, transform, test, reconcile. It ends with reconciliation
passing, which is the headline: the pipeline was correct, not merely successful.

**2. `make status`** — four RAG columns. Explain why four rather than one.

**3. The dashboard, Rejections page** — the rule leaderboard, then `is_concentrated`. Over 80% of hits
from one source means an upstream release broke a feed; spread evenly means our reference data or the rule
is wrong. Opposite responses, identical total counts, and invisible from a count alone. Then drill into
`raw_payload` to show why the payload is retained.

**4. `make dbt-unit-test`** — business rules proved in seconds with no warehouse. Then open the poisoned
high-water-mark test and explain the phase ordering. This is the strongest single thing to show.

**5. Break something deliberately.** `TRADE_SIM_ERROR_RATE=0.40 make demo` trips the reject-rate gate.
Show that the DAG stopped, that `FCT_TRADE` is unchanged, and that the alert carries a runbook anchor for
that specific failure class.

**6. `make selfcheck`** — then delete a runbook heading and run it again to show it fails. It makes the
point that the documentation is verified rather than asserted.

If there is time, `dbt docs` shows the rule catalogue generated from the same macro that executes it — so
the published documentation is true by construction.
