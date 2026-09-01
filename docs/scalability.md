# Scalability

What breaks first as volume grows, in what order, and what to do about it.

The local configuration handles roughly 5,000 events per hourly batch on XSMALL warehouses. This
document works through 100× and 10,000× that, naming the specific component that fails at each
stage. Where a change would be needed, it says what the change is and what it costs.

---

## Contents

- [How to read this](#how-to-read-this)
- [The current configuration](#the-current-configuration)
- [What the design gets right for free](#what-the-design-gets-right-for-free)
- [Stage 1: 10× — 50k per batch](#stage-1-10--50k-per-batch)
- [Stage 2: 100× — 500k per batch](#stage-2-100--500k-per-batch)
- [Stage 3: 1,000× — 5M per batch](#stage-3-1000--5m-per-batch)
- [Stage 4: 10,000× — 50M per batch](#stage-4-10000--50m-per-batch)
- [Scaling latency rather than volume](#scaling-latency-rather-than-volume)
- [Scaling concurrency](#scaling-concurrency)
- [Cost at scale](#cost-at-scale)
- [The ordered levers](#the-ordered-levers)
- [What would not change](#what-would-not-change)

---

## How to read this

Two failure modes are easy to confuse and have opposite remedies:

- **Volume** — each batch is bigger. Fixed by more compute per query: a larger warehouse.
- **Concurrency** — more queries at once. Fixed by more clusters of the same size.

Sizing up to fix queueing costs more and fixes nothing. `VW_DBT_QUERY_PERFORMANCE.tuning_signal`
distinguishes them directly: `REMOTE_SPILL_SIZE_UP_WAREHOUSE` is volume,
`QUEUEING_ADD_CLUSTER` is concurrency. Read it before changing anything.

The general principle throughout: **change configuration before changing architecture.** Most of
the first three stages are warehouse sizes and clustering keys, not rewrites. That is the payoff
for keeping the transformation relational and inside the warehouse.

---

## The current configuration

| | Setting |
| --- | --- |
| Volume | ~5,000 events per hourly batch, ~120k/day |
| Warehouses | XSMALL, single cluster, 60s auto-suspend |
| Time Travel | 14 days RAW, 1 day intermediate |
| dbt | Incremental merge on `FCT_TRADE` and `FCT_TRADE_VERSION` |
| Clustering | None — unnecessary below roughly a terabyte |
| Batch runtime | Under a minute end to end |
| Cost | Well under one credit per day |

---

## What the design gets right for free

Five properties mean the first few orders of magnitude need no structural change. They are worth
naming because "why does this scale" is a fair interview question and the answer is specific.

**Compute and storage are separate.** The transform warehouse can go from XSMALL to 4XLARGE — 128×
the compute — without touching a model, a table or a query. That single lever covers most of the
first three stages.

**Incremental by default.** Every fact model merges a delta rather than rebuilding. Cost scales with
*new* data, not with total data, so a table growing from 100k rows to 10bn rows does not make the
hourly build slower.

**Change capture is offset-based.** A stream's cost is proportional to the delta, not to the source
table. `RAW.TRADE_EVENT` at ten billion rows drains no more slowly than at ten thousand, provided
the stream is drained regularly.

**Workloads are already isolated.** Three warehouses with three resource monitors. Sizing the
transform warehouse for a heavy build does not change ingestion cost, and a runaway BI query cannot
starve the pipeline.

**The queue table is the contract.** dbt reads `RAW.TRADE_EVENT_QUEUE` and nothing upstream of it.
The ingestion mechanism can be replaced entirely — Snowpipe Streaming, a Kafka connector, an
external table over object storage — without a single dbt model changing. This is what makes the
architectural change at Stage 4 tractable rather than a rewrite.

---

## Stage 1: 10× — 50k per batch

**Roughly 1.2M events per day. Nothing needs to change.**

XSMALL handles this comfortably. The batch takes a few minutes instead of under one. Storage grows
to a few gigabytes.

### What to watch

```bash
python scripts/run_sql.py "
  select model_name, round(sum(elapsed_seconds),1) as total_seconds,
         count_if(tuning_signal <> 'OK') as flagged
  from TRADES_PROD.monitoring.vw_dbt_query_performance
  where start_time >= dateadd('day', -7, current_timestamp())
  group by 1 order by 2 desc"
```

`int_trade_event_adjudicated` will be the largest line, which is expected — it does the most work.
The signal to act on is `flagged > 0`, particularly `REMOTE_SPILL_SIZE_UP_WAREHOUSE`.

The `dbt_run_adjudication` SLA of 20 minutes is the tripwire. If it starts being missed, the answer
at this stage is Stage 2's first lever.

---

## Stage 2: 100× — 500k per batch

**Roughly 12M events per day. Configuration changes only.**

### 1. Size up the transform warehouse

```hcl
# terraform/envs/prod/main.tf, in the module "warehouses" block
transform = {
  size         = "MEDIUM"   # from XSMALL
  credit_quota = 1000
}
```

Each size doubles compute and doubles cost per second — but a query that takes half as long on a
warehouse costing twice as much costs *the same*, while finishing sooner. Snowflake billing is
per-second, so sizing up for a compute-bound query is often cost-neutral and always latency-positive.
That symmetry breaks once the query is no longer compute-bound, which is why `tuning_signal` matters
more than intuition here.

Size up **only the transform warehouse**. Ingestion is I/O-bound on file transfer and gains nothing
from a bigger warehouse. This is the payoff for having three.

### 2. Cluster the large fact tables

At this volume `FCT_TRADE_VERSION` is in the hundreds of millions of rows and micro-partition
pruning stops being effective on its own.

```sql
ALTER TABLE CORE.FCT_TRADE_VERSION CLUSTER BY (trade_date, trade_id);
ALTER TABLE RAW.TRADE_EVENT        CLUSTER BY (to_date(load_ts));
```

`trade_date` first because almost every query filters on a date range; `trade_id` second for
point lookups within a day. Clustering keys should follow the *filter* pattern, not the join
pattern — Snowflake prunes on filters.

Automatic clustering is a **serverless cost that does not appear against any warehouse**, which is
why `VW_SERVERLESS_CREDITS` exists. Check it after enabling clustering, not before, and be aware
that clustering a table with a high update rate can cost more than the scans it saves.

### 3. Reduce intermediate retention

```yaml
# dbt_project.yml
+post-hook: "alter table {{ this }} set data_retention_time_in_days = 0"
```

For `intermediate` models only. They are rebuildable from RAW, so paying Time Travel storage on
them is paying twice for the same recoverability. Leave RAW at 14 days — that is the layer where
recovery actually happens.

### 4. Batch the producer's files

Snowpipe bills **per file** as well as per byte. A producer emitting many small files costs more
than the data warrants. At this stage, aim for files in the 100–250 MB range, which is also the
range that parallelises best across a warehouse's threads.

### What has not changed

No model rewritten. No new component. Two Terraform variables, two `ALTER TABLE`s, a retention
setting.

---

## Stage 3: 1,000× — 5M per batch

**Roughly 120M events per day. The first structural pressure appears.**

### 1. Warehouse sizing, and where it stops paying

LARGE or XLARGE for transform, SMALL for load. Beyond XLARGE, watch for diminishing returns — if
doubling the warehouse does not roughly halve the runtime, the query is no longer compute-bound and
sizing up is buying nothing. That is the point at which to look at the SQL instead.

### 2. The adjudication model becomes the bottleneck

This is the specific thing that breaks first, and it is worth being precise about why.

Version arbitration is a window function partitioned by `trade_id`. At 5M events per batch the
window computation dominates, and — more importantly — the arbitration must consider the *stored*
version of every trade it touches, so the model joins the delta against `FCT_TRADE`.

The fix is to narrow that join rather than to make it faster:

```sql
-- Only the trades this batch actually mentions need their prior state.
with touched_trades as (
    select distinct trade_id from {{ ref('int_trade_event_typed') }}
)
select ft.* from {{ ref('fct_trade') }} ft
inner join touched_trades tt using (trade_id)
```

A semi-join against the delta rather than a scan of the whole golden record. The correctness
argument is that arbitration only ever compares against trades present in the batch, so restricting
the prior state to those trades cannot change any verdict. The unit tests are what make this
refactor safe to attempt at all.

### 3. Split the batch by book or region

At 5M per batch, one transaction becomes an availability risk: a failure late in the build wastes
the whole run, and a single long-running merge holds locks for a long time.

dbt supports this without duplicating models:

```bash
dbt build --select int_trade_event_adjudicated+ --vars '{book_group: EMEA}'
```

Partition on a dimension that is stable and evenly distributed. Book or region works; `trade_date`
does not, because a batch spans dates unevenly and amendments arrive for old dates.

The cost is real: version arbitration must not be split across partitions for the *same* trade, so
the partition key has to be an attribute a trade cannot change. That is a genuine constraint on the
choice, not a detail.

### 4. Multi-cluster for the BI warehouse

```hcl
bi = {
  size              = "SMALL"
  min_cluster_count = 1
  max_cluster_count = 3
  scaling_policy    = "STANDARD"
}
```

Concurrency, not volume — dashboard users, not bigger queries. `min = 1` so the idle cost stays at
one cluster.

### 5. Separate the reject-analysis workload

Rejection analysis at this volume is a heavy scan of the audit tables, and it is exploratory —
someone investigating an incident, running ad-hoc queries. Give it its own warehouse so an
investigation cannot slow the pipeline it is investigating.

---

## Stage 4: 10,000× — 50M per batch

**Roughly 1.2bn events per day. This is where the architecture genuinely changes.**

At this volume the file-based hourly batch stops being the right shape. Three changes, in
dependency order.

### 1. Replace file ingestion with Snowpipe Streaming

Files at this rate mean either enormous files or an enormous number of them, and both are bad —
large files delay the whole batch behind the slowest one, many files multiply Snowpipe's
per-file cost. Snowpipe Streaming ingests row-by-row with per-row billing and seconds of latency.

**This is where the queue-table contract pays for itself.** The Snowpipe Streaming client writes into
`RAW.TRADE_EVENT`, the stream and drain continue unchanged, and **not one dbt model changes**. The
architectural change is confined to the layer above the contract, which is exactly what the contract
was for.

### 2. Micro-batch the transformation

Hourly stops making sense when data arrives continuously. Move to 5- or 15-minute builds:

```python
schedule = "*/15 * * * *"
```

Smaller batches are also *cheaper per event*, because the arbitration window is smaller and the
merge touches fewer partitions. The constraint is that a build must finish well inside its interval,
or runs will queue behind each other — and `max_active_runs=1` means they will queue rather than
overlap, which is correct but will show up as growing latency rather than as an error. Watch the
DAG's queue depth, not just its success rate.

### 3. Consider Dynamic Tables for the mart layer

At continuous ingestion, the marts want continuous refresh, and orchestrating a 15-minute dbt build
purely to update aggregates is the wrong tool. Dynamic Tables with a `TARGET_LAG` of a few minutes
handle that declaratively.

Keep dbt for adjudication. That is where the testable business logic lives, and Dynamic Tables offer
no unit testing against mock data, no rule catalogue, no `--store-failures`. **The split is: dbt
where correctness must be proved, Dynamic Tables where freshness must be maintained.** That is a
defensible boundary rather than a hedge.

### 4. Partition the pipeline by region

At 1.2bn events per day, one pipeline is a single point of failure and probably crosses regulatory
boundaries anyway. Separate databases and warehouses per region, with a federated reporting layer
over the top. Data residency usually forces this before volume does.

### What still would not change

The rule book. The audit model. The reconciliation approach. The test suite. Those are the parts
that took the design thought, and none of them is volume-dependent.

---

## Scaling latency rather than volume

A different requirement, and worth separating because the answers barely overlap.

If the business wants trades visible in **seconds** rather than an hour:

| Change | Latency after |
| --- | --- |
| Hourly → 15-minute schedule | ~15 min |
| Snowpipe (auto-ingest on file arrival) | ~1–2 min to RAW |
| Snowflake Task drain at 1 minute | already the case |
| Snowpipe Streaming | seconds to RAW |
| Dynamic Tables on marts, 1-minute lag | ~1 min end to end |

**The hard limit is version arbitration.** R2 says the later of two same-version arrivals wins, which
requires knowing whether a later one exists. That is inherently a windowing operation over a period
of time, and no amount of infrastructure removes it. Sub-second adjudication would require changing
the *rule* — for example, accepting optimistically and correcting on late arrival, which trades
correctness-on-first-read for latency.

That is a business decision, not an engineering one, and it is the honest answer to "can you make
this real-time".

---

## Scaling concurrency

Consumers, rather than data.

| Consumers | Change |
| --- | --- |
| Under 10 | Current configuration |
| 10–50 | Multi-cluster BI warehouse, `min=1 max=3` |
| 50–200 | `max=5`, `ECONOMY` scaling policy, plus materialised aggregates so dashboards do not scan facts |
| 200+ | Separate warehouses per consumer group, each with its own resource monitor |

`ECONOMY` over `STANDARD` above about fifty users: `STANDARD` starts a cluster as soon as a query
queues, which minimises latency and maximises cost. `ECONOMY` waits until there is enough work to
keep a new cluster busy. For dashboard traffic, a few seconds of queueing is invisible and the cost
difference is not.

The reporting marts already exist partly for this reason — a dashboard reading
`AGG_TRADE_STATUS_DAILY` scans thousands of rows, not hundreds of millions.

---

## Cost at scale

Rough order of magnitude, monthly credits, prod:

| Stage | Load | Transform | BI | Serverless | Total |
| --- | --- | --- | --- | --- | --- |
| Current | <1 | ~2 | <1 | <1 | **~5** |
| 100× | ~10 | ~80 | ~20 | ~10 | **~120** |
| 1,000× | ~60 | ~600 | ~150 | ~100 | **~900** |
| 10,000× | ~400 | ~4,000 | ~800 | ~1,200 | **~6,400** |

Three observations that matter more than the numbers.

**Transform dominates at every stage**, which is why it is the warehouse to size and the one with the
largest resource monitor quota.

**Serverless grows fastest** — from under 1 credit to 1,200. Snowpipe per-file charges, serverless
tasks and automatic clustering are all serverless, none appears against a warehouse, and none is
capped by a warehouse resource monitor. This is the line item that produces surprise bills, and it
is why `VW_SERVERLESS_CREDITS` is a separate view rather than folded into the warehouse one.

**Storage is absent from the table** because it is small by comparison — but Time Travel and
Fail-safe multiply it, which is why retention is tuned per schema rather than set once globally.

Resource monitors scale with the `credit_quota` values in `terraform/envs/prod/main.tf`. The
notify-at-50/75/90 and suspend-at-90 triggers do not need to change; they are proportional.

---

## The ordered levers

When something is too slow, in this order. Stop as soon as it is fast enough.

1. **Read `tuning_signal`.** Establish whether it is volume, concurrency, pruning or compilation.
   Everything below depends on getting this right.
2. **Size up the transform warehouse.** One variable. Often cost-neutral for a compute-bound query.
3. **Add clustering keys** to the large fact tables. Check `VW_SERVERLESS_CREDITS` afterwards.
4. **Reduce retention** on rebuildable schemas.
5. **Batch the producer's files** into the 100–250 MB range.
6. **Add clusters** for concurrency — never size up for concurrency.
7. **Narrow the adjudication join** to touched trades only. The first change requiring a code review.
8. **Split batches** by a stable dimension.
9. **Shorten the schedule** to micro-batches.
10. **Move to Snowpipe Streaming.** The first genuine architectural change.
11. **Dynamic Tables for marts**, keeping dbt for adjudication.
12. **Partition by region.** Usually forced by regulation before volume.

Levers 1–6 are configuration. 7–9 are code changes with tests to protect them. 10–12 are
architecture. Most platforms never need to go past 6, and reaching for 10 when 2 would have done is
the most common expensive mistake in this space.

---

## What would not change

Worth stating plainly, because it is the measure of whether the design was right.

**The rule book.** Declarative, one file, generated SQL. Volume-independent.

**The audit model.** Immutable RAW, retained payloads, per-rule logging. Volume-independent.

**Idempotency.** Merge-based models, transactional drains, load-history-aware COPY. These properties
matter *more* at scale, not less, because failures become routine.

**The test suite.** Unit tests against mock rows run in seconds regardless of production volume, and
that stays true at every stage. It is the thing that makes levers 7 through 12 attemptable at all.

**Reconciliation.** Proving no event was lost matters more at 1.2bn events a day than at 120k,
because loss is proportionally easier to hide.

The parts that change are warehouse sizes, clustering keys, schedules, and eventually the ingestion
mechanism. The parts that took design thought stay as they are — which is the argument for having
put the thought there.
