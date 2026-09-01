# ADR 0004: Snowflake Streams for change capture, drained transactionally

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`overview.md`](../overview.md#change-capture--streams-and-tasks),
[runbook: Recreating the stream](../runbook.md#recreating-the-stream)

---

## Context

`RAW.TRADE_EVENT` is append-only and grows without limit. Each pipeline run must process only the rows
that arrived since the last run. Reprocessing everything is wasteful at any volume and impossible at
scale; missing rows is silent data loss.

So the platform needs a delta, and it needs one that cannot lose a row.

## Decision

**A Snowflake Stream on `RAW.TRADE_EVENT` with `APPEND_ONLY = TRUE`, drained into
`RAW.TRADE_EVENT_QUEUE` by a stored procedure in a single transaction.**

The drain runs two ways, deliberately: `TASK_DRAIN_TRADE_EVENT_STREAM` every minute gated on
`SYSTEM$STREAM_HAS_DATA`, and as an explicit Airflow step. Both call the same procedure, and because the
procedure is transactional, both running is harmless.

dbt reads `RAW.TRADE_EVENT_QUEUE` and never the stream. **The queue table is the contract.**

## Alternatives considered

### A high-water mark query

The obvious approach:

```sql
select * from raw.trade_event where load_ts > (select max(load_ts) from processed_marker)
```

Rejected, and this is the most important argument in the ADR because the alternative looks correct.

**It has a race that loses rows.** Two sessions inserting concurrently commit in an order unrelated to
their timestamps. Session A stamps `10:00:05` and commits at `10:00:09`; session B stamps `10:00:07` and
commits at `10:00:08`. A reader at `10:00:08` sees B, advances the mark to `10:00:07`, and A's row —
committed one second later with an earlier timestamp — is **never visible to any future query**. It is
lost silently, the loss is load-dependent so it does not reproduce, and there is nothing in any log.

Mitigations exist and all are worse: overlap the window and deduplicate (which needs a deduplication key
and turns a delta read into a scan), or serialise inserts (which sacrifices ingestion throughput to work
around a bookkeeping problem).

A stream tracks a **transactional offset**, not a timestamp. Rows become part of the delta when their
transaction commits, so the ordering problem cannot arise. This is the single reason streams were chosen.

### dbt's own incremental `is_incremental()` pattern

dbt can filter on a max timestamp from the target table, which is the standard incremental idiom, and
it is used *within* the dbt layer for the fact models.

Rejected at the ingestion boundary because it has the same race as the manual high-water mark — it is
the same technique — and because it couples dbt to the physical shape of `RAW`. With the queue table as
the contract, the ingestion mechanism can change completely (Snowpipe, Snowpipe Streaming, a Kafka
connector, an external table) without a single dbt model changing. That contract is what makes the
architectural change at 10,000× volume tractable rather than a rewrite.

### An external queue — Kafka, SQS

Rejected as a component with an operational cost and no benefit here. The upstream produces files
periodically; a broker would be introduced solely to move data that is already inside Snowflake from one
Snowflake table to another.

### `CHANGES` clause / Time Travel diffing

`SELECT ... FROM t CHANGES (INFORMATION => APPEND_ONLY) AT (TIMESTAMP => ...)` gives a stream-like delta
without a stream object. Rejected because the caller then owns the offset — storing it, advancing it,
and making the advance atomic with the consumption. That is exactly the bookkeeping a stream does
correctly, and reimplementing it is how the high-water-mark race gets reintroduced by accident.

### Full reprocessing every run

Honestly considered, because it is the simplest correct thing and at 5,000 events per batch it would
work. Rejected because it does not survive growth, and the point of the design is to be defensible at
volume. It also makes adjudication non-idempotent in an awkward way: re-adjudicating an event that was
previously rejected under a since-corrected rule would silently change history, whereas the append-only
queue makes reprocessing a deliberate act.

## Consequences

### Good

- **The offset is transactional, so no row can be missed.** The property that motivated the whole
  decision.
- **The drain is atomic.** The insert into the queue, the batch record, and the offset advance commit
  together. A failed drain leaves the offset exactly where it was — nothing half-consumed, nothing lost —
  so **re-running is always safe**. This single property is why most of the runbook says "retry it", and
  why an operator never needs to establish what a failed attempt managed to complete.
- **Cost is proportional to the delta, not the table.** `RAW.TRADE_EVENT` at ten billion rows drains no
  more slowly than at ten thousand.
- **Free polling.** `SYSTEM$STREAM_HAS_DATA` lets the task run every minute and skip without starting
  compute, so change capture keeps up with arrivals for effectively nothing. It also means the queue is
  already drained by the time the hourly transform starts.
- **Change capture survives Airflow being down**, because the Snowflake Task keeps draining. Had the
  drain lived only in the DAG, an Airflow outage longer than Time Travel retention would be
  unrecoverable loss.
- **Selecting from a stream does not advance it**, so `VW_STREAM_LAG` can be inspected freely during an
  incident without side effects — which is not true of most delta mechanisms.
- `APPEND_ONLY = TRUE` is cheaper than a standard stream and correct here, since the source is
  insert-only by design.

### Bad

- **A stream goes stale if not consumed within the source table's Time Travel retention** (14 days
  here), and past that point the delta is **unrecoverable from the stream**. This is the one failure in
  the platform that a retry cannot fix, and it is the genuine cost of the decision. Mitigated by three
  things: the task drains every minute, `VW_STREAM_LAG` reports lag against the limit explicitly, and
  the runbook has a tested recovery procedure with a backfill step.
- **`CREATE OR REPLACE` on the stream resets the offset**, which with `SHOW_INITIAL_ROWS = TRUE` would
  re-emit every row in the table. So the stream is the single exception to the deployment scripts'
  otherwise uniform `CREATE OR REPLACE` style — it uses `CREATE STREAM IF NOT EXISTS`. This looks like an
  inconsistency and is commented at length, because someone will eventually try to make it uniform.
- **Two things drain the same stream** (the task and the Airflow step), which needs explaining to anyone
  reading the DAG. It is safe because the procedure is transactional, and it is deliberate: the task
  keeps up continuously, the Airflow step guarantees the queue is current before the transform begins.
- **One more table.** The queue duplicates rows that are already in `RAW.TRADE_EVENT` until pruned at 7
  days. That is storage spent on decoupling, and it is worth it.

### Neutral

- The queue is pruned to 7 days by `TASK_PRUNE_TRADE_EVENT_QUEUE`, chained *after* the drain rather than
  independently scheduled, so pruning can never run concurrently with the insert that fills it.

## Notes

`SHOW_INITIAL_ROWS = TRUE` on first creation is what makes the initial deployment work: rows already in
`TRADE_EVENT` when the stream is created are included, so a stream created after a first load does not
skip that load.

It is also precisely why recreating the stream is dangerous, and why the recovery procedure in the
runbook recreates it with `SHOW_INITIAL_ROWS = FALSE` and backfills the gap explicitly against a
recorded high-water mark. The same flag is correct on day one and wrong on day two — worth knowing
before an incident rather than during one.
