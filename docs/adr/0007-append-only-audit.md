# ADR 0007: Nothing is deleted — an append-only audit model

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`overview.md`](../overview.md#auditability),
[`validation-logic.md`](../validation-logic.md#where-a-rejected-event-goes)

---

## Context

The stated requirement is to validate trades and reject invalid ones. The unstated requirement, which
matters more, is that the platform must be able to **prove what it did**.

"Why was this trade not in yesterday's position?" must be answerable. So must "why was this trade
rejected", "what did we know about it at 4pm on Tuesday", and "who changed the rule that rejected it".
In a regulated context, "the pipeline ran successfully" answers none of these.

Rejected data is the easiest thing in a pipeline to discard, and the most expensive to have discarded.

## Decision

**Nothing is ever deleted or updated. Every layer is append-only or merge-only, and rejected data is a
first-class output rather than a diagnostic byproduct.**

Concretely:

- **`RAW.TRADE_EVENT` is insert-only**, storing the payload as `VARIANT`. Never updated, never deleted
  from. 14-day Time Travel.
- **`AUDIT.FCT_TRADE_REJECTED`** holds every refused and superseded event **with `raw_payload` as it
  arrived**.
- **`AUDIT.TRADE_RULE_RESULT`** holds one row per (event, rule that fired) — including `WARN` hits on
  trades that were *accepted*.
- **`RAW.COPY_ERROR`** holds lines that never parsed, with the raw line, so events that never reached
  the rule engine are still evidenced.
- **`CORE.FCT_TRADE_VERSION`** keeps every accepted version rather than only the current one.
- **`SNAPSHOTS.SNP_TRADE`** historises state changes that have no corresponding event.
- **Adjudication verdicts are never revised.** A corrected rule does not retroactively change an old
  verdict; reprocessing is a deliberate, logged act.

## Alternatives considered

### Log rejection counts, discard rejected rows

The most common approach, and the cheapest. Rejected because a count is not an audit trail. "347 trades
were rejected for RJ008 yesterday" is a metric; it does not let anyone establish *which* trades, or what
was wrong with them, and so it does not let anyone fix the upstream. The team that needs to act cannot.

### Keep rejected rows but not the original payload

Store the typed columns and the rule code. Rejected because the cases that most need investigating are
exactly the ones where the typed columns are wrong or absent. `RJ008` fires *because* casting failed —
so the typed row is empty, and without the payload "RJ008 fired" is a dead end. The payload is the only
artefact that survives the failure it describes.

### Update rows in place as trades amend

The natural relational model: one row per trade, updated on amendment. Rejected because it destroys
history — "what did we know at 4pm" becomes unanswerable — and because `FCT_TRADE` is then the only
record, so a bug in adjudication is unrecoverable rather than merely inconvenient.

The chosen model keeps `FCT_TRADE` as the current-state table people query *and* `FCT_TRADE_VERSION` as
the ledger, with a singular test asserting the former is exactly the max-version projection of the
latter. The redundancy is checked rather than trusted.

### Retroactively re-adjudicate when a rule is corrected

Superficially the right thing: the rule was wrong, so fix the verdicts. Rejected because a verdict is a
**historical fact about what the platform decided at a point in time**. Silently changing it means a
report run last month cannot be reproduced, and the audit trail asserts something that was never true.

Reprocessing is supported, but as an explicit operation that re-queues events from RAW and produces a
*new* acceptance alongside the *original* rejection. An auditor sees both, which is what they want.

### Delete old data on a retention schedule

Rejected within the pipeline's own tables. Retention is applied only to `RAW.TRADE_EVENT_QUEUE` (7 days)
and to loaded stage files (3 days) — both of which are **transient copies** of data held durably
elsewhere, not the record itself.

## Consequences

### Good

- **Every rejection is investigable.** The payload, the rules that fired, the source file and the row
  number are all available. This is what turns a rejection report from a metric into something a capture
  team can act on.
- **Silent loss becomes detectable.** Because every event reaches exactly one of three destinations,
  `assert_no_event_is_silently_dropped` can assert it. Without an audit destination for rejections there
  would be nothing to compare against, and loss would be indistinguishable from valid rejection.
- **Everything downstream of RAW is reconstructible.** A bug in adjudication is fixed by correcting the
  model and rebuilding, not by going back to the source system. That is what makes `--full-refresh` a
  recovery tool rather than a data-loss event.
- **`WARN` becomes possible.** Recording rule hits independently of verdicts is what allows a rule to
  flag a trade without blocking it — which is what makes the `RJ018` limit-breach design work. Without
  per-rule logging, a rule could only reject or be invisible.
- **`rules_never_fired` is computable**, so a silently broken rule can be distinguished from a rule that
  is genuinely never violated.
- **Reprocessing shows both outcomes.** The original rejection and the later acceptance both appear,
  which is exactly the history an auditor asks for.

### Bad

- **Storage grows monotonically.** Every event is stored at least three times: the payload in RAW, the
  drained copy in the queue until pruned, and the verdict downstream. Real, and small relative to
  compute — but it is a cost that compounds, and at high volume it is why retention is tuned per schema
  (14 days on RAW, 1 day on the rebuildable intermediate layer) rather than set once globally.
- **Raw payloads are retained, including anything sensitive in them.** This is the significant
  consequence and it is a governance obligation rather than a storage one. Mitigated by tag-based
  masking and by `AUDIT` being readable only by roles that need it — but the honest statement is that a
  full payload sits in the warehouse, and in a real deployment its retention period would be a
  compliance decision, not an engineering one.
- **Amendment history makes queries more careful.** "Current position" must read `FCT_TRADE`, or filter
  `FCT_TRADE_VERSION` to the max version. Getting it wrong double-counts amended trades. Mitigated by
  `FCT_TRADE` existing precisely so the common query is simple.
- **A wrongly rejected event is not automatically reconsidered.** Correct, and it surprises people. The
  re-queue procedure is in the runbook.

### Neutral

- Time Travel and Fail-safe multiply the storage cost of the append-only tables, which is why retention
  is per-schema. Fail-safe is 7 days on permanent tables and not configurable, so it is a fixed
  multiplier to account for rather than a lever.

## Notes

The snapshot deserves a note, because it looks redundant next to `FCT_TRADE_VERSION` and is not.

The version ledger records every accepted **event**. But some state transitions have no event — most
importantly expiry, where a trade moves from LIVE to EXPIRED because a date passed rather than because
anyone sent a message. That transition appears nowhere in the ledger. `SNP_TRADE` is what captures it, so
"what did this trade look like on 3 March" is answerable across event-driven *and* time-driven changes.

The snapshot runs **after** the tests, deliberately. Snapshotting data that has just failed its tests
writes a bad state into immutable history, and SCD2 history cannot be tidied up afterwards without
destroying the audit trail that is its entire purpose. Ordering it after the tests is a one-line
decision in the DAG that would be very expensive to get wrong.

And `assert_snapshot_covers_material_columns` exists because an audit trail that silently stops covering
a column is worse than one that is obviously incomplete: nobody checks a control they believe is working.
