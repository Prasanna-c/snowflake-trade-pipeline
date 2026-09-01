{% docs adjudication_pipeline %}

### The four-step pipeline

The order of these steps is load-bearing, not cosmetic.

**1. Field rules.** Everything decidable from a single event plus reference data: missing
fields, unknown counterparties, invalid currencies, non-positive notionals, impossible
dates, unsupported products. Evaluated first, and evaluated *completely* -- every rule is
checked rather than short-circuiting on the first failure, so an upstream team is told all
four of its problems at once instead of resubmitting four times.

**2. Deduplication.** Among field-valid events only, rank by `(trade_id, trade_version)`
ordered by business time, then arrival, then surrogate key. Rank 1 wins; the rest are
`SUPERSEDED` with code RJ009. This is business rule 2 — a same-version resend replaces the
stored trade — and the losers are recorded rather than discarded so the replacement is
evidenced.

**3. High-water mark.** For each event, the highest version already accepted for that trade:
the greater of what previous runs stored (read from the model's own table) and what earlier
arrivals in *this* run accepted (a window function over preceding rows only).

**4. State rules.** Version arbitration (business rule 1) and lifecycle checks
(amend-after-cancel), evaluated against the mark from step 3.

### Why step 1 must precede step 3

If a malformed version 5 were allowed to set the high-water mark, a subsequent perfectly
valid version 3 would be rejected as stale — on the authority of an event we discarded.
Restricting the mark to field-valid events is what prevents that, and it is the subtlest
piece of logic in the project. It is pinned by the unit test
`rule1_ignores_invalid_events_when_setting_the_mark`.

### Cases the window function handles correctly

| Stored | Batch contains | Outcome |
| ------ | -------------- | ------- |
| v2 | v3 then v5 | both accepted; v5 becomes current |
| v2 | v5 then v3 | v5 accepted; v3 rejected RJ001 |
| v3 | v3 | accepted as REPLACE (business rule 2) |
| v3 | v3 twice | later accepted as REPLACE; earlier SUPERSEDED |
| v2 | v9 invalid, then v4 | v9 rejected; v4 accepted |

### Idempotency

The incremental strategy is `merge` on `event_sk`, not `append`. Running the model twice
over the same input is a no-op, which is what makes automatic retry safe — and automatic
retry is the only reason the pipeline can be left unattended overnight.

{% enddocs %}


{% docs trade_id %}
Stable business identifier for a trade, in the form `TRD-` followed by nine digits.
Constant across every version of the trade. Trimmed on ingestion, because a trailing space
would silently create a second, parallel trade history.
{% enddocs %}


{% docs trade_version %}
Monotonically increasing version number, starting at 1. Business rules 1 and 2 turn
entirely on this field: a lower version is rejected, an equal version replaces.

Upstream systems do legitimately skip numbers, so a gap in accepted versions is a question
rather than a defect — see `assert_version_history_has_no_gaps`.
{% enddocs %}


{% docs verdict %}
The adjudication outcome for one event.

- `ACCEPTED` — enters the golden record.
- `REJECTED` — refused; logged to `AUDIT.FCT_TRADE_REJECTED` with its reason codes.
- `SUPERSEDED` — replaced by a later arrival of the same version, under business rule 2.

`SUPERSEDED` is deliberately not counted as a rejection anywhere. It is a rule working
correctly, and folding it into the reject rate would make the alerting thresholds fire on
healthy amendment traffic.
{% enddocs %}


{% docs lifecycle_status %}
The trade's current state, in precedence order `CANCELLED > EXPIRED > LIVE`.

- `LIVE` — open, maturity in the future or absent.
- `EXPIRED` — maturity date has passed (business rule 4).
- `CANCELLED` — withdrawn. Terminal: an amendment after cancellation is rejected with RJ010.

A cancelled trade that later passes its maturity date stays `CANCELLED` — it was withdrawn,
and it never matured.
{% enddocs %}


{% docs notional_amount %}
Trade size in the notional currency. **Always positive.** Direction is carried by
`buy_sell`, never by the sign of the notional; a negative value is always an upstream
mapping defect and is rejected with RJ007.

Use `signed_notional_amount` for net exposure, which applies the sign from `buy_sell` once,
centrally, so that every downstream query cannot get it wrong differently.
{% enddocs %}


{% docs event_sk %}
Arrival surrogate key assigned by `RAW.TRADE_EVENT`. Two properties matter: it is the merge
key that makes reprocessing idempotent, and it is the final deterministic tie-break when
ordering events whose business timestamps are identical.

It is not gap-free and carries no business meaning. Do not use it to infer order beyond
arrival sequence.
{% enddocs %}


{% docs raw_payload %}
The event exactly as received, stored as a VARIANT and never modified.

Retained all the way through to `AUDIT.FCT_TRADE_REJECTED` because the first question asked
about a rejected trade is "what exactly did you receive?" — not "what did your parser make
of it?". Answering that by joining back to `RAW` months later is slow and, once `RAW` is
archived, sometimes impossible.
{% enddocs %}


{% docs batch_seq %}
Monotonic sequence assigned by each stream drain, giving a total order across batches.

Two things depend on it: the incremental watermark, and the event ordering used by version
arbitration. A gap or a repeat would therefore be a correctness problem rather than a
cosmetic one, which is why it is tested for uniqueness at the source.
{% enddocs %}
