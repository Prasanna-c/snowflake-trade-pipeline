# Validation logic

Every business rule the platform applies, what it does, and why it is written the way it is.

The rules are declared exactly once, in
[`dbt/macros/rules/trade_validation_rules.sql`](../dbt/macros/rules/trade_validation_rules.sql).
That file generates the evaluation SQL, the audit log columns, and the documentation table
published by `dbt docs`. This document explains the *reasoning*; the macro is the truth.

---

## Contents

- [The four required rules](#the-four-required-rules)
- [How adjudication works](#how-adjudication-works)
- [Why phases exist, and why the order is load-bearing](#why-phases-exist-and-why-the-order-is-load-bearing)
- [The three severities](#the-three-severities)
- [The full rule catalogue](#the-full-rule-catalogue)
- [Rule-by-rule reasoning](#rule-by-rule-reasoning)
- [Version arbitration in detail](#version-arbitration-in-detail)
- [Where a rejected event goes](#where-a-rejected-event-goes)
- [How the rules are tested](#how-the-rules-are-tested)
- [Adding a rule](#adding-a-rule)

---

## The four required rules

The case study specifies four rules. Each maps to a `requirement` tag on the rule declaration, so
the mapping from requirement to implementation is queryable rather than asserted in a document:

| Requirement | Stated rule | Implementation |
| --- | --- | --- |
| **R1** | Reject a version lower than one already accepted | `RJ001` Stale version |
| **R2** | On a same-version resend, the later arrival wins | `RJ009` Superseded within run, plus the arbitration logic |
| **R3** | Reject a trade whose maturity date has passed | `RJ003` Maturity in the past |
| **R4** | A trade reaching maturity becomes EXPIRED | The expiry sweep in `fct_trade`, not a rule |

**R4 is deliberately not a rule.** The other three are decisions about an *incoming event*; R4 is a
state transition driven by the passage of time, with no event to adjudicate. Implementing it as a
rule would mean a trade only expires if something happens to arrive and remind us — so a quiet
book would keep matured trades marked LIVE indefinitely. Instead `fct_trade` unions newly accepted
trades with existing trades whose maturity date has now passed, and re-derives the status on every
build.

That choice has a valuable side effect. Because the sweep runs on every build, a matured trade
still marked LIVE is positive proof that no build has completed since it matured. It is the
platform's most reliable staleness detector, and it is why `overdue_expiry_trades > 0` is the
first RED condition in the scorecard. See
[Expiry sweep failure](runbook.md#expiry-sweep-failure).

The remaining sixteen rules are tagged `OWN`: controls a real trade platform needs that the case
study did not enumerate. They are marked as such so a reviewer can separate "what was asked for"
from "what was added".

---

## How adjudication works

`int_trade_event_adjudicated` is where every rule is applied. It runs in four steps.

**1. Type the payload.** `int_trade_event_typed` casts the `VARIANT` into typed columns using
`try_to_date`, `try_to_number` and friends, and counts how many casts failed. A `try_` cast
returns NULL rather than raising, so one malformed field cannot abort the batch — but a NULL from
a failed cast is then indistinguishable from a field that was never sent, which is why the model
also records `cast_failure_count`. That count is what `RJ008` fires on, and it is what makes
"you did not send it" (`RJ004`) distinguishable from "you sent it in a format we cannot read"
(`RJ008`). Those two go to different teams. Asking whether a field was sent is itself subtle on
`VARIANT` — an explicit JSON null is a value as far as `IS NOT NULL` is concerned — so the
question goes through `payload_has_value()`; see [known limitations](known-limitations.md).

**2. Evaluate the FIELD phase.** Every rule decidable from one event plus reference data is
evaluated against the event joined to the seeds.

**3. Arbitrate versions.** Only field-valid events participate. This computes the effective prior
version and the intra-run ordering — see [below](#version-arbitration-in-detail).

**4. Evaluate the STATE phase and assign a verdict.** Rules comparing the event against stored
state are evaluated, and the union of everything that fired determines the verdict.

Every rule is evaluated; the model never short-circuits on the first failure. That costs a little
compute and buys something operationally significant: a trade capture team told all four of its
problems at once fixes them in one pass, rather than resubmitting four times and learning about one
new rejection each time. `violated_rule_codes` is therefore an array, not a single value, and
`primary_rule_code` is the highest-precedence REJECT-severity code in it for the common case where
one label is wanted — except on a superseded event, where it is `RJ009` regardless of what else
fired, so that the headline reason agrees with the disposition rather than reporting a supersession
as a rejection.

---

## Why phases exist, and why the order is load-bearing

This is the subtlest piece of logic in the project, and the one most worth understanding.

Rules are split into two phases:

- **FIELD** — decidable from one event plus reference data. Evaluated first.
- **STATE** — requires comparing the event against the stored state of the trade. Evaluated only
  for events that passed FIELD.

The ordering is not for efficiency. Consider what happens without it.

A malformed version 5 arrives — say its notional is negative, so `RJ007` fires. If version
arbitration ran over all events rather than only field-valid ones, that version 5 would set the
high-water mark to 5. A perfectly valid version 3 arriving next would then be rejected by `RJ001`
as stale — **on the authority of an event the platform had already refused**.

One bad message would poison the trade's entire subsequent history, and the resulting rejections
would each look individually correct. Nothing in the audit log would point at the real cause.
Filtering to field-valid events before computing the mark is what prevents it, and it is why the
phase split exists at all.

The general principle: a rejected event must have no influence on any later decision.

---

## The three severities

| Severity | Enters the golden record? | Logged where | Counts toward the reject-rate gate? |
| --- | --- | --- | --- |
| `REJECT` | No | `AUDIT.FCT_TRADE_REJECTED` | Yes |
| `WARN` | **Yes**, flagged | `AUDIT.TRADE_RULE_RESULT` | No |
| `SUPERSEDE` | No | `AUDIT.FCT_TRADE_REJECTED` | No |

**Why `WARN` exists at all.** Two of the rules describe conditions that are real problems but
where refusing the trade would make things worse rather than better. `RJ018`, a desk notional
limit breach, is the clearest case: reject it and the desk simply resubmits as three smaller
tickets, each individually under the limit. The breach then never appears anywhere, and the
position is larger than the limit was meant to allow. Accepting the trade with the breach recorded
against it keeps the risk visible, which is the actual objective. A control that people can route
around is not a control.

**`WARN` describes the rule, not the event.** A flagged event is accepted only if nothing else
refuses it. An over-limit notional carried by a version that has already been superseded by a later
one is still stale, and `RJ001` still rejects it; the `WARN` on the same event does not argue with
that. Severity is a property of each rule's own finding, and the verdict is the strongest finding
across all of them — never the severity of whichever rule happens to be most interesting.

**Why `SUPERSEDE` is separate from `REJECT`.** A superseded event was not wrong — it was replaced
by a later arrival of the same version, which is R2 working exactly as specified. Counting it as a
rejection would make the reject-rate gate fire on healthy amendment traffic, and the gate would be
turned off within a week. It is excluded from the numerator for that reason.

---

## The full rule catalogue

Generated from the macro. `dbt docs` renders this same table from the same source, so the
published documentation cannot drift from the code.

| Code | Rule | Severity | Phase | Req |
| --- | --- | --- | --- | --- |
| RJ001 | Stale version | REJECT | STATE | R1 |
| RJ002 | Maturity before trade date | REJECT | FIELD | OWN |
| RJ003 | Maturity in the past | REJECT | FIELD | R3 |
| RJ004 | Missing mandatory field | REJECT | FIELD | OWN |
| RJ005 | Unknown counterparty | REJECT | FIELD | OWN |
| RJ006 | Invalid currency | REJECT | FIELD | OWN |
| RJ007 | Non-positive notional | REJECT | FIELD | OWN |
| RJ008 | Malformed payload | REJECT | FIELD | OWN |
| RJ009 | Superseded within run | SUPERSEDE | STATE | R2 |
| RJ010 | Amendment after cancellation | REJECT | STATE | OWN |
| RJ011 | Unknown book | REJECT | FIELD | OWN |
| RJ012 | Settlement before trade date | REJECT | FIELD | OWN |
| RJ013 | Unsupported product | REJECT | FIELD | OWN |
| RJ014 | Trade date in the future | REJECT | FIELD | OWN |
| RJ015 | Invalid direction | REJECT | FIELD | OWN |
| RJ016 | Inactive counterparty | REJECT | FIELD | OWN |
| RJ017 | Non-deliverable currency on physical settlement | REJECT | FIELD | OWN |
| RJ018 | Desk notional limit breached | **WARN** | FIELD | OWN |
| RJ019 | Duplicate trade identifier | **WARN** | FIELD | OWN |

The human-readable descriptions live in `dbt/seeds/ref_rejection_reason.csv`, and
`assert_rule_catalogue_matches_macro` fails if the two ever disagree on the set of codes or their
severities. `make selfcheck` catches the same drift offline in under a second.

---

## Rule-by-rule reasoning

The rules with a non-obvious design decision behind them. The rest are self-explanatory from the
catalogue.

### RJ003 — Maturity in the past, and why CANCEL is exempt

```
t.maturity_date is not null
  and t.maturity_date < to_date('<business_date>')
  and coalesce(t.action, '') <> 'CANCEL'
```

The requirement says reject a trade whose maturity has passed. Applied literally, that would also
refuse a cancellation of a matured trade — and cancelling a trade that has already matured is a
legitimate correction. Refusing it would leave a wrongly-booked trade permanently in the book with
no way to withdraw it: the platform would have made a mistake uncorrectable.

The exemption is narrow and deliberate. `NEW` and `AMEND` on a matured trade are still refused.

Note `coalesce(t.action, '')` rather than `t.action <> 'CANCEL'`. With a NULL action the bare
comparison yields NULL, the rule does not fire, and a message missing its action field would slip
past. Every condition in the rule book is written NULL-safe for this reason: a null input should
trip the completeness rule `RJ004`, not silently satisfy every other rule because `null > 5` is
neither true nor false.

`business_date` is a dbt variable rather than `current_date()`. That is what makes the rules
testable — a unit test can pin the business date and assert a deterministic verdict — and it is
what allows a reprocessing run to be evaluated as of a stated date.

### RJ004 — Missing mandatory field, as one rule rather than ten

Ten mandatory fields, one rule code. The alternative — `RJ004a` through `RJ004j` — would triple
the catalogue and make the rejection report unreadable: a single truncated message would emit ten
near-identical rejections and dominate every count.

The specific fields are not lost. `missing_mandatory_fields` on the adjudicated row lists them, so
the report says "these fields are missing" once, with detail available.

### RJ005 vs RJ016 — unknown versus inactive counterparty

Both concern the counterparty and could have been one rule. They are separate because the
remediation differs completely: an unknown counterparty is a data problem, fixed by onboarding the
counterparty or correcting the identifier, and the trade can be resubmitted. An inactive
counterparty — defaulted, or in administration — is a credit control, and the resolution is
escalation to credit risk, not resubmission. Merging them would send half the cases to the wrong
team.

The same reasoning separates `RJ004` from `RJ008`.

### RJ007 — Non-positive notional

Direction is carried by `buy_sell`, so a negative notional is always an upstream mapping defect
where sign was encoded twice. Accepting it would double-count the sign and produce an exposure
figure with the wrong direction, which is worse than rejecting the trade because it is wrong
without looking wrong.

### RJ013 — Unsupported product as an authorisation boundary

`ref_product` is not merely a lookup table. A product absent from it is one the platform is not
permitted to process — a new asset class that has not been through risk approval. Treating the
seed as an allow-list means the platform fails closed on products it does not understand, which is
the correct default for anything touching a book.

### RJ017 — Non-deliverable currency on physical settlement

```
p.is_physically_settled = true
  and scur.currency_code is not null
  and scur.is_deliverable = false
```

A physically-settled product cannot settle in a non-deliverable currency; it has to be booked as
an NDF instead. Included as an example of a genuine cross-reference rule — it needs the product
dimension *and* the settlement currency dimension together, and it is the reason the adjudication
model joins the currency seed twice, once for notional and once for settlement.

### RJ019 — Duplicate trade identifier

The same UTI against two different `trade_id`s suggests a double booking. `WARN` rather than
`REJECT`: it is a reporting obligation more than a data quality failure, and which of the two
bookings is the wrong one cannot be determined from the message. Blocking would risk rejecting the
correct one.

---

## Version arbitration in detail

Requirements R1 and R2 together define which version of a trade wins. Three columns computed
during arbitration carry the whole answer.

### `effective_prior_version`

The greater of two things: the version already stored in `FCT_TRADE`, and the highest version
accepted **earlier in the same run**.

The second half is essential and easy to miss. A batch can contain versions 1, 2 and 3 of the same
trade. Comparing each only against the stored version would accept all three, and the golden
record would land on whichever the merge happened to apply last. Carrying the high-water mark
forward *within* the run makes the outcome deterministic and independent of row order.

Computed only over field-valid events, for the reason in
[Why phases exist](#why-phases-exist-and-why-the-order-is-load-bearing).

### `intra_run_rank`

For a same-version resend inside one batch, R2 says the later arrival wins. "Later" is ordered by
arrival sequence, not by any timestamp in the payload — a payload timestamp is set by the sender
and cannot be trusted to order two messages the sender itself may have emitted out of order.

Rank 1 is accepted; ranks above 1 fire `RJ009` and are recorded as `SUPERSEDED`. The losers are
logged rather than dropped, so the replacement is evidenced. "Why does this trade show notional
X when the file said Y?" is answerable.

### `prior_is_cancelled`

Cancellation is terminal, which `RJ010` enforces. Reinstating a cancelled trade requires a new
`trade_id`, because silently allowing an amendment to resurrect a cancelled trade would mean the
cancellation was never really a cancellation.

### Worked example

A batch arrives containing, in order: version 3 (notional negative), version 2, version 2 again,
version 4. `FCT_TRADE` currently holds version 2.

| Arrival | FIELD | Arbitration | STATE | Verdict |
| --- | --- | --- | --- | --- |
| v3, notional −5m | `RJ007` fires | Excluded — not field-valid | Not evaluated | **REJECTED** `RJ007` |
| v2 (first) | passes | prior = 2, rank 2 of 2 | `RJ009` | **SUPERSEDED** |
| v2 (second) | passes | prior = 2, rank 1 of 2 | `RJ001`: 2 < 2 is false | **ACCEPTED** |
| v4 | passes | prior = max(2, 2) = 2 | passes | **ACCEPTED** |

The golden record ends at version 4. Note that the bad version 3 did **not** set the mark to 3 —
had it done so, the valid version 2 would have been rejected as stale, and version 4 would have
been the only survivor of a batch in which three events were legitimate.

---

## Where a rejected event goes

Nothing is discarded. Four destinations, and which one an event reaches is itself diagnostic:

| Destination | What lands there |
| --- | --- |
| `CORE.FCT_TRADE` / `FCT_TRADE_VERSION` | Accepted events. The golden record and the version ledger |
| `AUDIT.FCT_TRADE_REJECTED` | Refused and superseded events, **with `raw_payload` as it arrived** |
| `AUDIT.TRADE_RULE_RESULT` | One row per (event, rule that fired) — including WARN hits on accepted trades |
| `RAW.COPY_ERROR` | Lines that never parsed, so never reached the rule engine at all |

Retaining `raw_payload` is what makes a rejection investigable. Without it, "RJ008 fired" is a
dead end; with it, the malformed bytes are in front of you.

`assert_no_event_is_silently_dropped` asserts that every event entering the queue reaches exactly
one of the first three. It is the most important test in the project: a violation means silent
data loss, which in a regulated pipeline is the worst outcome and the hardest to notice.

---

## How the rules are tested

Four independent layers, deliberately overlapping.

**1. dbt unit tests** —
[`_int_trade_event_adjudicated__unit_tests.yml`](../dbt/models/intermediate/_int_trade_event_adjudicated__unit_tests.yml).
Fixed input rows, asserted verdicts, no warehouse and no real data. `make dbt-unit-test` runs in
seconds, which is what makes it usable as the loop while writing a rule. Every requirement rule has
one, including the poisoned-high-water-mark case from above — the failure mode most likely to be
reintroduced by a well-meaning refactor.

**2. Generated faults and reconciliation.** The simulator injects specific faults and records, in
its `BatchManifest`, the verdict and rule code each event *must* receive. `make reconcile` compares
that ground truth against what the pipeline actually decided. This is the only check that can
detect an event which is **absent** — every other check validates rows that are present. See
[Reconciliation mismatch](runbook.md#reconciliation-mismatch).

**3. Singular tests.** Cross-model invariants: no event silently dropped, no gaps in version
history, `FCT_TRADE` is exactly the max-version projection of `FCT_TRADE_VERSION`.

**4. Catalogue agreement.** The macro and the seed must describe the same codes with the same
severities, enforced offline by `make selfcheck` and in-warehouse by
`assert_rule_catalogue_matches_macro`.

### The gap these do not close

A rule that is declared but never violated by any test or any real data is indistinguishable from a
rule that is silently broken — both produce zero hits and a green test suite. That is what
`rules_never_fired` in the scorecard exists to surface, and it is listed on the dashboard's
**Rule catalogue** section. It is a prompt to investigate, not an alarm; some rules genuinely
should never fire.

---

## Adding a rule

Four steps, in this order:

**1. Declare it** in `trade_validation_rules.sql`:

```jinja
{%- do rules.append({
    'code': 'RJ020',
    'name': 'Short name for reports',
    'severity': 'REJECT',
    'phase': 'FIELD',
    'requirement': 'OWN',
    'condition': 't.some_column is not null and t.some_column > bk.some_limit'
}) -%}
```

The condition must be TRUE **only** when the rule is violated, and must be NULL-safe. Guard every
comparison with an `is not null` check on the inputs, or add a `coalesce`.

**2. Describe it** in `dbt/seeds/ref_rejection_reason.csv`, with matching code and severity.
`make selfcheck` will tell you immediately if you forget.

**3. Add a unit test** in `_int_trade_event_adjudicated__unit_tests.yml` — one row that violates
the rule, one that does not.

**4. Run** `make dbt-unit-test && make selfcheck`. Neither needs a warehouse, so the whole loop is
seconds.

No model SQL changes. `evaluate_rules()` generates the CASE expression, the array, the audit rows
and the documentation from the declaration.

### Choosing the phase

Ask whether the rule needs the stored state of the trade. If it only needs the event and the
seeds, it is FIELD. Putting a FIELD rule in STATE is a real bug rather than a stylistic one: the
rule would then not be evaluated for events that already failed a FIELD rule, and — worse — the
event would have participated in setting the version high-water mark before being refused.

### Choosing the severity

`REJECT` if the event must not enter the golden record. `WARN` if the condition should be visible
but blocking it would push the problem somewhere less visible — the `RJ018` reasoning. If in doubt,
`REJECT`: nothing is deleted, and a wrongly rejected event can be re-queued from RAW. See
[Backfill and replay](runbook.md#backfill-and-replay).
