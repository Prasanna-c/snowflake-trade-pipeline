# ADR 0005: Business rules declared as data in one macro

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`validation-logic.md`](../validation-logic.md)

---

## Context

Nineteen business rules decide whether a trade event is accepted, rejected, or superseded. Each rule
needs to:

- be evaluated against the event,
- be recorded in the audit log when it fires, with its code and severity,
- appear in the published documentation,
- have a human-readable description available to whoever reads a rejection report,
- and be unit-testable.

The obvious implementation is a `CASE` expression in the adjudication model, with a matching `CASE`
elsewhere for the audit log, and a document listing the rules.

## Decision

**Declare every rule once, as data, in `dbt/macros/rules/trade_validation_rules.sql`. Generate
everything else from that declaration.**

Each rule is a dictionary:

```jinja
{%- do rules.append({
    'code': 'RJ003',
    'name': 'Maturity in the past',
    'severity': 'REJECT',
    'phase': 'FIELD',
    'requirement': 'R3',
    'condition': "t.maturity_date is not null and t.maturity_date < to_date('" ~ var('business_date') ~ "') and coalesce(t.action, '') <> 'CANCEL'"
}) -%}
```

From that list, `evaluate_rules(phase)` generates the `array_construct_compact(case when ... end, ...)`
expression, the audit rows are keyed on the same codes, and `rule_book_markdown()` renders the
documentation table into `dbt docs`.

Adding a rule is a four-line change in one file. No model SQL changes.

## Alternatives considered

### Hand-written `CASE` expressions in the model

The default approach. Rejected for three reasons.

**The rule set stops being reviewable.** Adding a rule means editing a large `CASE` expression, and the
diff shows restructured SQL rather than a rule. A reviewer asked "is this rule correct" has to read
around it, and logic errors survive review — which for a validation platform is the failure that matters
most.

**Rules drift out of the audit log.** With evaluation in one place and audit logging in another, the two
are kept in sync by discipline. Eventually a rule fires without being recorded, and the audit trail is
quietly incomplete. Generating both from one list makes that impossible by construction, not by care.

**The rule set is not queryable.** "Which rules are REJECT severity, and which requirement does each
satisfy?" should be a question you answer from a table, not by reading SQL.

### Rules in a seed table, evaluated dynamically

Store the conditions as rows and build SQL from them at run time. Genuinely attractive — rules become
data that a business user could edit without a deployment.

Rejected because it moves the rules outside version control. A rule change would not appear in a pull
request, could not be reviewed, and could not be tested before taking effect. For a platform whose
output is a regulated record, "who changed this rule and when" must be answerable from git.

The seed (`ref_rejection_reason.csv`) therefore holds *descriptions* — text that a report displays —
while the *logic* stays in the macro. A singular test asserts they describe the same set of codes with
the same severities.

### Rules in Python, applied before loading

Rejected with the ELT decision — see [ADR 0001](0001-snowflake-native-elt.md). It also does not solve
this problem, only relocates it: a list of Python predicates has the same audit-drift issue unless
generated from one declaration.

### `dbt_expectations` or a generic test framework

These express *assertions* — "this column is never null" — and the package is used for exactly that.
They cannot express "reject this row for this reason and log it", because a dbt test's output is
pass/fail, not a per-row verdict with a code. The rules are transformation logic, not tests.

### A rules engine — Drools, or JSON-configured

Rejected as disproportionate. Nineteen rules over one entity does not need an engine, and an engine
would put the logic outside SQL, where it could no longer be tested against mock rows with dbt's own
tooling.

## Consequences

### Good

- **One place to look**, and one place to change. The four-line diff to add a rule is the whole diff.
- **A rule cannot fire without being logged**, because the same list generates the evaluation and the
  audit rows. This is the property that makes the audit trail trustworthy rather than merely present.
- **Reviewable.** A pull request adding a rule shows the rule and its reasoning, not restructured SQL.
- **The reasoning is next to the rule.** Every non-obvious rule has a comment explaining why it is
  written that way — why `CANCEL` is exempt from `RJ003`, why `RJ018` is `WARN` rather than `REJECT`, why
  `RJ005` and `RJ016` are separate despite both concerning the counterparty. That reasoning is the part
  that gets lost, and it is why the list is built with `append()` rather than as a single literal: Jinja
  does not permit comments inside a tag, and a rule book whose reasoning cannot live next to the rule is
  one nobody maintains correctly.
- **Queryable metadata.** The `phase`, `severity` and `requirement` fields make the rule set analysable,
  and `requirement` gives a direct mapping from each stated requirement to its implementation.
- **Documentation is true by construction.** `rule_book_markdown()` renders into `dbt docs` from the same
  source, so it cannot describe a rule set that no longer exists.
- **All rules are evaluated, not short-circuited.** `array_construct_compact` collects every code that
  fired, so a capture team is told all four of its problems at once rather than discovering one per
  resubmission. That is a small compute cost for a large operational gain.

### Bad

- **The compiled SQL is generated**, so someone debugging reads `target/compiled/...` rather than the
  model. Mitigated by `make dbt-compile`, and by the generated SQL being simple and repetitive — but it
  is a genuine indirection, and the first thing to explain to a new reader.
- **Jinja string conditions are not validated until compile time.** A typo in a column name is a
  compilation error rather than a syntax error in an editor, and there is no autocomplete. `make
  dbt-parse` catches it in seconds without a warehouse, which is the mitigation.
- **The `t.` prefix convention is load-bearing.** `evaluate_rules` rewrites `t.` to the caller's alias,
  so a condition written without the prefix silently fails to be re-aliased. A convention enforced by
  nothing but consistency, and the sharpest edge in the design.
- **Two artefacts must agree** — the macro and the seed. The duplication is deliberate, since the seed
  serves reporting and the macro serves execution, and it is guarded by both `make selfcheck` (offline,
  instant) and a singular test (in-warehouse).

### Neutral

- The FIELD/STATE phase split is expressed as a field on each rule rather than as two separate lists.
  That keeps the whole catalogue readable in one place, at the cost of the phase being a value to check
  rather than a structural fact. Given the phase ordering is load-bearing for correctness — a rejected
  event must not influence version arbitration — making it visible on every rule is the better trade.

## Notes

The declarative structure paid for itself in a way that was not the original motivation.

`rules_never_fired` in the data quality scorecard exists only because the rule set is enumerable. A
declared rule with no recorded hit is either genuinely never violated or silently broken, and a passing
test suite looks identical in both cases. With rules as hand-written `CASE` branches there would be
nothing to enumerate and no way to compute this at all — the question could not be asked.

It is reported but excluded from the RAG rollup, since many rules legitimately never fire. It is a prompt
to investigate, not an alarm.
