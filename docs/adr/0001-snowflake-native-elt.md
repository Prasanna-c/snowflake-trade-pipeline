# ADR 0001: Transform inside Snowflake with dbt, not in a separate compute engine

**Status:** Accepted
**Date:** 2026-08

---

## Context

Trade files arrive periodically and must be validated against business rules before affecting the
firm's record of its positions. The validation work is: cast a semi-structured payload to types, join
against four reference datasets, arbitrate versions with a window function, assign a verdict, and
merge the result into a fact table.

Snowflake was specified as the platform. The open question was *where the transformation runs* — the
decision every other choice in this project follows from.

## Decision

**ELT: land data in Snowflake unchanged, then transform it in SQL inside Snowflake, managed by dbt.**

No external compute engine. `RAW` holds the payload as `VARIANT`, and every transformation from there
to the marts is SQL executed by Snowflake, defined as a dbt model, and covered by dbt tests.

Python is used only where SQL is genuinely the wrong tool: generating data, orchestrating a sequence,
moving files, rendering a UI. **Nothing that decides whether a trade is valid is written in Python.**

## Alternatives considered

### Spark / EMR / Databricks for transformation

The conventional answer, and wrong here for a specific reason: the data is *already in Snowflake*, and
the transformation is *relational*. Choosing Spark would mean reading data out of the warehouse,
processing it on a separate cluster, and writing it back — paying network transfer twice — to perform
joins and window functions less well than a columnar warehouse performs them natively.

It would also add a cluster to size, tune, secure and upgrade; a second language for business logic;
and a second place for that logic to live. In exchange for nothing measurable.

Spark earns its keep on unstructured data, on ML feature engineering, on workloads needing libraries
with no SQL equivalent, and where compute must live outside the warehouse for cost or data-locality
reasons. None applies. Reaching for it anyway would be resume-driven design.

### ETL: validate in Python before loading

Superficially attractive — reject bad data at the door and keep the warehouse clean. Rejected because
it inverts the audit requirement.

A rejected event must be *explainable*, which means keeping the original message. If validation
happens before landing, the platform must build its own store of rejected payloads, its own audit
trail, and its own reprocessing path — reimplementing, in Python, what the warehouse already provides.
And it would still land *something*, so there would be two stores rather than one.

Worse, the version arbitration rules need to compare an incoming event against the *stored* state of
the trade. In Python that is a query back into Snowflake per batch, holding trade state in
application memory — a distributed-state problem invented for no reason.

Landing first makes an upstream schema change a *transformation* problem, discovered in dbt with the
original bytes still available, rather than an *ingestion failure that discards the evidence*.

### Snowflake stored procedures instead of dbt

Snowpark or JavaScript procedures could express the same logic, and would need no extra tool.
Rejected because procedures give none of the surrounding machinery: no dependency graph, no lineage,
no unit tests against mock data, no generated documentation, no `--store-failures`, no per-model
materialisation strategy. Those are the things that make the transformation *maintainable*, and for a
platform whose main requirement is provable correctness they matter more than the SQL itself.

### Snowflake Dynamic Tables instead of dbt

The closest call in the project, and the alternative most worth revisiting later. Dynamic Tables give
declarative incremental refresh with no orchestrator at all.

Rejected because they do not provide what this platform needs most: unit testing of business rules
against mock rows, a versioned declarative rule catalogue, generated documentation, or a way to
inspect the rows that failed an assertion. A platform that must *prove* correctness needs a testing
framework more than it needs automatic refresh.

They remain the right answer for the mart layer at high volume, where freshness matters more than
provability — see [`scalability.md`](../scalability.md#stage-4-10000--50m-per-batch). The split would
be: dbt where correctness must be proved, Dynamic Tables where freshness must be maintained.

## Consequences

### Good

- **One place for business logic**, in SQL, under test, reviewable in a pull request by anyone who
  reads SQL — which is a much larger group than those who read Spark.
- **Scaling is a configuration change.** XSMALL to 4XLARGE is 128× the compute and touches no code.
  Most of the first three growth stages in [`scalability.md`](../scalability.md) are warehouse sizes
  and clustering keys. This is the single largest payoff of the decision.
- **No data movement**, so no transfer cost, no egress, no second copy to secure, and no window in
  which data exists outside the governed platform.
- **dbt's surrounding machinery comes free**: lineage, generated docs, 162 generic data tests, six singular
  tests, unit tests that run against mock rows with no warehouse at all.
- **One security boundary.** RBAC, masking policies and tags are enforced by Snowflake for every
  consumer, because every consumer is a Snowflake client. An external engine would need its own
  credentials and its own equivalent controls.

### Bad

- **Snowflake is a single point of failure**, including for the monitoring, since the monitoring runs
  inside Snowflake too. Accepted knowingly; the mitigation would be external uptime checking, and it
  is out of scope for a local deployment.
- **Vendor coupling is real.** The models use `VARIANT`, `try_to_*`, `array_construct_compact`,
  Streams and Tasks. Porting to BigQuery or Redshift would be a rewrite of the SQL, though not of the
  design — the layering, the rule book structure and the audit model would survive.
- **Compute cost is warehouse cost**, billed per second with a 60-second minimum. A trivial query on a
  suspended warehouse pays that minimum. Mitigated by workload-separated warehouses and short
  auto-suspend, and made visible by query tagging.
- **SQL is a poor language for complex procedural logic.** The rule engine works around this with a
  Jinja macro generating the CASE expressions, which is effective but means part of the logic is
  templating rather than SQL. Someone reading the compiled model sees generated code.

### Neutral

- dbt is a build tool, not a runtime. Something still has to invoke it, which is why an orchestrator
  is needed at all — see [ADR 0008](0008-orchestration-choice.md).

## Notes

The decision that makes this reversible is that **`RAW` is immutable and append-only**. Every table
downstream is derived, so a different transformation engine could be pointed at the same landing zone
without re-ingesting anything. Choosing where to transform is therefore a decision that can be
revisited; choosing what to keep is not, which is why the audit model
([ADR 0007](0007-append-only-audit.md)) got more design attention than the compute choice.
