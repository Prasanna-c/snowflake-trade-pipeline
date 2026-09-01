# ADR 0010: Streamlit for the operations dashboard

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** `dashboard/app.py`

---

## Context

The pipeline needs a visual surface for three audiences that turn out to be one person:

- **Trade status** — volumes, lifecycle mix, exposure, maturities.
- **Rejections** — which rule is firing, whether it is one upstream system or all of them, and what
  the offending message actually looked like.
- **Pipeline health** — file arrival, load batches, stream lag, dbt runs, parse errors, cost.

In practice the reader is whoever is on call. The requirement is therefore not "a BI tool" but "a
diagnostic surface that ships with the pipeline and cannot disagree with it".

Constraints: it must run on a laptop with `pip install`, cost nothing, and be testable in CI.

## Decision

**Streamlit, in the same repository, reading only marts and monitoring views.**

Four pages — a landing scorecard, trade status, rejections, pipeline health — sharing one query
library (`dashboard/lib/queries.py`), one connection module that reuses `trade_sim.SnowflakeSession`,
and one set of presentation components.

Three constraints are enforced by tests rather than by convention:

1. **All SQL lives in `queries.py`.** No page composes SQL inline.
2. **Only `marts`, `reporting` and `monitoring` may be read.** A test parses every query and fails on
   a reference to `raw`, `staging` or `intermediate`.
3. **Every query helper must be exercised by a render test**, with fixture fragments proven
   unambiguous, so a query cannot silently rot.

## Alternatives considered

**Tableau or Power BI.** The right answer for business users, and what a real deployment would put in
front of a trading desk. Rejected here because it is licensed software that cannot be committed to a
repository, cannot be tested in CI, and cannot be installed by a reviewer with `make install`. The
marts are deliberately shaped so that pointing a BI tool at them later requires no change.

**Snowsight dashboards.** Genuinely appealing: no deployment, no dependencies, no Python. Rejected
because the dashboard definitions live in Snowflake rather than in git, so they are not
version-controlled, not reviewable in a pull request, and not reproducible on a fresh account. The
monitoring *views* are the reusable part, and Snowsight can be pointed at them by anyone who prefers
it.

**A Jupyter notebook.** Zero extra dependencies. Rejected because a notebook is a working document,
not an operational surface — cell execution order matters, output is stale by default, and nobody
should be running cells during an incident.

**Grafana.** Excellent at time series, awkward at the row-level drill-down that matters most here.
"Show me the raw payload of this rejected trade" is the single most useful thing the dashboard does,
and it is not a Grafana panel.

**A FastAPI service with a React front end.** Complete control, and a week of work to reproduce what
Streamlit gives in an afternoon. The dashboard is not the deliverable.

## Consequences

### Good

- **One repository, one test suite, one install.** The dashboard is versioned with the models it
  reads, so a mart rename breaks a test rather than a production panel.
- **It reuses the pipeline's own connection code**, so there is one place where Snowflake
  authentication, query tagging and warehouse selection are decided. A dashboard with its own
  connector would be a second thing to fix when authentication changes.
- **The marts-only rule prevents a second source of truth.** A dashboard querying `RAW` directly
  would eventually show numbers that disagree with the tested models, and the dashboard would win the
  argument because it is what people look at. Enforcing it in a test rather than a code review comment
  is what makes it hold.
- **Testable without a warehouse.** `AppTest` with mocked query responses renders every page in CI,
  so a broken page is caught before merge. This is the thing most dashboard implementations cannot
  do, and it is why the query-library indirection exists.
- Query tagging (`component=dashboard`) means dashboard cost is attributable and separable from
  pipeline cost in `VW_DBT_QUERY_PERFORMANCE`.

### Bad

- **Streamlit's API moves.** This project already hit `st.button(width=...)` not existing in 1.42 and
  `use_container_width` being superseded for charts. The mitigation is exact version pinning plus
  render tests in CI, which turns an API change into a failing test rather than a broken page — but
  it does mean an upgrade is a small piece of work rather than free.
- **It re-runs the whole script on every interaction.** Mitigated with `st.cache_resource` for the
  connection and `st.cache_data` with a TTL for query results. Without caching this design would
  issue a warehouse query per widget interaction, which is both slow and expensive.
- **Not built for many concurrent users.** Fine for an on-call engineer; wrong for fifty analysts.
  That is what the BI-tool-on-marts path is for, and the marts already support it.
- Another `requirements.txt` and another virtualenv concern, since the dashboard's `pyarrow` pin
  conflicts with the Snowflake connector's declared range. The conflict is benign — the connector
  only needs `pyarrow` for its pandas path — but it produces a warning on every run that has to be
  explained rather than silenced.

### Neutral

- Streamlit in Snowflake (SiS) would remove the local runtime entirely and is the obvious production
  path. It was not used here because it requires uploading the app to a stage, which puts the code
  outside git again — the same objection as Snowsight. The pages would port with minimal change.

## Notes

The design decision inside the dashboard that matters most is that the landing page states
**conclusions in words**, not just metrics. Expecting a tired engineer to notice that
`overdue_expiry_trades` reads 4 rather than 0 among five tiles is how incidents get missed. The
Findings section says "4 matured trades are still marked LIVE — no dbt build has completed since they
matured" and links to the runbook section.

The same reasoning drives the Rejections page being organised around three questions — which rule,
one source or all sources, what did the message look like — rather than around the shape of the
underlying tables. `is_concentrated` (over 80% of hits from one source system) is the single most
useful field on the page, because it distinguishes "an upstream release broke a feed" from "our
reference data is wrong", and those have opposite responses and identical total counts.
