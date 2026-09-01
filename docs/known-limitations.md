# Known limitations

Constraints this platform works around rather than solves. Each one was found by hitting it,
so each entry says what the symptom looks like — the point is that the next person recognises
it in minutes instead of hours.

---

## `IS NOT NULL` on a VARIANT path cannot find an absent field

**Symptom.** The reject rate sits near 90%, almost all of it `RJ008 Malformed payload`, and
`cast_failed_fields` names one optional field — for us `quantity` — on events whose product
never has that field. `AUDIT.FCT_TRADE_REJECTED` shows a payload that looks perfectly valid.

**Cause.** A VARIANT path has three outcomes where SQL has two:

| payload | `payload:quantity` | `IS NOT NULL` |
| --- | --- | --- |
| `{"quantity": 500}` | `500` | TRUE |
| `{"quantity": null}` | JSON null | TRUE |
| `{}` | SQL NULL | FALSE |

JSON null is a value, so `IS NOT NULL` says the field was sent, while `::varchar` over it
returns SQL NULL and every `try_to_*` cast therefore returns NULL too. `int_trade_event_typed`
derives "malformed" from precisely that pair of facts — present, yet NULL after casting — so an
explicitly-null optional field satisfies both halves. Pydantic serialises optional fields as
`"quantity": null` rather than omitting the key, so every FX and rates event, which cannot have
a quantity, was rejected for a defect the sender did not commit.

**What we do.** Presence is asked through `payload_has_value()`
(`dbt/macros/utils/payload_presence.sql`), which uses `IS_NULL_VALUE` to fold JSON null in with
absence. `json_null_counts_as_absent_not_as_a_failed_cast` in
`models/intermediate/_int_trade_event_typed__unit_tests.yml` pins all three outcomes.

**Why the tests missed it for so long.** Every adjudication unit test mocks
`int_trade_event_typed`, so the rules were verified against hand-written presence flags while
nothing verified the flags themselves. Mocking a layer on both sides of a boundary leaves the
boundary untested; the typed model now has unit tests of its own.

---

## A malformed line can cost the rest of the file

**Symptom.** `COPY_HISTORY` reports `status = 'Partially loaded'` with `row_parsed` far below the
line count in the file name, and `first_error_message` reads `Error parsing JSON: misplaced {`.

**Cause.** With `TYPE = 'JSON'`, Snowflake does not treat the newline as a record separator. It
scans the byte stream for complete JSON documents. A line containing an *unbalanced* object —
a mid-object truncation, say — therefore consumes the following line's opening brace, and the
parser can never resynchronise. Everything after the bad line is lost.

`ON_ERROR = 'CONTINUE'` does not help. It skips a bad *record*; here the damage is to the
*framing*, and there are no longer any record boundaries to skip to.

**What we do.** The generator's `unparseable_json` fault emits a line that is invalid JSON but
still structurally closed: quotes paired, braces balanced. `ON_ERROR = 'CONTINUE'` then rejects
that single line and the load continues, which is the behaviour the pipeline is meant to
demonstrate. See `_corrupt()` in `ingestion/src/trade_sim/generator.py`.

**What we do not do.** Survive a genuinely truncated file from an upstream producer. That needs
the newline to be authoritative, which means loading each line as text and parsing it in SQL:

```sql
file_format = (type = csv field_delimiter = none record_delimiter = '\n')
-- then: try_parse_json($1) as payload, with null marking a parse failure
```

That is the more robust design and the one to reach for at real volume. It is not here because
it moves parse-failure detection out of `COPY` and into the transform layer, which changes where
`RJ008` is raised and what reconciliation counts.

**Detection if it happens anyway.** Reconciliation catches it. The manifest records every event
the generator emitted, so events that never arrived are reported as missing. That check is the
reason a silent 99% data loss cannot pass as a green run.

---

## Rejected rows are captured per file, not per row

**Symptom.** Attempting `VALIDATE()` returns:

```
VALIDATE and VALIDATE_PIPE_LOAD do not support COPY with transform.
```

**Cause.** `VALIDATE()` is the only source of row-level load errors, and it refuses any `COPY`
that uses a transform — that is, `COPY INTO ... FROM (SELECT ...)`. Our `COPY` needs the
transform to attach `METADATA$FILENAME`, `METADATA$FILE_ROW_NUMBER` and the content key, which
are what make a loaded row traceable to its source line. The two requirements are mutually
exclusive.

Worth knowing en route: `VALIDATE()` reports errors by replaying the original statement's SQL
text, so it also cannot validate a `COPY` written with bind variables — the replayed text has
no bindings and `pattern = :p_pattern` comes back as `pattern =`, which does not parse. Both
paths are closed.

**What we do.** `sp_load_trade_files` writes one `RAW.COPY_ERROR` row per file that had errors,
built from the `RESULT_SCAN` of the `COPY`: `errors_seen` gives the true count of rejected rows,
and `first_error`, `first_error_line` and `first_error_column_name` identify the defect. That is
what an operator triages on, and unlike `COPY_HISTORY` it is not purged after 14 days.

**What is lost.** The verbatim bytes of each rejected line. To recover them, re-read the file as
text and test `TRY_PARSE_JSON` per line — a second pass, worth it only when a rejection needs
forensic reconstruction rather than a count and a reason.

---

## Table function arguments must be constants

**Symptom.**

```
Invalid argument [job_id must be specified as a constant string] for table function.
```

**Cause.** Some table functions reject bind variables outright, so a value only known at
runtime cannot be passed the obvious way. Confusingly this is not uniform:
`RESULT_SCAN(:v_query_id)` accepts a bind, while `VALIDATE(..., job_id => :v_query_id)` does not.

**What we do.** Where the argument must be a literal, build the statement as text and run it
with `EXECUTE IMMEDIATE`. Nothing in the pipeline currently needs this, since `RESULT_SCAN`
accepts binds — but it is the escape hatch when a table function refuses one.

---

## A subquery cannot aggregate against the row that contains it

**Symptom.**

```
002031 (42601): SQL compilation error:
Unsupported subquery type cannot be evaluated
```

**Cause.** Snowflake will correlate a subquery on equality, but not when the correlated column
appears in an aggregate comparison. `where not exists (select 1 from r where r.id = outer.id
group by r.id having count(*) >= outer.n)` is the shape it refuses: the `having` compares an
aggregate of the inner table against a column of the outer row.

**What we do.** Express it as a join and an aggregate instead, so the comparison happens between
two columns of one grouped result rather than across a correlation boundary.
`assert_version_history_has_no_gaps` counts the rejections falling inside each version gap that
way. The rewrite also reads better in a failure: both the required and the found count land in
the output, so the test says how much of the gap was explained rather than only that something
was unexplained.

Note that a plain `exists (select ... having ...)` with no outer reference is fine — the alert
definitions in `snowflake/40_alerts/02_alerts.sql` use it. The restriction is specifically about
correlation.

---

## Dev schemas carry a prefix

dbt prefixes every schema it builds outside prod with the target schema, so a laptop builds
`DBT_LOCAL_CORE` while production builds `CORE`. `RAW` and `MONITORING` are built by the
Snowflake-native SQL layer and keep their bare names everywhere.

Anything reading dbt-built objects must resolve this: `SnowflakeSettings.dbt_schema()` for
Python, the `{{ core_schema }}`-style placeholders for the SQL layer. Hand-written ad-hoc SQL
must spell the prefix out. Objects appearing absent in dev when they exist is almost always
this.

---

## Compose reads two different env files, at two different times

**Symptom.**

```
WARN[0000] The "SNOWFLAKE_ACCOUNT" variable is not set. Defaulting to a blank string.
```

repeated once per service, from `docker compose` in the `airflow/` directory.

**Cause.** `${VAR}` in a compose file is substituted while the file is being read, from the shell
environment and from `.env` beside the compose file. `env_file:` is different: it names a file
handed to the container at start, and Compose does not consult it for substitution. The repo's
`.env` is the second kind, so `${SNOWFLAKE_ACCOUNT}` never saw it.

The quiet half of this is worse than the warning. A key written in `environment:` overrides the
same key from `env_file:`, so `ALERT_EMAIL: "${ALERT_EMAIL:-}"` did not pass an address through --
it replaced whatever `.env` said with an empty string, and failure alerts would have gone nowhere
without a warning to say so.

**What we do.** `environment:` in `airflow/docker-compose.yml` now carries only values that must
differ inside the container -- the mounted key path, the mount paths -- and every operator setting
comes from `env_file: ../.env` alone. Defaults live in the code that reads each setting rather than
being restated in compose, so there is one default per setting instead of two that can disagree.

`AIRFLOW_UID` is the deliberate exception. It is used in `user:`, which is structure rather than
container environment, so it must be substituted at read time and therefore has to live in
`airflow/.env`. That is why setup writes it there and not to the repo `.env`.

---

## `VW_STREAM_LAG` assumes 14 days of retention

The staleness threshold is hardcoded at 14 days, but a trial account retains 1 day, so the view
can report a limit the account cannot honour. Cosmetic in dev; set `DATA_RETENTION_TIME_IN_DAYS`
to match before trusting it in prod.

---

## A same-version resend can only be arbitrated within one build

Business rule 2 says a resend of a version already held must overwrite it, not be rejected. The
audit half of that rule records the losing arrivals as `SUPERSEDED` under RJ009, and
`adjudicated_one_accepted_event_per_trade_version` asserts that exactly one arrival per
`(trade_id, trade_version)` survives as accepted.

Both halves hold within a build, because `intra_run_rank` can see every contender at once. Across
builds they cannot. If a version accepted last night is resent tonight, `FCT_TRADE` is correctly
overwritten, but the earlier arrival was already published to an append-only audit fact and there
is no honest way to retro-mark it as superseded — so the warehouse would hold two accepted events
for one version and the uniqueness assertion would fire on legitimate data.

Two consequences worth stating plainly:

- **The simulator confines same-version resends to a single batch.** `TradeGenerator` draws
  replacements only from trades booked in the current batch. Rule 2 is still exercised on every
  run; what is avoided is generating a case the platform cannot represent.
- **A production feed that does this needs effective-dating, not a stricter test.** The fix is an
  `is_current_arrival` flag or a validity interval on the audit fact, maintained by the merge, so
  supersession becomes a property of the row rather than of the build that wrote it. That is a
  schema change, deliberately out of scope here, and the assertion is left strict so the case
  cannot pass unnoticed.

## One environment, one writer

The Airflow DAG is scheduled hourly with `PIPELINE_SIMULATE_ARRIVALS=true`, so it generates its
own batch, loads it, drains the stream and builds the models. `make demo` does the same thing on
demand. Both point at one Snowflake database and, because `airflow/docker-compose.yml` mounts
`../data`, at one simulator state directory. Running them at the same time breaks two things at
once:

- **Two dbt runs, one incremental table.** A merge matches against committed rows, so two runs
  building `int_trade_event_adjudicated` concurrently both find an `event_sk` absent and both
  insert it. `unique_int_trade_event_adjudicated_event_sk` then fails, with the copies distinguished
  only by `adjudicated_at`. dbt has no cross-run locking and does not claim to.
- **Two generators, one trade book.** `TradeBook.exclusive()` now serialises this: the book is read,
  extended and written back under an advisory lock, so the second simulator continues the universe
  instead of forking it. Before that lock, both read the same `next_sequence` and minted the same
  identifiers, leaving the warehouse with two universes claiming one `(trade_id, trade_version)`.

The lock closes the second hole. The first is operational, and the rule is that the scheduler owns
the environment: pause the DAG before driving the pipeline by hand. `make pause-writers` does that
and suspends the drain task with it; `make resume-writers` puts both back. `max_active_runs=1` on
the DAG only ever protected the DAG from itself.

`dbt build --full-refresh` is the repair. Duplicate rows cannot be merged away afterwards, because
the merge key is exactly what is duplicated.

## The simulator's expectations depend on business-time ordering

Version arbitration orders a trade's events by `effective_event_ts`, not by arrival, because
upstream clocks are the authority on what happened first. The manifest that `trade-sim reconcile`
checks against is therefore only predictable if each trade's events are stamped in the order they
were emitted — an amendment stamped before its own booking is a sequence that cannot happen, and
the pipeline is right to reject the earlier version as stale even though the generator meant it to
be accepted.

`TradeBookEntry.last_event_ts` exists for that reason, and it advances on every emission including
rejected ones: a rejected event must not move the trade's *version*, but it did occur, so the next
event has to be stamped after it.

## Linting the SQL requires a warehouse

`sqlfluff` cannot run offline against this project. It lints rendered SQL, and rendering these
models means running dbt's Jinja: the rule catalogue is a macro that hands back a list of
dictionaries through dbt's `return()`, and the models loop over it to build their `CASE`
expressions. Plain Jinja has no `return()`, so the usual offline arrangement — `--templater jinja`,
no credentials, runs in a pre-commit hook — fails on every model that touches the rules with
`'return' is undefined`. Pointing sqlfluff at the macros directory does not help; it only changes
which error it stops on.

The dbt templater renders them correctly and needs a profile and a live connection to do it,
because dbt resolves relations through the adapter before it will compile an incremental model. So
SQL linting sits with the credentialed work: `make lint-sql` locally, and a step in the warehouse
tier of CI. `make lint` and `make ci-local` stay offline and cover Python only.

The alternative was to stop generating the rules from one definition, so that the SQL could be
linted without dbt. That trade — a duplicated rule catalogue, in exchange for a faster linter —
is not one worth making: the duplication is the failure mode the macro exists to prevent, and
`assert_rule_catalogue_matches_macro` exists to prove it has not crept back in.
