# Architecture decision records

Each record states the decision, the alternatives that were seriously considered, and the
consequences — including the bad ones. A record with no downsides listed is a record that has not
been thought about properly.

They are numbered but not chronological in importance. ADR 0001 is the one everything else follows
from.

| # | Decision | The alternative most worth arguing about |
| --- | --- | --- |
| [0001](0001-snowflake-native-elt.md) | Transform inside Snowflake with dbt, not a separate engine | Dynamic Tables instead of dbt |
| [0002](0002-terraform-scope.md) | Terraform owns lifecycle objects; SQL scripts own code | Everything in Terraform |
| [0003](0003-two-tier-rbac.md) | Two-tier RBAC: access roles and functional roles | Direct grants to users |
| [0004](0004-streams-and-tasks-for-cdc.md) | Streams for change capture, drained transactionally | A high-water-mark query |
| [0005](0005-rules-as-declarative-macro.md) | Business rules declared as data in one macro | Hand-written CASE expressions |
| [0006](0006-keypair-authentication.md) | Key pair authentication for service access | Username and password |
| [0007](0007-append-only-audit.md) | Nothing is deleted — append-only audit model | Log rejection counts, discard rows |
| [0008](0008-orchestration-choice.md) | Airflow for batch, Snowflake Tasks for continuous work | Snowflake Tasks for everything |
| [0009](0009-airflow-dbt-packaging.md) | A custom Airflow image with dbt inside it | `_PIP_ADDITIONAL_REQUIREMENTS` |
| [0010](0010-dashboard-choice.md) | Streamlit for the operations dashboard | Snowsight dashboards |

## The four decisions that carry the most weight

**[0001](0001-snowflake-native-elt.md) — ELT in Snowflake.** Every other choice follows. The data is
already in the warehouse and the transformation is relational, so moving it to Spark would pay
network transfer twice to do joins and window functions less well.

**[0004](0004-streams-and-tasks-for-cdc.md) — Streams, not a high-water mark.** The alternative looks
correct and loses rows: two sessions committing out of timestamp order means a row can become visible
after the mark has passed it, silently and unreproducibly. A stream tracks a transactional offset.

**[0005](0005-rules-as-declarative-macro.md) — Rules as data.** A rule cannot fire without being
logged, because the same declaration generates the evaluation and the audit rows. Correctness by
construction rather than by discipline.

**[0007](0007-append-only-audit.md) — Nothing is deleted.** The requirement behind the requirement.
The platform must prove what it did, and a rejection count cannot.

## Format

Context, Decision, Alternatives considered, Consequences (good / bad / neutral), Notes.

The **Alternatives considered** section is the load-bearing one. The purpose of these records is to
answer "why not X" for the X a reader will actually ask about, and to be honest about the cases where
X would have been reasonable.

## Adding one

Copy the structure of [0004](0004-streams-and-tasks-for-cdc.md) — it is the most complete example.
Take the next number, add a row to the table above, and if code or docs depend on the reasoning,
reference the ADR from there. `make selfcheck` will fail if a reference points at a file that does
not exist.
