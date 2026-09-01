#!/usr/bin/env python3
"""
Deploy the Snowflake-native SQL layer.

Applies the .sql files under snowflake/ in dependency order, substituting the
{{ placeholder }} tokens with values from the environment.

    python scripts/deploy_snowflake_sql.py --env dev                     # pre-dbt then post-dbt
    python scripts/deploy_snowflake_sql.py --env dev --phase pre-dbt     # safe on a new account
    python scripts/deploy_snowflake_sql.py --env dev --phase post-dbt    # after a dbt run
    python scripts/deploy_snowflake_sql.py --env dev --dry-run
    python scripts/deploy_snowflake_sql.py --only 30_monitoring

On a brand-new account the phases must straddle the first dbt run -- see PHASES below for
why. `make bootstrap` sequences that correctly; prefer it over calling this directly.

-------------------------------------------------------------------------------
WHY A PYTHON DEPLOYER AND NOT TERRAFORM, DBT OR SNOWSQL

Four candidates, and the boundary between them is the interesting design decision:

**Terraform** owns what has state worth tracking and destroying: warehouses, the database,
schemas, roles, grants, tags and masking policies. Those are long-lived, and Terraform's value
is that it will tell you what drifted and remove what you deleted from the config.

It is a poor fit for the objects here. `snowflake_task` and `snowflake_pipe` resources exist,
but the provider has historically lagged Snowflake's feature releases, and expressing a
multi-statement stored procedure body inside HCL means a heredoc containing SQL -- which loses
syntax highlighting, linting and reviewability, and gains nothing. Worse, a small change to a
task's SQL forces a destroy-and-recreate of the task tree, which on a suspended-resumed
dependency graph is genuinely awkward.

**dbt** owns transformations: things defined by a SELECT. Streams, pipes, tasks and alerts are
not selects. They could be forced in via `run-operation` macros, but then dbt's dependency
graph -- its entire reason for existing -- does not model them, and `dbt run` would create
objects that no model refs.

**SnowSQL** would work and needs no code. It is being retired in favour of the newer Snowflake
CLI, it has no templating, and it cannot easily give per-statement error reporting: a 400-line
script failing at statement 37 reports a line number in a file that has been through a shell
heredoc.

**This script**, ~250 lines, gives what the others cannot: templating so one directory serves
dev and prod, ordered application, per-statement error messages naming the file and the failing
statement, and a `--dry-run` that renders exactly what would be executed. The SQL stays as
reviewable .sql files with comments explaining each choice, which for an interview deliverable
is most of the point.

-------------------------------------------------------------------------------
IDEMPOTENCY

Every statement in snowflake/ is CREATE OR REPLACE, CREATE IF NOT EXISTS, or an ALTER that
converges. So this script can be run repeatedly and in CI. The one exception is deliberate and
handled below: `create or replace stream` resets the stream's offset, which would re-emit rows
already drained, so streams use CREATE IF NOT EXISTS and the script says so when it skips one.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = REPO_ROOT / "snowflake"

INGESTION_SRC = REPO_ROOT / "ingestion" / "src"
if INGESTION_SRC.is_dir() and str(INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(INGESTION_SRC))

#: Directories are applied in this order, and it is not alphabetical by accident -- the numeric
#: prefixes encode the dependency chain. Monitoring views select from RAW tables, alerts call
#: procedures created alongside them, and tasks consume a stream that must already exist.
#:
#: Lexical order alone is not sufficient, though, because the chain leaves this layer and
#: passes through dbt. MONITORING.VW_PIPELINE_SLA and three of the alerts select from
#: INTERMEDIATE, CORE and AUDIT objects that only dbt creates, and Snowflake validates a
#: view's SELECT at CREATE time -- so deploying them before the first dbt run fails with
#: "object does not exist". Hence phases rather than one ordered pass:
#:
#:   manual    Run once by hand as ACCOUNTADMIN, before Terraform. It creates the identity
#:             that automation subsequently uses, and you cannot bootstrap a chain of trust
#:             from inside the chain. This script never deploys it.
#:   pre-dbt   Depends only on what Terraform built. Safe on a brand-new account.
#:   post-dbt  Reads dbt-created objects, so it must follow at least one dbt run.
PHASES: dict[str, tuple[str, ...]] = {
    "manual": ("00_bootstrap",),
    "pre-dbt": ("10_ingestion", "20_streams_tasks"),
    "post-dbt": ("30_monitoring", "40_alerts"),
}

#: What `--phase all` means: everything this script is allowed to deploy, in dependency order.
#: It excludes `manual` by construction rather than by remembering to.
DEPLOYABLE_PHASES: tuple[str, ...] = ("pre-dbt", "post-dbt")

PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*([a-z_]+)\s*\}\}")


@dataclass
class Statement:
    """One SQL statement, with enough context to report a failure usefully."""

    sql: str
    file: Path
    index: int
    line: int

    @property
    def preview(self) -> str:
        """A one-line summary with leading comments stripped.

        These files carry long comment blocks above each object -- the comments are the design
        documentation -- so a naive preview of the first 110 characters shows only a row of
        dashes. Stripping comments makes `--dry-run` show what will actually execute, which is
        the entire reason to have a dry run.
        """
        body = re.sub(r"/\*.*?\*/", " ", self.sql, flags=re.DOTALL)
        body = re.sub(r"--[^\n]*", " ", body)
        collapsed = " ".join(body.split())
        return collapsed[:110] + ("..." if len(collapsed) > 110 else "")

    @property
    def location(self) -> str:
        return f"{self.file.relative_to(REPO_ROOT)}:{self.line} (statement {self.index})"


def build_context(env: str) -> dict[str, str]:
    """Resolve every placeholder the SQL can reference.

    Read from the environment rather than hard-coded so the same directory deploys to a dev and
    a prod account with no file changes -- which is what makes the SQL layer promotable rather
    than copy-pasted.
    """
    from trade_sim.config import snowflake_settings

    settings = snowflake_settings()

    context = {
        "env": env,
        "database": settings.database,
        "load_warehouse": settings.effective_load_warehouse,
        "transform_role": settings.role,
        # Alert notifications need an integration that must be created by an ACCOUNTADMIN and
        # verified by email. On a trial account it usually does not exist, so the default is a
        # name the deploy will report as missing rather than a silent no-op.
        "notification_integration": os.environ.get(
            "SNOWFLAKE_NOTIFICATION_INTEGRATION", "NI_TRADE_PIPELINE_EMAIL"
        ),
        "alert_email": os.environ.get("ALERT_EMAIL", ""),
        # Budget for the cost alert. A number, not a credit limit Snowflake enforces: the alert
        # warns, it does not cap, because a hard cap on a trading pipeline's warehouse is a way
        # to turn a cost problem into an outage.
        "daily_credit_budget": os.environ.get("SNOWFLAKE_DAILY_CREDIT_BUDGET", "5"),
    }

    # dbt does not write to the same schema names in every environment: prod uses the bare
    # layer name (CORE), while dev and CI prefix it with the target schema (DBT_LOCAL_CORE,
    # PR_412_CORE) so concurrent builds cannot collide. See
    # dbt/macros/utils/generate_schema_name.sql -- this must mirror it exactly.
    #
    # The monitoring views and alerts select from dbt-built objects, and Snowflake validates a
    # view's SELECT at creation time, so hard-coding the bare names would make the whole
    # post-dbt phase undeployable anywhere except prod.
    dbt_schema = os.environ.get("DBT_SCHEMA", "DBT_LOCAL").strip().upper()
    bare_schemas = env.strip().lower() in ("prod", "production")
    for layer in ("staging", "intermediate", "core", "reporting", "audit", "snapshots"):
        context[f"{layer}_schema"] = (
            layer.upper() if bare_schemas else f"{dbt_schema}_{layer.upper()}"
        )

    return context


def render(text: str, context: dict[str, str], file: Path) -> str:
    """Substitute placeholders, failing loudly on any that is unknown or empty.

    Deliberately not `str.format` or Jinja. `format` would choke on the many literal braces in
    Snowflake JSON paths and JavaScript procedure bodies, and Jinja would be a dependency and a
    second templating language in a project that already has dbt's. A single regex over an
    explicit whitelist of names is enough, and it can report the unknown name.
    """
    unknown: set[str] = set()
    empty: set[str] = set()

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in context:
            unknown.add(name)
            return match.group(0)
        value = context[name]
        if value == "":
            empty.add(name)
        return value

    rendered = PLACEHOLDER_PATTERN.sub(substitute, text)

    if unknown:
        raise SystemExit(
            f"{file.relative_to(REPO_ROOT)} references unknown placeholder(s): "
            f"{', '.join(sorted(unknown))}.\n"
            f"Known placeholders: {', '.join(sorted(context))}."
        )
    if empty:
        # An empty substitution produces syntactically valid but wrong SQL -- an alert with no
        # recipient, silently never notifying anyone. Failing here is much better than
        # discovering it during an incident.
        raise SystemExit(
            f"{file.relative_to(REPO_ROOT)} needs {', '.join(sorted(empty))}, which is empty.\n"
            "Set it in .env. For alert_email, set ALERT_EMAIL."
        )
    return rendered


def split_statements(sql: str, file: Path) -> list[Statement]:
    """Split a script into statements on semicolons, respecting SQL literals and comments.

    A naive `sql.split(";")` breaks on this project's actual content: the stored procedures
    contain semicolons inside their bodies, and `$$ ... $$`-quoted blocks contain many. Getting
    this wrong produces statements that are syntactically valid fragments, which fail with
    errors that look like the SQL is wrong.

    Handled: line comments, block comments, single-quoted strings with doubled-quote escapes,
    double-quoted identifiers, and `$$` dollar-quoted bodies.
    """
    statements: list[Statement] = []
    buffer: list[str] = []
    index = 1
    line_number = 1
    statement_start_line = 1

    position = 0
    length = len(sql)
    in_line_comment = False
    in_block_comment = False
    in_single_quote = False
    in_double_quote = False
    in_dollar_quote = False

    while position < length:
        char = sql[position]
        next_two = sql[position : position + 2]

        if char == "\n":
            line_number += 1

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            buffer.append(char)
            position += 1
            continue

        if in_block_comment:
            if next_two == "*/":
                in_block_comment = False
                buffer.append(next_two)
                position += 2
                continue
            buffer.append(char)
            position += 1
            continue

        if in_dollar_quote:
            if next_two == "$$":
                in_dollar_quote = False
                buffer.append(next_two)
                position += 2
                continue
            buffer.append(char)
            position += 1
            continue

        if in_single_quote:
            # '' inside a string is an escaped quote, not a terminator.
            if next_two == "''":
                buffer.append(next_two)
                position += 2
                continue
            if char == "'":
                in_single_quote = False
            buffer.append(char)
            position += 1
            continue

        if in_double_quote:
            if char == '"':
                in_double_quote = False
            buffer.append(char)
            position += 1
            continue

        # Not inside anything: look for the start of one, or a statement terminator.
        if next_two == "--":
            in_line_comment = True
            buffer.append(next_two)
            position += 2
            continue
        if next_two == "/*":
            in_block_comment = True
            buffer.append(next_two)
            position += 2
            continue
        if next_two == "$$":
            in_dollar_quote = True
            buffer.append(next_two)
            position += 2
            continue
        if char == "'":
            in_single_quote = True
            buffer.append(char)
            position += 1
            continue
        if char == '"':
            in_double_quote = True
            buffer.append(char)
            position += 1
            continue

        if char == ";":
            candidate = "".join(buffer).strip()
            if _is_executable(candidate):
                statements.append(
                    Statement(sql=candidate, file=file, index=index, line=statement_start_line)
                )
                index += 1
            buffer = []
            statement_start_line = line_number
            position += 1
            continue

        buffer.append(char)
        position += 1

    trailing = "".join(buffer).strip()
    if _is_executable(trailing):
        statements.append(
            Statement(sql=trailing, file=file, index=index, line=statement_start_line)
        )

    return statements


def _is_executable(candidate: str) -> bool:
    """True if the text contains SQL, not only comments and whitespace.

    Necessary because these files are heavily commented -- the comments are the design
    documentation -- so the text between two semicolons is frequently a comment block alone.
    """
    if not candidate:
        return False
    without_line_comments = re.sub(r"--[^\n]*", "", candidate)
    without_comments = re.sub(r"/\*.*?\*/", "", without_line_comments, flags=re.DOTALL)
    return bool(without_comments.strip())


def assert_every_directory_has_a_phase() -> None:
    """Fail if a directory under snowflake/ is not assigned to a phase.

    Without this check, adding snowflake/50_whatever/ would silently never deploy, and the
    failure mode is the worst kind: an object that exists in the repo, passes review, and is
    absent from the account.
    """
    assigned = {directory for directories in PHASES.values() for directory in directories}
    present = {p.name for p in SQL_ROOT.iterdir() if p.is_dir() and not p.name.startswith(".")}

    if unassigned := sorted(present - assigned):
        raise SystemExit(
            f"snowflake/ contains directories that no phase claims: {', '.join(unassigned)}.\n"
            f"Add them to PHASES in scripts/{Path(__file__).name}."
        )
    if missing := sorted(assigned - present):
        raise SystemExit(f"PHASES names directories that do not exist: {', '.join(missing)}.")


def discover(only: str | None, phase: str) -> list[Path]:
    if not SQL_ROOT.is_dir():
        raise SystemExit(f"no SQL directory at {SQL_ROOT}")

    assert_every_directory_has_a_phase()

    wanted = DEPLOYABLE_PHASES if phase == "all" else (phase,)
    files: list[Path] = []
    for phase_name in wanted:
        for directory in PHASES[phase_name]:
            files.extend(
                sorted(
                    path
                    for path in (SQL_ROOT / directory).rglob("*.sql")
                    # `._name.sql` is an AppleDouble sidecar: macOS writes one per file when
                    # archiving to a filesystem that cannot hold extended attributes, and it
                    # matches *.sql. Executing one sends binary metadata to Snowflake, which
                    # fails with a parse error against a file the reader assumes is real SQL.
                    # Cheap to skip, and the alternative is a genuinely mystifying deploy.
                    if not path.name.startswith("._")
                    # `*_reference.sql` files document a pattern rather than deploy it -- the
                    # external-stage example needs a real cloud account and ACCOUNTADMIN. They
                    # are worked examples for the reader, not part of any environment.
                    and "_reference" not in path.stem
                )
            )

    if only:
        files = [f for f in files if only in str(f.relative_to(SQL_ROOT))]
        if not files:
            raise SystemExit(f"--only {only!r} matched no files in phase {phase!r}")
    return files


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy the Snowflake-native SQL layer.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--env", default=os.environ.get("DBT_TARGET", "dev"))
    parser.add_argument(
        "--phase",
        default="all",
        choices=("all", *DEPLOYABLE_PHASES),
        help=(
            "Which dependency phase to apply. 'pre-dbt' needs only Terraform and is safe on a "
            "new account; 'post-dbt' reads dbt-created objects and must follow a dbt run; "
            "'all' (default) does both and suits redeploying an established account. The "
            "one-time manual ACCOUNTADMIN script in 00_bootstrap is never deployed."
        ),
    )
    parser.add_argument(
        "--only",
        help="Apply only files whose path contains this substring, e.g. 30_monitoring",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render and split the SQL, print what would run, connect to nothing.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help=(
            "Keep going after a failed statement. Off by default: the files are ordered by "
            "dependency, so continuing past a failure usually produces a cascade of errors "
            "that bury the real one."
        ),
    )
    args = parser.parse_args()

    files = discover(args.only, args.phase)
    context = build_context(args.env)

    print()
    print(f"Deploying the Snowflake SQL layer  env={args.env}  database={context['database']}")
    print(f"role={context['transform_role']}  warehouse={context['load_warehouse']}")
    print(f"phase={args.phase}  {len(files)} file(s) from {SQL_ROOT.relative_to(REPO_ROOT)}")
    if args.dry_run:
        print("\033[33mDRY RUN -- nothing will be executed\033[0m")
    print()

    # Render and split everything before executing anything. A placeholder typo in the last
    # file should not be discovered after the first ten have already been applied, leaving the
    # account half-deployed.
    plan: list[tuple[Path, list[Statement]]] = []
    for file in files:
        rendered = render(file.read_text(encoding="utf-8"), context, file)
        plan.append((file, split_statements(rendered, file)))

    total = sum(len(statements) for _, statements in plan)
    print(f"{total} statement(s) to apply")
    print()

    if args.dry_run:
        for file, statements in plan:
            print(f"\033[1m{file.relative_to(REPO_ROOT)}\033[0m  ({len(statements)} statements)")
            for statement in statements:
                print(f"  {statement.index:>3}. {statement.preview}")
            print()
        print("Dry run complete. Re-run without --dry-run to apply.")
        return 0

    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    failures: list[tuple[Statement, Exception]] = []
    applied = 0
    started = time.monotonic()

    session = SnowflakeSession(
        warehouse=context["load_warehouse"],
        query_tag_suffix=f"component=deploy|env={args.env}",
    )

    with session:
        for file, statements in plan:
            print(f"\033[1m{file.relative_to(REPO_ROOT)}\033[0m")
            for statement in statements:
                try:
                    session.execute(statement.sql)
                    applied += 1
                    print(f"  \033[32mok\033[0m   {statement.index:>3}. {statement.preview}")
                except Exception as exc:  # noqa: BLE001
                    failures.append((statement, exc))
                    print(f"  \033[31mFAIL\033[0m {statement.index:>3}. {statement.preview}")
                    print(f"       {exc}")
                    if not args.continue_on_error:
                        print()
                        print(f"\033[31mStopped at {statement.location}\033[0m")
                        print(_diagnose(exc))
                        return 1
            print()

    elapsed = time.monotonic() - started
    print(f"{applied} of {total} statement(s) applied in {elapsed:.1f}s")

    if failures:
        print()
        print(f"\033[31m{len(failures)} statement(s) failed:\033[0m")
        for statement, exc in failures:
            print(f"  {statement.location}: {exc}")
        return 1

    print("\033[32mSnowflake SQL layer deployed.\033[0m")
    print("Next: make dbt-build")
    return 0


def _diagnose(exc: Exception) -> str:
    """Translate the handful of errors this step actually produces into an instruction."""
    message = str(exc).lower()

    if "does not exist or not authorized" in message and "integration" in message:
        return (
            "-> The notification integration does not exist. Email alerts need one, created by "
            "an ACCOUNTADMIN and verified by clicking a link in an email:\n"
            "     CREATE NOTIFICATION INTEGRATION NI_TRADE_PIPELINE_EMAIL\n"
            "       TYPE = EMAIL ENABLED = TRUE\n"
            "       ALLOWED_RECIPIENTS = ('you@example.com');\n"
            "   Or deploy everything except the alerts: --only 30_monitoring"
        )
    if "insufficient privileges" in message or "not authorized" in message:
        return (
            "-> The role lacks a privilege this statement needs. Run `make tf-apply` to apply "
            "the grants, or check SNOWFLAKE_ROLE in .env is the functional role and not a "
            "read-only one."
        )
    if "no active warehouse" in message:
        return (
            "-> The role has no USAGE on the warehouse. `make tf-apply`, or:\n"
            "     GRANT USAGE ON WAREHOUSE <wh> TO ROLE <role>;"
        )
    if "object does not exist" in message:
        return (
            "-> A dependency has not been created. The files apply in numeric order for exactly "
            "this reason, so if you used --only, run the full deploy instead."
        )
    if "syntax error" in message:
        return (
            "-> A rendered statement is malformed. Run with --dry-run to see the exact SQL that "
            "would be sent; the likely cause is a statement split at the wrong semicolon inside "
            "a procedure body."
        )
    return "-> See docs/runbook.md for the deployment failure playbook."


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
