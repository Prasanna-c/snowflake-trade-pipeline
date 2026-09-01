#!/usr/bin/env python3
"""
Verify that the repository does not lie about itself.

    python scripts/selfcheck.py

WHY
---
This codebase makes a lot of promises in prose. Airflow failure messages say "see
docs/runbook.md#data-quality-gate-tripped". The dashboard's empty states say "run
`make dbt-build`". The doctor says "make keypair". Every one of those is a promise that a
command or an anchor exists.

Prose rots faster than code, and it rots silently: renaming a Makefile target does not break
anything a test would notice, it just means that at 3am an on-call engineer pastes a command
which prints "No rule to make target". An error message that points nowhere is worse than an
error message with no suggestion, because it costs the reader time before they conclude the
docs are unreliable -- after which they stop reading them.

So the references are checked mechanically:

  1. every backticked or line-leading `make` command mentioned anywhere is a real target,
  2. every link into the docs directory resolves to a real file, and its anchor to a real
     heading in that file,
  3. every repository file path mentioned in a message exists,
  4. the dbt models the dashboard queries exist in the dbt project,
  5. the rejection rule codes in the seed match those the rule macro declares.

None of this needs a warehouse, so it runs in CI on every commit.

Exit codes: 0 clean, 1 at least one broken reference.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Where prose lives. Deliberately includes source files, because the highest-value references
#: are the ones inside error messages, not the ones in the README.
SEARCH_GLOBS = (
    "airflow/dags/**/*.py",
    "dashboard/**/*.py",
    "ingestion/src/**/*.py",
    "scripts/*.py",
    "dbt/models/**/*.sql",
    "dbt/models/**/*.yml",
    "dbt/macros/**/*.sql",
    "dbt/tests/**/*.sql",
    "docs/**/*.md",
    "snowflake/**/*.sql",
    "README.md",
    "Makefile",
)

EXCLUDE_PARTS = {"dbt_packages", "target", "node_modules", "__pycache__", ".venv"}

#: Two patterns rather than a bare `make\s+(\w+)`, because this codebase's comments are prose
#: and prose says "make sure", "make the loss visible", "make it harder to". A permissive
#: pattern reports dozens of those, and a check that cries wolf gets deleted rather than fixed.
#:
#: So a reference only counts when it is written the way a command is written: inside backticks,
#: or at the start of a line, optionally after a shell prompt or an arrow.
MAKE_PATTERNS = (
    re.compile(r"`make\s+([a-z][a-z0-9-]*)`"),
    re.compile(r"^\s*(?:[-#/*]*\s*)?(?:\$\s*|->\s*)?make\s+([a-z][a-z0-9-]*)\s*$"),
)

DOC_LINK_PATTERN = re.compile(r"(docs/[A-Za-z0-9_/-]+\.md)(#[A-Za-z0-9_-]+)?")
#: `tfvars` precedes `tf`, and the trailing lookahead stops the extension matching a prefix of a
#: longer one -- without it, `example.tfvars` is reported as a missing `example.tf`.
SCRIPT_PATTERN = re.compile(
    r"((?:scripts|dbt|airflow|terraform|snowflake|ingestion|dashboard)"
    r"/[A-Za-z0-9_./-]+\.(?:py|sql|yml|yaml|tfvars|tf|md|txt|csv))(?![A-Za-z0-9])"
)

#: Paths a reference may legitimately name even though they are absent from a fresh clone:
#: build output, and the two files the setup instructions tell the user to create from an
#: example. Flagging these would train people to ignore this check's output.
ALLOWED_MISSING_FRAGMENTS = (
    "target/",
    "dbt_packages/",
    "logs/",
    ".example",
    "dbt/profiles.yml",
    ".env",
    "terraform.tfvars",
)


def files_to_scan() -> list[Path]:
    seen: set[Path] = set()
    for pattern in SEARCH_GLOBS:
        for path in REPO_ROOT.glob(pattern):
            if not path.is_file():
                continue
            if EXCLUDE_PARTS & set(path.parts):
                continue
            seen.add(path)
    return sorted(seen)


def makefile_targets() -> set[str]:
    makefile = REPO_ROOT / "Makefile"
    if not makefile.is_file():
        raise SystemExit("no Makefile at the repo root")
    return {
        match.group(1)
        for match in re.finditer(
            r"^([a-zA-Z0-9_-]+):", makefile.read_text(encoding="utf-8"), re.MULTILINE
        )
    }


def anchors_in(markdown: Path) -> set[str]:
    """Slugs GitHub would generate for the headings in a markdown file.

    Approximates GitHub's algorithm: lower-case, strip anything that is not a word character,
    space or hyphen, then replace spaces with hyphens. Explicit `<a id="...">` anchors are also
    collected, since the runbook uses a few for stable links whose heading text may change.
    """
    text = markdown.read_text(encoding="utf-8")
    anchors: set[str] = set()

    for line in text.splitlines():
        if not line.startswith("#"):
            continue
        heading = line.lstrip("#").strip()
        slug = re.sub(r"[^\w\s-]", "", heading.lower())
        anchors.add(re.sub(r"\s+", "-", slug).strip("-"))

    anchors.update(re.findall(r'<a\s+(?:id|name)="([^"]+)"', text))
    return anchors


def check_make_targets(files: list[Path]) -> list[str]:
    targets = makefile_targets()
    problems: list[str] = []
    references: dict[str, list[str]] = defaultdict(list)

    for path in files:
        if path.name == "Makefile":
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for pattern in MAKE_PATTERNS:
                for match in pattern.finditer(line):
                    candidate = match.group(1)
                    if candidate in targets:
                        continue
                    references[candidate].append(f"{path.relative_to(REPO_ROOT)}:{line_number}")

    for target, locations in sorted(references.items()):
        problems.append(
            f"`make {target}` does not exist; referenced at {', '.join(locations[:4])}"
            + (f" and {len(locations) - 4} more" if len(locations) > 4 else "")
        )
    return problems


def check_doc_links(files: list[Path]) -> list[str]:
    problems: list[str] = []
    seen: set[tuple[str, str]] = set()

    for path in files:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in DOC_LINK_PATTERN.finditer(line):
                relative, anchor = match.group(1), (match.group(2) or "").lstrip("#")
                if (relative, anchor) in seen:
                    continue
                seen.add((relative, anchor))

                target = REPO_ROOT / relative
                where = f"{path.relative_to(REPO_ROOT)}:{line_number}"

                if not target.is_file():
                    problems.append(f"{relative} does not exist; referenced at {where}")
                    continue
                if anchor and anchor not in anchors_in(target):
                    problems.append(
                        f"{relative}#{anchor} has no matching heading; referenced at {where}"
                    )
    return problems


def _is_fragment_of_an_absolute_path(line: str, start: int) -> bool:
    """True when the match is the tail of an absolute path rather than a repo-relative one.

    A container path like `/opt/airflow/scripts/<name>.py` ends with a run of characters the
    pattern above matches -- `airflow/scripts/<name>.py` -- which exists nowhere on the host: the
    file lives under `scripts/` and reaches that path through a compose mount. Judging a container
    path needs the mount table, so the honest answer is to say nothing about it rather than to
    report a working command as broken.

    A leading `./` is still checked, being a repo path written long-hand.

    The examples above use a placeholder rather than a real filename because this check scans its
    own source, and a docstring is not exempt from it.
    """
    trailing_path_characters = re.search(r"[A-Za-z0-9_./-]*$", line[:start])
    return bool(trailing_path_characters) and trailing_path_characters.group(0).startswith("/")


def check_file_references(files: list[Path]) -> list[str]:
    problems: list[str] = []
    seen: set[str] = set()

    for path in files:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in SCRIPT_PATTERN.finditer(line):
                relative = match.group(1)
                if relative in seen:
                    continue
                if _is_fragment_of_an_absolute_path(line, match.start(1)):
                    continue
                seen.add(relative)
                if (REPO_ROOT / relative).exists():
                    continue
                if any(part in relative for part in ALLOWED_MISSING_FRAGMENTS):
                    continue
                problems.append(
                    f"{relative} does not exist; referenced at "
                    f"{path.relative_to(REPO_ROOT)}:{line_number}"
                )
    return problems


def check_dockerfile_copies_resolve() -> list[str]:
    """Every COPY source in the Airflow Dockerfile must exist relative to its build context.

    A COPY path is resolved against the context, not against the Dockerfile's own directory, and
    docker-compose.yml sets the context to the repository root so that `ingestion` is reachable.
    The two conventions look identical in the file, so `COPY requirements-airflow.txt` reads as
    correct while resolving to a path that does not exist.

    Nothing else catches this. The image is not built in CI -- it would add several minutes and a
    large download to every run for a container that only serves local development -- so the first
    evidence is a failed `make airflow-up` on someone's laptop, reported as "failed to compute
    cache key: not found", which does not obviously mean "wrong directory".
    """
    problems: list[str] = []
    compose_path = REPO_ROOT / "airflow" / "docker-compose.yml"
    dockerfile_path = REPO_ROOT / "airflow" / "Dockerfile"

    if not compose_path.exists() or not dockerfile_path.exists():
        return [f"{compose_path.name} or {dockerfile_path.name} is missing from airflow/"]

    # Read the context with a regex rather than a YAML parser, to keep this script runnable with
    # nothing installed -- it is the one check that must work in a bare checkout.
    context_match = re.search(
        r"^\s*context:\s*(\S+)", compose_path.read_text(encoding="utf-8"), re.MULTILINE
    )
    if context_match is None:
        return ["airflow/docker-compose.yml declares no build context"]
    context = (REPO_ROOT / "airflow" / context_match.group(1)).resolve()

    for line_number, line in enumerate(
        dockerfile_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip().upper().startswith("COPY "):
            continue
        # Drop flags such as --chown=airflow:root, then the destination: what remains is the
        # list of sources.
        words = [word for word in line.split()[1:] if not word.startswith("--")]
        for source in words[:-1]:
            if "*" in source or "?" in source:
                continue
            if not (context / source).exists():
                problems.append(
                    f"COPY {source} does not exist in the build context "
                    f"{context.relative_to(REPO_ROOT) if context != REPO_ROOT else '.'}; "
                    f"referenced at airflow/Dockerfile:{line_number}"
                )
    return problems


def check_dag_resolves_dbt_schemas() -> list[str]:
    """A DAG query naming a dbt schema literally must resolve the prefix instead.

    dbt prefixes its schemas everywhere except prod, so `{database}.intermediate.x` finds
    DBT_LOCAL_INTERMEDIATE on a laptop and fails with "Object does not exist or not authorized"
    -- while passing review, because the same line is correct in production. The prefix has to
    come from `settings.dbt_schema("intermediate")`.

    Three schemas are exempt: RAW and MONITORING, built by the versioned SQL layer and never
    prefixed, and INFORMATION_SCHEMA, which is Snowflake's own.
    """
    problems: list[str] = []
    never_prefixed = ("raw", "monitoring", "information_schema")
    literal_schema = re.compile(r"\{database\}\.(?!\{)([a-z_]+)\.")

    for path in sorted((REPO_ROOT / "airflow" / "dags").rglob("*.py")):
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in literal_schema.finditer(line):
                schema = match.group(1)
                if schema in never_prefixed:
                    continue
                problems.append(
                    f"{path.relative_to(REPO_ROOT)}:{line_number} names schema "
                    f"'{schema}' literally; use settings.dbt_schema('{schema}') so the "
                    f"dev prefix is applied"
                )
    return problems


def check_dashboard_reads_real_models() -> list[str]:
    """Every table the dashboard queries must be a real dbt model, seed or Snowflake view.

    Catches the failure mode the dashboard is otherwise blind to: a mart renamed in dbt leaves
    the dashboard querying a table that no longer exists, `run_query` catches the Snowflake
    error, and the panel renders blank. Blank reads as "no data", which is indistinguishable
    from a quiet trading day.
    """
    sys.path.insert(0, str(REPO_ROOT / "dashboard"))
    try:
        from lib import queries  # type: ignore[import-not-found]
    except ImportError as exc:
        return [f"could not import the dashboard's queries module: {exc}"]

    # Names the dbt project defines: models, seeds and snapshots.
    dbt_names = {
        path.stem.lower()
        for pattern in ("dbt/models/**/*.sql", "dbt/seeds/*.csv", "dbt/snapshots/*.sql")
        for path in REPO_ROOT.glob(pattern)
        if not path.stem.startswith("_")
    }
    # Names the Snowflake SQL layer defines.
    sql_text = "\n".join(
        path.read_text(encoding="utf-8") for path in REPO_ROOT.glob("snowflake/**/*.sql")
    )
    native_names = {
        name.lower()
        for name in re.findall(
            r"create\s+(?:or\s+replace\s+)?(?:table|view|stream)\s+(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_]*)",
            sql_text,
            re.IGNORECASE,
        )
    }
    # dbt's own audit table, created by an on-run-start hook rather than as a model.
    hook_names = {"dbt_run_result"}

    known = dbt_names | native_names | hook_names

    import inspect

    referenced: set[str] = set()
    for name in dir(queries):
        if name.startswith("_"):
            continue
        fn = getattr(queries, name)
        if not callable(fn):
            continue
        signature = inspect.signature(fn)
        if "database" not in signature.parameters:
            continue
        kwargs: dict[str, object] = {"database": "DB"}
        for param_name, param in signature.parameters.items():
            if param_name != "database" and param.default is inspect.Parameter.empty:
                kwargs[param_name] = 30
        sql = fn(**kwargs)
        # DB.SCHEMA.OBJECT, which is how every query in that module is written.
        referenced.update(obj.lower() for obj in re.findall(r"\bDB\.[a-z_]+\.([a-z_]+)\b", sql))

    missing = sorted(referenced - known)
    if missing:
        return [
            "the dashboard queries objects that no dbt model, seed or Snowflake view defines: "
            + ", ".join(missing)
        ]
    return []


def check_rule_catalogue_agrees() -> list[str]:
    """Seed and macro must declare the same rule codes.

    dbt asserts this too (tests/singular/assert_rule_catalogue_matches_macro.sql), but that
    needs a warehouse. Checking it here as well means the commonest form of the mistake --
    adding a rule to the macro and forgetting the seed -- fails in CI within seconds rather
    than at the first dbt build against a real account.
    """
    seed = REPO_ROOT / "dbt" / "seeds" / "ref_rejection_reason.csv"
    macro = REPO_ROOT / "dbt" / "macros" / "rules" / "trade_validation_rules.sql"
    if not seed.is_file() or not macro.is_file():
        return ["the rule seed or the rule macro is missing"]

    seed_codes = {
        line.split(",", 1)[0].strip()
        for line in seed.read_text(encoding="utf-8").splitlines()[1:]
        if line.strip()
    }
    macro_codes = set(
        re.findall(r"['\"]code['\"]\s*:\s*['\"](RJ\d+)['\"]", macro.read_text(encoding="utf-8"))
    )

    problems: list[str] = []
    if not macro_codes:
        problems.append(
            "no rule codes found in trade_validation_rules.sql -- the macro's shape has "
            "changed and this check needs updating"
        )
        return problems

    only_in_macro = sorted(macro_codes - seed_codes)
    only_in_seed = sorted(seed_codes - macro_codes)
    if only_in_macro:
        problems.append(
            f"declared in the rule macro but absent from ref_rejection_reason.csv: "
            f"{', '.join(only_in_macro)}. Rejections would carry a code with no human "
            "explanation."
        )
    if only_in_seed:
        problems.append(
            f"present in ref_rejection_reason.csv but not declared in the rule macro: "
            f"{', '.join(only_in_seed)}. These rules can never fire."
        )
    return problems


def main() -> int:
    print()
    print("\033[1mRepository selfcheck\033[0m")
    print(f"repo: {REPO_ROOT}")
    print()

    files = files_to_scan()
    print(f"scanning {len(files)} file(s)")
    print()

    checks = (
        ("make targets", lambda: check_make_targets(files)),
        ("doc links and anchors", lambda: check_doc_links(files)),
        ("file references", lambda: check_file_references(files)),
        ("dockerfile build context", check_dockerfile_copies_resolve),
        ("dag resolves dbt schemas", check_dag_resolves_dbt_schemas),
        ("dashboard reads real models", check_dashboard_reads_real_models),
        ("rule catalogue agreement", check_rule_catalogue_agrees),
    )

    total = 0
    for name, check in checks:
        problems = check()
        total += len(problems)
        if problems:
            print(f"[\033[31m fail \033[0m] {name:<32} {len(problems)} problem(s)")
            for problem in problems:
                print(f"         - {problem}")
        else:
            print(f"[\033[32m  ok  \033[0m] {name}")

    print()
    if total:
        print(f"\033[31m{total} broken reference(s).\033[0m")
        print(
            "Each one is a message, comment or doc that points somewhere that does not exist. "
            "Fix the reference or the thing it names."
        )
        return 1

    print("\033[32mEvery command, link and model reference in the repo resolves.\033[0m")
    return 0


if __name__ == "__main__":
    sys.exit(main())
