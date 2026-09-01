#!/usr/bin/env python3
"""
Import every Airflow DAG and fail on any error, without needing a running Airflow.

    python scripts/validate_dags.py

WHY THIS IS A SEPARATE CHECK FROM `ruff` AND `pytest`
----------------------------------------------------
Because a DAG file can be perfectly valid Python and still be broken. Ruff sees no problem with
a task that references an undefined upstream, a `schedule` string cron cannot parse, a task_id
duplicated inside a task group, or a cyclic dependency. Airflow discovers all of those at
parse time -- which, without this script, means discovering them in the Airflow UI as an
"Import Error" banner after a rebuild, minutes later.

WHAT IT CHECKS BEYOND "DOES IT IMPORT"
--------------------------------------
The import is necessary but weak. The additional assertions below encode operational
requirements that this project has deliberately chosen, so that a future edit which quietly
violates one fails here rather than in production:

  * every DAG has an owner and a non-empty description,
  * `catchup` is explicitly False (an accidental True on an hourly DAG with a 2026 start date
    would queue thousands of runs the moment it is unpaused),
  * `max_active_runs` is 1 (two concurrent runs both MERGE into FCT_TRADE on trade_id, and one
    run's amendment would be lost non-deterministically),
  * every task has retries configured, since every step in this pipeline is idempotent and
    therefore safe to retry -- a task with retries=0 is either a mistake or a deliberate
    exception that should be visible,
  * a failure callback exists, because a silent failure in an unattended pipeline is the worst
    available outcome.

Exit codes: 0 all good, 1 a DAG is broken or violates a requirement.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# Overridable so this can also run inside the Airflow container, where the dags folder is
# /opt/airflow/dags and there is no repository root above it. The compose file sets the variable.
DAGS_DIR = Path(os.environ.get("AIRFLOW_DAGS_DIR") or REPO_ROOT / "airflow" / "dags")
INGESTION_SRC = REPO_ROOT / "ingestion" / "src"

# The DAGs do `from utils import alerting`, which resolves because Airflow puts the dags folder
# on sys.path. Replicate that here so the import behaves as it does in the container.
for path in (DAGS_DIR, INGESTION_SRC):
    if path.is_dir() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

# A DagBag needs somewhere to put its metadata database and logs. Pointing AIRFLOW_HOME at a
# scratch directory keeps validation from writing into the repo or, worse, into a real Airflow
# home and touching a live metadata database.
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-validate")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")

# The DAGs read Snowflake settings at parse time via trade_sim.config. Placeholders let the
# import succeed with no real credentials, which is the point: DAG validation must run in CI.
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "validate.eu-central-1")
os.environ.setdefault("SNOWFLAKE_USER", "VALIDATE")
os.environ.setdefault("SNOWFLAKE_ROLE", "VALIDATE")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "VALIDATE")
os.environ.setdefault("SNOWFLAKE_DATABASE", "VALIDATE")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "validate")


def main() -> int:
    try:
        from airflow.models import DagBag
    except ImportError:
        print("Airflow is not installed in this interpreter, and it is not meant to be:")
        print("the DAGs run in the container, so the local virtualenv stays small.")
        # The container's working directory is /opt/airflow, where compose mounts this folder,
        # so the same relative path works there.
        print("-> Validate inside the container instead, where Airflow already exists:")
        print("     make airflow-shell")
        print("     python scripts/validate_dags.py")
        print("-> Or install Airflow locally, if you want the check without Docker:")
        print("     pip install 'apache-airflow==2.10.5' \\")
        print(
            "       --constraint 'https://raw.githubusercontent.com/apache/airflow/"
            "constraints-2.10.5/constraints-3.11.txt'"
        )
        # Not a failure of the DAGs. Returning 0 would hide a missing dependency in CI, and
        # returning 1 would report a DAG problem that does not exist, so this is its own code.
        return 2

    print()
    print(f"Validating DAGs in {DAGS_DIR.relative_to(REPO_ROOT)}")
    print()

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    if bag.import_errors:
        print(f"\033[31m{len(bag.import_errors)} import error(s):\033[0m")
        for filename, error in bag.import_errors.items():
            print(f"\n  \033[1m{filename}\033[0m")
            for line in str(error).splitlines():
                print(f"    {line}")
        return 1

    if not bag.dags:
        print("\033[31mNo DAGs found.\033[0m")
        print("-> A file in the dags folder must instantiate a DAG at module scope.")
        return 1

    problems: list[str] = []

    for dag_id, dag in sorted(bag.dags.items()):
        print(f"\033[1m{dag_id}\033[0m  {len(dag.tasks)} tasks  schedule={dag.schedule_interval!r}")

        if not dag.description:
            problems.append(f"{dag_id}: no description")
        owner = (dag.default_args or {}).get("owner") or dag.owner
        if not owner or owner == "airflow":
            problems.append(f"{dag_id}: owner is unset or left as the default 'airflow'")

        if dag.catchup:
            problems.append(
                f"{dag_id}: catchup is True. Re-adjudicating a historical window against "
                "today's state is meaningless, and an hourly DAG with a 2026 start date would "
                "queue thousands of runs when unpaused."
            )

        if dag.max_active_runs != 1:
            problems.append(
                f"{dag_id}: max_active_runs is {dag.max_active_runs}, not 1. Concurrent runs "
                "both MERGE into FCT_TRADE on trade_id; Snowflake would not error and one "
                "run's amendment would be lost non-deterministically."
            )

        has_failure_callback = bool(
            dag.default_args.get("on_failure_callback")
            or any(task.on_failure_callback for task in dag.tasks)
        )
        if not has_failure_callback:
            problems.append(
                f"{dag_id}: no on_failure_callback anywhere. An unattended pipeline that fails "
                "silently is the worst available outcome."
            )

        # Duplicate task ids inside groups, and tasks with no retry policy.
        seen: set[str] = set()
        for task in dag.tasks:
            if task.task_id in seen:
                problems.append(f"{dag_id}: duplicate task_id {task.task_id}")
            seen.add(task.task_id)

            # Sensors are the intended exception: a sensor that has timed out has already
            # waited its whole window, and retrying restarts it from zero, silently tripling
            # the effective SLA.
            is_sensor = "sensor" in type(task).__name__.lower() or task.task_id.startswith(
                "wait_for"
            )
            is_trivial = type(task).__name__ == "EmptyOperator"
            if task.retries == 0 and not is_sensor and not is_trivial:
                problems.append(
                    f"{dag_id}.{task.task_id}: retries=0. Every step in this pipeline is "
                    "idempotent, so retrying is safe and not retrying pages a human for "
                    "something that fixes itself."
                )

        # A cycle would already have raised on import in Airflow 2.x, but asserting it makes
        # the guarantee explicit rather than incidental.
        try:
            dag.topological_sort()
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{dag_id}: dependency graph is not a DAG: {exc}")

        roots = [task.task_id for task in dag.tasks if not task.upstream_list]
        leaves = [task.task_id for task in dag.tasks if not task.downstream_list]

        # The check that earns this script its place in `ci-local`.
        #
        # A `@task_group`-decorated function evaluates to whatever it returns -- typically the
        # group's last task -- not to the TaskGroup object. So `preflight >> ingest_group()`
        # attaches preflight to the group's *tail* and leaves its head with no upstream, which
        # means the group runs immediately and in parallel with everything before it. Nothing
        # errors, the graph view looks plausible, and the pipeline transforms data that has not
        # been loaded yet.
        #
        # Every task being reachable from `start` is the invariant that makes that visible,
        # and it costs one traversal.
        if "start" in {task.task_id for task in dag.tasks}:
            start_task = dag.get_task("start")
            reachable = {"start"}
            frontier = [start_task]
            while frontier:
                current = frontier.pop()
                for downstream in current.downstream_list:
                    if downstream.task_id not in reachable:
                        reachable.add(downstream.task_id)
                        frontier.append(downstream)

            orphans = sorted({task.task_id for task in dag.tasks} - reachable)
            if orphans:
                problems.append(
                    f"{dag_id}: {len(orphans)} task(s) are not downstream of `start` and will "
                    f"run unordered: {', '.join(orphans)}. The usual cause is chaining a "
                    "@task_group with `>>` from outside, which attaches to the group's last "
                    "task rather than its first."
                )

        print(f"  roots:  {', '.join(sorted(roots)) or '(none)'}")
        print(f"  leaves: {', '.join(sorted(leaves)) or '(none)'}")
        print()

    if problems:
        print(f"\033[31m{len(problems)} requirement violation(s):\033[0m")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(f"\033[32m{len(bag.dags)} DAG(s) valid.\033[0m")
    return 0


if __name__ == "__main__":
    sys.exit(main())
