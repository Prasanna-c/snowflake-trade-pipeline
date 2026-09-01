#!/usr/bin/env python3
"""
Diagnose the local setup, in the order things go wrong.

WHY THIS SCRIPT EXISTS
----------------------
Every failure this project can produce on a fresh machine has the same two properties: it is
one of about a dozen known causes, and the error message it produces names none of them.
`snowflake.connector.errors.DatabaseError: 250001 (08001)` is technically accurate and tells a
newcomer nothing.

So the checks run in dependency order and stop being useful to run further once one fails --
there is no point testing a Snowflake role when the private key does not parse. Each check
reports what it found, what it expected, and the exact command that fixes it.

The ordering is the design. Checking connectivity first and file layout last would mean the
commonest problem (a missing .env) surfaces as a connection error.

Exit codes:
    0  everything required passed
    1  at least one required check failed
    2  the script could not run at all (wrong Python, missing dependency)
"""

from __future__ import annotations

import importlib.util
import os
import platform
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Make the simulator importable whether or not it has been pip-installed. `doctor` must work
# *before* `make install` has succeeded, since diagnosing a failed install is one of its jobs.
INGESTION_SRC = REPO_ROOT / "ingestion" / "src"
if INGESTION_SRC.is_dir() and str(INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(INGESTION_SRC))

MIN_PYTHON = (3, 11)


class Status(Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class Result:
    status: Status
    detail: str
    #: Shown only on WARN or FAIL. The literal command to run, not a description of it: the
    #: difference between "configure your credentials" and a line that can be pasted.
    fix: str = ""
    #: Extra lines printed indented under the detail. For values worth seeing even on success,
    #: such as which account and role were actually reached.
    notes: list[str] = field(default_factory=list)


ICONS = {
    Status.OK: "\033[32m  ok  \033[0m",
    Status.WARN: "\033[33m warn \033[0m",
    Status.FAIL: "\033[31m fail \033[0m",
    Status.SKIP: "\033[90m skip \033[0m",
}


class Doctor:
    def __init__(self) -> None:
        self.failed = 0
        self.warned = 0
        #: Set once a failure makes later checks meaningless, so they report SKIP with a reason
        #: instead of a second, misleading error.
        self.blocked_reason: str | None = None

    def check(self, name: str, fn: Callable[[], Result]) -> Result:
        if self.blocked_reason:
            result = Result(Status.SKIP, f"skipped: {self.blocked_reason}")
        else:
            try:
                result = fn()
            except Exception as exc:  # noqa: BLE001
                # A check that raises is a bug in the check, not in the user's setup. Say so
                # explicitly rather than letting a traceback imply their environment is broken.
                result = Result(
                    Status.FAIL,
                    f"the check itself raised {type(exc).__name__}: {exc}",
                    fix="This is a bug in scripts/doctor.py; the underlying setup may be fine.",
                )

        print(f"[{ICONS[result.status]}] {name:<34} {result.detail}")
        for note in result.notes:
            print(f"                                        {note}")
        if result.status in (Status.FAIL, Status.WARN) and result.fix:
            for line in result.fix.splitlines():
                print(f"         \033[36m->\033[0m {line}")

        if result.status is Status.FAIL:
            self.failed += 1
        elif result.status is Status.WARN:
            self.warned += 1
        return result

    def block(self, reason: str) -> None:
        self.blocked_reason = reason


# ===========================================================================
# 1. Toolchain
# ===========================================================================
def check_python() -> Result:
    version = sys.version_info
    label = f"{version.major}.{version.minor}.{version.micro} on {platform.machine()}"
    if version[:2] < MIN_PYTHON:
        return Result(
            Status.FAIL,
            f"{label} -- too old",
            fix=(
                f"Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ is required. The codebase uses "
                "`X | None` unions and `tomllib`.\n"
                "Install with: brew install python@3.11   (macOS)\n"
                "Then: rm -rf .venv && make install"
            ),
        )
    return Result(Status.OK, label)


def check_venv() -> Result:
    """Warn if running outside the project virtualenv.

    Not a failure: `python scripts/doctor.py` with a system interpreter is a reasonable thing
    to do. But a mismatch here explains a large share of "it works for me" reports, so it is
    worth naming.
    """
    venv = REPO_ROOT / ".venv"
    running_in_venv = sys.prefix != sys.base_prefix
    expected = str(venv.resolve())

    if not venv.is_dir():
        return Result(
            Status.WARN,
            "no .venv in the repo",
            fix="make install",
        )
    if not running_in_venv:
        return Result(
            Status.WARN,
            "running outside a virtualenv",
            fix=f"source {venv}/bin/activate   (or use `make doctor`, which does this for you)",
        )
    if not sys.prefix.startswith(expected):
        return Result(
            Status.WARN,
            f"a different virtualenv is active: {sys.prefix}",
            fix=f"source {venv}/bin/activate",
        )
    return Result(Status.OK, "project virtualenv active")


def check_python_packages() -> Result:
    """Confirm the imports the pipeline actually needs are importable.

    Checked by import rather than by reading requirements.txt, because a pinned line in a
    requirements file proves nothing about what is installed in the interpreter now running.
    """
    required = {
        "snowflake.connector": "snowflake-connector-python",
        "cryptography": "cryptography",
        "pydantic": "pydantic",
        "pydantic_settings": "pydantic-settings",
        "typer": "typer",
        "faker": "Faker",
        "trade_sim": "the local trade-sim package (pip install -e ./ingestion)",
    }
    missing = [
        dist for module, dist in required.items() if importlib.util.find_spec(module) is None
    ]
    if missing:
        return Result(
            Status.FAIL,
            f"{len(missing)} package(s) missing",
            fix="make install\n" + "\n".join(f"missing: {name}" for name in missing),
        )

    import trade_sim

    return Result(Status.OK, f"all present, trade_sim {trade_sim.__version__}")


def check_dbt() -> Result:
    dbt_bin = shutil.which("dbt") or str(REPO_ROOT / ".venv" / "bin" / "dbt")
    if not Path(dbt_bin).is_file() and not shutil.which("dbt"):
        return Result(Status.FAIL, "dbt not found", fix="make install")

    try:
        proc = subprocess.run(
            [dbt_bin, "--version"], capture_output=True, text=True, timeout=60, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Result(Status.FAIL, f"could not run dbt: {exc}", fix="make install")

    # `dbt --version` prints a block per component:
    #     Core:
    #       - installed: 1.9.4
    #     Plugins:
    #       - snowflake: 1.9.2
    # so the version and the component it belongs to are on different lines, and the component
    # has to be tracked while scanning rather than matched on a single line.
    output = proc.stdout + proc.stderr
    versions: dict[str, str] = {}
    section = ""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.endswith(":") and not stripped.startswith("-"):
            section = stripped.rstrip(":").lower()
        elif stripped.startswith("-") and ":" in stripped:
            name, _, value = stripped.lstrip("- ").partition(":")
            key = "core" if section == "core" else name.strip().lower()
            versions[key] = value.strip()

    core = versions.get("core", "version unknown")
    adapter = versions.get("snowflake")

    if adapter is None:
        return Result(
            Status.FAIL,
            f"dbt-core {core} but the Snowflake adapter is not installed",
            fix="pip install dbt-snowflake",
        )
    return Result(Status.OK, f"dbt-core {core}, dbt-snowflake {adapter}")


def check_optional_tools() -> Result:
    """Docker, terraform and plantuml. Absence is not fatal to the core pipeline."""
    tools = {
        "docker": "Airflow runs in Docker (make airflow-up)",
        "terraform": "Infrastructure-as-code (make tf-apply). The SQL in snowflake/ is an alternative.",
        "plantuml": "Renders the architecture diagrams (make diagrams). The .puml sources are readable as text.",
    }
    missing = {name: why for name, why in tools.items() if shutil.which(name) is None}
    if not missing:
        return Result(Status.OK, "docker, terraform, plantuml all present")
    return Result(
        Status.WARN,
        f"{', '.join(missing)} not on PATH",
        fix="\n".join(f"{name}: {why}" for name, why in missing.items()),
    )


# ===========================================================================
# 2. Repository configuration
# ===========================================================================
def check_env_file() -> Result:
    env_path = REPO_ROOT / ".env"
    example = REPO_ROOT / ".env.example"
    if not env_path.is_file():
        return Result(
            Status.FAIL,
            "no .env in the repo root",
            fix=f"cp {example.name} .env    then fill in SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER",
        )

    # Compare keys against the example so a variable added to the project after someone copied
    # their .env is reported, rather than silently defaulting.
    def keys_of(path: Path) -> set[str]:
        return {
            line.split("=", 1)[0].strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if "=" in line and not line.strip().startswith("#")
        }

    notes: list[str] = []
    if example.is_file():
        absent = sorted(keys_of(example) - keys_of(env_path))
        if absent:
            notes.append(f"not set (defaults apply): {', '.join(absent[:8])}")

    return Result(Status.OK, ".env present", notes=notes)


def check_dbt_profile() -> Result:
    profile = REPO_ROOT / "dbt" / "profiles.yml"
    if not profile.is_file():
        return Result(
            Status.FAIL,
            "no dbt/profiles.yml",
            fix="cp dbt/profiles.yml.example dbt/profiles.yml",
        )

    text = profile.read_text(encoding="utf-8")
    if "trade_pipeline:" not in text:
        return Result(
            Status.FAIL,
            "profiles.yml has no `trade_pipeline` profile",
            fix=(
                "The profile name must match `profile:` in dbt/dbt_project.yml.\n"
                "cp dbt/profiles.yml.example dbt/profiles.yml"
            ),
        )
    return Result(Status.OK, "dbt/profiles.yml present")


def check_dbt_packages() -> Result:
    packages_dir = REPO_ROOT / "dbt" / "dbt_packages"
    if not packages_dir.is_dir() or not any(packages_dir.iterdir()):
        return Result(
            Status.FAIL,
            "dbt packages not installed",
            fix="make dbt-deps",
        )
    installed = sorted(p.name for p in packages_dir.iterdir() if p.is_dir())
    return Result(Status.OK, ", ".join(installed))


def running_under_wsl() -> bool:
    if os.environ.get("WSL_DISTRO_NAME"):
        return True
    try:
        return "microsoft" in Path("/proc/version").read_text().lower()
    except OSError:
        return False


def check_filesystem() -> Result:
    """Reject a repository living on a Windows drive mount under WSL.

    /mnt/c is served by DrvFs, which cannot represent Unix permission bits. `chmod 600` on the
    Snowflake private key silently does nothing there, so check_private_key below reports a
    world-readable key that no amount of chmod will fix -- which is a genuinely baffling failure
    unless you already know the cause. Small-file IO is also an order of magnitude slower, and
    `make install` writing a virtualenv feels it acutely.

    Diagnosing it here means the fix is named once, at the top, instead of being inferred from a
    permissions error three sections later.
    """
    if not running_under_wsl():
        return Result(Status.OK, f"{platform.system()}, native filesystem")

    if str(REPO_ROOT).startswith("/mnt/"):
        return Result(
            Status.FAIL,
            f"the repository is on a Windows drive mount: {REPO_ROOT}",
            fix=(
                "DrvFs cannot store Unix permissions, so the private key cannot be secured and "
                "the credentials check below will fail whatever you chmod. Move the repository "
                "into the Linux filesystem and work there:\n"
                f"  cp -r {REPO_ROOT} ~/ && cd ~/{REPO_ROOT.name}"
            ),
        )

    return Result(Status.OK, "WSL, Linux filesystem")


def check_private_key() -> Result:
    """Load and parse the key, rather than merely checking the file exists.

    The most confusing key failure is a file that is present but in the wrong format -- an
    OpenSSL PEM rather than PKCS#8, which Snowflake rejects with an authentication error that
    says nothing about formats. Parsing it here surfaces that immediately.
    """
    from trade_sim.config import snowflake_settings

    try:
        settings = snowflake_settings()
    except Exception as exc:  # noqa: BLE001
        return Result(
            Status.FAIL,
            f"configuration is invalid: {exc}",
            fix="Check .env against .env.example. Either a key path or a password is required.",
        )

    if settings.private_key_path is None:
        if settings.password is not None:
            return Result(
                Status.WARN,
                "using password authentication",
                fix=(
                    "Key-pair authentication is what a service account should use: it cannot be "
                    "typed into a phishing page, it is rotatable without a password reset, and "
                    "Snowflake is deprecating single-factor password auth for programmatic "
                    "access.\n"
                    "make keypair"
                ),
            )
        return Result(
            Status.FAIL,
            "no credentials configured",
            fix="make keypair    then set SNOWFLAKE_PRIVATE_KEY_PATH in .env",
        )

    key_path = settings.private_key_path
    if not key_path.is_file():
        return Result(
            Status.FAIL,
            f"private key not found at {key_path}",
            fix="make keypair",
        )

    mode = key_path.stat().st_mode & 0o777
    notes = []
    if mode & 0o077:
        notes.append(
            f"permissions are {mode:o}; a private key should be 600 (chmod 600 {key_path})"
        )

    from trade_sim.loaders.snowflake_loader import load_private_key

    passphrase = (
        settings.private_key_passphrase.get_secret_value()
        if settings.private_key_passphrase
        else None
    )
    try:
        load_private_key(key_path, passphrase or None)
    except Exception as exc:  # noqa: BLE001
        return Result(
            Status.FAIL,
            f"key present but unusable: {exc}",
            fix=(
                "Snowflake needs an unencrypted PKCS#8 key, which is what `make keypair` "
                "produces. A key generated with plain `openssl genrsa` is PKCS#1 and will "
                "not load.\n"
                "If the key is passphrase-protected, set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.\n"
                "make keypair"
            ),
        )

    public = key_path.with_suffix(".pub")
    if public.is_file():
        notes.append(f"public key to register: {public}")
    return Result(Status.OK, f"key-pair auth, key parses ({key_path.name})", notes=notes)


def check_repo_layout() -> Result:
    """Confirm the directories the pipeline writes to exist and are writable.

    A read-only or absent landing directory shows up much later as a confusing failure inside
    the generator, several minutes into a demo.
    """
    from trade_sim.config import simulator_settings

    settings = simulator_settings()
    problems: list[str] = []
    for label, path in (("output_dir", settings.output_dir), ("state_dir", settings.state_dir)):
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".doctor_write_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            problems.append(f"{label} {path}: {exc}")

    if problems:
        return Result(
            Status.FAIL,
            "cannot write to the data directories",
            fix="\n".join(problems),
        )
    return Result(
        Status.OK,
        "data directories writable",
        notes=[f"landing: {settings.output_dir}", f"state:   {settings.state_dir}"],
    )


# ===========================================================================
# 3. Connectivity
# ===========================================================================
def check_dns() -> Result:
    """Resolve the account host before attempting to authenticate.

    Separating DNS from authentication matters because the commonest account-identifier mistake
    -- omitting the region, or using the account *name* instead of the locator -- produces a
    resolution failure, and Snowflake's driver reports it in a way that looks like a
    credentials problem.
    """
    from trade_sim.config import snowflake_settings

    account = snowflake_settings().account
    host = f"{account}.snowflakecomputing.com"
    try:
        socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        return Result(
            Status.FAIL,
            f"cannot resolve {host} ({exc.strerror})",
            fix=(
                "SNOWFLAKE_ACCOUNT must be the account identifier including the region, for "
                "example ab12345.eu-central-1 or myorg-myaccount.\n"
                "Find it in Snowsight: run   select current_account(), current_region();\n"
                "or take it from the browser URL of your Snowsight session."
            ),
        )
    return Result(Status.OK, f"{host} resolves")


def check_snowflake_connection() -> Result:
    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    session = SnowflakeSession(query_tag_suffix="component=doctor")
    try:
        with session:
            row = session.execute(
                "select current_account() as account, current_user() as user, "
                "current_role() as role, current_warehouse() as warehouse, "
                "current_database() as database, current_version() as version"
            )[0]
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        hints = [
            "Common causes, most likely first:",
            "  250001 / JWT token is invalid  -> the public key is not registered on the user.",
            "      ALTER USER <user> SET RSA_PUBLIC_KEY='<contents of .secrets/rsa_key.pub>';",
            "  Incorrect username or password -> SNOWFLAKE_USER does not exist, or is disabled.",
            "  Role 'X' is not granted        -> GRANT ROLE <role> TO USER <user>;",
            "  No active warehouse            -> the role lacks USAGE on the warehouse.",
            "  Account is suspended           -> a 30-day trial has expired.",
        ]
        return Result(Status.FAIL, f"connection failed: {message}", fix="\n".join(hints))

    return Result(
        Status.OK,
        "connected",
        notes=[
            f"account   {row['ACCOUNT']}",
            f"user      {row['USER']}",
            f"role      {row['ROLE']}",
            f"warehouse {row['WAREHOUSE']}",
            f"database  {row['DATABASE']}",
            f"version   {row['VERSION']}",
        ],
    )


def check_warehouses() -> Result:
    """Confirm the role can actually use each warehouse the pipeline routes work to.

    Visibility is not usage: `show warehouses` lists what the role can see, and a role can see
    a warehouse it has no USAGE on. So each one is exercised with a real statement.
    """
    from trade_sim.config import snowflake_settings
    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    settings = snowflake_settings()
    wanted = {settings.warehouse, settings.effective_load_warehouse}

    unusable: list[str] = []
    notes: list[str] = []
    for warehouse in sorted(wanted):
        session = SnowflakeSession(warehouse=warehouse, query_tag_suffix="component=doctor")
        try:
            with session:
                session.execute("select 1")
            notes.append(f"{warehouse}: usable")
        except Exception as exc:  # noqa: BLE001
            unusable.append(f"{warehouse}: {exc}")

    if unusable:
        return Result(
            Status.FAIL,
            f"{len(unusable)} warehouse(s) unusable",
            fix="\n".join(unusable) + "\nmake tf-apply    (or grant USAGE manually)",
        )
    return Result(Status.OK, f"{len(wanted)} warehouse(s) usable", notes=notes)


def check_snowflake_objects() -> Result:
    """Report which layers have been deployed, as a checklist rather than pass/fail.

    A fresh account legitimately has none of these, so failing here would tell a first-time
    user their setup is broken when it is merely empty. Reporting the state and naming the
    command that creates each layer is the useful behaviour.
    """
    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    # dbt-owned schemas are prefixed with the target schema outside prod (DBT_LOCAL_CORE on a
    # laptop, PR_412_CORE in CI), while RAW and MONITORING are built by the SQL layer and keep
    # their bare names. Checking for the bare names everywhere would report a correctly built
    # dev platform as half-deployed.
    if os.environ.get("SNOWFLAKE_ENV", "dev").strip().lower() in ("prod", "production"):
        dbt = ""
    else:
        dbt = os.environ.get("DBT_SCHEMA", "DBT_LOCAL").strip().upper() + "_"

    expected: dict[str, tuple[str, str]] = {
        "RAW.TRADE_EVENT": ("table", "make deploy-sql-pre"),
        "RAW.TRADE_EVENT_QUEUE": ("table", "make deploy-sql-pre"),
        "RAW.LOAD_BATCH": ("table", "make deploy-sql-pre"),
        "RAW.COPY_ERROR": ("table", "make deploy-sql-pre"),
        f"{dbt}CORE.FCT_TRADE": ("table", "make dbt-build"),
        f"{dbt}CORE.FCT_TRADE_VERSION": ("table", "make dbt-build"),
        f"{dbt}AUDIT.FCT_TRADE_REJECTED": ("table", "make dbt-build"),
        "MONITORING.VW_PIPELINE_SLA": ("view", "make deploy-sql-post"),
        f"{dbt}REPORTING.RPT_DATA_QUALITY_SCORECARD": ("view", "make dbt-build"),
    }

    session = SnowflakeSession(query_tag_suffix="component=doctor")
    with session:
        database = session.settings.database
        rows = session.execute(
            f"""
            select table_schema || '.' || table_name as object_name
            from {database}.information_schema.tables
            union all
            select table_schema || '.' || table_name
            from {database}.information_schema.views
            """
        )
    present = {str(row["OBJECT_NAME"]).upper() for row in rows}

    missing = {name: hint for name, (_, hint) in expected.items() if name not in present}
    notes = [f"{'present' if name in present else 'MISSING'}  {name}" for name in expected]

    if not missing:
        return Result(Status.OK, f"all {len(expected)} expected objects present", notes=notes)

    # A missing RAW layer blocks everything; a missing mart just means dbt has not run.
    raw_missing = [name for name in missing if name.startswith("RAW.")]
    status = Status.FAIL if raw_missing else Status.WARN
    return Result(
        status,
        f"{len(missing)} of {len(expected)} objects not deployed",
        fix="\n".join(sorted(set(missing.values()))),
        notes=notes,
    )


def check_pipeline_health() -> Result:
    """Read the SLA view, if it exists. Informational."""
    from trade_sim.loaders.snowflake_loader import SnowflakeSession

    session = SnowflakeSession(query_tag_suffix="component=doctor")
    try:
        with session:
            database = session.settings.database
            rows = session.execute(f"select * from {database}.monitoring.vw_pipeline_sla")
    except Exception as exc:  # noqa: BLE001
        return Result(Status.SKIP, f"SLA view not readable yet ({type(exc).__name__})")

    if not rows:
        return Result(Status.SKIP, "SLA view returned no rows")

    row = rows[0]
    stages = {
        "ingestion": row.get("INGESTION_STATUS"),
        "capture": row.get("CAPTURE_STATUS"),
        "transform": row.get("TRANSFORM_STATUS"),
        "correctness": row.get("CORRECTNESS_STATUS"),
    }
    reds = [name for name, value in stages.items() if value == "RED"]
    summary = "  ".join(f"{name}={value}" for name, value in stages.items())

    if reds:
        return Result(
            Status.WARN,
            summary,
            fix=(
                f"{', '.join(reds)} is RED. On a freshly deployed platform this is expected -- "
                "nothing has been loaded yet.\n"
                "make demo    then re-run make doctor"
            ),
        )
    return Result(Status.OK, summary)


# ===========================================================================
def main() -> int:
    print()
    print("\033[1mTrade pipeline doctor\033[0m")
    print(f"repo: {REPO_ROOT}")
    print()

    doctor = Doctor()

    print("\033[1m1. Toolchain\033[0m")
    python_result = doctor.check("python version", check_python)
    if python_result.status is Status.FAIL:
        # Nothing below can be trusted on an unsupported interpreter, and every subsequent
        # import would fail for reasons that have nothing to do with the user's setup.
        print()
        print("\033[31mStopping: the Python version is unsupported.\033[0m")
        return 1

    # Ahead of the virtualenv check on purpose: if the repo is on /mnt/c under WSL, the venv that
    # check is about to inspect is the slow, permission-less one we want the user to abandon.
    doctor.check("filesystem", check_filesystem)
    doctor.check("virtualenv", check_venv)
    packages = doctor.check("python packages", check_python_packages)
    doctor.check("dbt", check_dbt)
    doctor.check("optional tools", check_optional_tools)

    if packages.status is Status.FAIL:
        doctor.block("python packages are missing; run `make install` first")

    print()
    print("\033[1m2. Repository configuration\033[0m")
    env = doctor.check(".env", check_env_file)
    doctor.check("dbt profile", check_dbt_profile)
    doctor.check("dbt packages", check_dbt_packages)
    # Ahead of the .env gate on purpose: the simulator's paths have defaults, so this check is
    # meaningful with no configuration at all, and `make generate` works before any Snowflake
    # setup exists.
    doctor.check("data directories", check_repo_layout)

    if env.status is Status.FAIL:
        doctor.block("no .env, so there is no Snowflake configuration to test")

    credentials = doctor.check("credentials", check_private_key)

    if credentials.status is Status.FAIL:
        doctor.block("credentials are not usable")

    print()
    print("\033[1m3. Snowflake\033[0m")
    dns = doctor.check("account DNS", check_dns)
    if dns.status is Status.FAIL:
        doctor.block("the account host does not resolve")

    connection = doctor.check("connection", check_snowflake_connection)
    if connection.status is Status.FAIL:
        doctor.block("cannot connect to Snowflake")

    doctor.check("warehouses", check_warehouses)
    doctor.check("deployed objects", check_snowflake_objects)
    doctor.check("pipeline health", check_pipeline_health)

    print()
    if doctor.failed:
        print(
            f"\033[31m{doctor.failed} check(s) failed\033[0m"
            + (f", {doctor.warned} warning(s)" if doctor.warned else "")
        )
        print("Work top to bottom: a later check is often only failing because an earlier one is.")
        return 1

    if doctor.warned:
        print(f"\033[33mAll required checks passed, with {doctor.warned} warning(s).\033[0m")
    else:
        print("\033[32mAll checks passed.\033[0m")
    print("Next: make demo")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
    except ImportError as exc:
        # Reached when trade_sim itself cannot be imported, which the package check would
        # normally catch -- but only if the import at module scope succeeded.
        print(f"\n\033[31mCould not import a required module: {exc}\033[0m")
        print("-> make install")
        sys.exit(2)
