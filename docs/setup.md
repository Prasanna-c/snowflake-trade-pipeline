# Setup guide

From a clean laptop to a running pipeline. About 30 minutes, most of it waiting for downloads.

`make quickstart` prints the same sequence as a checklist once you are past the first read.

---

## Contents

- [What you need](#what-you-need)
- [Step 0: Get the project onto the machine](#step-0-get-the-project-onto-the-machine)
- [Step 1: Prerequisites](#step-1-prerequisites)
- [Step 2: Get a Snowflake trial](#step-2-get-a-snowflake-trial)
- [Step 3: Install the project](#step-3-install-the-project)
- [Step 4: Configure credentials](#step-4-configure-credentials)
- [Step 5: Generate and register a key pair](#step-5-generate-and-register-a-key-pair)
- [Step 6: Verify with doctor](#step-6-verify-with-doctor)
- [Step 7: Provision Snowflake](#step-7-provision-snowflake)
- [Step 8: First end-to-end run](#step-8-first-end-to-end-run)
- [Step 9: The dashboard](#step-9-the-dashboard)
- [Step 10: Airflow](#step-10-airflow)
- [Working without a warehouse](#working-without-a-warehouse)
- [Troubleshooting](#troubleshooting)
- [Tearing it down](#tearing-it-down)

---

## What you need

| | Requirement | Notes |
| --- | --- | --- |
| OS | macOS, Linux, or Windows with WSL2 | Developed on macOS. On Windows, WSL2 is required rather than optional — see [Windows](#windows-use-wsl2) |
| Python | **3.11 or 3.12** | 3.13 not yet supported by all pinned dependencies |
| Docker Desktop | 4.x, **at least 4 GB** allocated to it | Only needed for Airflow |
| Terraform | 1.5+ | Only needed for provisioning |
| Snowflake | A free trial | 30 days, $400 of credits |
| Disk | ~3 GB | Mostly the Airflow image |

**Everything except Airflow runs without Docker**, and a large amount of the project — the
simulator, all the dbt unit tests, linting, DAG validation — runs without Snowflake either. See
[Working without a warehouse](#working-without-a-warehouse).

---

## Step 0: Get the project onto the machine

Skip this if you already have the repository. It matters when moving the project between machines
by hand rather than cloning it.

**Copy the whole directory, not selected files.** The documentation under `docs/` explains the
code; it is not a copy of it. Every runnable artefact is a real file in the tree — `dbt/models/`,
`snowflake/`, `terraform/`, `ingestion/src/`, `airflow/dags/`, `scripts/`, the `Makefile`. Rebuilding
the project by copying snippets out of Markdown would silently omit most of it.

Two things must **not** travel with it:

| Exclude | Why |
| --- | --- |
| `.venv`, `.venv-*` | A virtualenv contains platform-specific compiled binaries. It is around a gigabyte, and on a different machine it is a gigabyte of the wrong binaries. `make install` rebuilds it in a few minutes. |
| `dbt/dbt_packages`, `dbt/target`, `dbt/logs`, `__pycache__`, `.pytest_cache`, `.DS_Store` | Regenerated on demand — `make dbt-deps` restores packages, `dbt` rewrites `target/`. |

Everything on that list is already in `.gitignore`, so `git clone` or `git archive` excludes it for
free. Without git, make a clean archive from the source machine:

```bash
cd ..
COPYFILE_DISABLE=1 tar --exclude='.venv*' \
    --exclude='dbt/dbt_packages' --exclude='dbt/target' --exclude='dbt/logs' \
    --exclude='__pycache__' --exclude='.pytest_cache' --exclude='.DS_Store' \
    --exclude='._*' \
    -czf snowflake-trade-pipeline.tar.gz snowflake-trade-pipeline
```

`COPYFILE_DISABLE=1` and `--exclude='._*'` matter when the source machine is a Mac. macOS `tar`
otherwise emits an AppleDouble sidecar named `._<original>` for any file carrying extended
attributes, and `._something.sql` matches the `*.sql` globs that the deploy script and dbt use. The
result is binary metadata being submitted to Snowflake as if it were SQL. Harmless to set
everywhere; on Linux both are no-ops.

That is a few megabytes rather than a gigabyte. Unpack it on the target machine with
`tar -xzf snowflake-trade-pipeline.tar.gz`, then continue below.

On Windows the archive normally lands in your Windows Downloads folder, which is outside WSL.
Install WSL2 first ([Windows](#windows-use-wsl2)), then copy it into the Linux filesystem before
unpacking — not the other way round, for the permission reason described there:

```bash
cd ~
cp /mnt/c/Users/YourName/Downloads/snowflake-trade-pipeline.tar.gz .
tar -xzf snowflake-trade-pipeline.tar.gz
cd snowflake-trade-pipeline
```

`ls /mnt/c/Users/` lists your Windows user folders if you are unsure of the name.

Verify the copy arrived intact before trusting it — this needs no Python, no Snowflake and no
network:

```bash
cd snowflake-trade-pipeline
ls Makefile dbt/dbt_project.yml terraform/envs/dev/main.tf snowflake/10_ingestion/02_raw_tables.sql
```

Also confirm no credentials came across. These files are machine-specific and must be recreated
here, never copied — the archive command above does not exclude them, because on a correctly kept
source tree they do not exist:

```bash
ls .env .secrets dbt/profiles.yml 2>/dev/null || echo "clean -- nothing to remove"
```

---

## Step 1: Prerequisites

```bash
python3 --version     # need 3.11.x or 3.12.x
terraform --version   # need 1.5+
docker --version
docker info | grep -i memory
```

On macOS:

```bash
brew install python@3.11 terraform
brew install --cask docker
```

`openssl` is also needed for the key pair and ships with both macOS and Linux.

### Windows: use WSL2

The project will not run against native Windows, and the reasons are structural rather than
incidental:

- The `Makefile` declares `SHELL := /bin/bash`. Every target routes through it.
- Paths are Unix — `.venv/bin/python`, where a Windows virtualenv puts `.venv\Scripts\python.exe`.
- `make keypair` calls `openssl` and `chmod`, and `make help` uses `grep` and `awk`.

Porting all of that would double the surface area of every command for no gain, because **Docker
Desktop on Windows runs on the WSL2 backend anyway** — you would end up needing WSL2 for Airflow
regardless. So WSL2 is the supported Windows path, not a workaround.

```powershell
# PowerShell as Administrator, then reboot when prompted
wsl --install
```

That installs WSL2 and Ubuntu, and prompts for a Linux username and password on first launch. The
password is your `sudo` password and is unrelated to your Windows login. Then, inside Ubuntu:

```bash
lsb_release -d       # expect Ubuntu 24.04
python3 --version    # expect 3.12.x -- see the version note above
sudo apt update && sudo apt install -y python3-venv python3-pip make openssl unzip wget gpg
```

Terraform is not in Ubuntu's default repositories, so add HashiCorp's:

```bash
wget -O- https://apt.releases.hashicorp.com/gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
  | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install -y terraform
```

For Docker, install **Docker Desktop for Windows**, then enable Settings → Resources → WSL
Integration for your Ubuntu distribution. That makes `docker` work from inside Ubuntu. Verify with
`docker run --rm hello-world` from the Ubuntu prompt, not PowerShell.

#### Keep the repository in the Linux filesystem

Work in `~/`, never in `/mnt/c/...`. This is enforced by a `make doctor` check, because the failure
it prevents is otherwise very hard to diagnose: `/mnt/c` is served by DrvFs, which cannot represent
Unix permission bits, so `chmod 600` on the Snowflake private key silently does nothing and the
credentials check reports a world-readable key that no amount of `chmod` will fix. Small-file IO is
also an order of magnitude slower there, which writing a virtualenv feels acutely.

#### The container user, on Linux and WSL

`make airflow-up` handles this and you do not have to do anything. It is worth knowing about
anyway, because it is the single most confusing thing that can go wrong here.

The containers run as `AIRFLOW_UID` and, on every start, take ownership of the directories they
share with the host: `data/`, the Airflow logs, `dbt_packages`. If that id is not yours, each start
quietly revokes your own write access, and the failure appears later somewhere unrelated —
`trade-sim load` stopping on `data/state/trade_book.lock`, or `dbt deps` unable to replace a file it
does not own. Neither looks like a file-ownership problem, which is what makes it expensive.

`make airflow-uid` writes `id -u` into both `.env` and `airflow/.env`, and `make airflow-up`
depends on it. If you drive Compose directly instead, run it first — the init container checks and
refuses to start on a mismatch rather than letting you find out later. macOS needs none of this:
Docker Desktop's VM maps uids for you.

### Why 3.11 or 3.12 specifically

`snowflake-connector-python` pins `pyarrow`, and on 3.13 the pinned build has no wheel for every
platform, so installation falls back to compiling from source and fails on a machine without a
toolchain. Pinning the interpreter is cheaper than debugging that.

If you have several Pythons, be explicit:

```bash
python3.11 -m venv .venv
```

### Docker memory

Airflow with `LocalExecutor` plus Postgres needs about 4 GB. Docker Desktop defaults to 2 GB on
some installs, and the failure mode is not a clear error — the scheduler is killed by the OOM
killer and tasks simply stop being picked up, which looks like a DAG problem.

On macOS, or Windows using the Hyper-V backend, set it in Docker Desktop under Settings, Resources.

On the **WSL2 backend there is no memory slider**, because Docker runs inside WSL and WSL owns the
limit. Setting it in Docker Desktop is not possible and looking for the control there wastes time.
Create `%UserProfile%\.wslconfig` in Windows:

```ini
[wsl2]
memory=6GB
processors=4
```

Then, from PowerShell, `wsl --shutdown` and reopen Ubuntu. Confirm with `free -g` inside Ubuntu, or
`docker info | grep -i memory`. Allow 6 GB rather than exactly 4, since your virtualenv, dbt and
the dashboard are living in the same WSL instance as the containers.

---

## Step 2: Get a Snowflake trial

Sign up at [signup.snowflake.com](https://signup.snowflake.com). Choose **Enterprise** edition and
whichever cloud and region is nearest you.

Enterprise rather than Standard matters, because this project uses two Enterprise features:

- **Object tagging and masking policies** — the data governance layer in `terraform/`.
- **Multi-cluster warehouses** — referenced in [`scalability.md`](scalability.md), though the local
  configuration keeps every warehouse single-cluster.

On Standard, `make tf-apply` fails when it reaches the masking policies. If you are already on
Standard, comment out the `module "governance"` block in `terraform/envs/dev/main.tf` and the rest
works.

Note your **account identifier** — it is in the URL of your Snowflake session, in the form
`abc12345.eu-west-1` or an organisation-account pair like `MYORG-MYACCOUNT`. This is the single
most common thing to get wrong, so `make doctor` checks it first and separately.

### Credit budget

The trial gives $400, roughly 100 credits. This project uses **well under one credit a day** in
normal use: the warehouses are XSMALL with 60-second auto-suspend, and the drain task is gated on
`SYSTEM$STREAM_HAS_DATA` so it costs nothing when idle.

The resource monitors cap dev at **20 credits a month across all three warehouses** and suspend the
warehouse at 90%. That is deliberately tight — a runaway loop in development is the most likely way
to lose a trial account, and the cap makes the worst case a suspended warehouse rather than an
exhausted trial. See [monitoring.md](monitoring.md#resource-monitors).

---

## Step 3: Install the project

```bash
cd snowflake-trade-pipeline
make install
```

This creates `.venv`, installs `requirements.txt` and the dashboard requirements, installs the
`trade_sim` package in editable mode, and installs the pre-commit hooks.

**Every dependency is pinned to an exact version.** A setup guide that produces a different
environment each month is not reproducible, and "works on my machine" is precisely the failure this
project cannot afford at an interview.

The `-e ./ingestion` editable install is what lets Airflow, the dashboard and the reconciler all
import the same `trade_sim` code rather than each carrying its own copy of the Snowflake connection
logic. One place to fix a connection bug.

---

## Step 4: Configure credentials

```bash
cp .env.example .env
cp dbt/profiles.yml.example dbt/profiles.yml
cp terraform/envs/dev/example.tfvars terraform/envs/dev/terraform.tfvars
```

There are two configuration files with two different jobs, and confusing them is the most common
setup mistake:

- **`terraform/envs/dev/terraform.tfvars`** — the identity Terraform *itself* connects as, in order
  to create everything. This is the bootstrap credential.
- **`.env`** — the identity the *pipeline* connects as, using service users that Terraform created.
  So it can only be correct after provisioning.

### `terraform.tfvars`

```hcl
snowflake_organization_name = "MYORG"       # from Snowsight, bottom-left account menu
snowflake_account_name      = "MYACCOUNT"
snowflake_user              = "YOUR_USERNAME"
snowflake_role              = "ACCOUNTADMIN"
snowflake_bootstrap_warehouse = "COMPUTE_WH"   # exists by default on a trial
snowflake_private_key_path    = "/absolute/path/to/.secrets/rsa_key.p8"
alert_email                   = "you@example.com"
```

`alert_email` **must be a verified email on a Snowflake user in this account**, or the alerts will
appear to deploy and then silently never deliver. That is a Snowflake behaviour worth knowing before
you conclude the alerting does not work.

The three `*_public_key` values are for the service users Terraform creates. Leave them commented on
the first pass — see [Step 5](#step-5-generate-and-register-a-key-pair).

### `.env`

Only the account identifier must change before provisioning:

```bash
SNOWFLAKE_ACCOUNT=abc12345.eu-west-1
```

The variables worth knowing about:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SNOWFLAKE_USER` | `SVC_TRADES_DBT_DEV` | Created by Terraform |
| `SNOWFLAKE_ROLE` | `FR_TRADES_TRANSFORM_DEV` | Created by Terraform |
| `SNOWFLAKE_DATABASE` | `TRADES_DEV` | |
| `SNOWFLAKE_WAREHOUSE` | `WH_TRADES_TRANSFORM_DEV` | dbt and query workload |
| `SNOWFLAKE_LOAD_WAREHOUSE` | `WH_TRADES_LOAD_DEV` | Ingestion, sized separately |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | `.secrets/rsa_key.p8` | |
| `TRADE_SIM_SEED` | `42` | Fixed, so generated data is reproducible |
| `TRADE_SIM_ERROR_RATE` | `0.08` | Fraction of events with an injected fault |
| `TRADE_SIM_AMEND_RATE` | `0.20` | Fraction that amend a previously emitted trade |
| `ALERT_EMAIL` | — | Where failure alerts go. Leave blank to disable |
| `DQ_MAX_REJECT_RATE` | `0.25` | The reject-rate gate. See the [runbook](runbook.md#data-quality-gate-tripped) |

Confirm the user and role names against `terraform output` after provisioning rather than trusting
the defaults — they carry an environment suffix and it is easy to be one letter out.

`.env` and `terraform.tfvars` are both gitignored, as is `.secrets/`, and Gitleaks runs in CI as a
second line of defence.

### On roles and service users

`ACCOUNTADMIN` is needed for the **initial Terraform run only**, because creating roles, warehouses
and resource monitors requires it. It is a bootstrap credential, not a runtime one.

Terraform then creates three service users, each with its own key pair and exactly one persona:

| Service user | Functional role | Can do |
| --- | --- | --- |
| `SVC_TRADES_INGEST_DEV` | `FR_TRADES_INGEST_DEV` | Write `RAW` only — **cannot touch curated data** |
| `SVC_TRADES_DBT_DEV` | `FR_TRADES_TRANSFORM_DEV` | DDL across the modelling layers |
| `SVC_TRADES_BI_DEV` | `FR_TRADES_ANALYST_DEV` | Read `CORE`, `REPORTING`, `SNAPSHOTS` — masked |

Two more functional roles exist without a service user: `FR_TRADES_COMPLIANCE_DEV`, which reads the
audit layer including rejected payloads, and `FR_TRADES_PLATFORM_DEV` for operating warehouses and
tasks.

Running day-to-day work as `ACCOUNTADMIN` would mean the RBAC model is never actually exercised, and
"does it work" would be unanswerable. The split is also a real containment boundary: the loader
cannot corrupt the golden record, and the dashboard cannot read a raw payload even by accident.
[`adr/0003-two-tier-rbac.md`](adr/0003-two-tier-rbac.md).

### `TRADE_SIM_ERROR_RATE` is 8% on purpose

Far higher than any real feed. The point is that every rule has something to fire on within a single
small batch, so the audit tables, the rejection dashboard and the reconciler all have data on the
first run. A realistic 0.1% would leave the entire rejection half of the platform looking empty and
untested.

8% is comfortably below the 25% gate, so a normal run passes. Setting it above 0.25 is the easiest way
to watch the gate work.

---

## Step 5: Generate and register a key pair

```bash
make keypair
```

Writes `.secrets/rsa_key.p8` (private, mode 600) and `.secrets/rsa_key.pub`.

The key is used in two places, and it is worth being clear which is which:

**1. Your own user, so Terraform can connect.** Registered by hand, once, below.

**2. The service users Terraform creates.** Terraform sets their public keys from
`ingest_public_key`, `dbt_public_key` and `bi_public_key` in `terraform.tfvars`.

For a local single-developer setup, paste the *same* public key into all three. That is a deliberate
simplification: the three service users still have genuinely different privileges, which is where the
security value is, and they simply share a credential. In production each would get its own key from
a secrets manager — the Terraform variables are already separate precisely so that requires no code
change.

```bash
# Prints the single-line value for all four places.
grep -v '^-----' .secrets/rsa_key.pub | tr -d '\n'; echo
```

Now register it against your own user. In a Snowflake worksheet as `ACCOUNTADMIN`:

```sql
ALTER USER <your_user> SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

The value is the contents of `.secrets/rsa_key.pub` **with the `-----BEGIN PUBLIC KEY-----` and
`-----END PUBLIC KEY-----` lines removed and all newlines stripped**, so it is one long string.

Verify:

```sql
DESC USER <your_user>;  -- RSA_PUBLIC_KEY_FP should now have a fingerprint
```

Then uncomment the three `*_public_key` lines in `terraform.tfvars` and paste the same value.

Note that the service users are created with `snowflake_service_user`, not `snowflake_user`. That is
deliberate: Snowflake's service user type is exempt from the MFA enrolment that now applies to human
users, which is what makes non-interactive authentication supported rather than merely working for now.

### Why key pair rather than a password

Three reasons, and this is a likely interview question:

**Passwords cannot be used non-interactively once MFA is on.** Snowflake now enforces MFA for
human users, and a password prompt in an Airflow task is not an option. Key pair authentication is
the supported path for service accounts.

**A key can be rotated without downtime.** Snowflake accepts `RSA_PUBLIC_KEY` and
`RSA_PUBLIC_KEY_2` simultaneously, so the sequence is: add the new key as the second, deploy the
new private key, remove the first. No window in which authentication is broken.

**Nothing reusable crosses the network.** A password is transmitted; a key pair signs a JWT and the
private key never leaves the machine.

The trade-off — a file on disk is a real attack surface — and the reasoning in full is in
[`adr/0006-keypair-authentication.md`](adr/0006-keypair-authentication.md).

The key is generated **unencrypted** here for local convenience. In production it belongs in a
secrets manager, and the code already supports a passphrase via
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

---

## Step 6: Verify with doctor

```bash
make doctor
```

This is the checkpoint. Do not continue past it with a red line.

It checks, in this order: Python version, required tools, repository layout, `.env` completeness,
private key presence and permissions, Snowflake connectivity, role and warehouse usage, and whether
the ingestion layer is deployed.

**The order is the design.** DNS failure, wrong credentials, missing role grant and missing
warehouse usage all surface from the driver as nearly identical errors, and each check narrows the
cause before the next runs. So a red line names the actual problem rather than reporting "could not
connect".

On a fresh account the last check — the ingestion layer — is expected to fail, since nothing has
been deployed yet. Everything above it should be green.

---

## Step 7: Provision Snowflake

```bash
make deploy-sql-plan   # optional: prints every statement, connects to nothing
make bootstrap
```

`bootstrap` runs `tf-init`, `tf-plan`, `tf-apply`, `deploy-sql-pre`, `dbt-deps`, `dbt-scaffold`,
`deploy-sql-post`. Around three minutes.

The SQL layer deploys in two phases, either side of a first dbt run, and the reason is worth
knowing because it explains a failure you would otherwise find confusing. `MONITORING.VW_PIPELINE_SLA`
and three of the alerts select from `INTERMEDIATE`, `CORE` and `AUDIT` objects that only dbt
creates, and Snowflake validates a view's `SELECT` when the view is created — not when it is first
queried. Deploying them on an account where dbt has never run therefore fails with "object does not
exist". `dbt-scaffold` sits between the two phases and builds every dbt object while the pipeline is
still empty, purely so the monitoring layer has something real to reference. It runs `dbt run`
rather than `dbt build`, because tests against a zero-row pipeline prove nothing and a test that
legitimately requires rows would fail there and make provisioning look broken.

### What gets created, and by which tool

The split between Terraform and SQL is the design decision most likely to be probed. Full
reasoning in [`adr/0002-terraform-scope.md`](adr/0002-terraform-scope.md); the short version:

**Terraform** owns things with a lifecycle: warehouses, the database, schemas, both role tiers and
their grants, resource monitors, tags and masking policies. Terraform's value is in the state file
telling you what drifted, and these are objects where drift matters.

**SQL scripts** own things that are code: file formats, the stage, the pipe, the stream and tasks,
monitoring views, alerts and stored procedures. A view definition in HCL is a string that Terraform
cannot validate, cannot format and cannot diff meaningfully. As SQL it is reviewable, and every
script is written `CREATE OR REPLACE` so re-running is a no-op.

The one exception is the stream, which uses `CREATE STREAM IF NOT EXISTS` — replacing a stream
resets its offset, which would re-emit every row already processed. See
[Recreating the stream](runbook.md#recreating-the-stream).

**dbt** owns everything derived: staging, the adjudication layer, marts, snapshots and tests.

Then confirm:

```bash
make doctor    # all green now, including the ingestion layer
```

---

## Step 8: First end-to-end run

```bash
make demo
```

That is `load` → `drain` → `dbt-build-incremental` → `reconcile`:

1. **Generate** ~5,000 trade events as NDJSON, including deliberate faults, plus a manifest
   recording the verdict each event must receive.
2. **PUT** the files to the internal stage and **COPY** them into `RAW.TRADE_EVENT`.
3. **Drain** the stream into `RAW.TRADE_EVENT_QUEUE`.
4. **dbt build** — staging, typing, adjudication, marts, snapshot, and every test.
5. **Reconcile** — compare the manifest's required verdicts against what the pipeline decided.

Expect roughly 12% of events rejected across most of the nineteen rules.

### Verify

```bash
make status                       # four RAG columns, all GREEN

make sql Q="select lifecycle_status, count(*) from TRADES_DEV.dbt_local_core.fct_trade group by 1"
make sql Q="select primary_rule_code, count(*) from TRADES_DEV.dbt_local_audit.fct_trade_rejected group by 1 order by 2 desc"
```

The `DBT_LOCAL_` prefix is not a typo. dbt prefixes its schemas with the target schema in every
environment except prod, so your laptop builds `DBT_LOCAL_CORE` while production builds `CORE`.
Two people can then build the same project into one database without overwriting each other. The
`--preset` diagnostics and the dashboard resolve this for you; only hand-written SQL needs the
prefix spelled out.

**The reconciliation step is the one that matters.** Everything else proves the pipeline *ran*;
reconciliation proves it reached the *right answers*, including that no event went missing — which
is the only failure mode no other check can detect. See
[Reconciliation mismatch](runbook.md#reconciliation-mismatch).

---

## Step 9: The dashboard

```bash
make dashboard        # http://localhost:8501
```

Four pages: a landing scorecard, trade status, rejections, and pipeline health. It reads only marts
and monitoring views — never RAW, and never a dbt intermediate model — which a test enforces, so
the dashboard cannot become a second, divergent definition of the truth.

If panels show a connection warning rather than a traceback, that is the intended behaviour for a
credential problem. Run `make doctor`.

---

## Step 10: Airflow

```bash
make airflow-up       # first run pulls and builds; a few minutes
```

That target sets `AIRFLOW_UID` to your own user id first, in both env files, which on Linux and WSL
is what keeps the directories the containers share with you writable by you — see
[the container user](#the-container-user-on-linux-and-wsl).

Then <http://localhost:8080>, `admin` / `admin`. Unpause `trade_pipeline` and:

```bash
make airflow-trigger
make airflow-logs     # tail the scheduler
```

The DAG runs hourly with `max_active_runs=1` and `catchup=False`. Twenty tasks in three groups:
ingest, transform, verify.

### The custom image

`make airflow-up` builds a custom image rather than using `_PIP_ADDITIONAL_REQUIREMENTS` on the
stock one. That variable reinstalls dependencies on **every container start**, which is slow, needs
network at runtime, and — worst — means the container that starts on Monday can differ from Friday's
because a transitive dependency moved. Baking dependencies into a built image makes the environment
a build artifact. Reasoning in
[`adr/0009-airflow-dbt-packaging.md`](adr/0009-airflow-dbt-packaging.md).

dbt is installed in the same image and invoked with `BashOperator` rather than through
`astronomer-cosmos`. That is a deliberate choice about the local footprint, explained in
[`adr/0008-orchestration-choice.md`](adr/0008-orchestration-choice.md).

If tasks queue and never start, it is nearly always Docker memory — see
[Step 1](#step-1-prerequisites).

---

## Working without a warehouse

Useful when the trial has expired, on a plane, or in CI. This is what the offline tier of
`.github/workflows/ci.yml` runs:

```bash
make ci-local          # lint, pytest, dbt parse, DAG validation, selfcheck
```

Individually:

| Command | Proves |
| --- | --- |
| `make generate` | The simulator works; writes NDJSON to `./data` |
| `make dbt-unit-test` | **Every business rule, against mock rows.** No warehouse |
| `make dbt-parse` | Every `ref()` resolves and every model compiles |
| `make airflow-validate` | Every DAG imports, and no task is orphaned from `start` |
| `make selfcheck` | Every command, path and anchor named anywhere in the repo exists |
| `make pytest` | 113 tests: simulator, dashboard render and DAG helper tests |
| `make lint` | Ruff and mypy, offline |
| `make lint-sql` | sqlfluff over the models, snapshots and singular tests — needs `profiles.yml` and a warehouse, because the dbt templater compiles the project before linting it |

`make dbt-unit-test` deserves emphasis: dbt unit tests run the real model SQL against fixed input
rows, so the business rules are provable in seconds with no credentials at all. That is the loop to
use when changing a rule.

`make selfcheck` is unusual and worth understanding — it scans the repo for references to Makefile
targets, file paths, documentation anchors and dbt models, and fails if any resolves to nothing. It
is what stops this guide from telling you to run a command that was renamed six months ago.

---

## Troubleshooting

`make doctor` first. It diagnoses most of the below directly.

### "Incorrect username or password" with a correct key

Almost always the public key is not registered, or was pasted with newlines or the PEM header
included. Re-run:

```bash
grep -v '^-----' .secrets/rsa_key.pub | tr -d '\n'; echo
```

and confirm `DESC USER` shows `RSA_PUBLIC_KEY_FP`.

### "Could not connect to Snowflake" / DNS failure

The account identifier is wrong. It is not your username and not the URL. `abc12345.eu-west-1` or
`MYORG-MYACCOUNT`. Copy it from the Snowflake UI, bottom-left account menu.

### "No active warehouse selected in the current session"

The role can *see* the warehouse but has no `USAGE` on it. Visibility is not usage. `make tf-apply`,
or grant directly.

### Terraform "object already exists"

Something was created by hand that Terraform now wants to own. Either import it or drop it:

```bash
cd terraform/envs/dev
terraform import 'module.warehouses.snowflake_warehouse.this["load"]' WH_TRADES_LOAD_DEV
```

### dbt "Object does not exist" on the first run

`make deploy-sql` has not run. `make bootstrap` covers it.

### dbt "Compilation Error: model not found"

`make dbt-parse` will name it, without touching a warehouse.

### "invalid identifier 'rows_loaded'" on a second `make load`

**Known issue, found by code review and not yet fixed.** Expect it the first time you run
`make load` when there are no new files to ingest.

`COPY INTO` has two result shapes. When it processes files, it returns one row per file with
`rows_loaded` and `errors_seen` columns. When it matches nothing — which is what happens on a
re-run, because COPY skips files it has already loaded for 64 days — it returns a single row whose
only column is `status`. `SP_LOAD_TRADE_FILES` reads the metric columns unconditionally via
`RESULT_SCAN`, so the no-op case fails.

The load itself has not gone wrong; nothing is lost, and `RAW.TRADE_EVENT` is unaffected. To get
past it, generate fresh files so COPY has real work to do:

```bash
make generate        # new file names, so COPY does not skip them
make load
```

The fix is to detect the shape before reading the metric columns, in the `RESULT_SCAN` block of
`snowflake/10_ingestion/03_snowpipe_and_copy.sql`.

### `VW_STREAM_LAG` reports a 14-day staleness limit in dev

**Known issue, documentation and code disagreeing.** `staleness_limit_minutes` is hardcoded to
`14 * 24 * 60`, and the comments in `snowflake/10_ingestion/02_raw_tables.sql` describe 14-day Time
Travel — but `terraform/envs/dev/main.tf` sets `data_retention_time_in_days = 1`. In dev the stream
therefore goes stale after one day while the monitor claims a fortnight of headroom.

Harmless for a short-lived demo, since the drain task runs every minute. It matters if you leave the
platform idle for more than a day and then resume, because the stream may have gone stale and the
delta has to be replayed with COPY. Either raise dev retention to match, or derive the limit from
`SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS'` rather than hardcoding it.

### `make dbt-deps` fails with an SSL error

A corporate proxy or restricted network intercepting `hub.getdbt.com`. Vendor the packages instead:

```bash
mkdir -p dbt/dbt_packages && cd dbt/dbt_packages
git clone --depth 1 --branch 1.3.0 https://github.com/dbt-labs/dbt-utils.git dbt_utils
git clone --depth 1 --branch 0.10.4 https://github.com/calogica/dbt-expectations.git dbt_expectations
rm -rf dbt_utils/.git dbt_expectations/.git
```

### Airflow tasks stay queued

Docker memory. Give it 4 GB. See [Step 1](#step-1-prerequisites).

### Airflow "dbt: command not found"

The image was not rebuilt after a dependency change:

```bash
make airflow-down && make airflow-up
```

### The trial has expired

Nothing in the repository depends on a specific account. Sign up again, update `SNOWFLAKE_ACCOUNT`
in `.env`, re-register a public key, `make bootstrap`. Meanwhile
[everything offline](#working-without-a-warehouse) still runs.

---

## Tearing it down

```bash
make airflow-down     # containers and volumes
make tf-destroy       # every Snowflake object Terraform created
make clean-all        # venv, dbt packages, generated data, artefacts
```

`tf-destroy` removes the database and therefore all the data. It does **not** remove the public key
from your user — do that with `ALTER USER <you> UNSET RSA_PUBLIC_KEY`.

---

## Next

- [`overview.md`](overview.md) — the architecture and why each component was chosen
- [`validation-logic.md`](validation-logic.md) — every business rule and its reasoning
- [`runbook.md`](runbook.md) — what to do when something breaks
- [`monitoring.md`](monitoring.md) — every threshold and why that number
- [`scalability.md`](scalability.md) — what changes at 100× and 10,000× volume
- [`interview-notes.md`](interview-notes.md) — the design questions, answered
