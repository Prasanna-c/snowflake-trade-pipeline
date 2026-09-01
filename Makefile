# =============================================================================
# Trade Lifecycle Pipeline -- one entrypoint for every local operation.
#
# Run `make help` for the catalogue, or `make quickstart` for the guided path.
#
# WHY A MAKEFILE AND NOT A SET OF SHELL SCRIPTS
#
# Because the commands are the documentation. A README that says "activate the venv, cd into
# dbt, export DBT_PROFILES_DIR, then run dbt build with these four flags" is a README that goes
# stale and that everyone gets subtly wrong. `make dbt-build` cannot go stale: if it breaks, CI
# breaks, because CI runs the same targets.
#
# Target names here are the names used in error messages throughout the codebase (the Airflow
# DAG, the dashboard's empty states, the runbook). That is deliberate -- an error message that
# names a command which does not exist is worse than no suggestion at all -- and
# `make selfcheck` verifies the correspondence.
# =============================================================================
SHELL := /bin/bash
.DEFAULT_GOAL := help
.ONESHELL:

# Load .env into the environment of every recipe if it exists.
ifneq (,$(wildcard ./.env))
include .env
export
endif

VENV        := .venv
PY          := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
DBT         := $(CURDIR)/$(VENV)/bin/dbt
DBT_DIR     := dbt
DBT_TARGET  ?= dev
BATCHES     ?= 1
TRADES      ?= 5000

# Everything Python that we lint, test and format. Listed once so the lint and format targets
# cannot drift apart -- which is how a directory quietly stops being checked.
PY_PATHS    := ingestion airflow/dags airflow/tests dashboard scripts

export DBT_PROFILES_DIR := $(CURDIR)/dbt

# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_0-9-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

.PHONY: quickstart
quickstart: ## Print the ordered first-run sequence
	@cat docs/quickstart.txt

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
.PHONY: venv
venv: ## Create the virtualenv (Python 3.11+ required)
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip wheel

.PHONY: install
install: venv ## Install all Python dependencies + pre-commit hooks
	@# One pip invocation, not three. Given every requirement at once, pip resolves them together
	@# and refuses an environment it cannot satisfy; given them one command at a time, it installs
	@# each in turn and lets a later file silently overwrite an earlier file's pin. That is not
	@# hypothetical -- this project shipped with two files naming different versions of streamlit
	@# and of pytest, and which one a developer ended up with depended on install order alone.
	$(PIP) install -r requirements.txt -r dashboard/requirements.txt -e ./ingestion
	@# `pre-commit install` writes into .git/hooks and fails when there is no repository yet, which
	@# is the state a fresh download is in. That is not a failed install, so it is reported and
	@# stepped over rather than taking the whole target down with it.
	@$(VENV)/bin/pre-commit install || \
		echo ">> No git repository here yet, so no hooks were installed. Run 'pre-commit install' after 'git init'."
	@echo ""
	@echo ">> Installed. Next:"
	@echo ">>   cp .env.example .env          and fill in your Snowflake account"
	@echo ">>   cp dbt/profiles.yml.example dbt/profiles.yml"
	@echo ">>   make keypair                  then register the public key in Snowflake"
	@echo ">>   make doctor                   to verify everything"

.PHONY: keypair
keypair: ## Generate an unencrypted RSA key pair for Snowflake service auth
	@mkdir -p .secrets
	@chmod 700 .secrets
	openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out .secrets/rsa_key.p8 -nocrypt
	openssl rsa -in .secrets/rsa_key.p8 -pubout -out .secrets/rsa_key.pub
	@chmod 600 .secrets/rsa_key.p8
	@echo ""
	@echo ">> Private key: $(CURDIR)/.secrets/rsa_key.p8  (set SNOWFLAKE_PRIVATE_KEY_PATH to this)"
	@echo ">> Register the public key on the Snowflake user with:"
	@echo ""
	@echo "ALTER USER <user> SET RSA_PUBLIC_KEY='$$(grep -v 'KEY-----' .secrets/rsa_key.pub | tr -d '\n')';"

.PHONY: doctor
doctor: ## Verify local prerequisites and Snowflake connectivity
	$(PY) scripts/doctor.py

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
.PHONY: tf-init tf-plan tf-apply tf-destroy
tf-init: ## terraform init (DBT_TARGET selects dev or prod)
	cd terraform/envs/$(DBT_TARGET) && terraform init

tf-plan: ## terraform plan
	cd terraform/envs/$(DBT_TARGET) && terraform plan -out=tfplan

tf-apply: ## terraform apply the saved plan
	cd terraform/envs/$(DBT_TARGET) && terraform apply tfplan

tf-destroy: ## terraform destroy -- tears down all Snowflake objects
	cd terraform/envs/$(DBT_TARGET) && terraform destroy

.PHONY: deploy-sql
deploy-sql: ## Deploy all Snowflake-native SQL. Needs dbt to have run at least once
	$(PY) scripts/deploy_snowflake_sql.py --env $(DBT_TARGET)

.PHONY: deploy-sql-pre
deploy-sql-pre: ## Deploy the SQL that depends only on Terraform (file formats, stage, pipe, streams, tasks)
	$(PY) scripts/deploy_snowflake_sql.py --env $(DBT_TARGET) --phase pre-dbt

.PHONY: deploy-sql-post
deploy-sql-post: ## Deploy the SQL that reads dbt-created objects (monitoring views, alerts)
	$(PY) scripts/deploy_snowflake_sql.py --env $(DBT_TARGET) --phase post-dbt

.PHONY: deploy-sql-plan
deploy-sql-plan: ## Show what deploy-sql would execute, without executing it
	$(PY) scripts/deploy_snowflake_sql.py --env $(DBT_TARGET) --dry-run

# The order below is a dependency chain, not a preference. The monitoring views and three of
# the alerts select from INTERMEDIATE, CORE and AUDIT objects that only dbt creates, and
# Snowflake validates a view's SELECT when the view is created -- so they cannot be deployed
# until dbt has built those objects at least once. dbt-scaffold does that with no data.
.PHONY: bootstrap
bootstrap: tf-init tf-plan tf-apply deploy-sql-pre dbt-deps dbt-scaffold deploy-sql-post ## Full first-time provisioning
	@echo ">> Infrastructure ready. Next: make demo"

# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
.PHONY: generate
generate: ## Generate trade files locally, no Snowflake needed.  make generate TRADES=20000
	$(PY) -m trade_sim.cli generate --trades $(TRADES) --batches $(BATCHES)

.PHONY: emit-seeds
emit-seeds: ## Regenerate the dbt reference seeds from reference.py
	$(PY) -m trade_sim.cli emit-seeds --out $(DBT_DIR)/seeds

.PHONY: load
load: ## Generate + PUT to the Snowflake stage + COPY into RAW.TRADE_EVENT
	$(PY) -m trade_sim.cli load --trades $(TRADES) --batches $(BATCHES)

.PHONY: load-stream
load-stream: ## Continuous producer: emit a file every N seconds (Ctrl-C to stop)
	$(PY) -m trade_sim.cli stream --trades-per-file 500 --interval-seconds 30

.PHONY: drain
drain: ## Drain the Snowflake stream into the queue table dbt reads
	$(PY) -m trade_sim.cli drain

.PHONY: reconcile
reconcile: ## Compare the generator's manifests against the verdicts the pipeline reached
	$(PY) -m trade_sim.cli reconcile

.PHONY: status
status: ## Print pipeline health from MONITORING.VW_PIPELINE_SLA
	$(PY) -m trade_sim.cli status

.PHONY: sql
sql: ## Run one statement.  make sql Q="select current_version()"
	$(PY) scripts/run_sql.py "$(Q)"

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------
.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	cd $(DBT_DIR) && $(DBT) deps

.PHONY: dbt-seed
dbt-seed: ## Load reference seeds (counterparties, currencies, rejection reasons)
	cd $(DBT_DIR) && $(DBT) seed --target $(DBT_TARGET)

# Creates every dbt object while the pipeline is still empty, so that the monitoring views and
# alerts have something to reference. Deliberately `run` and not `build`: tests against a
# zero-row pipeline prove nothing, and a data test that legitimately requires rows would fail
# here and make provisioning look broken. The real tests run in `make demo`, with data.
.PHONY: dbt-scaffold
dbt-scaffold: ## Create all dbt objects empty, so the monitoring layer has something to reference
	cd $(DBT_DIR) && $(DBT) seed --target $(DBT_TARGET) && $(DBT) run --target $(DBT_TARGET)

.PHONY: dbt-build
dbt-build: ## dbt build: seeds, models, tests and snapshots, in dependency order
	cd $(DBT_DIR) && $(DBT) build --target $(DBT_TARGET)

.PHONY: dbt-build-incremental
dbt-build-incremental: ## dbt build excluding seeds -- the normal batch path
	cd $(DBT_DIR) && $(DBT) build --target $(DBT_TARGET) --exclude resource_type:seed

# Reprocesses every event still in RAW.TRADE_EVENT_QUEUE in a single run, rather than only the
# rows drained since the last build. That is also the recovery path when local generator state
# and warehouse history have diverged: a duplicate (trade_id, version) spanning two runs becomes
# a duplicate within one run, which is the case business rule 2 resolves -- latest arrival
# accepted, earlier one recorded as SUPERSEDED. Snapshots are unaffected, by design: SCD2 history
# records what the warehouse believed at the time and rewriting it would defeat the purpose.
.PHONY: dbt-rebuild
dbt-rebuild: ## dbt build --full-refresh: rebuild every model from scratch
	cd $(DBT_DIR) && $(DBT) build --target $(DBT_TARGET) --full-refresh

.PHONY: dbt-snapshot
dbt-snapshot: ## Capture SCD2 trade history
	cd $(DBT_DIR) && $(DBT) snapshot --target $(DBT_TARGET)

.PHONY: dbt-test
dbt-test: ## dbt test only, storing failing rows for inspection
	cd $(DBT_DIR) && $(DBT) test --target $(DBT_TARGET) --store-failures

.PHONY: dbt-unit-test
dbt-unit-test: ## Run only the dbt unit tests (the business-rule proofs)
	cd $(DBT_DIR) && $(DBT) test --target $(DBT_TARGET) --select test_type:unit

.PHONY: dbt-freshness
dbt-freshness: ## Check RAW source freshness (file-arrival-delay detection)
	cd $(DBT_DIR) && $(DBT) source freshness --target $(DBT_TARGET)

.PHONY: dbt-parse dbt-compile
dbt-parse: ## Parse the dbt project (no warehouse connection needed)
	cd $(DBT_DIR) && $(DBT) parse --target $(DBT_TARGET)

dbt-compile: ## Compile the dbt project to SQL
	cd $(DBT_DIR) && $(DBT) compile --target $(DBT_TARGET)

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs, including the rule book
	cd $(DBT_DIR) && $(DBT) docs generate --target $(DBT_TARGET) && $(DBT) docs serve

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
.PHONY: airflow-up airflow-down airflow-logs airflow-trigger airflow-shell airflow-uid
# The containers run as AIRFLOW_UID and chown the directories they share with the host -- data/,
# the logs, dbt_packages -- to it on every start. Any value that is not the host user's id
# therefore revokes your own write access each time Airflow starts, and the symptom surfaces later
# somewhere else entirely: `trade-sim load` failing on data/state/trade_book.lock, or `dbt deps`
# unable to replace a file it does not own.
#
# Compose substitutes AIRFLOW_UID into `user:` while it reads the file, so it has to be in
# airflow/.env before the daemon starts and cannot be worked out at runtime. It can, however, be
# written by a Makefile that knows how to ask -- which is better than a setup document asking a
# human to keep the same number correct in two files.
airflow-uid: ## Point AIRFLOW_UID at the current user in both env files
	@for env_file in .env airflow/.env; do \
		if [ ! -f $$env_file ]; then \
			echo "AIRFLOW_UID=$$(id -u)" > $$env_file; \
		elif grep -q '^AIRFLOW_UID=' $$env_file; then \
			sed -i.bak 's/^AIRFLOW_UID=.*/AIRFLOW_UID='"$$(id -u)"'/' $$env_file && rm -f $$env_file.bak; \
		else \
			printf '\nAIRFLOW_UID=%s\n' "$$(id -u)" >> $$env_file; \
		fi; \
		echo ">> $$env_file: $$(grep '^AIRFLOW_UID=' $$env_file)"; \
	done

airflow-up: airflow-uid ## Start Airflow in Docker (http://localhost:8080, admin/admin)
	cd airflow && docker compose up -d --build
	@echo ">> Airflow starting at http://localhost:8080 -- first build takes a few minutes"

airflow-down: ## Stop Airflow and remove volumes
	cd airflow && docker compose down -v

airflow-logs: ## Tail Airflow scheduler logs
	cd airflow && docker compose logs -f airflow-scheduler

airflow-trigger: ## Trigger the trade pipeline DAG once
	cd airflow && docker compose exec airflow-scheduler airflow dags trigger trade_pipeline

airflow-shell: ## Open a shell inside the Airflow image (dbt and trade-sim available)
	cd airflow && docker compose run --rm airflow-cli bash

# Exits non-zero when the latest run failed, so this is a check rather than a display. Runs in
# the container because the scheduler owns the metadata database.
.PHONY: airflow-status
airflow-status: ## Verdict on the most recent DAG run, task by task
	cd airflow && docker compose exec -T airflow-scheduler \
		python /opt/airflow/scripts/airflow_run_status.py

# ---------------------------------------------------------------------------
# Visualisation
# ---------------------------------------------------------------------------
.PHONY: dashboard
dashboard: ## Launch the Streamlit operations dashboard (http://localhost:8501)
	$(VENV)/bin/streamlit run dashboard/app.py

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
.PHONY: lint
lint: ## Ruff + mypy (offline; SQL linting is 'make lint-sql')
	$(VENV)/bin/ruff check $(PY_PATHS)
	$(VENV)/bin/ruff format --check $(PY_PATHS)
	$(VENV)/bin/mypy ingestion/src

# Separate from `lint`, and absent from `ci-local`, because sqlfluff uses the dbt templater and
# therefore needs profiles.yml and a warehouse connection to compile the project first. Keeping it
# here would mean the offline check suite could not run offline.
#
# `macros` is not linted. A macro file is a Jinja program, not SQL -- the rule catalogue is a list
# of dicts -- and the dbt templater skips those files by design, reporting each as a warning.
.PHONY: lint-sql
lint-sql: ## sqlfluff over the models and tests (needs profiles.yml + warehouse)
	cd $(DBT_DIR) && $(CURDIR)/$(VENV)/bin/sqlfluff lint models snapshots tests

.PHONY: format
format: ## Auto-format Python (SQL is 'make format-sql')
	$(VENV)/bin/ruff format $(PY_PATHS)
	$(VENV)/bin/ruff check --fix $(PY_PATHS)

.PHONY: format-sql
format-sql: ## Auto-fix SQL layout (needs profiles.yml + warehouse)
	cd $(DBT_DIR) && $(CURDIR)/$(VENV)/bin/sqlfluff fix models snapshots tests --force

.PHONY: pytest
pytest: ## Run the Python unit tests (simulator, dashboard render, DAG helpers)
	$(VENV)/bin/pytest ingestion/tests dashboard/tests airflow/tests -q

.PHONY: airflow-validate
airflow-validate: ## Import every DAG and fail on any parse error
	@# Exit code 2 means Airflow is not installed in this virtualenv, which is the normal state
	@# here -- the DAGs run in the container and the local venv stays small. The script chose a
	@# third code for exactly this reason, so treat it as a skip rather than letting `ci-local`
	@# fail on a missing dependency it is not supposed to have. CI runs the script directly, where
	@# Airflow is installed on purpose and a 2 is a broken install worth failing on.
	@$(PY) scripts/validate_dags.py; status=$$?; \
	if [ $$status -eq 2 ]; then \
		echo ">> Skipped: validate inside the container with 'make airflow-shell'."; \
	elif [ $$status -ne 0 ]; then \
		exit $$status; \
	fi

.PHONY: selfcheck
selfcheck: ## Verify commands named in messages, docs and code all exist
	$(PY) scripts/selfcheck.py

.PHONY: ci-local
ci-local: lint pytest dbt-parse airflow-validate selfcheck ## Everything CI runs that needs no warehouse

# ---------------------------------------------------------------------------
# Demo / packaging
# ---------------------------------------------------------------------------
.PHONY: demo
demo: load drain dbt-build-incremental reconcile ## End-to-end: load, drain, transform, test, prove correct
	@echo ""
	@echo ">> Demo complete and reconciled. Run 'make dashboard' to inspect the result."

# The scheduler owns this environment. Driving the pipeline by hand while the hourly DAG is
# unpaused means two dbt runs merging into one incremental table -- both find an event_sk absent
# and both insert it -- and, before the trade book lock, two generators minting one set of trade
# identifiers. Run this first, and `make resume-writers` when you are done.
.PHONY: pause-writers
pause-writers: ## Pause the DAG and the drain task so a manual run is the only writer
	-$(PY) scripts/run_sql.py "alter task raw.task_drain_trade_event_stream suspend"
	-cd airflow && docker compose exec -T airflow-scheduler airflow dags pause trade_pipeline
	@echo ">> Scheduled writers paused. This shell is now the only writer."

.PHONY: resume-writers
resume-writers: ## Hand the environment back to the scheduler
	-$(PY) scripts/run_sql.py "alter task raw.task_drain_trade_event_stream resume"
	-cd airflow && docker compose exec -T airflow-scheduler airflow dags unpause trade_pipeline
	@echo ">> Scheduled writers resumed."

.PHONY: diagrams
diagrams: ## Render PlantUML diagrams to PNG (requires plantuml)
	plantuml -tpng -o . docs/diagrams/*.puml

# data/state is spared deliberately. It holds the generator's trade book -- its memory of every
# trade it has ever emitted -- and Snowflake holds the matching adjudicated history. Delete one
# without the other and the next run re-emits TRD-000000001, colliding with trade versions the
# warehouse has already accepted, which trips
# adjudicated_one_accepted_event_per_trade_version. `trade-sim reset-book` drops it explicitly
# and documents what else has to be cleared alongside it.
.PHONY: clean
clean: ## Remove build artefacts and generated files, keeping the generator's trade book
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs data/landing data/manifests out
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type d -name .pytest_cache -prune -exec rm -rf {} +

.PHONY: clean-all
clean-all: clean ## Also remove the virtualenv and dbt packages
	rm -rf $(VENV) $(DBT_DIR)/dbt_packages
