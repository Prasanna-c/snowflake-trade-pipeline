# =============================================================================
# Airflow image with dbt and the trade simulator installed.
#
# WHY A CUSTOM IMAGE RATHER THAN _PIP_ADDITIONAL_REQUIREMENTS
#
# The official compose file offers _PIP_ADDITIONAL_REQUIREMENTS, which installs packages at
# container start. It is convenient and wrong for anything beyond a first look:
#
#   * every container restart re-downloads and re-resolves the dependency tree, so startup
#     takes minutes instead of seconds,
#   * the resolved versions can differ between the scheduler and the worker, which produces
#     failures that appear only on one of them,
#   * a network problem at start time becomes an Airflow outage.
#
# Building once gives a deterministic, offline-startable image. That is worth a Dockerfile.
#
# WHY DBT IS IN THE SAME IMAGE AS AIRFLOW
#
# The alternative -- dbt in its own container, invoked by DockerOperator or
# KubernetesPodOperator -- is the better production pattern: dbt's dependency tree and
# Airflow's are both large, both pin things aggressively, and keeping them apart avoids a
# resolver conflict blocking an Airflow upgrade.
#
# Here they share an image because the whole point is that a reviewer can run this on a
# laptop with `docker compose up`. Requiring a container registry, or docker-in-docker, to
# see the DAG work is a poor trade. The constraint file below is what keeps the shared
# environment resolvable, and docs/adr/0009-airflow-dbt-packaging.md records the decision and
# the migration path.
# =============================================================================

FROM apache/airflow:2.10.5-python3.11

USER root

# Build dependencies for cryptography (used by the Snowflake key-pair authentication) and
# git for dbt package installation. Removed from the layer immediately: they are needed to
# build wheels, not to run the image, and leaving them in enlarges the image and the
# vulnerability surface for no benefit.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
        git \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow's own constraint file. Installing anything into an Airflow image without it is how
# a transitive dependency silently upgrades a provider and breaks the scheduler at 2am.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Paths here are relative to the build context, which docker-compose.yml sets to the repository
# root so that `ingestion` below is reachable -- not to this file's own directory.
COPY airflow/requirements-airflow.txt /tmp/requirements-airflow.txt

RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        -r /tmp/requirements-airflow.txt

# dbt is installed WITHOUT the Airflow constraints. dbt-core pins its own Jinja2 and
# protobuf ranges, and forcing Airflow's pins onto it produces an unsolvable resolution.
# Installing it in a separate step lets pip satisfy dbt's requirements while leaving the
# already-installed Airflow tree alone.
RUN pip install --no-cache-dir \
        "dbt-core==1.9.4" \
        "dbt-snowflake==1.9.2"

# The trade simulator, installed as a package so the DAG imports the same code a developer
# runs locally -- one implementation of Snowflake connection handling, not two.
# Editable so a change to the simulator is picked up by restarting the container rather than
# rebuilding the image.
COPY --chown=airflow:root ingestion /opt/airflow/ingestion
RUN pip install --no-cache-dir -e /opt/airflow/ingestion

# Fail the build if the versions that matter are not what we asked for. A silent downgrade
# during resolution is otherwise discovered at DAG-parse time, in production.
RUN python -c "import dbt.version; print('dbt', dbt.version.get_installed_version().to_version_string())" \
    && python -c "import trade_sim; print('trade_sim', trade_sim.__version__)" \
    && python -c "import snowflake.connector; print('snowflake-connector', snowflake.connector.__version__)" \
    && python -c "from importlib.metadata import version; print('snowflake provider', version('apache-airflow-providers-snowflake'))"

ENV PYTHONPATH="/opt/airflow/dags:${PYTHONPATH}" \
    DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1
