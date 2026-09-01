# ADR 0009: A custom Airflow image, with dbt inside it

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** [`setup.md`](../setup.md#the-custom-image)

---

## Context

The Airflow tasks need `dbt-core`, `dbt-snowflake`, the `trade_sim` package, and the Snowflake
connector. The official `apache/airflow` image has none of them.

Airflow's `docker-compose` quickstart offers `_PIP_ADDITIONAL_REQUIREMENTS`, which pip-installs a list
at container start. It is the path of least resistance and is what most tutorials use.

## Decision

**Build a custom image from `apache/airflow` with every dependency baked in, and install dbt into the
same image.**

Dependencies are installed at build time against Airflow's published constraints file. The build also
runs version assertions — `dbt --version` and an import of `trade_sim` — so a broken image fails
during `docker build` rather than at task runtime.

`docker-compose.yml` mounts the `dbt/` project, the `data/` directory and `.secrets/` as volumes, so
editing a model does not require a rebuild.

## Alternatives considered

### `_PIP_ADDITIONAL_REQUIREMENTS` on the stock image

Rejected for three reasons, the third being decisive:

1. **It reinstalls on every container start.** Slow enough to be irritating in local development,
   where containers restart often.
2. **It needs network access at runtime.** A container that cannot reach PyPI cannot start, so a
   transient PyPI problem becomes an Airflow outage. Runtime dependencies on external services are
   how a working system stops working without anything having changed.
3. **The environment is not reproducible.** Without a lock, the container that starts on Friday can
   differ from Monday's because a transitive dependency published a release. The failure appears as a
   pipeline breaking with no commit to blame, which is among the worst things to debug. Airflow's own
   documentation describes this variable as suitable for testing only.

A built image makes the environment a **build artifact**: it is produced once, it is identical
everywhere it runs, and it can be tagged and rolled back.

### dbt in a separate container or a `KubernetesPodOperator`

The cleanest separation, and the right answer at scale — dbt and Airflow have genuinely conflicting
dependency preferences, and isolating them removes a class of resolution problems permanently.

Rejected for the local deployment on footprint grounds. It requires either a second image built and
orchestrated by compose, or Kubernetes. The setup guide's cost is already dominated by Docker, and
`make airflow-up` needing a cluster would put the project out of reach of the person meant to install
it in half an hour.

The dependency conflict this risks is real but small in practice: both pin the Snowflake connector,
and installing against Airflow's constraints file resolves it. If it stopped resolving, splitting the
image is the answer and the DAG would not change, because `BashOperator` invoking `dbt` becomes a
`DockerOperator` invoking the same command.

### `dbt-core` installed on the host, invoked over SSH

Rejected immediately. It reintroduces "works on my machine" as an architectural property.

### The `astronomer` runtime image

Well maintained and includes much of this. Rejected to keep the base image the vanilla upstream one,
so the Dockerfile shows exactly what was added and why — which matters more for a project whose
purpose is to be read.

## Consequences

### Good

- **Reproducible.** The same image runs on a laptop, in CI and in a registry. A dependency change is a
  commit and a rebuild, both visible in git.
- **Fails at build, not at 3am.** The build asserts that dbt runs and `trade_sim` imports. A
  dependency resolution problem surfaces during `docker build` with the resolver's output in front of
  you, rather than as a task failure with `dbt: command not found`.
- **Starts fast.** No install on boot, so container restarts are seconds. This matters more than it
  sounds during development, where the compose stack is cycled repeatedly.
- **Works offline.** Once built, the stack needs no network except to reach Snowflake.
- **dbt and Airflow share the `trade_sim` install**, so the loader, the reconciler and the DAG use the
  same connection code. One place to fix an authentication bug.
- Mounting the dbt project as a volume keeps the fast iteration loop: edit a model, re-run the task,
  no rebuild.

### Bad

- **First build takes several minutes** and is the slowest step in the setup guide. Unavoidable, and
  layer ordering puts the slow-changing dependency install above the fast-changing project files so
  subsequent rebuilds hit the cache.
- **A dependency change requires `make airflow-down && make airflow-up`.** People forget, and the
  symptom — `dbt: command not found`, or an old version behaving unexpectedly — does not obviously
  point at a stale image. It is in the troubleshooting section for that reason.
- **The image is large**, around 2 GB. It dominates the project's disk requirement.
- **dbt and Airflow are coupled in one dependency graph.** Upgrading Airflow can force a dbt
  reinstall and vice versa. Accepted knowingly, with the separate-container path as the exit if it
  becomes painful.

### Neutral

- Installing against Airflow's constraints file means Airflow's pins win any conflict. That is the
  correct precedence — a broken scheduler is worse than an older `dbt-snowflake` — but it does mean a
  brand-new dbt release may not be installable immediately.

## Notes

Two details in the Dockerfile are deliberate and easy to remove by accident.

**Version assertions in the build.** `RUN dbt --version && python -c "import trade_sim"` costs a
second and converts the most confusing possible failure — a task failing at runtime because a package
resolved but did not actually work — into a build failure with the resolver's full output.

**Layer ordering.** Requirements files are copied and installed *before* the DAGs and project files
are copied. Reversing this would invalidate the dependency layer on every source edit, turning a
five-second rebuild into a five-minute one. It is the single most common Dockerfile mistake and the
cost is paid on every iteration.
