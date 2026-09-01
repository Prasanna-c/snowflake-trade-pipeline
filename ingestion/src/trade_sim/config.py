"""Configuration, resolved from the environment with `.env` support.

Everything the simulator and loader need comes from here, so there is exactly one
place that reads `os.environ`. That is what makes the same code usable from the CLI,
from pytest and from an Airflow task without three different config paths.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_repo_root() -> Path:
    """Walk up from this file until a directory containing `dbt/` is found."""
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "dbt").is_dir() and (candidate / "terraform").is_dir():
            return candidate
    # Installed as a wheel outside the repo; fall back to the working directory.
    return Path.cwd()


REPO_ROOT = _find_repo_root()


class SnowflakeSettings(BaseSettings):
    """Snowflake connection parameters."""

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        env_prefix="SNOWFLAKE_",
        extra="ignore",
    )

    account: str = Field(..., description="Account identifier, e.g. ab12345.eu-central-1.")
    user: str
    role: str
    warehouse: str
    database: str
    schema_name: str = Field(default="RAW", alias="SNOWFLAKE_SCHEMA")

    load_warehouse: str | None = Field(default=None)

    #: Deployment environment. Only prod addresses dbt's schemas by their bare names.
    env: str = Field(default="dev")
    #: dbt's target schema, which becomes the prefix on every schema dbt builds outside prod.
    dbt_target_schema: str = Field(default="DBT_LOCAL", alias="DBT_SCHEMA")

    private_key_path: Path | None = Field(default=None)
    private_key_passphrase: SecretStr | None = Field(default=None)
    password: SecretStr | None = Field(default=None)

    #: Applied to every session. Makes every statement attributable in QUERY_HISTORY.
    query_tag_prefix: str = Field(default="project=trade-pipeline")

    # Retry policy for transient failures. Snowflake surfaces network blips and
    # warehouse-resume races as retryable errors; retrying in the client is cheaper
    # than failing an Airflow task and waiting for its retry.
    login_timeout_seconds: int = Field(default=60)
    network_timeout_seconds: int = Field(default=300)
    max_retries: int = Field(default=3)

    @model_validator(mode="after")
    def _require_one_credential(self) -> SnowflakeSettings:
        has_key = self.private_key_path is not None and str(self.private_key_path).strip() != ""
        has_password = self.password is not None and self.password.get_secret_value().strip() != ""
        if not has_key and not has_password:
            raise ValueError(
                "No Snowflake credential found. Set SNOWFLAKE_PRIVATE_KEY_PATH (preferred) "
                "or SNOWFLAKE_PASSWORD in .env. Run `make keypair` to generate a key pair."
            )
        if has_key and not Path(self.private_key_path or "").expanduser().is_file():
            raise ValueError(
                f"SNOWFLAKE_PRIVATE_KEY_PATH points at {self.private_key_path}, which does not exist."
            )
        return self

    @property
    def effective_load_warehouse(self) -> str:
        return self.load_warehouse or self.warehouse

    def dbt_schema(self, layer: str) -> str:
        """Resolve a dbt layer name to the schema it actually lives in.

        Outside prod, dbt prefixes its schemas with the target schema -- DBT_LOCAL_CORE on a
        laptop, PR_412_CORE in CI -- so concurrent builds can share one database without
        overwriting each other. Mirrors dbt/macros/utils/generate_schema_name.sql.

        RAW and MONITORING are built by the Snowflake-native SQL layer, not by dbt, and keep
        their bare names in every environment; do not pass them here.
        """
        if self.env.strip().lower() in ("prod", "production"):
            return layer.upper()
        return f"{self.dbt_target_schema.strip().upper()}_{layer.upper()}"


class SimulatorSettings(BaseSettings):
    """Trade generation parameters."""

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        env_prefix="TRADE_SIM_",
        extra="ignore",
    )

    seed: int = Field(default=42, description="Fixed by default so demos are reproducible.")
    output_dir: Path = Field(default=REPO_ROOT / "data" / "landing")
    state_dir: Path = Field(default=REPO_ROOT / "data" / "state")

    #: Fraction of events that carry an injected fault.
    error_rate: float = Field(default=0.08, ge=0.0, le=1.0)
    #: Fraction of events that amend a previously emitted trade rather than being new.
    amend_rate: float = Field(default=0.20, ge=0.0, le=1.0)
    #: Fraction of amendments that are cancellations.
    cancel_rate: float = Field(default=0.05, ge=0.0, le=1.0)
    #: Fraction of events written as a same-version replacement (exercises rule 2).
    replace_rate: float = Field(default=0.04, ge=0.0, le=1.0)
    #: Fraction of events written with a stale version (exercises rule 1).
    stale_version_rate: float = Field(default=0.04, ge=0.0, le=1.0)
    #: Fraction of new trades given a maturity date within 3 days (exercises rule 4).
    near_maturity_rate: float = Field(default=0.06, ge=0.0, le=1.0)

    #: gzip the NDJSON. Always true in practice; switchable for readable debugging.
    compress: bool = Field(default=True)

    @model_validator(mode="after")
    def _rates_are_coherent(self) -> SimulatorSettings:
        state_mutating = self.replace_rate + self.stale_version_rate
        if state_mutating >= 1.0:
            raise ValueError("replace_rate + stale_version_rate must be below 1.0.")
        return self


class DataQualitySettings(BaseSettings):
    """Thresholds for the pipeline's data quality gate."""

    model_config = SettingsConfigDict(
        env_file=REPO_ROOT / ".env",
        env_file_encoding="utf-8",
        env_prefix="DQ_",
        extra="ignore",
    )

    max_reject_rate: float = Field(default=0.25, ge=0.0, le=1.0)
    max_file_delay_minutes: int = Field(default=90, ge=1)
    #: Minimum events before the reject-rate gate is meaningful. Below this, a single
    #: rejection produces a misleading percentage.
    min_events_for_gate: int = Field(default=50, ge=1)


@lru_cache(maxsize=1)
def snowflake_settings() -> SnowflakeSettings:
    return SnowflakeSettings()


@lru_cache(maxsize=1)
def simulator_settings() -> SimulatorSettings:
    return SimulatorSettings()


@lru_cache(maxsize=1)
def dq_settings() -> DataQualitySettings:
    return DataQualitySettings()
