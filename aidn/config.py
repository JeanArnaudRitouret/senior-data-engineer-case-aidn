from __future__ import annotations

"""Runtime settings — loads all pipeline parameters from environment variables."""

from pathlib import Path
from typing import Self

from pydantic import PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# sslmode values that permit a plaintext connection — rejected because it's insecure
_INSECURE_SSLMODES: frozenset[str] = frozenset(
    {"sslmode=disable", "sslmode=allow", "sslmode=prefer"}
)


class Settings(BaseSettings):
    """Pipeline runtime settings loaded from environment variables or .env file.

    Attributes:
        postgres_url: Source Postgres connection string; must use sslmode=require or stricter.
        postgres_repl_url: Replication connection string; same TLS requirement as postgres_url.
        postgres_source_schema: Postgres schema containing the source tables (default: public).
        duckdb_path: Path to the DuckDB analytical store file.
        dlt_data_dir: Directory where dlt stores pipeline state and secrets.
        log_level: Python logging level name (default: INFO).
    """

    postgres_url: PostgresDsn
    postgres_repl_url: PostgresDsn
    postgres_source_schema: str = "public"
    duckdb_path: Path
    dlt_data_dir: Path
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def _refuse_insecure_sslmode(self) -> Self:
        """Raise ValueError if either connection URL uses an sslmode that allows plaintext."""
        for field_name, url in (
            ("postgres_url", self.postgres_url),
            ("postgres_repl_url", self.postgres_repl_url),
        ):
            url_str = str(url)
            for insecure in _INSECURE_SSLMODES:
                if insecure in url_str:
                    raise ValueError(
                        f"{field_name} uses '{insecure}' — transport encryption is required. "
                        "Use sslmode=require, sslmode=verify-ca, or sslmode=verify-full."
                    )
        return self
