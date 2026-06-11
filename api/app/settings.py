"""Typed application settings loaded from environment variables.

Driven by pydantic-settings. The single module-level `settings` instance
is constructed at import time so any module can `from app.settings import
settings` and get a fully validated object.

Variables follow the `.env.example` naming. Anything Phase 00 doesn't
need is intentionally absent from this class — phase-specific settings
get added when the matching phase lands.
"""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration for the API."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # tolerate phase-future vars in .env
        case_sensitive=False,
    )

    # Service identity
    service_name: str = Field(default="afm-api")
    service_version: str = Field(default="1.0.0")
    environment: Literal["dev", "prod"] = Field(default="dev")

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")
    log_format: Literal["json", "console"] = Field(default="console")

    # Database — driven by DATABASE_URL in .env. Required (no default) so
    # missing config fails at import time instead of at first DB call in
    # Phase 01. Tests will pass a test DSN via env override or a test .env.
    database_url: str = Field(...)

    # Dagster's run store lives in a separate `dagster` database on the same
    # Postgres server (see pipelines/dagster.yaml). Phase 09 observability
    # reads it for pipeline-run metrics. If DAGSTER_DATABASE_URL isn't in the
    # API's env, `dagster_dsn` derives it from database_url (same creds/host,
    # db name -> "dagster").
    dagster_database_url: str | None = Field(default=None)

    @property
    def dagster_dsn(self) -> str:
        """DSN for the Dagster run-store database (observability metrics only)."""
        if self.dagster_database_url:
            return self.dagster_database_url
        parts = urlsplit(self.database_url)
        return urlunsplit(parts._replace(path="/dagster"))

    # Parquet lakehouse root. `/lake` in the docker-compose `parquet_lake`
    # volume; same env var name (AFM_LAKE_PATH) pipelines uses, so a single
    # value in .env covers both services.
    afm_lake_path: str = Field(default="/lake")

    # Whether to expose the FastAPI auto-generated docs (/docs, /redoc,
    # /openapi.json). Defaults to False — secure by default. Set
    # EXPOSE_DOCS=true in .env only when running locally without public
    # exposure, or once Phase 06 lands JWT auth that can gate the routes.
    expose_docs: bool = Field(default=False)

    # Phase 04 — Salesforce (Agentforce DE org). Optional so the API still
    # boots in Phase 02/03 (and any env without SF) — SalesforceService
    # validates presence at first use and raises a clean 503 otherwise.
    # AFM→SF auth is OAuth 2.0 Client Credentials against the org's My
    # Domain token endpoint (see docs/build/04_salesforce_setup.md).
    salesforce_instance_url: str | None = Field(default=None)
    salesforce_client_id: str | None = Field(default=None)
    salesforce_client_secret: str | None = Field(default=None)
    salesforce_case_record_type: str = Field(default="Fleet_Operations")


settings = Settings()
"""Module-level singleton. Import as `from app.settings import settings`."""
