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


settings = Settings()
"""Module-level singleton. Import as `from app.settings import settings`."""
