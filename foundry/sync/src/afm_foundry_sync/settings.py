"""Settings loader for the AFM Foundry sync service.

Loads Foundry tenant configuration from ``_private/foundry/.env`` (gitignored)
via pydantic-settings. The .env file is the single source of truth for tenant
URL, bearer token, and target Ontology identifiers — these values must never
appear in the public-tree repo, in commit messages, or in ``.env.example``.

Auth model: bearer token (universal scope). Developer-tier Foundry doesn't
issue OAuth client credentials, so the user-scoped token is passed directly
as ``Authorization: Bearer <token>`` on every Foundry request. No refresh
dance — rotation is manual before the token's expiry.

Failure mode: if any required field is missing, pydantic-settings raises
``ValidationError`` at load time. The sync-job layer is responsible for
catching this and translating it into a ``FoundrySyncSkipped`` outcome (see
``_private/docs/build/03_foundry_dashboard.md`` Implementation notes), so
the local stack continues to run when Foundry credentials aren't configured.
"""

from pathlib import Path

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve _private/foundry/.env relative to the repo root. This file lives at
# foundry/sync/src/afm_foundry_sync/settings.py, so the repo root is four
# levels up. Works for editable installs (dev workflow). A wheel install would
# break this assumption — not in scope for Phase 03.
_REPO_ROOT = Path(__file__).resolve().parents[4]
_ENV_PATH = _REPO_ROOT / "_private" / "foundry" / ".env"


class FoundrySettings(BaseSettings):
    """Foundry tenant + Ontology configuration.

    Loaded from ``_private/foundry/.env`` at process start. Raises pydantic
    ``ValidationError`` if any required field is missing or malformed.
    """

    model_config = SettingsConfigDict(
        env_file=_ENV_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )

    FOUNDRY_TENANT_URL: HttpUrl = Field(
        ...,
        description="Base URL of the Foundry tenant.",
    )
    FOUNDRY_TOKEN: str = Field(
        ...,
        min_length=1,
        description="Bearer token for the Foundry tenant. Universal scope, user-issued.",
    )
    FOUNDRY_ONTOLOGY_API_NAME: str = Field(
        ...,
        min_length=1,
        description="apiName of the AFM Ontology inside the tenant.",
    )
    FOUNDRY_ONTOLOGY_RID: str = Field(
        ...,
        min_length=1,
        description="Resource identifier (rid) of the AFM Ontology.",
    )
    FOUNDRY_ACTION_UPSERT_AIRCRAFT: str = Field(
        ...,
        min_length=1,
        description="apiName of the modify-or-create Action that upserts Aircraft objects.",
    )
    FOUNDRY_ACTION_UPSERT_SITE: str = Field(
        ...,
        min_length=1,
        description="apiName of the modify-or-create Action that upserts Site objects.",
    )
    AFM_API_BASE: str = Field(
        default="http://localhost:8000",
        description="Base URL of the local AFM /v1 API the sync reads from.",
    )
