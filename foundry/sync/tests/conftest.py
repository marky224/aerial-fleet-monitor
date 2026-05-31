"""Shared fixtures for the AFM Foundry sync test suite."""

from __future__ import annotations

import pytest

from afm_foundry_sync.settings import FoundrySettings


@pytest.fixture
def settings() -> FoundrySettings:
    """A FoundrySettings instance built from fixed test values.

    Bypasses ``_private/foundry/.env`` so tests don't depend on the real
    tenant config.
    """
    return FoundrySettings(
        FOUNDRY_TENANT_URL="https://tenant.example.com/",
        FOUNDRY_TOKEN="t-test-token",
        FOUNDRY_ONTOLOGY_API_NAME="afm",
        FOUNDRY_ONTOLOGY_RID="ri.ontology.test.afm",
        FOUNDRY_ACTION_UPSERT_AIRCRAFT="upsert-aircraft",
        FOUNDRY_ACTION_UPSERT_SITE="upsert-site",
        FOUNDRY_ACTION_UPSERT_FLIGHT="upsert-flight",
        FOUNDRY_ACTION_DELETE_AIRCRAFT="delete-aircraft",
        FOUNDRY_ACTION_DELETE_FLIGHT="delete-flight",
        FOUNDRY_ACTION_UPSERT_CASE="upsert-case",
        FOUNDRY_ACTION_DELETE_CASE="delete-case",
        # Pin the liveness flag OFF explicitly (init kwargs override the
        # env_file): otherwise FoundrySettings reads _private/foundry/.env and a
        # locally-flipped FOUNDRY_FLIGHT_ISLIVE_ENABLED=true would leak in and
        # silently turn the sweep on in every flag-off test. settings_islive
        # flips it back on where the liveness paths are exercised.
        FOUNDRY_FLIGHT_ISLIVE_ENABLED=False,
        AFM_API_BASE="http://api.test",
    )


@pytest.fixture
def settings_islive(settings: FoundrySettings) -> FoundrySettings:
    """``settings`` with the Flight.isLive liveness flag enabled.

    Models the post-provisioning state (FOUNDRY_FLIGHT_ISLIVE_ENABLED=true)
    so the liveness write paths + reconcile sweep are exercised; the default
    ``settings`` keeps the flag off (the safe pre-provisioning default).
    """
    return settings.model_copy(update={"FOUNDRY_FLIGHT_ISLIVE_ENABLED": True})
