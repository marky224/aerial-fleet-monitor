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
        AFM_API_BASE="http://api.test",
    )
