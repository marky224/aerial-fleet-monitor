"""Case endpoints.

Three system-internal triggers, each driven by a pipelines asset:

* `POST /v1/cases/sync-pending` (push) — drains `sf_sync_status='pending'`
  cases into Salesforce via `CaseSyncService`. The `sf_case_push` asset
  polls it; each call also retries cases left pending by a prior transient
  failure.
* `POST /v1/cases/sync-from-sf` (pull) — mirrors Cases modified in
  Salesforce back into `app.cases` since the persisted watermark. The
  `sf_case_sync` asset polls it (PIPELINES.md §3.5).
* `GET /v1/cases/all-for-sync` (read) — paginated server-to-server snapshot
  of `app.cases` for the Foundry sync (`foundry_cases_sync` asset). No
  scope filter; the customer-facing scope-gated reads (`GET /v1/cases`,
  `GET /v1/cases/{id}`) land in a later Phase-05 slice.

The two SF endpoints keep field/region translation inside `SalesforceService`
(SALESFORCE.md §10.1); the pipelines venv reaches Salesforce only through
the API.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_postgres_pool, get_salesforce_service
from app.models.cases import CasesForSyncPage
from app.models.salesforce import CasePullSummary, CaseSyncSummary
from app.services.case_sync import CaseSyncService
from app.services.postgres import PostgresPool
from app.services.salesforce import SalesforceService

router = APIRouter(prefix="/v1/cases", tags=["cases"])


@router.post("/sync-pending", response_model=CaseSyncSummary)
async def sync_pending(
    postgres: Annotated[PostgresPool, Depends(get_postgres_pool)],
    sf: Annotated[SalesforceService, Depends(get_salesforce_service)],
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> CaseSyncSummary:
    """Push up to `limit` pending cases to Salesforce; reconcile app.cases."""
    return await CaseSyncService(postgres, sf).push_pending(limit=limit)


@router.post("/sync-from-sf", response_model=CasePullSummary)
async def sync_from_sf(
    postgres: Annotated[PostgresPool, Depends(get_postgres_pool)],
    sf: Annotated[SalesforceService, Depends(get_salesforce_service)],
    limit: Annotated[int, Query(ge=1, le=200)] = 200,
) -> CasePullSummary:
    """Pull up to `limit` Salesforce-modified Cases into app.cases; advance watermark."""
    return await CaseSyncService(postgres, sf).pull_from_sf(limit=limit)


@router.get("/all-for-sync", response_model=CasesForSyncPage)
async def all_for_sync(
    postgres: Annotated[PostgresPool, Depends(get_postgres_pool)],
    since: Annotated[
        datetime | None,
        Query(description="Return rows with updated_at > since (incremental cursor)."),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> CasesForSyncPage:
    """Paginated server-to-server snapshot of `app.cases` for Foundry sync."""
    return await CaseSyncService(postgres).list_for_sync(since=since, limit=limit)
