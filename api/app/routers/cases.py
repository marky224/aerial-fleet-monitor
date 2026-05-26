"""Case endpoints.

Two consumer families:

System-internal triggers (each driven by a pipelines asset):

* `POST /v1/cases/sync-pending` (push) — drains `sf_sync_status='pending'`
  cases into Salesforce via `CaseSyncService`. The `sf_case_push` asset
  polls it; each call also retries cases left pending by a prior transient
  failure.
* `POST /v1/cases/sync-from-sf` (pull) — mirrors Cases modified in
  Salesforce back into `app.cases` since the persisted watermark. The
  `sf_case_sync` asset polls it (PIPELINES.md §3.5).
* `GET /v1/cases/all-for-sync` (read) — paginated server-to-server snapshot
  of `app.cases` for the Foundry sync (`foundry_cases_sync` asset). No
  scope filter.

Customer-facing scope-gated reads (Phase 05 task #4, API.md §6.1/§6.2):

* `GET /v1/cases` — list cases visible in the caller's scope.
* `GET /v1/cases/{case_id}` — one case + ordered timeline.

The two SF endpoints keep field/region translation inside `SalesforceService`
(SALESFORCE.md §10.1); the pipelines venv reaches Salesforce only through
the API.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import (
    get_postgres_pool,
    get_query_service,
    get_salesforce_service,
    get_scope,
)
from app.models.cases import CaseDetail, CaseListResponse, CasesForSyncPage
from app.models.common import Region, Scope
from app.models.salesforce import CasePullSummary, CaseSyncSummary
from app.services.case_sync import CaseSyncService
from app.services.postgres import PostgresPool
from app.services.query_service import QueryService
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


# Customer-facing reads — declared LAST so the static `/{name}` routes above
# (`/all-for-sync`, `/sync-pending`, `/sync-from-sf`) win over `/{case_id}`.


@router.get("", response_model=CaseListResponse)
def list_cases(
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
    status: Annotated[
        list[str] | None,
        Query(
            description=(
                "Status filter. Repeatable (`?status=open&status=in_progress`). "
                "Default: open + acknowledged + in_progress (omits resolved, "
                "matching the Workshop App-1 panel)."
            ),
        ),
    ] = None,
    severity: Annotated[
        str | None,
        Query(description="Severity filter (low/medium/high/critical)."),
    ] = None,
    site: Annotated[
        str | None,
        Query(description="Site ICAO filter (uppercased server-side)."),
    ] = None,
    region: Annotated[
        Region | None,
        Query(
            description=(
                "Override scope to a specific region. Rejected with 403 if the "
                "caller's scope is narrower than the requested region."
            ),
        ),
    ] = None,
) -> CaseListResponse:
    """List cases visible in caller's scope (API.md §6.1)."""
    return query_service.list_cases(
        scope=scope, status=status, severity=severity, site=site, region=region
    )


@router.get("/{case_id}", response_model=CaseDetail)
def get_case(
    case_id: str,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
) -> CaseDetail:
    """One case + ordered timeline (API.md §6.2). 404 / 403 standard."""
    return query_service.get_case(scope=scope, case_id=case_id)
