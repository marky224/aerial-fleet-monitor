"""Case endpoints.

`POST /v1/cases/sync-pending` is the system-internal push trigger: it
drains a batch of `sf_sync_status='pending'` cases into Salesforce via
`CaseSyncService`. The pipelines `sf_case_push` asset polls it; each call
also retries cases left pending by a prior transient failure.

The customer-facing read endpoints (`GET /v1/cases`, `GET /v1/cases/{id}`)
land in a later Phase-05 slice.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_postgres_pool, get_salesforce_service
from app.models.salesforce import CaseSyncSummary
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
