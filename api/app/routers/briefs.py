"""Daily-brief endpoint (Phase 11).

System-internal trigger, driven by the per-region ``daily_brief_*`` Dagster
schedules (one fire per region per local day):

* ``POST /v1/briefs/generate`` — compose the prior-local-day brief for the
  request's region, synthesize it via the Salesforce Models API, post it to the
  Fleet Operations Chatter group, and persist ``app.briefs``. Server-to-server,
  no scope filter (mirrors ``/v1/cases/sync-pending``).

The Models / Chatter round-trip lives in ``SalesforceService`` → the
``AFM_BriefSynthesis`` Apex action; the API never names a model or a ConnectApi
type (SALESFORCE.md).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_postgres_pool, get_salesforce_service
from app.models.briefs import BriefGenerateRequest, BriefGenerateSummary
from app.services.brief_service import BriefService
from app.services.postgres import PostgresPool
from app.services.salesforce import SalesforceService
from app.settings import settings

router = APIRouter(prefix="/v1/briefs", tags=["briefs"])


@router.post("/generate", response_model=BriefGenerateSummary)
async def generate_brief(
    body: BriefGenerateRequest,
    postgres: Annotated[PostgresPool, Depends(get_postgres_pool)],
    sf: Annotated[SalesforceService, Depends(get_salesforce_service)],
    force: Annotated[
        bool, Query(description="Regenerate even if this region's brief for the day exists.")
    ] = False,
) -> BriefGenerateSummary:
    """Generate, post, and persist the prior-local-day brief for ``body.region``."""
    return await BriefService(
        postgres, sf, chatter_group_id=settings.salesforce_chatter_brief_group_id
    ).generate_and_post(body.region, force=force)
