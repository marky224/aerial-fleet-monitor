"""Sites endpoints (API.md §5).

GET /v1/sites                       — list watched sites.
GET /v1/sites/{icao}                — single-site detail + current weather.
GET /v1/sites/{icao}/sla            — SLA scorecard (Phase 05 populates metrics).
GET /v1/sites/{icao}/inbound        — live arrivals within 60 minutes.
GET /v1/sites/{icao}/outbound       — live departures within 60 minutes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.dependencies import get_query_service, get_scope
from app.models.common import Region, Scope, SlaPeriod
from app.models.sites import (
    SiteDetail,
    SiteFlightListResponse,
    SiteListResponse,
    SiteSla,
)
from app.services.query_service import QueryService

router = APIRouter(prefix="/v1/sites", tags=["sites"])

IcaoPath = Annotated[
    str,
    Path(
        pattern=r"^[A-Za-z0-9]{4}$",
        description="4-character ICAO airport identifier, e.g. 'KSFO'.",
        examples=["KSFO"],
    ),
]


@router.get("", response_model=SiteListResponse)
def list_sites(
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
    region: Annotated[
        Region | None,
        Query(description="Filter by region. Defaults to caller's scope; can't broaden it."),
    ] = None,
) -> SiteListResponse:
    """All watched sites in scope."""
    return query_service.list_sites(scope=scope, region=region)


@router.get("/{icao}", response_model=SiteDetail)
def get_site(
    icao: IcaoPath,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
) -> SiteDetail:
    """Single-site detail + most-recent weather + 60-min flow counts."""
    return query_service.get_site(scope=scope, icao=icao)


@router.get("/{icao}/sla", response_model=SiteSla)
def get_site_sla(
    icao: IcaoPath,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
    period: Annotated[
        SlaPeriod,
        Query(description="SLA aggregation window."),
    ] = "last_24h",
) -> SiteSla:
    """SLA scorecard for a site. Metric fields are null/0 until Phase 05."""
    return query_service.get_site_sla(scope=scope, icao=icao, period=period)


@router.get("/{icao}/inbound", response_model=SiteFlightListResponse)
def list_inbound(
    icao: IcaoPath,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
) -> SiteFlightListResponse:
    """Live arrivals to the site within the last 60 minutes."""
    return query_service.list_inbound(scope=scope, icao=icao)


@router.get("/{icao}/outbound", response_model=SiteFlightListResponse)
def list_outbound(
    icao: IcaoPath,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
) -> SiteFlightListResponse:
    """Live departures from the site within the last 60 minutes."""
    return query_service.list_outbound(scope=scope, icao=icao)
