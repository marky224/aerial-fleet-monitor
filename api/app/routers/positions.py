"""Positions endpoints (API.md §3).

GET /v1/positions/live      — all in-scope live aircraft positions.
GET /v1/positions/stream    — reserved for v2, returns 501.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_query_service, get_scope
from app.exceptions import BadRequest, NotImplementedYet
from app.models.common import Region, Scope
from app.models.positions import PositionsLiveResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/v1/positions", tags=["positions"])


def parse_bbox(
    bbox: Annotated[
        str | None,
        Query(
            description=(
                "Optional bounding box, 'lat_min,lon_min,lat_max,lon_max'. "
                "Lat in [-90, 90], lon in [-180, 180]; min must be <= max on each axis."
            ),
            examples=["32.0,-125.0,49.0,-114.0"],
        ),
    ] = None,
) -> tuple[float, float, float, float] | None:
    """Parse and validate the bbox query param. None passes through; bad input raises 400."""
    if bbox is None:
        return None
    parts = bbox.split(",")
    if len(parts) != 4:
        raise BadRequest("bbox must have 4 comma-separated values")
    try:
        lat_min, lon_min, lat_max, lon_max = (float(p) for p in parts)
    except ValueError as e:
        raise BadRequest("bbox values must be numeric") from e
    if not (-90 <= lat_min <= lat_max <= 90):
        raise BadRequest("bbox latitudes must be in [-90, 90] and lat_min <= lat_max")
    if not (-180 <= lon_min <= lon_max <= 180):
        raise BadRequest("bbox longitudes must be in [-180, 180] and lon_min <= lon_max")
    return (lat_min, lon_min, lat_max, lon_max)


@router.get("/live", response_model=PositionsLiveResponse)
def list_live(
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
    bbox: Annotated[tuple[float, float, float, float] | None, Depends(parse_bbox)],
    region: Annotated[
        Region | None,
        Query(
            description=(
                "Override scope to a specific region. Rejected with 403 if the "
                "caller's scope is narrower than the requested region."
            ),
        ),
    ] = None,
) -> PositionsLiveResponse:
    """All in-scope live positions. Optional bbox + region filters.

    Polling cadence from the dashboard sync: every 30 s (API.md §3.1).
    """
    return query_service.list_live_positions(scope=scope, bbox=bbox, region=region)


@router.get("/stream")
def stream_stub() -> None:
    """Reserved for v2 — emits the standard 501 envelope.

    API.md §3.2 designates this path for a server-pushed positions
    snapshot stream. Phase 02 ships the live polling endpoint instead;
    the stream path is kept on the surface so clients see an explicit
    not-implemented response, not a 404.
    """
    raise NotImplementedYet(
        "WebSocket /v1/positions/stream is reserved for v2. "
        "Use GET /v1/positions/live with 30s polling for now."
    )
