"""Flights endpoints (API.md §4).

GET /v1/flights/{icao24}            — current flight detail + registry metadata.
GET /v1/flights/{icao24}/trail      — historical trail from the lakehouse.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query

from app.dependencies import get_query_service, get_scope
from app.models.common import Scope, TrailLookback
from app.models.flights import FlightDetail, TrailResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/v1/flights", tags=["flights"])

Icao24Path = Annotated[
    str,
    Path(
        pattern=r"^[0-9a-fA-F]{6}$",
        description="6-character hex ICAO 24-bit aircraft identifier, e.g. 'a1b2c3'.",
        examples=["a1b2c3"],
    ),
]


@router.get("/{icao24}", response_model=FlightDetail)
def get_flight(
    icao24: Icao24Path,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
) -> FlightDetail:
    """Current state + registry metadata for one flight.

    404 if the icao24 hasn't been observed in the last 30 minutes.
    """
    return query_service.get_flight(scope=scope, icao24=icao24)


@router.get("/{icao24}/trail", response_model=TrailResponse)
def get_flight_trail(
    icao24: Icao24Path,
    scope: Annotated[Scope, Depends(get_scope)],
    query_service: Annotated[QueryService, Depends(get_query_service)],
    lookback: Annotated[
        TrailLookback,
        Query(
            description=(
                "Trail lookback window. 'since_takeoff' is capped at 6 hours "
                "server-side to bound query cost."
            ),
        ),
    ] = "2h",
) -> TrailResponse:
    """Historical trail for one flight from the Parquet lakehouse."""
    return query_service.get_flight_trail(scope=scope, icao24=icao24, lookback=lookback)
