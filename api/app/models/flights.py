"""Flight models for /v1/flights/* (API.md §4).

Two endpoints: GET /v1/flights/{icao24} (FlightDetail) and
GET /v1/flights/{icao24}/trail (TrailResponse). FlightDetail composes
Position from `positions.py`; lookback values come from TrailLookback
in `models/common.py`.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from app.models.common import CustomerRegion, FlightStage, TrailLookback
from app.models.positions import Position


class FlightStatusEvent(BaseModel):
    """A discrete event in a flight's lifecycle timeline.

    Phase 02 returns an empty timeline — events are populated in Phase 05
    when the case-detector pipeline begins emitting them.
    """

    stage: FlightStage = Field(description="Lifecycle stage of the event.")
    occurred_at: datetime = Field(description="UTC timestamp the stage was entered.")


class FlightDetail(BaseModel):
    """Current state + metadata for a single flight.

    Read path: app.current_positions (telemetry) joined with
    ref.aircraft_registry (type/registration). `origin_icao` and
    `destination_icao` are reserved for a future flight-plan asset
    and are always null in v1.
    """

    icao24: str = Field(description="Lowercase 24-bit ICAO transponder address.")
    callsign: str | None = Field(description="Mode S callsign or null if not transmitted.")
    registration: str | None = Field(description="Tail number from the aircraft registry.")
    aircraft_type: str | None = Field(description="ICAO type code (e.g. 'B738') from the registry.")
    operator_icao: str | None = Field(
        description="ICAO operator code (e.g. 'UAL'); often derived from callsign prefix."
    )
    origin_icao: str | None = Field(
        description=(
            "Departure airport ICAO. Reserved for a future flight-plan asset " "— null in v1."
        )
    )
    destination_icao: str | None = Field(
        description=(
            "Arrival airport ICAO. Reserved for a future flight-plan asset " "— null in v1."
        )
    )
    customer_region: CustomerRegion = Field(
        description="Region attribution if currently inside a customer zone."
    )
    position: Position = Field(description="Most recent observed position.")
    eta_minutes: int | None = Field(
        description="Estimated minutes to destination if computable; null otherwise."
    )
    status_timeline: list[FlightStatusEvent] = Field(
        description="Lifecycle events for this flight. Empty until Phase 05 populates it."
    )
    open_case_ids: list[str] = Field(
        description="AFM case IDs currently open against this flight. Empty until Phase 05."
    )


class TrailPoint(BaseModel):
    """One historical position in a flight's trail."""

    ts: datetime = Field(description="UTC timestamp of this position observation.")
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees (WGS84).")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees (WGS84).")
    altitude_ft: int | None = Field(description="Barometric altitude above MSL in feet.")
    speed_kt: int | None = Field(description="Ground speed in knots.")


class TrailResponse(BaseModel):
    """Response for GET /v1/flights/{icao24}/trail.

    Trail points are ordered chronologically (oldest -> newest). For
    `lookback='since_takeoff'`, the server caps at 6 hours of history
    to bound DuckDB query cost.
    """

    icao24: str = Field(description="Lowercase 24-bit ICAO transponder address.")
    points: list[TrailPoint] = Field(description="Ordered list of historical positions.")
    lookback: TrailLookback = Field(
        description="Requested lookback window (echoed from the query param)."
    )
    point_count: int = Field(
        description="Length of `points` (mirrors API.md §1.5 list convention)."
    )
