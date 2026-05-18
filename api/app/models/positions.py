"""Position models for GET /v1/positions/live (API.md §3.1).

Doubles as the response shape and (via OpenAPI → Anthropic tool defs)
the v2 NL-chat tool description for the same query. Field-level
descriptions are kept here so the OpenAPI spec carries them straight
through to the NL-chat tool layer.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from app.models.common import CustomerRegion, Staleness


class Position(BaseModel):
    """A single aircraft's current state on the surface or in the air.

    Sourced from the OpenSky `/states/all` poller (every 30 s) joined
    with `ref.aircraft_registry` for type-code enrichment. `lat`/`lon`
    are WGS84.
    """

    icao24: str = Field(description="Lowercase 24-bit ICAO transponder address (6 hex characters).")
    callsign: str | None = Field(
        description=(
            "Callsign reported by Mode S (e.g. 'UAL1234'). Often null on the "
            "ground or briefly after takeoff."
        )
    )
    lat: float = Field(ge=-90, le=90, description="Latitude in decimal degrees (WGS84).")
    lon: float = Field(ge=-180, le=180, description="Longitude in decimal degrees (WGS84).")
    altitude_ft: int | None = Field(
        description="Barometric altitude above MSL in feet. Null if not transmitted."
    )
    speed_kt: int | None = Field(description="Ground speed in knots.")
    heading_deg: int | None = Field(
        description="True track over ground in degrees from north (0-360)."
    )
    vertical_rate_fpm: int | None = Field(
        description="Climb (+) / descent (-) rate in feet per minute."
    )
    on_ground: bool = Field(description="True when the aircraft reports squat-switch on.")
    customer_region: CustomerRegion = Field(
        description=(
            "Region attribution if the aircraft is inside a customer-defined zone; "
            "null otherwise."
        )
    )
    last_seen_at: datetime = Field(
        description="UTC timestamp of the most recent observation for this aircraft."
    )
    staleness: Staleness = Field(
        description=(
            "Bucket derived from `last_seen_at`: `fresh` <60 s, `stale` <5 min, "
            "`lost` otherwise."
        )
    )


class PositionsLiveResponse(BaseModel):
    """Response shape for GET /v1/positions/live.

    `pipeline_lag_seconds` is the age of the most recent successful
    OpenSky poll — surfaces ingestion delay to the dashboard so stale
    rows can be rendered with the right marker color.
    """

    items: list[Position] = Field(description="Position rows scoped to the caller's region.")
    count: int = Field(description="Length of `items` (mirrors API.md §1.5 list convention).")
    server_time: datetime = Field(description="Server clock at response-build time, in UTC.")
    pipeline_lag_seconds: int = Field(description="Seconds since the last successful OpenSky poll.")
    truncated: bool = Field(
        default=False,
        description=(
            "True when the in-scope live set exceeded the server's safety "
            "ceiling and `items` is the freshest slice rather than the "
            "complete snapshot. False in normal operation; consumers that "
            "need completeness (e.g. the Foundry sync) should alert on True."
        ),
    )
