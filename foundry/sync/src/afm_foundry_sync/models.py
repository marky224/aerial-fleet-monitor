"""Local response models mirroring the AFM /v1 API shapes.

These are deliberately a copy, not an import, of ``api/app/models/*``. The
sync venv is intentionally separated from api/'s dep tree (see
``foundry/sync/pyproject.toml``); cross-importing would defeat that. The
shapes are locked by ``docs/API.md`` so drift risk is bounded.

Only fields the sync consumes are mirrored — Scope, ErrorResponse, and
other auth-side types from ``app.models.common`` are out of scope here.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Shared Literals (mirror of app.models.common)
# ---------------------------------------------------------------------------

CustomerRegion = Literal["west", "east", "all", None]
FlightCategory = Literal["VFR", "MVFR", "IFR", "LIFR"]
Staleness = Literal["fresh", "stale", "lost"]
FlightStage = Literal["departed", "climb", "cruise", "descent", "approach", "landed"]
FlightStatus = Literal["scheduled", "departed", "enroute", "approaching", "landed", "unknown"]
TrailLookback = Literal["1h", "2h", "4h", "since_takeoff"]
SlaPeriod = Literal["last_24h", "last_7d"]
WeatherImpact = Literal["low", "medium", "high"]
SiteFlightDirection = Literal["inbound", "outbound"]


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------


class Position(BaseModel):
    icao24: str
    callsign: str | None
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    altitude_ft: int | None
    speed_kt: int | None
    heading_deg: int | None
    vertical_rate_fpm: int | None
    on_ground: bool
    customer_region: CustomerRegion
    last_seen_at: datetime
    staleness: Staleness


class PositionsLiveResponse(BaseModel):
    items: list[Position]
    count: int
    server_time: datetime
    pipeline_lag_seconds: int


# ---------------------------------------------------------------------------
# Flights
# ---------------------------------------------------------------------------


class FlightStatusEvent(BaseModel):
    stage: FlightStage
    occurred_at: datetime


class FlightDetail(BaseModel):
    icao24: str
    callsign: str | None
    registration: str | None
    aircraft_type: str | None
    operator_icao: str | None
    origin_icao: str | None
    destination_icao: str | None
    customer_region: CustomerRegion
    position: Position
    eta_minutes: int | None
    status_timeline: list[FlightStatusEvent]
    open_case_ids: list[str]


class TrailPoint(BaseModel):
    ts: datetime
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    altitude_ft: int | None
    speed_kt: int | None


class TrailResponse(BaseModel):
    icao24: str
    points: list[TrailPoint]
    lookback: TrailLookback
    point_count: int


# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------


class SiteListItem(BaseModel):
    icao: str
    iata: str | None
    name: str
    state: str
    customer_regions: list[str]
    is_in_scope: bool


class SiteListResponse(BaseModel):
    items: list[SiteListItem]
    count: int


class SiteWeather(BaseModel):
    metar_raw: str
    metar_plain_english: str | None
    flight_category: FlightCategory
    wind_kt: int | None
    visibility_sm: float | None
    ceiling_ft: int | None
    observed_at: datetime


class SiteDetail(BaseModel):
    icao: str
    iata: str | None
    name: str
    city: str | None
    state: str
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    elevation_ft: int | None
    timezone: str | None
    weather: SiteWeather | None
    inbound_count_60m: int
    outbound_count_60m: int
    active_case_count: int
    customer_regions: list[str]


class SparklinePoint(BaseModel):
    day: date
    on_time_pct: float | None
    avg_delay_min: float | None


class SiteSla(BaseModel):
    icao: str
    period: SlaPeriod
    inbound_count: int
    outbound_count: int
    on_time_arrival_pct: float | None
    on_time_departure_pct: float | None
    avg_arrival_delay_min: float | None
    avg_departure_delay_min: float | None
    weather_impact: WeatherImpact
    flight_category: FlightCategory
    active_cases: int
    sparkline_7d: list[SparklinePoint]


class FlightSummary(BaseModel):
    icao24: str
    callsign: str | None
    origin_icao: str | None
    destination_icao: str | None
    eta_minutes: int | None
    status: FlightStatus
    aircraft_type: str | None


class SiteFlightListResponse(BaseModel):
    items: list[FlightSummary]
    count: int
