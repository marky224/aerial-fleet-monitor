"""Shared models, error envelope, the internal Scope, and every cross-cutting Literal.

This module is the one-stop-shop for types that span multiple model files
or query params. Per-router models (Position, FlightDetail, SiteDetail,
...) live next to their router in `positions.py` / `flights.py` /
`sites.py` — they import their Literals from here.

Pagination helpers are deliberately absent: no Phase 02 endpoint uses
cursor pagination (every list response is bounded — `count` + `items`
only, no `next_cursor`). Cursor pagination lands when `/v1/cases` does
(Phase 04+).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Shared Literal aliases (every Phase 02 endpoint draws from here)
# ---------------------------------------------------------------------------

Region = Literal["west", "east", "all"]
"""A user's region scope. Always one of west/east/all (never None)."""

CustomerRegion = Literal["west", "east", "all", None]
"""Row-level region attribution. `None` for aircraft not yet region-tagged."""

FlightCategory = Literal["VFR", "MVFR", "IFR", "LIFR"]
"""METAR-derived flight category per FAA AIM §7-1-9. Site weather + SLA."""

Staleness = Literal["fresh", "stale", "lost"]
"""Position freshness bucket: `fresh` <60s, `stale` <5min, `lost` otherwise."""

FlightStage = Literal["departed", "climb", "cruise", "descent", "approach", "landed"]
"""A discrete event in a flight's timeline (FlightStatusEvent.stage)."""

FlightStatus = Literal["scheduled", "departed", "enroute", "approaching", "landed", "unknown"]
"""Current point-in-time status for a flight summary at a site."""

TrailLookback = Literal["1h", "2h", "4h", "since_takeoff"]
"""Trail-query lookback window per API.md §4.2. `since_takeoff` is capped
at 6 hours of history server-side to bound query cost."""

SlaPeriod = Literal["last_24h", "last_7d"]
"""SLA scorecard aggregation window (API.md §5.3)."""

WeatherImpact = Literal["low", "medium", "high"]
"""Qualitative weather-impact bucket on the SLA scorecard."""


# ---------------------------------------------------------------------------
# Scope — the internal auth-decision object
# ---------------------------------------------------------------------------


class Scope(BaseModel):
    """Caller's authorization context.

    Built by the auth layer (real in Phase 04, stubbed in Phase 02). Every
    `QueryService` method takes a `Scope` and refuses to return rows outside
    it (raising `ScopeViolation` → 403).

    `Scope` is the internal decision object — the wire shape returned by
    `GET /v1/auth/me` is `MeResponse` in `routers/auth_stub.py`, which adds
    presentation fields (`read_only`, `expires_at`, `salesforce_user_id`).
    """

    user_handle: str
    region: Region
    custom_perms: list[str]
    sites_in_scope: list[str]  # ICAO codes the user can read

    def includes_site(self, icao: str) -> bool:
        """True if `icao` is in scope. `region='all'` is a wildcard."""
        return self.region == "all" or icao in self.sites_in_scope

    def includes_region(self, region: Region) -> bool:
        """True if `region` is in scope. `self.region='all'` matches any region."""
        return self.region == "all" or region == self.region


# ---------------------------------------------------------------------------
# Error envelope (API.md §1.3)
# ---------------------------------------------------------------------------


class ErrorDetail(BaseModel):
    """Inner object of the standard error envelope."""

    code: str
    message: str
    request_id: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """Standard error envelope per API.md §1.3.

    Constructed by the global exception handler in `app.main` from an
    `AFMException` subclass + the request-scoped `request_id`.
    """

    error: ErrorDetail
