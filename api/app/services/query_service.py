"""QueryService — the central read-only data abstraction.

Every API.md §3-§5 endpoint goes through QueryService. v2 NL chat will
reuse the same methods as LLM tools — the Pydantic return types double
as Anthropic tool-output schemas.

Scope enforcement happens at the method boundary: a method that detects
an out-of-scope ask raises ScopeViolation (mapped to 403 by the global
handler). The caller can't get rows for resources outside scope.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.exceptions import NotFoundError, ScopeViolation
from app.models.common import (
    FlightCategory,
    Region,
    Scope,
    SlaPeriod,
    Staleness,
    TrailLookback,
    WeatherImpact,
)
from app.models.flights import FlightDetail, TrailPoint, TrailResponse
from app.models.positions import Position, PositionsLiveResponse
from app.models.sites import (
    FlightSummary,
    SiteDetail,
    SiteFlightListResponse,
    SiteListItem,
    SiteListResponse,
    SiteSla,
    SiteWeather,
)
from app.services.lakehouse import LakehouseQuery
from app.services.postgres import PostgresPool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TRAIL_INTERVALS: dict[TrailLookback, str] = {
    "1h": "1 hour",
    "2h": "2 hours",
    "4h": "4 hours",
    "since_takeoff": "6 hours",  # capped per API.md §4.2
}

# `/v1/positions/live` is spec'd (API.md §3.1) as "all currently airborne
# aircraft". app.current_positions retains the last-known row for every
# aircraft ever seen (icao24-keyed upsert, no eviction in the ingestion
# path), so the query MUST bound by recency or it returns long-landed
# traffic — without this filter ~62% of the result is hours-to-days old.
# 15 min keeps fresh (<60s) + stale (<5min) + recently-lost "for context"
# (the Workshop map dims these) while dropping the multi-hour/day backlog.
# The companion `prune_stale_positions` Dagster job bounds the table
# itself; this filter guarantees the contract even between prunes.
LIVE_POSITION_WINDOW = "15 minutes"


def _compute_staleness(last_seen_at: datetime, now: datetime) -> Staleness:
    """Bucket age since last observation into fresh / stale / lost."""
    age_seconds = (now - last_seen_at).total_seconds()
    if age_seconds < 60:
        return "fresh"
    if age_seconds < 300:
        return "stale"
    return "lost"


def _compute_weather_impact(flight_category: FlightCategory | None) -> WeatherImpact:
    """Derive v1 weather_impact from current flight_category."""
    if flight_category == "LIFR":
        return "high"
    if flight_category == "IFR":
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# QueryService
# ---------------------------------------------------------------------------


class QueryService:
    """Read-only data access for the API and (in v2) the NL-chat layer."""

    def __init__(self, postgres: PostgresPool, lakehouse: LakehouseQuery) -> None:
        self._postgres = postgres
        self._lakehouse = lakehouse

    # === Positions ===

    def list_live_positions(
        self,
        scope: Scope,
        bbox: tuple[float, float, float, float] | None = None,
        region: Region | None = None,
    ) -> PositionsLiveResponse:
        """All in-scope live positions. Optional bbox / region filters.

        `region` override is rejected (403) unless scope.region == 'all'.
        Rows with NULL customer_region (un-attributed aircraft) surface to
        every caller — they are public-airspace observations.
        """
        if region and region != scope.region and scope.region != "all":
            raise ScopeViolation(
                f"User scope '{scope.region}' cannot request region '{region}'",
                details={"requested_region": region},
            )

        effective_region: Region = region or scope.region
        where: list[str] = []
        params: dict[str, Any] = {}

        if effective_region != "all":
            where.append("(customer_region = %(region)s OR customer_region IS NULL)")
            params["region"] = effective_region

        if bbox is not None:
            lat_min, lon_min, lat_max, lon_max = bbox
            where.append("lat BETWEEN %(lat_min)s AND %(lat_max)s")
            where.append("lon BETWEEN %(lon_min)s AND %(lon_max)s")
            params.update(
                {
                    "lat_min": lat_min,
                    "lat_max": lat_max,
                    "lon_min": lon_min,
                    "lon_max": lon_max,
                }
            )

        # Always bound to recently-seen aircraft — see LIVE_POSITION_WINDOW.
        # This makes the endpoint match its "currently airborne" contract;
        # the window literal is a code constant, not user input.
        where.append(f"last_seen_at >= NOW() - INTERVAL '{LIVE_POSITION_WINDOW}'")

        where_sql = " AND ".join(where) if where else "TRUE"
        rows = self._postgres.fetchall(
            f"""
            SELECT icao24, callsign, lat, lon, altitude_ft, speed_kt,
                   heading_deg, vertical_rate_fpm, on_ground,
                   customer_region, last_seen_at
            FROM app.current_positions
            WHERE {where_sql}
            ORDER BY last_seen_at DESC
            LIMIT 10000
            """,
            params,
        )

        now = datetime.now(UTC)
        positions = [
            Position(**row, staleness=_compute_staleness(row["last_seen_at"], now)) for row in rows
        ]
        # Pipeline lag = age of newest row. Proxy for "last successful poll".
        pipeline_lag_seconds = int((now - rows[0]["last_seen_at"]).total_seconds()) if rows else 0
        return PositionsLiveResponse(
            items=positions,
            count=len(positions),
            server_time=now,
            pipeline_lag_seconds=pipeline_lag_seconds,
        )

    # === Flights ===

    def get_flight(self, scope: Scope, icao24: str) -> FlightDetail:
        """One flight's current state + registry metadata. 404 if not seen <30 min."""
        icao24_lower = icao24.lower()
        row = self._postgres.fetchone(
            """
            SELECT p.icao24, p.callsign, p.lat, p.lon, p.altitude_ft, p.speed_kt,
                   p.heading_deg, p.vertical_rate_fpm, p.on_ground,
                   p.customer_region, p.last_seen_at,
                   p.origin_icao, p.destination_icao, p.aircraft_type,
                   r.registration, r.operator_icao
            FROM app.current_positions p
            LEFT JOIN ref.aircraft_registry r ON r.icao24 = p.icao24
            WHERE p.icao24 = %(icao24)s
              AND p.last_seen_at >= NOW() - INTERVAL '30 minutes'
            """,
            {"icao24": icao24_lower},
        )
        if not row:
            raise NotFoundError(f"Flight '{icao24}' not seen in the last 30 minutes")

        self._check_flight_scope(scope, row["customer_region"], icao24)

        now = datetime.now(UTC)
        position = Position(
            icao24=row["icao24"],
            callsign=row["callsign"],
            lat=row["lat"],
            lon=row["lon"],
            altitude_ft=row["altitude_ft"],
            speed_kt=row["speed_kt"],
            heading_deg=row["heading_deg"],
            vertical_rate_fpm=row["vertical_rate_fpm"],
            on_ground=row["on_ground"],
            customer_region=row["customer_region"],
            last_seen_at=row["last_seen_at"],
            staleness=_compute_staleness(row["last_seen_at"], now),
        )
        return FlightDetail(
            icao24=row["icao24"],
            callsign=row["callsign"],
            registration=row["registration"],
            aircraft_type=row["aircraft_type"],
            operator_icao=row["operator_icao"],
            origin_icao=row["origin_icao"],
            destination_icao=row["destination_icao"],
            customer_region=row["customer_region"],
            position=position,
            eta_minutes=None,
            status_timeline=[],
            open_case_ids=[],
        )

    def get_flight_trail(self, scope: Scope, icao24: str, lookback: TrailLookback) -> TrailResponse:
        """Historical trail from the Parquet lakehouse, partition-pruned by ts_polled."""
        icao24_lower = icao24.lower()

        # Scope check via current state. If aged out of current_positions,
        # allow the trail (read-only positional data, not identity-bearing).
        pos_row = self._postgres.fetchone(
            "SELECT customer_region FROM app.current_positions WHERE icao24 = %(icao24)s",
            {"icao24": icao24_lower},
        )
        if pos_row:
            self._check_flight_scope(scope, pos_row["customer_region"], icao24)

        interval = TRAIL_INTERVALS[lookback]
        rows = self._lakehouse.query(
            f"""
            SELECT COALESCE(ts_position, ts_polled) AS ts,
                   lat, lon, altitude_ft, speed_kt
            FROM read_parquet($lake_glob, hive_partitioning = true)
            WHERE icao24 = $icao24
              AND ts_polled >= now() - INTERVAL '{interval}'
            ORDER BY ts_polled ASC
            """,
            lake_glob=self._lakehouse.positions_glob,
            icao24=icao24_lower,
        )
        points = [TrailPoint(**row) for row in rows]
        return TrailResponse(
            icao24=icao24_lower,
            points=points,
            lookback=lookback,
            point_count=len(points),
        )

    # === Sites ===

    def list_sites(self, scope: Scope, region: Region | None = None) -> SiteListResponse:
        """List watched airports. `is_in_scope` is informational (soft scope)."""
        if region and region != scope.region and scope.region != "all":
            raise ScopeViolation(
                f"User scope '{scope.region}' cannot request region '{region}'",
                details={"requested_region": region},
            )
        rows = self._postgres.fetchall(
            """
            SELECT icao, iata, name, state, customer_regions
            FROM ref.airports
            WHERE is_watched = TRUE
            ORDER BY icao
            """
        )
        items: list[SiteListItem] = []
        for row in rows:
            site_regions: list[str] = list(row["customer_regions"] or [])
            if region and region not in site_regions:
                continue
            is_in_scope = scope.region == "all" or any(
                scope.includes_region(r)  # type: ignore[arg-type]
                for r in site_regions
                if r in ("west", "east", "all")
            )
            items.append(
                SiteListItem(
                    icao=row["icao"],
                    iata=row["iata"],
                    name=row["name"],
                    state=row["state"] or "",
                    customer_regions=site_regions,
                    is_in_scope=is_in_scope,
                )
            )
        return SiteListResponse(items=items, count=len(items))

    def get_site(self, scope: Scope, icao: str) -> SiteDetail:
        """One site detail including most-recent weather + 60-min flow counts."""
        icao_upper = icao.upper()
        if not scope.includes_site(icao_upper):
            raise ScopeViolation(f"User scope cannot access site '{icao_upper}'")
        row = self._postgres.fetchone(
            """
            SELECT a.icao, a.iata, a.name, a.city, a.state, a.lat, a.lon,
                   a.elevation_ft, a.timezone, a.customer_regions,
                   w.metar_raw, w.flight_category, w.wind_kt, w.visibility_sm,
                   w.ceiling_ft, w.metar_observed_at
            FROM ref.airports a
            LEFT JOIN app.airport_conditions w ON w.site_icao = a.icao
            WHERE a.icao = %(icao)s AND a.is_watched = TRUE
            """,
            {"icao": icao_upper},
        )
        if not row:
            raise NotFoundError(f"Site '{icao_upper}' not found or not watched")

        weather = None
        if row["metar_raw"]:
            weather = SiteWeather(
                metar_raw=row["metar_raw"],
                metar_plain_english=None,
                flight_category=row["flight_category"],
                wind_kt=row["wind_kt"],
                visibility_sm=row["visibility_sm"],
                ceiling_ft=row["ceiling_ft"],
                observed_at=row["metar_observed_at"],
            )

        inbound_cnt = self._count_flights(icao_upper, "destination_icao")
        outbound_cnt = self._count_flights(icao_upper, "origin_icao")
        return SiteDetail(
            icao=row["icao"],
            iata=row["iata"],
            name=row["name"],
            city=row["city"],
            state=row["state"] or "",
            lat=row["lat"],
            lon=row["lon"],
            elevation_ft=row["elevation_ft"],
            timezone=row["timezone"],
            weather=weather,
            inbound_count_60m=inbound_cnt,
            outbound_count_60m=outbound_cnt,
            active_case_count=0,  # Phase 05 populates
            customer_regions=list(row["customer_regions"] or []),
        )

    def get_site_sla(self, scope: Scope, icao: str, period: SlaPeriod) -> SiteSla:
        """SLA scorecard shape. All metric fields are null/0 until Phase 05."""
        icao_upper = icao.upper()
        if not scope.includes_site(icao_upper):
            raise ScopeViolation(f"User scope cannot access site '{icao_upper}'")
        weather = self._postgres.fetchone(
            "SELECT flight_category FROM app.airport_conditions WHERE site_icao = %(icao)s",
            {"icao": icao_upper},
        )
        flight_category: FlightCategory = (
            weather["flight_category"] if weather and weather["flight_category"] else "VFR"
        )
        return SiteSla(
            icao=icao_upper,
            period=period,
            inbound_count=0,
            outbound_count=0,
            on_time_arrival_pct=None,
            on_time_departure_pct=None,
            avg_arrival_delay_min=None,
            avg_departure_delay_min=None,
            weather_impact=_compute_weather_impact(flight_category),
            flight_category=flight_category,
            active_cases=0,
            sparkline_7d=[],
        )

    def list_inbound(self, scope: Scope, icao: str) -> SiteFlightListResponse:
        """Aircraft inbound to a site within the last 60 minutes."""
        return self._list_flow(scope, icao, "destination_icao")

    def list_outbound(self, scope: Scope, icao: str) -> SiteFlightListResponse:
        """Aircraft outbound from a site within the last 60 minutes."""
        return self._list_flow(scope, icao, "origin_icao")

    # === Internal helpers ===

    def _check_flight_scope(self, scope: Scope, customer_region: str | None, icao24: str) -> None:
        """Raise ScopeViolation if customer_region is set and out of scope."""
        if (
            customer_region
            and customer_region in ("west", "east")
            and not scope.includes_region(customer_region)  # type: ignore[arg-type]
        ):
            raise ScopeViolation(
                f"User scope '{scope.region}' cannot access flight "
                f"'{icao24}' in region '{customer_region}'",
                details={"flight_region": customer_region},
            )

    def _count_flights(self, icao_upper: str, direction_column: str) -> int:
        """Count flights with origin/destination = icao_upper, seen <60 min."""
        row = self._postgres.fetchone(
            f"""
            SELECT COUNT(*) AS cnt FROM app.current_positions
            WHERE {direction_column} = %(icao)s
              AND last_seen_at >= NOW() - INTERVAL '60 minutes'
            """,
            {"icao": icao_upper},
        )
        return int(row["cnt"]) if row else 0

    def _list_flow(self, scope: Scope, icao: str, direction_column: str) -> SiteFlightListResponse:
        """Shared body for list_inbound / list_outbound."""
        icao_upper = icao.upper()
        if not scope.includes_site(icao_upper):
            raise ScopeViolation(f"User scope cannot access site '{icao_upper}'")
        rows = self._postgres.fetchall(
            f"""
            SELECT p.icao24, p.callsign, p.origin_icao, p.destination_icao,
                   p.aircraft_type
            FROM app.current_positions p
            WHERE p.{direction_column} = %(icao)s
              AND p.last_seen_at >= NOW() - INTERVAL '60 minutes'
            ORDER BY p.last_seen_at DESC
            LIMIT 500
            """,
            {"icao": icao_upper},
        )
        items = [
            FlightSummary(
                icao24=row["icao24"],
                callsign=row["callsign"],
                origin_icao=row["origin_icao"],
                destination_icao=row["destination_icao"],
                eta_minutes=None,
                status="unknown",
                aircraft_type=row["aircraft_type"],
            )
            for row in rows
        ]
        return SiteFlightListResponse(items=items, count=len(items))
