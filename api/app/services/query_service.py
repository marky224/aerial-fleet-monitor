"""QueryService — the central read-only data abstraction.

Every API.md §3-§5 endpoint goes through QueryService. v2 NL chat will
reuse the same methods as LLM tools — the Pydantic return types double
as Anthropic tool-output schemas.

Scope enforcement happens at the method boundary: a method that detects
an out-of-scope ask raises ScopeViolation (mapped to 403 by the global
handler). The caller can't get rows for resources outside scope.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

import structlog

from app.exceptions import NotFoundError, ScopeViolation
from app.models.cases import (
    CaseDetail,
    CaseListItem,
    CaseListResponse,
    CaseTimelineEvent,
)
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
from app.services._lightning import case_lightning_url
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

# Hard safety ceiling on a single /v1/positions/live snapshot. This is an
# implementation guardrail (bounds memory + response size), NOT a spec'd
# page size — API.md §3.1 contracts this endpoint to return *all* in-scope
# live aircraft. Real US airborne traffic is single-digit thousands, so
# 50k is generous headroom; if it is ever exceeded the response carries
# `truncated=True` and a WARNING is logged, so the clip is observable
# instead of silent (the prior behaviour: a bare `LIMIT 10000`).
LIVE_POSITION_CEILING = 50_000

# Same shape + rationale as LIVE_POSITION_CEILING, applied to `GET /v1/cases`.
# Tenant case volume is ~1k-2k at Phase-05 close, so 50k is ~30-50x headroom.
# Cases also has its own pagination story (see API.md §6.1 / build-05 Decisions
# log entry "Pagination = 50k ceiling, not cursor"): every page is bounded by
# the ceiling and the response carries `truncated` so a clip is observable.
CASE_LIST_CEILING = 50_000

# Default statuses for `GET /v1/cases` when no `status` filter is given —
# matches the Workshop App-1 panel filter (WORKSHOP_APPS.md §1.4), which
# hides resolved cases from the operator view. A caller can opt into
# resolved (or any subset) by passing `status` explicitly.
DEFAULT_CASE_STATUSES: tuple[str, ...] = ("open", "acknowledged", "in_progress")

logger = structlog.get_logger(__name__)


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

    def __init__(
        self,
        postgres: PostgresPool,
        lakehouse: LakehouseQuery,
        salesforce_instance_url: str | None = None,
    ) -> None:
        self._postgres = postgres
        self._lakehouse = lakehouse
        self._salesforce_instance_url = salesforce_instance_url

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
            LIMIT %(ceiling_probe)s
            """,
            {**params, "ceiling_probe": LIVE_POSITION_CEILING + 1},
        )

        # Fetch one row past the ceiling so a full result is distinguishable
        # from a clipped one. ORDER BY last_seen_at DESC means the dropped
        # tail is the *oldest* rows — we keep the freshest CEILING.
        truncated = len(rows) > LIVE_POSITION_CEILING
        if truncated:
            rows = rows[:LIVE_POSITION_CEILING]
            logger.warning(
                "positions_live_truncated",
                ceiling=LIVE_POSITION_CEILING,
                region=effective_region,
                has_bbox=bbox is not None,
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
            truncated=truncated,
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

    def get_flight_trails_batch(
        self, scope: Scope, icao24s: list[str], lookback: TrailLookback
    ) -> Iterator[TrailResponse]:
        """Stream one TrailResponse per in-scope icao24 from a SINGLE lake scan.

        :meth:`get_flight_trail` runs one DuckDB scan of the lookback window
        *per aircraft*, and the ``WHERE icao24`` predicate prunes nothing
        (positions are written time-ordered, so icao24 is random within row
        groups) — N per-flight calls re-read the same ~1M-row window N times.
        This runs ONE scan filtered to the requested set, ordered by icao24,
        and yields each aircraft's TrailResponse as its run completes, so
        neither the api (streamed ``fetchmany``) nor the caller (streamed
        NDJSON) holds the whole result.

        icao24s with no rows in the window are simply not yielded (absent ==
        empty trail). Out-of-scope icao24s are filtered, not raised (see
        ``TrailBatchRequest``); aged out of ``current_positions`` == allowed,
        matching the single endpoint. One bulk scope lookup replaces N.
        """
        requested = list(dict.fromkeys(i.lower() for i in icao24s))
        if not requested:
            return

        scope_rows = self._postgres.fetchall(
            """
            SELECT icao24, customer_region
            FROM app.current_positions
            WHERE icao24 = ANY(%(icao24s)s)
            """,
            {"icao24s": requested},
        )
        region_by_icao = {r["icao24"]: r["customer_region"] for r in scope_rows}
        allowed = [i for i in requested if self._flight_in_scope(scope, region_by_icao.get(i))]
        if not allowed:
            return

        interval = TRAIL_INTERVALS[lookback]
        rows = self._lakehouse.query_stream(
            f"""
            SELECT icao24,
                   COALESCE(ts_position, ts_polled) AS ts,
                   lat, lon, altitude_ft, speed_kt
            FROM read_parquet($lake_glob, hive_partitioning = true)
            WHERE list_contains($icao24s, icao24)
              AND ts_polled >= now() - INTERVAL '{interval}'
            ORDER BY icao24 ASC, ts_polled ASC
            """,
            lake_glob=self._lakehouse.positions_glob,
            icao24s=allowed,
        )

        current: str | None = None
        points: list[TrailPoint] = []
        for row in rows:
            ic = row["icao24"]
            if ic != current:
                if current is not None:
                    yield TrailResponse(
                        icao24=current,
                        points=points,
                        lookback=lookback,
                        point_count=len(points),
                    )
                current = ic
                points = []
            points.append(
                TrailPoint(
                    ts=row["ts"],
                    lat=row["lat"],
                    lon=row["lon"],
                    altitude_ft=row["altitude_ft"],
                    speed_kt=row["speed_kt"],
                )
            )
        if current is not None:
            yield TrailResponse(
                icao24=current,
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

    def _flight_in_scope(self, scope: Scope, customer_region: str | None) -> bool:
        """Non-raising sibling of :meth:`_check_flight_scope` for bulk reads.

        Same rule (only ``west``/``east`` regions gate; null/``all`` is
        always visible) but returns a bool so the batch trail endpoint can
        filter out-of-scope icao24s instead of 403-ing the whole request.
        """
        if customer_region and customer_region in ("west", "east"):
            return scope.includes_region(customer_region)  # type: ignore[arg-type]
        return True

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

    # === Cases (customer-facing scope-gated reads, API.md §6.1/§6.2) ===

    def list_cases(
        self,
        scope: Scope,
        status: Sequence[str] | None = None,
        severity: str | None = None,
        site: str | None = None,
        region: Region | None = None,
    ) -> CaseListResponse:
        """Scope-gated `GET /v1/cases`.

        Region override is rejected (403) unless scope.region == 'all',
        matching `list_live_positions`. The scope filter is
        `customer_region IN (effective_region, 'all')` so a cross-region
        ('all'-tagged) case surfaces to both east and west callers — same
        rule the Workshop App-1 panel applies. `status` defaults to
        `DEFAULT_CASE_STATUSES` (no resolved); pass an explicit list to
        include resolved. `severity` / `site` are unvalidated pass-through
        filters; an unknown value just returns zero rows.
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
            where.append("customer_region = ANY(%(regions)s)")
            params["regions"] = [effective_region, "all"]

        statuses: tuple[str, ...] | Sequence[str] = (
            tuple(status) if status is not None else DEFAULT_CASE_STATUSES
        )
        where.append("status = ANY(%(statuses)s)")
        params["statuses"] = list(statuses)

        if severity is not None:
            where.append("severity = %(severity)s")
            params["severity"] = severity

        if site is not None:
            where.append("site_icao = %(site)s")
            params["site"] = site.upper()

        where_sql = " AND ".join(where) if where else "TRUE"
        rows = self._postgres.fetchall(
            f"""
            SELECT case_id, salesforce_id, case_type, status, severity,
                   customer_region, site_icao, flight_id, summary,
                   created_at, updated_at
            FROM app.cases
            WHERE {where_sql}
            ORDER BY created_at DESC, case_id DESC
            LIMIT %(ceiling_probe)s
            """,
            {**params, "ceiling_probe": CASE_LIST_CEILING + 1},
        )

        truncated = len(rows) > CASE_LIST_CEILING
        if truncated:
            rows = rows[:CASE_LIST_CEILING]
            logger.warning(
                "cases_list_truncated",
                ceiling=CASE_LIST_CEILING,
                region=effective_region,
                has_severity=severity is not None,
                has_site=site is not None,
            )

        items = [
            CaseListItem(
                **row,
                salesforce_url=case_lightning_url(
                    self._salesforce_instance_url, row["salesforce_id"]
                ),
            )
            for row in rows
        ]
        return CaseListResponse(items=items, count=len(items), truncated=truncated)

    def get_case(self, scope: Scope, case_id: str) -> CaseDetail:
        """One case + ordered timeline. 404 if absent; 403 if out of scope.

        Scope check mirrors the list endpoint: a case whose
        `customer_region` is the caller's region OR `'all'` is visible.
        `region='all'` scope is a wildcard.
        """
        case_row = self._postgres.fetchone(
            """
            SELECT case_id, salesforce_id, case_type, status, severity,
                   severity_justification, customer_region, site_icao,
                   flight_id, summary, detection_facts, runbook_refs,
                   created_at, updated_at, resolved_at
            FROM app.cases
            WHERE case_id = %(case_id)s
            """,
            {"case_id": case_id},
        )
        if not case_row:
            raise NotFoundError(f"Case '{case_id}' not found")

        case_region = case_row["customer_region"]
        if scope.region != "all" and case_region != "all" and case_region != scope.region:
            raise ScopeViolation(
                f"User scope '{scope.region}' cannot access case '{case_id}' "
                f"in region '{case_region}'",
                details={"case_region": case_region},
            )

        timeline_rows = self._postgres.fetchall(
            """
            SELECT event_type, detail, source, actor, occurred_at
            FROM app.case_timeline
            WHERE case_id = %(case_id)s
            ORDER BY occurred_at ASC, event_id ASC
            """,
            {"case_id": case_id},
        )
        timeline = [
            CaseTimelineEvent(
                event_type=row["event_type"],
                detail=row["detail"] or {},
                source=row["source"],
                actor=row["actor"],
                occurred_at=row["occurred_at"],
            )
            for row in timeline_rows
        ]
        return CaseDetail(
            case_id=case_row["case_id"],
            salesforce_id=case_row["salesforce_id"],
            salesforce_url=case_lightning_url(
                self._salesforce_instance_url, case_row["salesforce_id"]
            ),
            case_type=case_row["case_type"],
            status=case_row["status"],
            severity=case_row["severity"],
            severity_justification=case_row["severity_justification"],
            customer_region=case_row["customer_region"],
            site_icao=case_row["site_icao"],
            flight_id=case_row["flight_id"],
            summary=case_row["summary"],
            detection_facts=case_row["detection_facts"] or {},
            runbook_refs=list(case_row["runbook_refs"] or []),
            timeline=timeline,
            created_at=case_row["created_at"],
            updated_at=case_row["updated_at"],
            resolved_at=case_row["resolved_at"],
        )
