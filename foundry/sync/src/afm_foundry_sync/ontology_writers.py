"""Async writer for AFM Ontology objects via the Foundry Action API.

Writes go through modify-or-create ("upsert") Action types, not direct
object mutation, so create and update are one idempotent call keyed on the
primary key. Endpoint (verified against the tenant 2026-05-15)::

    POST {tenant}/api/v2/ontologies/{api_name}/actions/{action}/applyBatch
    {"requests": [{"parameters": {...}}, ...]}

Contract specifics established by dry-run probing the live Action:

  - **Parameter API names are camelCase.** Foundry's declarative
    object-edit actions auto-derive a parameter per object property, named
    after the (camelCase) property. Manual snake_case renames get undone
    whenever the action's rules are touched, so the durable contract is
    camelCase: ``_camel`` transforms each field name at serialization time
    and the result matches the property API names verbatim. (Reverses the
    earlier snake_case decision — see _private/docs/foundry/ONTOLOGY.md.)
  - The PK value is sent **twice**: once as the PK param (``icao24`` /
    ``icao`` / ``flightId``) and once as the object-locator param
    (``aircraft`` / ``site`` / ``flight``), which is ``required`` with an
    ``objectQueryResult`` constraint and takes the bare PK string. The
    locator names are single-token, so ``_camel`` leaves them unchanged.
  - ``position`` / ``location`` geopoints are GeoJSON Points with
    ``coordinates: [lon, lat]`` (GeoJSON axis order — locked decision).
    They are NOT on the upstream payloads; constructed here from lat/lon.
    Flight's lat/lon are nullable (null until enrichment), so its
    ``position`` is sent only when both are present.
  - JSON-encoded-string params (the tenant models them as ``string``):
    Site ``customerRegions`` / ``slaSparkline7d``; Flight ``openCaseIds``
    / ``statusTimeline`` / ``trail2h``. Empty collections serialize to
    ``"[]"`` (the v1 state for all of Flight's array fields).
  - Optional params are omitted when None (absent optionals validate clean).

Retry/error model is shared with ``api_readers`` via ``retry.transient_retry``.
A 4xx (e.g. a 400 from a malformed batch) raises immediately and is not
retried. ``sync_jobs.py`` translates Foundry-side failures into
``FoundrySyncSkipped`` so the local stack stays standalone.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Self

import httpx
import structlog

from afm_foundry_sync.models import Aircraft, Case, Flight, Site, TrailPoint
from afm_foundry_sync.retry import transient_retry
from afm_foundry_sync.settings import FoundrySettings

logger = structlog.get_logger(__name__)

_REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

# applyBatch's maximum request count is an open question (ONTOLOGY.md). 100 is
# a conservative default; tune once the live limit is discovered.
_MAX_BATCH = 100

# Page size for the objects-list GET used by the tenant reconcile (Fix C).
# 1000 is the value proven against the live tenant by the interim purge tool.
_OBJECTS_PAGE_SIZE = 1000


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Outcome of an upsert batch. ``attempted == succeeded`` on full success.

    Live round-trip (2026-05-15) confirmed the success envelope is HTTP 200
    with an empty body ``{}`` — there is no per-item result to parse, so
    ``_post_chunk``'s ``raise_for_status()`` + counting the chunk is exact.
    applyBatch is all-or-nothing per chunk (any invalid request 400s the whole
    chunk), so a partial chunk failure surfaces as an ``httpx.HTTPStatusError``
    raised out of the writer, not as a non-zero ``failed`` here. ``failed`` is
    reserved for any per-item error structure a future tenant config returns.
    """

    attempted: int
    succeeded: int
    failed: int = 0


def _camel(name: str) -> str:
    """snake_case → camelCase. Identity for single-token names (icao24, position).

    Verified to reproduce every recon'd Aircraft/Site property API name
    exactly (e.g. sla_sparkline_7d → slaSparkline7d, inbound_count_60m →
    inboundCount60m), so it is a safe mechanical transform, not a guess.
    """
    head, *rest = name.split("_")
    return head + "".join(p[:1].upper() + p[1:] for p in rest)


def _iso_utc(dt: datetime) -> str:
    """ISO-8601 with a trailing ``Z`` (the form the live Action validated against)."""
    return dt.isoformat().replace("+00:00", "Z")


def _geopoint(lat: float, lon: float) -> dict[str, Any]:
    """GeoJSON Point. Coordinates are [lon, lat] per the GeoJSON axis order."""
    return {"type": "Point", "coordinates": [lon, lat]}


def _linestring(points: list[TrailPoint]) -> dict[str, Any] | None:
    """GeoJSON LineString from a trail, or None if it can't form a line.

    Mirrors :func:`_geopoint` (coordinates are [lon, lat] per the GeoJSON
    axis order). A LineString needs >= 2 *distinct* positions. We first
    drop consecutive-identical coordinates: a stationary or parked
    aircraft whose 2 h trail collapses to one repeated point would
    otherwise emit a zero-length LineString, which Foundry's geoshape
    validator rejects with 400 INVALID_ARGUMENT — and since applyBatch is
    all-or-nothing per chunk, that one Flight 400s the whole chunk and the
    run skip-fails with enriched=0. After the dedup, fewer than 2 points
    (a 0/1-point or all-coincident trail) yields None and the param is
    omitted — Workshop renders nothing rather than an invalid shape. The
    full ordered point list still ships as the JSON-string ``trail_2h``
    (no data loss); ``trail_path`` is the geo-native projection the App 3
    polyline binds to (a JSON string can't drive a Vortex map layer).
    """
    coords: list[list[float]] = []
    for p in points:
        c = [p.lon, p.lat]
        if not coords or coords[-1] != c:
            coords.append(c)
    if len(coords) < 2:
        return None
    return {"type": "LineString", "coordinates": coords}


def _put_optional(params: dict[str, Any], key: str, value: Any) -> None:
    """Add ``key`` only when ``value`` is not None — absent optionals validate clean."""
    if value is not None:
        params[key] = value


def aircraft_params(a: Aircraft) -> dict[str, Any]:
    """Serialize an Aircraft to the upsert-aircraft parameter map."""
    params: dict[str, Any] = {
        "icao24": a.icao24,
        # Object-locator param: the bare PK string (verified contract).
        "aircraft": a.icao24,
        "lat": a.lat,
        "lon": a.lon,
        "position": _geopoint(a.lat, a.lon),
        "on_ground": a.on_ground,
        "last_seen_at": _iso_utc(a.last_seen_at),
        "staleness": a.staleness,
    }
    _put_optional(params, "callsign", a.callsign)
    _put_optional(params, "altitude_ft", a.altitude_ft)
    _put_optional(params, "speed_kt", a.speed_kt)
    _put_optional(params, "heading_deg", a.heading_deg)
    _put_optional(params, "vertical_rate_fpm", a.vertical_rate_fpm)
    _put_optional(params, "customer_region", a.customer_region)
    return {_camel(k): v for k, v in params.items()}


def site_params(s: Site) -> dict[str, Any]:
    """Serialize a Site to the upsert-site parameter map."""
    params: dict[str, Any] = {
        "icao": s.icao,
        # Object-locator param: the bare PK string (verified contract).
        "site": s.icao,
        "name": s.name,
        "state": s.state,
        "lat": s.lat,
        "lon": s.lon,
        "location": _geopoint(s.lat, s.lon),
        # array<string> / array<struct> are `string` in-tenant — JSON-encode.
        "customer_regions": json.dumps(s.customer_regions),
        "sla_sparkline_7d": json.dumps([p.model_dump(mode="json") for p in s.sla_sparkline_7d]),
        "inbound_count_60m": s.inbound_count_60m,
        "outbound_count_60m": s.outbound_count_60m,
        "active_case_count": s.active_case_count,
    }
    _put_optional(params, "iata", s.iata)
    _put_optional(params, "city", s.city)
    _put_optional(params, "elevation_ft", s.elevation_ft)
    _put_optional(params, "timezone", s.timezone)
    _put_optional(params, "metar_raw", s.metar_raw)
    _put_optional(params, "metar_plain_english", s.metar_plain_english)
    _put_optional(params, "wind_kt", s.wind_kt)
    _put_optional(params, "visibility_sm", s.visibility_sm)
    _put_optional(params, "ceiling_ft", s.ceiling_ft)
    _put_optional(
        params,
        "weather_observed_at",
        _iso_utc(s.weather_observed_at) if s.weather_observed_at is not None else None,
    )
    _put_optional(params, "flight_category", s.flight_category)
    _put_optional(params, "sla_period", s.sla_period)
    _put_optional(params, "sla_inbound_count", s.sla_inbound_count)
    _put_optional(params, "sla_outbound_count", s.sla_outbound_count)
    _put_optional(params, "on_time_arrival_pct", s.on_time_arrival_pct)
    _put_optional(params, "on_time_departure_pct", s.on_time_departure_pct)
    _put_optional(params, "avg_arrival_delay_min", s.avg_arrival_delay_min)
    _put_optional(params, "avg_departure_delay_min", s.avg_departure_delay_min)
    _put_optional(params, "weather_impact", s.weather_impact)
    return {_camel(k): v for k, v in params.items()}


def flight_params(f: Flight) -> dict[str, Any]:
    """Serialize a Flight to the upsert-flight parameter map.

    Same contract as Aircraft/Site: PK written twice (``flight_id`` param +
    ``flight`` locator), camelCase keys via ``_camel``. Unlike Aircraft,
    Flight's lat/lon are nullable, so ``position`` is emitted only when
    both are present (the takeoff-create payload has neither). The three
    list fields are JSON-encoded strings, exactly like Site's array fields.
    """
    params: dict[str, Any] = {
        "flight_id": f.flight_id,
        # Object-locator param: the bare PK string (verified contract).
        "flight": f.flight_id,
        "icao24": f.icao24,
        "takeoff_ts": _iso_utc(f.takeoff_ts),
        "open_case_count": f.open_case_count,
        # array<string>/array<struct> are `string` in-tenant — JSON-encode.
        "open_case_ids": json.dumps(f.open_case_ids),
        "status_timeline": json.dumps([e.model_dump(mode="json") for e in f.status_timeline]),
        "trail_2h": json.dumps([t.model_dump(mode="json") for t in f.trail_2h]),
    }
    _put_optional(
        params,
        "landed_at",
        _iso_utc(f.landed_at) if f.landed_at is not None else None,
    )
    _put_optional(params, "callsign", f.callsign)
    _put_optional(params, "registration", f.registration)
    _put_optional(params, "aircraft_type", f.aircraft_type)
    _put_optional(params, "operator_icao", f.operator_icao)
    _put_optional(params, "customer_region", f.customer_region)
    _put_optional(params, "origin_icao", f.origin_icao)
    _put_optional(params, "destination_icao", f.destination_icao)
    _put_optional(params, "eta_minutes", f.eta_minutes)
    _put_optional(params, "status", f.status)
    _put_optional(params, "current_stage", f.current_stage)
    # Geopoint only when both coordinates are known (null until enrichment).
    if f.lat is not None and f.lon is not None:
        params["lat"] = f.lat
        params["lon"] = f.lon
        params["position"] = _geopoint(f.lat, f.lon)
    # Geo-native trail projection for the App 3 polyline (omitted until the
    # trail has >= 2 points — see _linestring; the raw points still ship as
    # the JSON-string trail_2h above).
    _put_optional(params, "trail_path", _linestring(f.trail_2h))
    return {_camel(k): v for k, v in params.items()}


def case_params(c: Case) -> dict[str, Any]:
    """Serialize a Case to the upsert-case parameter map.

    Phase 05 task #5. **PK divergence from Aircraft/Site/Flight:** the
    upsert-case action has NO separate PK string parameter (``caseId``);
    the ``case`` object-locator alone handles both create (becomes the
    new Case's PK) and modify (looks up by PK). Confirmed against the
    live tenant 2026-05-24 when provisioning the Case action — the
    auto-derived param set omits the PK param because the locator's
    ``displayName=caseId`` already covers it.

    Same JSON-encoded-string contract as Site / Flight for the dict + list
    fields (``detection_facts`` / ``runbook_refs``): Foundry Action
    parameters don't support struct/list natively, so empty collections
    serialize to ``"{}"`` / ``"[]"`` (never None — the field is non-null
    on the tenant side).
    """
    params: dict[str, Any] = {
        # Object-locator: bare PK string. Foundry uses this for both
        # create (sets the new Case's PK) and modify (looks up by PK).
        # NO separate `caseId` param — different from upsert-aircraft.
        "case": c.case_id,
        "case_type": c.case_type,
        "status": c.status,
        "severity": c.severity,
        "customer_region": c.customer_region,
        "site_icao": c.site_icao,
        "flight_id": c.flight_id,
        # array<dict>/array<string> → JSON-encoded `string` (tenant model).
        "detection_facts": json.dumps(c.detection_facts),
        "runbook_refs": json.dumps(c.runbook_refs),
        "created_at": _iso_utc(c.created_at),
        "updated_at": _iso_utc(c.updated_at),
    }
    _put_optional(params, "salesforce_id", c.salesforce_id)
    _put_optional(params, "salesforce_url", c.salesforce_url)
    _put_optional(params, "subject", c.subject)
    _put_optional(params, "summary", c.summary)
    _put_optional(params, "severity_justification", c.severity_justification)
    _put_optional(
        params,
        "resolved_at",
        _iso_utc(c.resolved_at) if c.resolved_at is not None else None,
    )
    return {_camel(k): v for k, v in params.items()}


def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


class FoundryWriter:
    """Async client that upserts Ontology objects via Action applyBatch.

    Connection-pooled via one ``httpx.AsyncClient`` for the context-manager
    lifetime, re-entered each Dagster asset tick.
    """

    def __init__(self, settings: FoundrySettings) -> None:
        self._ontology = settings.FOUNDRY_ONTOLOGY_API_NAME
        self._action_aircraft = settings.FOUNDRY_ACTION_UPSERT_AIRCRAFT
        self._action_site = settings.FOUNDRY_ACTION_UPSERT_SITE
        self._action_flight = settings.FOUNDRY_ACTION_UPSERT_FLIGHT
        self._action_delete_aircraft = settings.FOUNDRY_ACTION_DELETE_AIRCRAFT
        self._action_delete_flight = settings.FOUNDRY_ACTION_DELETE_FLIGHT
        self._action_case = settings.FOUNDRY_ACTION_UPSERT_CASE
        self._action_delete_case = settings.FOUNDRY_ACTION_DELETE_CASE
        self._client = httpx.AsyncClient(
            base_url=str(settings.FOUNDRY_TENANT_URL).rstrip("/"),
            timeout=_REQUEST_TIMEOUT,
            headers={"Authorization": f"Bearer {settings.FOUNDRY_TOKEN}"},
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._client.aclose()

    @transient_retry
    async def _post_chunk(self, action: str, chunk: list[dict[str, Any]]) -> None:
        path = f"/api/v2/ontologies/{self._ontology}/actions/{action}/applyBatch"
        body = {"requests": [{"parameters": p} for p in chunk]}
        response = await self._client.post(path, json=body)
        response.raise_for_status()

    async def _apply_batch(self, action: str, param_dicts: list[dict[str, Any]]) -> BatchResult:
        succeeded = 0
        for index, chunk in enumerate(_chunks(param_dicts, _MAX_BATCH)):
            logger.info(
                "foundry_apply_batch",
                action=action,
                chunk=index,
                size=len(chunk),
            )
            await self._post_chunk(action, chunk)
            succeeded += len(chunk)
        return BatchResult(attempted=len(param_dicts), succeeded=succeeded)

    async def upsert_aircraft_batch(self, aircraft: list[Aircraft]) -> BatchResult:
        """Upsert a batch of Aircraft. No-op (no HTTP call) on an empty list."""
        if not aircraft:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(
            self._action_aircraft, [aircraft_params(a) for a in aircraft]
        )

    async def upsert_site_batch(self, sites: list[Site]) -> BatchResult:
        """Upsert a batch of Sites. No-op (no HTTP call) on an empty list."""
        if not sites:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(self._action_site, [site_params(s) for s in sites])

    async def upsert_flight_batch(self, flights: list[Flight]) -> BatchResult:
        """Upsert a batch of Flights. No-op (no HTTP call) on an empty list."""
        if not flights:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(self._action_flight, [flight_params(f) for f in flights])

    @transient_retry
    async def _get_objects_page(self, object_type: str, page_token: str | None) -> dict[str, Any]:
        path = f"/api/v2/ontologies/{self._ontology}/objects/{object_type}"
        params: dict[str, str] = {"pageSize": str(_OBJECTS_PAGE_SIZE)}
        if page_token:
            params["pageToken"] = page_token
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        result: dict[str, Any] = response.json()
        return result

    async def list_aircraft_pks(self) -> set[str]:
        """Return the icao24 primary key of every Aircraft object in the tenant.

        Paginates ``GET .../objects/Aircraft`` (pageSize ``_OBJECTS_PAGE_SIZE``,
        following ``nextPageToken``). Each row exposes the PK as the
        ``icao24`` property; ``__primaryKey`` is the documented fallback. The
        reconcile job (Fix C) diffs this against the live feed to find the
        departed-aircraft objects the upsert-only positions sync never
        removes. Read-only — no Action is applied here.
        """
        pks: set[str] = set()
        page_token: str | None = None
        while True:
            body = await self._get_objects_page("Aircraft", page_token)
            for obj in body.get("data", []):
                pk = obj.get("icao24") or obj.get("__primaryKey")
                if pk:
                    pks.add(str(pk))
            page_token = body.get("nextPageToken")
            if not page_token:
                break
        return pks

    async def delete_aircraft_batch(self, icao24s: list[str]) -> BatchResult:
        """Delete a batch of Aircraft by icao24. No-op on an empty list.

        The ``delete-aircraft`` Action's single parameter key is the
        **PascalCase object-type name** ``Aircraft`` (verified against the
        live tenant) — distinct from the lowercase upsert object-locator, so
        it is NOT routed through ``_camel``; the literal key is passed
        through ``_apply_batch`` unchanged.
        """
        if not icao24s:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(
            self._action_delete_aircraft, [{"Aircraft": pk} for pk in icao24s]
        )

    async def list_flight_pks(self) -> set[str]:
        """Return the flight_id primary key of every Flight object in the tenant.

        Paginates ``GET .../objects/Flight`` (pageSize ``_OBJECTS_PAGE_SIZE``,
        following ``nextPageToken``) — the exact pattern of
        ``list_aircraft_pks``. Each row exposes the PK as the ``flightId``
        property; ``__primaryKey`` is the documented fallback. The
        Flight-enrichment job iterates this to backfill the create-only
        takeoff Flights from ``/v1/flights``. Read-only — no Action applied.
        """
        pks: set[str] = set()
        page_token: str | None = None
        while True:
            body = await self._get_objects_page("Flight", page_token)
            for obj in body.get("data", []):
                pk = obj.get("flightId") or obj.get("__primaryKey")
                if pk:
                    pks.add(str(pk))
            page_token = body.get("nextPageToken")
            if not page_token:
                break
        return pks

    async def delete_flight_batch(self, flight_ids: list[str]) -> BatchResult:
        """Delete a batch of Flights by flight_id. No-op on an empty list.

        Same shape as ``delete_aircraft_batch`` / ``delete_case_batch``: the
        ``delete-flight`` Action's single parameter key is the PascalCase
        object-type name ``Flight`` (verified against the live tenant —
        param ``Flight``, required, object-ref Flight), distinct from the
        lowercase ``flight`` upsert object-locator, so it is NOT routed
        through ``_camel``; the literal key passes through ``_apply_batch``
        unchanged. Used by the tenant Flight-reconcile (Phase A) to evict
        completed/departed flights the upsert-only path never removes.
        """
        if not flight_ids:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(
            self._action_delete_flight, [{"Flight": pk} for pk in flight_ids]
        )

    async def upsert_case_batch(self, cases: list[Case]) -> BatchResult:
        """Upsert a batch of Cases. No-op (no HTTP call) on an empty list."""
        if not cases:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(self._action_case, [case_params(c) for c in cases])

    async def list_case_pks(self) -> set[str]:
        """Return the case_id primary key of every Case object in the tenant.

        Exact pattern of ``list_aircraft_pks`` / ``list_flight_pks``;
        paginates ``GET .../objects/Case`` and pulls ``caseId`` (with
        ``__primaryKey`` as the documented fallback). Provided for
        future tenant-side reconciles (the cases sync is upsert-only
        in v1; no caller exercises this yet).
        """
        pks: set[str] = set()
        page_token: str | None = None
        while True:
            body = await self._get_objects_page("Case", page_token)
            for obj in body.get("data", []):
                pk = obj.get("caseId") or obj.get("__primaryKey")
                if pk:
                    pks.add(str(pk))
            page_token = body.get("nextPageToken")
            if not page_token:
                break
        return pks

    async def delete_case_batch(self, case_ids: list[str]) -> BatchResult:
        """Delete a batch of Cases by case_id. No-op on an empty list.

        Same shape as ``delete_aircraft_batch``: the ``delete-case``
        Action's single parameter key is the PascalCase object-type name
        ``Case`` (NOT routed through ``_camel`` — literal). Provided
        for completeness; the cases sync is upsert-only in v1.
        """
        if not case_ids:
            return BatchResult(attempted=0, succeeded=0)
        return await self._apply_batch(self._action_delete_case, [{"Case": pk} for pk in case_ids])
