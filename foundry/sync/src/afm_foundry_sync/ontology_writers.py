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
    ``icao``) and once as the object-locator param (``aircraft`` /
    ``site``), which is ``required`` with an ``objectQueryResult``
    constraint and takes the bare PK string. Both names are single-token,
    so ``_camel`` leaves them unchanged.
  - ``position`` / ``location`` geopoints are GeoJSON Points with
    ``coordinates: [lon, lat]`` (GeoJSON axis order — locked decision).
    They are NOT on the upstream payloads; constructed here from lat/lon.
  - ``customerRegions`` and ``slaSparkline7d`` are JSON-encoded strings
    (the tenant models them as ``string``; ``"[]"`` sparkline in v1).
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

from afm_foundry_sync.models import Aircraft, Site
from afm_foundry_sync.retry import transient_retry
from afm_foundry_sync.settings import FoundrySettings

logger = structlog.get_logger(__name__)

_REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

# applyBatch's maximum request count is an open question (ONTOLOGY.md). 100 is
# a conservative default; tune once the live limit is discovered.
_MAX_BATCH = 100


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Outcome of an upsert batch. ``attempted == succeeded`` on full success.

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
        "sla_sparkline_7d": json.dumps(
            [p.model_dump(mode="json") for p in s.sla_sparkline_7d]
        ),
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

    async def _apply_batch(
        self, action: str, param_dicts: list[dict[str, Any]]
    ) -> BatchResult:
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
        return await self._apply_batch(
            self._action_site, [site_params(s) for s in sites]
        )
