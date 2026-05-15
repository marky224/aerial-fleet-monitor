"""Pure mappers from AFM /v1 API responses to Foundry Ontology objects.

This module is the contract between the API-side shapes
(``models.Position``, ``models.SiteDetail``, ...) and the Ontology-side
shapes (``models.Aircraft``, ``models.Site``). No I/O — fetching lives in
``api_readers.py`` and writing lives in ``ontology_writers.py``.

Flight, Operator, and Case transforms are deferred: ``Flight.flight_id``
synthesis requires takeoff detection, which is stateful logic that belongs
in ``sync_jobs.py``; Operator is derived from Flight; Case lands in Phase 05.
"""

from __future__ import annotations

from afm_foundry_sync.models import (
    Aircraft,
    Position,
    Site,
    SiteDetail,
    SiteSla,
)


def position_to_aircraft(position: Position) -> Aircraft:
    """Map a /v1/positions/live row to an Aircraft Ontology object.

    Strict 1:1 field passthrough. Identity-side fields (registration,
    aircraft_type, operator_icao) are NOT set here — they live on
    FlightDetail and would require a per-icao24 fanout the positions
    sync deliberately avoids.
    """
    return Aircraft(
        icao24=position.icao24,
        callsign=position.callsign,
        lat=position.lat,
        lon=position.lon,
        altitude_ft=position.altitude_ft,
        speed_kt=position.speed_kt,
        heading_deg=position.heading_deg,
        vertical_rate_fpm=position.vertical_rate_fpm,
        on_ground=position.on_ground,
        customer_region=position.customer_region,
        last_seen_at=position.last_seen_at,
        staleness=position.staleness,
    )


def site_to_site(detail: SiteDetail, sla: SiteSla | None = None) -> Site:
    """Merge SiteDetail + optional SiteSla into a flat Site Ontology object.

    Weather fields are pulled from ``detail.weather`` and are None when no
    recent METAR is available. SLA fields are populated only when ``sla``
    is provided.

    Field-conflict resolution:
    - ``flight_category``: prefer SLA value when present, else fall back to
      the weather block's value, else None.
    - ``active_case_count``: SiteDetail is the source of truth. SiteSla's
      ``active_cases`` is treated as a duplicate and ignored.
    """
    weather = detail.weather

    if sla is not None:
        flight_category = sla.flight_category
    elif weather is not None:
        flight_category = weather.flight_category
    else:
        flight_category = None

    return Site(
        # Identity
        icao=detail.icao,
        iata=detail.iata,
        name=detail.name,
        city=detail.city,
        state=detail.state,
        lat=detail.lat,
        lon=detail.lon,
        elevation_ft=detail.elevation_ft,
        timezone=detail.timezone,
        customer_regions=detail.customer_regions,
        # Live counts
        inbound_count_60m=detail.inbound_count_60m,
        outbound_count_60m=detail.outbound_count_60m,
        active_case_count=detail.active_case_count,
        # Weather (None when no METAR)
        metar_raw=weather.metar_raw if weather else None,
        metar_plain_english=weather.metar_plain_english if weather else None,
        wind_kt=weather.wind_kt if weather else None,
        visibility_sm=weather.visibility_sm if weather else None,
        ceiling_ft=weather.ceiling_ft if weather else None,
        weather_observed_at=weather.observed_at if weather else None,
        # Current flight category
        flight_category=flight_category,
        # SLA (None / empty when no SLA fetched)
        sla_period=sla.period if sla else None,
        sla_inbound_count=sla.inbound_count if sla else None,
        sla_outbound_count=sla.outbound_count if sla else None,
        on_time_arrival_pct=sla.on_time_arrival_pct if sla else None,
        on_time_departure_pct=sla.on_time_departure_pct if sla else None,
        avg_arrival_delay_min=sla.avg_arrival_delay_min if sla else None,
        avg_departure_delay_min=sla.avg_departure_delay_min if sla else None,
        weather_impact=sla.weather_impact if sla else None,
        sla_sparkline_7d=sla.sparkline_7d if sla else [],
    )
