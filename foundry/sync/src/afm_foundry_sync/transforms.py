"""Pure mappers from AFM /v1 API responses to Foundry Ontology objects.

This module is the contract between the API-side shapes
(``models.Position``, ``models.SiteDetail``, ...) and the Ontology-side
shapes (``models.Aircraft``, ``models.Site``). No I/O — fetching lives in
``api_readers.py`` and writing lives in ``ontology_writers.py``.

Operator and Case transforms remain deferred: Operator is derived from
Flight (its object type stays TBD); Case lands in Phase 05.

Flight has two construction paths, mirroring the modify-or-create write
model: ``takeoff_to_flight`` builds the minimal create payload from the
synthesized identity that ``sync_jobs.TakeoffDetector`` mints (passed as
primitives, not a ``Takeoff`` object, to keep this module decoupled from
the sync_jobs orchestration layer); ``flight_detail_to_flight`` builds the
full enriched payload from a later ``/v1/flights`` + trail fetch.
"""

from __future__ import annotations

from datetime import datetime

from afm_foundry_sync.models import (
    Aircraft,
    Flight,
    FlightDetail,
    FlightStage,
    FlightStatus,
    FlightStatusEvent,
    Position,
    Site,
    SiteDetail,
    SiteSla,
    TrailResponse,
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


# ---------------------------------------------------------------------------
# Flight
# ---------------------------------------------------------------------------

# FlightDetail carries a FlightStage timeline, not a FlightStatus. The
# Ontology denormalizes both: current_stage = the timeline tail's stage;
# status = that stage mapped to the coarser FlightStatus the Workshop app
# binds. Empty timeline => ("unknown", None).
_STAGE_TO_STATUS: dict[FlightStage, FlightStatus] = {
    "departed": "departed",
    "climb": "enroute",
    "cruise": "enroute",
    "descent": "enroute",
    "approach": "approaching",
    "landed": "landed",
}


def takeoff_to_flight(flight_id: str, icao24: str, takeoff_ts: datetime) -> Flight:
    """Build the minimal create payload from a detected takeoff edge.

    Takes the synthesized identity as primitives (the values
    ``sync_jobs.TakeoffDetector`` produces) rather than importing the
    ``Takeoff`` type, so transforms stays a leaf module under sync_jobs.

    A detected on-ground->airborne edge *is* a departure at ``takeoff_ts``,
    so the timeline is seeded with that one truthful event and the status
    scalars reflect it — the object is meaningful in the Workshop app
    before any /v1/flights enrichment arrives. The seed is overwritten
    wholesale by ``flight_detail_to_flight`` on the next re-upsert.
    """
    return Flight(
        flight_id=flight_id,
        icao24=icao24,
        takeoff_ts=takeoff_ts,
        landed_at=None,
        callsign=None,
        registration=None,
        aircraft_type=None,
        operator_icao=None,
        customer_region=None,
        origin_icao=None,
        destination_icao=None,
        eta_minutes=None,
        status="departed",
        current_stage="departed",
        lat=None,
        lon=None,
        open_case_count=0,
        open_case_ids=[],
        status_timeline=[FlightStatusEvent(stage="departed", occurred_at=takeoff_ts)],
        trail_2h=[],
    )


def flight_detail_to_flight(
    flight_id: str,
    takeoff_ts: datetime,
    detail: FlightDetail,
    trail: TrailResponse | None = None,
) -> Flight:
    """Build the full enriched Flight from a /v1/flights fetch (+ optional trail).

    ``flight_id`` and ``takeoff_ts`` are the synthesized identity (not
    present on ``FlightDetail``) carried over from the create. The full
    object is re-upserted (modify-or-create), so every field is set from
    the fresh fetch.

    Denormalization: ``current_stage`` is the status-timeline tail;
    ``status`` is that stage mapped via ``_STAGE_TO_STATUS`` (``unknown``
    when the timeline is empty); ``landed_at`` is the ``occurred_at`` of
    the first ``landed`` event, else None (still airborne).
    """
    timeline = detail.status_timeline
    current_stage = timeline[-1].stage if timeline else None
    status: FlightStatus = (
        _STAGE_TO_STATUS[current_stage] if current_stage is not None else "unknown"
    )
    landed_at = next(
        (e.occurred_at for e in timeline if e.stage == "landed"),
        None,
    )
    return Flight(
        flight_id=flight_id,
        icao24=detail.icao24,
        takeoff_ts=takeoff_ts,
        landed_at=landed_at,
        callsign=detail.callsign,
        registration=detail.registration,
        aircraft_type=detail.aircraft_type,
        operator_icao=detail.operator_icao,
        customer_region=detail.customer_region,
        origin_icao=detail.origin_icao,
        destination_icao=detail.destination_icao,
        eta_minutes=detail.eta_minutes,
        status=status,
        current_stage=current_stage,
        lat=detail.position.lat,
        lon=detail.position.lon,
        open_case_count=len(detail.open_case_ids),
        open_case_ids=detail.open_case_ids,
        status_timeline=timeline,
        trail_2h=trail.points if trail is not None else [],
    )
