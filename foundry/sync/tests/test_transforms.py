"""Unit tests for transforms.py — pure-function mappers.

Coverage per build doc: no field loss, None preservation, timezone
preservation, staleness verbatim, and weather/SLA fold semantics on Site.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from afm_foundry_sync.models import (
    Aircraft,
    Case,
    CaseForSync,
    Flight,
    FlightDetail,
    FlightStatusEvent,
    Position,
    Site,
    SiteDetail,
    SiteSla,
    SiteWeather,
    SparklinePoint,
    TrailPoint,
    TrailResponse,
)
from afm_foundry_sync.transforms import (
    case_for_sync_to_case,
    flight_detail_to_flight,
    position_to_aircraft,
    site_to_site,
    takeoff_to_flight,
)

# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_position(**overrides: Any) -> Position:
    base: dict[str, Any] = dict(
        icao24="a12345",
        callsign="UAL1234",
        lat=37.62,
        lon=-122.37,
        altitude_ft=12000,
        speed_kt=300,
        heading_deg=270,
        vertical_rate_fpm=0,
        on_ground=False,
        customer_region="west",
        last_seen_at=datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC),
        staleness="fresh",
    )
    base.update(overrides)
    return Position(**base)


def _make_weather(**overrides: Any) -> SiteWeather:
    base: dict[str, Any] = dict(
        metar_raw="KSFO 151200Z 27015KT 10SM FEW012 19/12 A2992",
        metar_plain_english=None,
        flight_category="VFR",
        wind_kt=15,
        visibility_sm=10.0,
        ceiling_ft=None,
        observed_at=datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC),
    )
    base.update(overrides)
    return SiteWeather(**base)


def _make_site_detail(**overrides: Any) -> SiteDetail:
    base: dict[str, Any] = dict(
        icao="KSFO",
        iata="SFO",
        name="San Francisco Intl",
        city="San Francisco",
        state="CA",
        lat=37.6188,
        lon=-122.3754,
        elevation_ft=13,
        timezone=None,
        weather=_make_weather(),
        inbound_count_60m=4,
        outbound_count_60m=2,
        active_case_count=0,
        customer_regions=["west"],
    )
    base.update(overrides)
    return SiteDetail(**base)


def _make_sla(**overrides: Any) -> SiteSla:
    base: dict[str, Any] = dict(
        icao="KSFO",
        period="last_24h",
        inbound_count=50,
        outbound_count=48,
        on_time_arrival_pct=92.0,
        on_time_departure_pct=88.5,
        avg_arrival_delay_min=4.2,
        avg_departure_delay_min=6.8,
        weather_impact="low",
        flight_category="MVFR",
        active_cases=0,
        sparkline_7d=[SparklinePoint(day=date(2026, 5, 14), on_time_pct=91.0, avg_delay_min=5.0)],
    )
    base.update(overrides)
    return SiteSla(**base)


# ---------------------------------------------------------------------------
# Position → Aircraft
# ---------------------------------------------------------------------------


def test_position_to_aircraft_passthrough_all_fields() -> None:
    a = position_to_aircraft(_make_position())
    assert isinstance(a, Aircraft)
    assert a.icao24 == "a12345"
    assert a.callsign == "UAL1234"
    assert a.lat == 37.62
    assert a.lon == -122.37
    assert a.altitude_ft == 12000
    assert a.speed_kt == 300
    assert a.heading_deg == 270
    assert a.vertical_rate_fpm == 0
    assert a.on_ground is False
    assert a.customer_region == "west"
    assert a.staleness == "fresh"


def test_position_to_aircraft_preserves_none_fields() -> None:
    a = position_to_aircraft(
        _make_position(
            callsign=None,
            altitude_ft=None,
            speed_kt=None,
            heading_deg=None,
            vertical_rate_fpm=None,
            customer_region=None,
        )
    )
    assert a.callsign is None
    assert a.altitude_ft is None
    assert a.speed_kt is None
    assert a.heading_deg is None
    assert a.vertical_rate_fpm is None
    assert a.customer_region is None


def test_position_to_aircraft_preserves_timezone() -> None:
    ts = datetime(2026, 5, 15, 12, 30, 45, tzinfo=UTC)
    a = position_to_aircraft(_make_position(last_seen_at=ts))
    assert a.last_seen_at == ts
    assert a.last_seen_at.tzinfo is not None


def test_position_to_aircraft_on_ground_true() -> None:
    a = position_to_aircraft(_make_position(on_ground=True, speed_kt=0, altitude_ft=None))
    assert a.on_ground is True
    assert a.speed_kt == 0
    assert a.altitude_ft is None


def test_position_to_aircraft_staleness_preserved_verbatim() -> None:
    for value in ("fresh", "stale", "lost"):
        a = position_to_aircraft(_make_position(staleness=value))
        assert a.staleness == value


# ---------------------------------------------------------------------------
# Site → Site
# ---------------------------------------------------------------------------


def test_site_to_site_with_sla_and_weather() -> None:
    s = site_to_site(_make_site_detail(), _make_sla())
    assert isinstance(s, Site)
    # Identity
    assert s.icao == "KSFO"
    assert s.name == "San Francisco Intl"
    assert s.customer_regions == ["west"]
    # Live counts (from detail)
    assert s.inbound_count_60m == 4
    assert s.outbound_count_60m == 2
    assert s.active_case_count == 0
    # Weather block flat
    assert s.metar_raw is not None and s.metar_raw.startswith("KSFO")
    assert s.wind_kt == 15
    assert s.visibility_sm == 10.0
    assert s.ceiling_ft is None
    assert s.weather_observed_at is not None
    # SLA block flat
    assert s.sla_period == "last_24h"
    assert s.sla_inbound_count == 50
    assert s.on_time_arrival_pct == 92.0
    assert s.avg_departure_delay_min == 6.8
    assert s.weather_impact == "low"
    assert len(s.sla_sparkline_7d) == 1


def test_site_to_site_flight_category_prefers_sla() -> None:
    detail = _make_site_detail(weather=_make_weather(flight_category="VFR"))
    sla = _make_sla(flight_category="MVFR")
    assert site_to_site(detail, sla).flight_category == "MVFR"


def test_site_to_site_flight_category_falls_back_to_weather_when_no_sla() -> None:
    detail = _make_site_detail(weather=_make_weather(flight_category="IFR"))
    assert site_to_site(detail, sla=None).flight_category == "IFR"


def test_site_to_site_without_sla_nulls_sla_fields() -> None:
    s = site_to_site(_make_site_detail(), sla=None)
    assert s.sla_period is None
    assert s.sla_inbound_count is None
    assert s.sla_outbound_count is None
    assert s.on_time_arrival_pct is None
    assert s.on_time_departure_pct is None
    assert s.avg_arrival_delay_min is None
    assert s.avg_departure_delay_min is None
    assert s.weather_impact is None
    assert s.sla_sparkline_7d == []


def test_site_to_site_without_weather_nulls_weather_fields() -> None:
    s = site_to_site(_make_site_detail(weather=None), sla=_make_sla())
    assert s.metar_raw is None
    assert s.metar_plain_english is None
    assert s.wind_kt is None
    assert s.visibility_sm is None
    assert s.ceiling_ft is None
    assert s.weather_observed_at is None
    # SLA still drives flight_category
    assert s.flight_category == "MVFR"


def test_site_to_site_no_weather_no_sla_yields_null_flight_category() -> None:
    s = site_to_site(_make_site_detail(weather=None), sla=None)
    assert s.flight_category is None
    assert s.metar_raw is None
    assert s.sla_period is None


def test_site_to_site_active_case_count_from_detail_only() -> None:
    """SiteSla.active_cases is ignored — SiteDetail is source of truth."""
    detail = _make_site_detail(active_case_count=7)
    sla = _make_sla(active_cases=99)  # divergent on purpose
    assert site_to_site(detail, sla).active_case_count == 7


# ---------------------------------------------------------------------------
# Flight factories
# ---------------------------------------------------------------------------

_TAKEOFF_TS = datetime(2026, 5, 15, 11, 30, 0, tzinfo=UTC)
_FLIGHT_ID = "a12345-1747308600"  # {icao24}-{unix(_TAKEOFF_TS)}


def _make_flight_detail(**overrides: Any) -> FlightDetail:
    base: dict[str, Any] = dict(
        icao24="a12345",
        callsign="UAL1234",
        registration="N12345",
        aircraft_type="B738",
        operator_icao="UAL",
        origin_icao="KSFO",
        destination_icao="KLAX",
        customer_region="west",
        position=_make_position(),
        eta_minutes=42,
        status_timeline=[
            FlightStatusEvent(stage="departed", occurred_at=_TAKEOFF_TS),
            FlightStatusEvent(
                stage="cruise", occurred_at=datetime(2026, 5, 15, 11, 50, tzinfo=UTC)
            ),
        ],
        open_case_ids=[],
    )
    base.update(overrides)
    return FlightDetail(**base)


def _make_trail(**overrides: Any) -> TrailResponse:
    base: dict[str, Any] = dict(
        icao24="a12345",
        points=[
            TrailPoint(
                ts=datetime(2026, 5, 15, 11, 35, tzinfo=UTC),
                lat=37.7,
                lon=-122.4,
                altitude_ft=8000,
                speed_kt=280,
            )
        ],
        lookback="2h",
        point_count=1,
    )
    base.update(overrides)
    return TrailResponse(**base)


# ---------------------------------------------------------------------------
# takeoff_to_flight (create path)
# ---------------------------------------------------------------------------


def test_takeoff_to_flight_sets_synthesized_identity_only() -> None:
    f = takeoff_to_flight(_FLIGHT_ID, "a12345", _TAKEOFF_TS)
    assert isinstance(f, Flight)
    assert f.flight_id == _FLIGHT_ID
    assert f.icao24 == "a12345"
    assert f.takeoff_ts == _TAKEOFF_TS
    # Every enrichment field is empty/None on the create payload.
    assert f.callsign is None
    assert f.registration is None
    assert f.aircraft_type is None
    assert f.operator_icao is None
    assert f.customer_region is None
    assert f.origin_icao is None
    assert f.destination_icao is None
    assert f.eta_minutes is None
    assert f.landed_at is None
    assert f.lat is None
    assert f.lon is None
    assert f.open_case_count == 0
    assert f.open_case_ids == []
    assert f.trail_2h == []


def test_takeoff_to_flight_seeds_truthful_departed_event() -> None:
    """A detected takeoff IS a departure at takeoff_ts — seed it so the
    object is meaningful before enrichment."""
    f = takeoff_to_flight(_FLIGHT_ID, "a12345", _TAKEOFF_TS)
    assert f.status == "departed"
    assert f.current_stage == "departed"
    assert len(f.status_timeline) == 1
    assert f.status_timeline[0].stage == "departed"
    assert f.status_timeline[0].occurred_at == _TAKEOFF_TS


# ---------------------------------------------------------------------------
# flight_detail_to_flight (enrich path)
# ---------------------------------------------------------------------------


def test_flight_detail_to_flight_full_passthrough() -> None:
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail())
    # Synthesized identity carried over (not on FlightDetail).
    assert f.flight_id == _FLIGHT_ID
    assert f.takeoff_ts == _TAKEOFF_TS
    # Enriched fields.
    assert f.icao24 == "a12345"
    assert f.callsign == "UAL1234"
    assert f.registration == "N12345"
    assert f.aircraft_type == "B738"
    assert f.operator_icao == "UAL"
    assert f.customer_region == "west"
    assert f.origin_icao == "KSFO"
    assert f.destination_icao == "KLAX"
    assert f.eta_minutes == 42
    assert f.lat == 37.62
    assert f.lon == -122.37
    assert len(f.status_timeline) == 2


def test_flight_detail_to_flight_denormalizes_status_from_timeline_tail() -> None:
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail())
    # Tail stage is "cruise" -> coarse status "enroute".
    assert f.current_stage == "cruise"
    assert f.status == "enroute"


def test_flight_detail_to_flight_stage_to_status_mapping() -> None:
    cases = {
        "departed": "departed",
        "climb": "enroute",
        "cruise": "enroute",
        "descent": "enroute",
        "approach": "approaching",
        "landed": "landed",
    }
    for stage, expected_status in cases.items():
        detail = _make_flight_detail(
            status_timeline=[FlightStatusEvent(stage=stage, occurred_at=_TAKEOFF_TS)]
        )
        f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, detail)
        assert f.current_stage == stage
        assert f.status == expected_status


def test_flight_detail_to_flight_empty_timeline_is_unknown() -> None:
    f = flight_detail_to_flight(
        _FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail(status_timeline=[])
    )
    assert f.current_stage is None
    assert f.status == "unknown"
    assert f.landed_at is None


def test_flight_detail_to_flight_landed_at_from_landed_event() -> None:
    landed_ts = datetime(2026, 5, 15, 12, 15, tzinfo=UTC)
    detail = _make_flight_detail(
        status_timeline=[
            FlightStatusEvent(stage="approach", occurred_at=_TAKEOFF_TS),
            FlightStatusEvent(stage="landed", occurred_at=landed_ts),
        ]
    )
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, detail)
    assert f.landed_at == landed_ts
    assert f.status == "landed"


def test_flight_detail_to_flight_open_case_count_matches_ids() -> None:
    detail = _make_flight_detail(open_case_ids=["CASE-1", "CASE-2", "CASE-3"])
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, detail)
    assert f.open_case_count == 3
    assert f.open_case_ids == ["CASE-1", "CASE-2", "CASE-3"]


def test_flight_detail_to_flight_trail_none_yields_empty_list() -> None:
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail(), trail=None)
    assert f.trail_2h == []


def test_flight_detail_to_flight_trail_points_carried() -> None:
    f = flight_detail_to_flight(
        _FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail(), trail=_make_trail()
    )
    assert len(f.trail_2h) == 1
    assert f.trail_2h[0].altitude_ft == 8000


def test_flight_detail_to_flight_preserves_timezone() -> None:
    f = flight_detail_to_flight(_FLIGHT_ID, _TAKEOFF_TS, _make_flight_detail())
    assert f.takeoff_ts.tzinfo is not None
    assert f.status_timeline[0].occurred_at.tzinfo is not None


# ---------------------------------------------------------------------------
# Case (Phase 05 task #5)
# ---------------------------------------------------------------------------


def _make_case_for_sync(**overrides: Any) -> CaseForSync:
    base: dict[str, Any] = dict(
        case_id="CASE-2026-000001",
        salesforce_id="500X000000abc",
        case_type="lost_signal",
        status="open",
        severity="high",
        customer_region="west",
        site_icao="KSFO",
        flight_id="a12345-1747308600",
        subject="Lost signal during cruise — UAL1234 near KSFO",
        summary=None,
        severity_justification=None,
        detection_facts={"callsign": "UAL1234", "gap_minutes": 12},
        runbook_refs=["lost-signal-cruise"],
        created_at=datetime(2026, 5, 24, 10, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 5, 24, 10, 5, 0, tzinfo=UTC),
        resolved_at=None,
    )
    base.update(overrides)
    return CaseForSync(**base)


def test_case_for_sync_to_case_passes_every_field_through() -> None:
    item = _make_case_for_sync()
    c = case_for_sync_to_case(item)
    assert isinstance(c, Case)
    assert c.case_id == item.case_id
    assert c.salesforce_id == item.salesforce_id
    assert c.case_type == item.case_type
    assert c.status == item.status
    assert c.severity == item.severity
    assert c.customer_region == item.customer_region
    assert c.site_icao == item.site_icao
    assert c.flight_id == item.flight_id
    assert c.subject == item.subject
    assert c.summary == item.summary
    assert c.severity_justification == item.severity_justification
    assert c.detection_facts == item.detection_facts
    assert c.runbook_refs == item.runbook_refs
    assert c.created_at == item.created_at
    assert c.updated_at == item.updated_at
    assert c.resolved_at == item.resolved_at


def test_case_for_sync_to_case_preserves_none_optionals() -> None:
    """Pending push (salesforce_id None) + still-open (resolved_at None) round-trip."""
    item = _make_case_for_sync(salesforce_id=None, resolved_at=None)
    c = case_for_sync_to_case(item)
    assert c.salesforce_id is None
    assert c.resolved_at is None


def test_case_for_sync_to_case_carries_wx_sentinel_flight_id() -> None:
    """Site-level cases use a `WX-{site_icao}` sentinel for flight_id."""
    item = _make_case_for_sync(case_type="weather_impact", flight_id="WX-KJFK", site_icao="KJFK")
    c = case_for_sync_to_case(item)
    assert c.flight_id == "WX-KJFK"
    assert c.site_icao == "KJFK"
