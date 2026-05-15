"""Unit tests for transforms.py — pure-function mappers.

Coverage per build doc: no field loss, None preservation, timezone
preservation, staleness verbatim, and weather/SLA fold semantics on Site.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from afm_foundry_sync.models import (
    Aircraft,
    Position,
    Site,
    SiteDetail,
    SiteSla,
    SiteWeather,
    SparklinePoint,
)
from afm_foundry_sync.transforms import position_to_aircraft, site_to_site

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
