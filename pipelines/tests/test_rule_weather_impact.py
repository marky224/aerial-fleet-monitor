"""Tests for the weather_impact rule (site-level)."""

from __future__ import annotations

from pipelines.rules.base import AirportConditions
from pipelines.rules.weather_impact import WeatherImpactRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import empty_cases, make_positions

BASELINE = HeuristicBaselineProvider({})
RULE = WeatherImpactRule()


def _cond(category: str | None) -> AirportConditions:
    return AirportConditions(
        site_icao="KSFO",
        flight_category=category,
        ceiling_ft=400,
        visibility_sm=0.5,
        wind_kt=18,
    )


def test_positive_ifr_fires() -> None:
    weather = {"KSFO": _cond("IFR")}
    out = RULE.detect(make_positions([]), weather, empty_cases(), BASELINE)
    assert len(out) == 1
    assert out[0].site_icao == "KSFO"
    assert out[0].icao24 == ""  # site-level
    assert out[0].customer_region == "all"
    assert out[0].severity_hint == "medium"


def test_positive_lifr_is_high_severity() -> None:
    out = RULE.detect(make_positions([]), {"KSFO": _cond("LIFR")}, empty_cases(), BASELINE)
    assert out[0].severity_hint == "high"
    assert out[0].detection_facts["flight_category"] == "LIFR"


def test_negative_vfr_no_fire() -> None:
    assert RULE.detect(make_positions([]), {"KSFO": _cond("VFR")}, empty_cases(), BASELINE) == []


def test_negative_mvfr_no_fire() -> None:
    assert RULE.detect(make_positions([]), {"KSFO": _cond("MVFR")}, empty_cases(), BASELINE) == []


def test_empty_weather_no_fire() -> None:
    assert RULE.detect(make_positions([]), {}, empty_cases(), BASELINE) == []


def test_multiple_sites_each_fire() -> None:
    weather = {
        "KSFO": _cond("IFR"),
        "KOAK": AirportConditions(site_icao="KOAK", flight_category="LIFR"),
        "KSJC": _cond("VFR"),
    }
    out = RULE.detect(make_positions([]), weather, empty_cases(), BASELINE)
    assert {a.site_icao for a in out} == {"KSFO", "KOAK"}
