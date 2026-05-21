"""Tests for the lost_signal rule."""

from __future__ import annotations

from pipelines.rules.lost_signal import LostSignalRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions, mins

BASELINE = HeuristicBaselineProvider({})
RULE = LostSignalRule()


def test_positive_cruise_aircraft_goes_quiet() -> None:
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 38_000,
                "on_ground": False,
                "ts_polled": NOW - mins(5),
                "nearest_site_icao": "KDEN",
                "callsign": "UAL99",
            },
            {"icao24": "live01", "ts_polled": NOW},  # fresh anchor sets "now"
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.icao24 for a in out] == ["lost01"]
    assert out[0].rule == "lost_signal"
    assert out[0].site_icao == "KDEN"
    assert out[0].detection_facts["gap_minutes"] == 5.0


def test_negative_still_tracking_no_gap() -> None:
    positions = make_positions([{"icao24": "lost01", "altitude_ft": 38_000, "ts_polled": NOW}])
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_low_altitude_not_cruise() -> None:
    positions = make_positions(
        [
            {"icao24": "low01", "altitude_ft": 9_000, "ts_polled": NOW - mins(5)},
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_on_ground() -> None:
    positions = make_positions(
        [
            {
                "icao24": "grnd01",
                "altitude_ft": 0,
                "on_ground": True,
                "ts_polled": NOW - mins(5),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_gap_too_large_treated_as_gone() -> None:
    positions = make_positions(
        [
            {"icao24": "gone01", "altitude_ft": 38_000, "ts_polled": NOW - mins(45)},
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_empty_frame_returns_nothing() -> None:
    assert RULE.detect(make_positions([]), {}, empty_cases(), BASELINE) == []
