"""Tests for the delay rule."""

from __future__ import annotations

import datetime as dt

from pipelines.rules.delay import DelayRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions

# KSFO→KJFK ≈ 2,250 nm ≈ 5.5 h expected. Delay threshold = 1.3x ≈ 7.15 h.
COORDS = {"KSFO": (37.6189, -122.3750), "KJFK": (40.6398, -73.7789)}
BASELINE = HeuristicBaselineProvider(COORDS)
RULE = DelayRule()


def _flight(icao: str, departed_hours_ago: float, *, origin="KSFO", dest="KJFK", on_ground=False):
    return {
        "icao24": icao,
        "on_ground": on_ground,
        "origin_icao": origin,
        "destination_icao": dest,
        "departure_time": NOW - dt.timedelta(hours=departed_hours_ago),
        "ts_polled": NOW,
        "callsign": "UAL1",
    }


def test_positive_flight_running_long() -> None:
    positions = make_positions([_flight("dl01", departed_hours_ago=8.0)])
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.icao24 for a in out] == ["dl01"]
    assert out[0].detection_facts["origin"] == "KSFO"
    assert out[0].detection_facts["destination"] == "KJFK"
    assert out[0].detection_facts["elapsed_minutes"] > out[0].detection_facts["expected_minutes"]


def test_negative_on_time() -> None:
    positions = make_positions([_flight("ot01", departed_hours_ago=4.0)])
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_unknown_baseline_skips() -> None:
    # Airports not in the coord table → expected_duration None → skip.
    positions = make_positions(
        [_flight("uk01", departed_hours_ago=12.0, origin="KZZZ", dest="KYYY")]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_on_ground() -> None:
    positions = make_positions([_flight("gd01", departed_hours_ago=12.0, on_ground=True)])
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_no_flight_plan() -> None:
    positions = make_positions(
        [
            {
                "icao24": "np01",
                "on_ground": False,
                "origin_icao": None,
                "destination_icao": None,
                "departure_time": None,
                "ts_polled": NOW,
            }
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []
