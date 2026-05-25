"""Tests for the go_around rule."""

from __future__ import annotations

from pipelines.rules.go_around import GoAroundRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions, mins

BASELINE = HeuristicBaselineProvider({})
RULE = GoAroundRule()


def _row(icao: str, alt: int, off: float, *, dist: float = 4.0) -> dict:
    return {
        "icao24": icao,
        "altitude_ft": alt,
        "on_ground": False,
        "ts_polled": NOW - mins(off),
        "nearest_site_icao": "KSEA",
        "nearest_site_distance_nm": dist,
        "callsign": "ASA12",
    }


def test_positive_descend_then_climb() -> None:
    # Descends to 1,500 ft on final, then climbs back to 4,000 ft.
    rows = [
        _row("ga01", 4_000, 6),
        _row("ga01", 2_500, 5),
        _row("ga01", 1_500, 4),  # min — short final
        _row("ga01", 2_800, 3),
        _row("ga01", 4_000, 2),  # climb-out
    ]
    out = RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE)
    assert [a.icao24 for a in out] == ["ga01"]
    assert out[0].site_icao == "KSEA"
    assert out[0].detection_facts["min_altitude_ft"] == 1_500
    assert out[0].detection_facts["climb_ft"] >= 1_000


def test_negative_departure_only_climbs() -> None:
    # A normal departure: the lowest near-field snapshot is the first one,
    # then it only climbs out — no descent leg, so not a go-around. (This is
    # the false positive that flooded the detector before the V-shape check.)
    rows = [
        _row("dep01", 1_200, 5),  # min — just airborne off the runway
        _row("dep01", 2_500, 4),
        _row("dep01", 4_000, 3),  # climbing away
    ]
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_normal_descent_no_climb() -> None:
    rows = [
        _row("ok01", 4_000, 6),
        _row("ok01", 2_500, 5),
        _row("ok01", 1_500, 4),
        _row("ok01", 800, 3),  # keeps descending to land
        _row("ok01", 200, 2),
    ]
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_never_low_enough() -> None:
    # Dips to 5,000 ft (above the 3,000 ft floor) then climbs — overflight,
    # not a go-around.
    rows = [
        _row("hi01", 8_000, 6),
        _row("hi01", 5_000, 4),
        _row("hi01", 8_000, 2),
    ]
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_not_near_airport() -> None:
    rows = [
        _row("far01", 2_500, 6, dist=40.0),
        _row("far01", 1_500, 4, dist=40.0),
        _row("far01", 4_000, 2, dist=40.0),
    ]
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_too_few_snapshots() -> None:
    rows = [_row("few01", 1_500, 4), _row("few01", 4_000, 2)]
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []
