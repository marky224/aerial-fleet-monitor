"""Tests for the diversion rule."""

from __future__ import annotations

from pipelines.rules.diversion import DiversionRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions

BASELINE = HeuristicBaselineProvider({})
RULE = DiversionRule()


def test_positive_landed_at_alternate() -> None:
    # Planned KSFO, on the ground at KOAK within 5 nm → diversion.
    positions = make_positions(
        [
            {
                "icao24": "div01",
                "on_ground": True,
                "altitude_ft": 0,
                "destination_icao": "KSFO",
                "origin_icao": "KJFK",
                "nearest_site_icao": "KOAK",
                "nearest_site_distance_nm": 2.0,
                "ts_polled": NOW,
                "callsign": "DAL88",
            }
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.icao24 for a in out] == ["div01"]
    assert out[0].detection_facts["expected_destination"] == "KSFO"
    assert out[0].detection_facts["alternate"] == "KOAK"


def test_negative_landed_at_planned_destination() -> None:
    positions = make_positions(
        [
            {
                "icao24": "ok01",
                "on_ground": True,
                "destination_icao": "KSFO",
                "nearest_site_icao": "KSFO",
                "nearest_site_distance_nm": 1.0,
                "ts_polled": NOW,
            }
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_still_airborne() -> None:
    positions = make_positions(
        [
            {
                "icao24": "air01",
                "on_ground": False,
                "destination_icao": "KSFO",
                "nearest_site_icao": "KOAK",
                "nearest_site_distance_nm": 2.0,
                "ts_polled": NOW,
            }
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_no_known_destination() -> None:
    positions = make_positions(
        [
            {
                "icao24": "nd01",
                "on_ground": True,
                "destination_icao": None,
                "nearest_site_icao": "KOAK",
                "nearest_site_distance_nm": 2.0,
                "ts_polled": NOW,
            }
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_on_ground_but_far_from_any_watched_field() -> None:
    # Landed at an unwatched field — nearest watched is far → not caught
    # (a recorded Phase-05 limitation).
    positions = make_positions(
        [
            {
                "icao24": "un01",
                "on_ground": True,
                "destination_icao": "KSFO",
                "nearest_site_icao": "KOAK",
                "nearest_site_distance_nm": 60.0,
                "ts_polled": NOW,
            }
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []
