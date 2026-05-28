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
                "ts_polled": NOW - mins(12),
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
    assert out[0].detection_facts["gap_minutes"] == 12.0


def test_negative_short_gap_is_poll_noise() -> None:
    # 5-minute gap is below the 8-min floor — ordinary feed jitter, not a
    # signal loss worth a case.
    positions = make_positions(
        [
            {"icao24": "blip01", "altitude_ft": 38_000, "ts_polled": NOW - mins(5)},
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


def test_negative_climbing_aircraft_excluded() -> None:
    # Past the 8-min floor and at cruise, but climbing hard — transitioning,
    # not a steady-cruise signal loss.
    positions = make_positions(
        [
            {
                "icao24": "clmb01",
                "altitude_ft": 30_000,
                "vertical_rate_fpm": 2_000,
                "ts_polled": NOW - mins(12),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    assert RULE.detect(positions, {}, empty_cases(), BASELINE) == []


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


# === severity gradation (B+C hybrid; PR-26-day-2 follow-up) ================
#
# Base tier from altitude. Hot cells demote one tier; gaps >=15min promote
# one tier. Tests use lat/lon coordinates chosen for known JSON cell
# membership (Gulf of Maine corner 42N/-71W is in the canonical list;
# mid-Atlantic 20N/-50W is not in any cell, no other rule will fire there
# either). The JSON file is checked into the repo so these tests are
# deterministic across machines.

_HOT_CELL_LAT = 42.5  # floor -> 42; pairs with -71 floor of -70.5 → cell (42, -71)
_HOT_CELL_LON = -70.5  # known hot cell (Gulf of Maine corner)
_COLD_CELL_LAT = 20.0  # mid-Atlantic, definitely not in any hot cell
_COLD_CELL_LON = -50.0


def test_severity_base_below_30k_is_high() -> None:
    """alt < 30k, non-hot cell, gap < 15min → base 'high', no shift."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 28_000,
                "lat": _COLD_CELL_LAT,
                "lon": _COLD_CELL_LON,
                "ts_polled": NOW - mins(10),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["high"]


def test_severity_base_30k_to_35k_is_medium() -> None:
    """30k <= alt < 35k, non-hot cell, gap < 15min → base 'medium'."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 32_000,
                "lat": _COLD_CELL_LAT,
                "lon": _COLD_CELL_LON,
                "ts_polled": NOW - mins(10),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["medium"]


def test_severity_base_at_or_above_35k_is_low() -> None:
    """alt >= 35k, non-hot cell, gap < 15min → base 'low'."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 38_000,
                "lat": _COLD_CELL_LAT,
                "lon": _COLD_CELL_LON,
                "ts_polled": NOW - mins(10),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["low"]


def test_severity_hot_cell_demotes_one_tier() -> None:
    """alt < 30k (base 'high'), in hot cell → demoted to 'medium'."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 28_000,
                "lat": _HOT_CELL_LAT,
                "lon": _HOT_CELL_LON,
                "ts_polled": NOW - mins(10),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["medium"]


def test_severity_long_gap_promotes_one_tier() -> None:
    """alt >= 35k (base 'low'), gap >= 15min → promoted to 'medium'."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 38_000,
                "lat": _COLD_CELL_LAT,
                "lon": _COLD_CELL_LON,
                "ts_polled": NOW - mins(16),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["medium"]


def test_severity_demote_and_promote_cancel() -> None:
    """alt < 30k (base 'high') in a hot cell with gap >=15min: demote then
    promote cancel out, net 'high'. Covers the combined-shift edge case."""
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                "altitude_ft": 28_000,
                "lat": _HOT_CELL_LAT,
                "lon": _HOT_CELL_LON,
                "ts_polled": NOW - mins(16),
            },
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    out = RULE.detect(positions, {}, empty_cases(), BASELINE)
    assert [a.severity_hint for a in out] == ["high"]
