"""Tests for the excessive_hold rule."""

from __future__ import annotations

from typing import Any

from pipelines.rules.excessive_hold import ExcessiveHoldRule
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions, mins

BASELINE = HeuristicBaselineProvider({})
RULE = ExcessiveHoldRule()


def _holding_rows(
    n: int,
    span_min: float,
    headings: list[int],
    *,
    distance_nm: float = 10.0,
    altitude_ft: int = 8_000,
) -> list[dict[str, Any]]:
    rows = []
    for i in range(n):
        offset = span_min * (n - 1 - i) / (n - 1) if n > 1 else 0
        rows.append(
            {
                "icao24": "hold01",
                "altitude_ft": altitude_ft,
                "heading_deg": headings[i % len(headings)],
                "on_ground": False,
                "ts_polled": NOW - mins(offset),
                "nearest_site_icao": "KDEN",
                "nearest_site_distance_nm": distance_nm,
                "callsign": "AAL55",
            }
        )
    return rows


def test_positive_circling_near_airport() -> None:
    # 12 snapshots over 35 min within 10 nm, headings sweep all 8 sectors → holding.
    rows = _holding_rows(12, 35, [0, 45, 90, 135, 180, 225, 270, 315])
    out = RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE)
    assert [a.icao24 for a in out] == ["hold01"]
    assert out[0].site_icao == "KDEN"
    assert out[0].detection_facts["distinct_heading_sectors"] >= 6


def test_negative_transiting_single_heading() -> None:
    # Within radius and over the duration floor, but constant heading (1
    # sector < 6) → transiting, not circling. Isolates the sector gate.
    rows = _holding_rows(12, 35, [90])
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_too_few_sectors() -> None:
    # Within radius and over the duration floor, sweeps only 5 of 8 sectors
    # (below the tuned 6) → not enough circling. Isolates the sector gate.
    rows = _holding_rows(12, 35, [0, 45, 90, 135, 180])
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_duration_too_short() -> None:
    # All 8 sectors, within radius, but only 20 min (< 30) → isolates duration.
    rows = _holding_rows(12, 20, [0, 45, 90, 135, 180, 225, 270, 315])
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_too_few_snapshots() -> None:
    # All gates pass except < 10 snapshots → isolates the snapshot gate.
    rows = _holding_rows(6, 35, [0, 45, 90, 135, 180, 225, 270, 315])
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_just_outside_hold_radius() -> None:
    # 18 nm — just outside the tuned 15 nm radius; everything else passes.
    rows = _holding_rows(12, 35, [0, 45, 90, 135, 180, 225, 270, 315], distance_nm=18.0)
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []


def test_negative_too_high_altitude() -> None:
    rows = _holding_rows(12, 35, [0, 45, 90, 135, 180, 225, 270, 315], altitude_ft=33_000)
    assert RULE.detect(make_positions(rows), {}, empty_cases(), BASELINE) == []
