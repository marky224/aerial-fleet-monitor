"""Pure-function tests: parse_bbox + QueryService static helpers.

No fixtures, no I/O. Exercises the pieces of business logic that don't
need the QueryService composition.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.exceptions import BadRequest
from app.models.common import FlightCategory
from app.routers.positions import parse_bbox
from app.services.query_service import _compute_staleness, _compute_weather_impact

# === parse_bbox ===


def test_parse_bbox_none_passes_through() -> None:
    assert parse_bbox(bbox=None) is None


def test_parse_bbox_valid_returns_tuple() -> None:
    assert parse_bbox(bbox="32.0,-125.0,49.0,-114.0") == (32.0, -125.0, 49.0, -114.0)


def test_parse_bbox_wrong_count_raises() -> None:
    with pytest.raises(BadRequest, match="4 comma-separated"):
        parse_bbox(bbox="32.0,-125.0,49.0")


def test_parse_bbox_non_numeric_raises() -> None:
    with pytest.raises(BadRequest, match="numeric"):
        parse_bbox(bbox="foo,bar,baz,qux")


def test_parse_bbox_lat_out_of_range_raises() -> None:
    with pytest.raises(BadRequest, match="latitudes"):
        parse_bbox(bbox="-100,-125,49,-114")


def test_parse_bbox_lon_misordered_raises() -> None:
    with pytest.raises(BadRequest, match="longitudes"):
        parse_bbox(bbox="32,-114,49,-125")


# === _compute_staleness ===


@pytest.mark.parametrize(
    ("age_seconds", "expected"),
    [
        (10, "fresh"),
        (59, "fresh"),
        (60, "stale"),
        (250, "stale"),
        (300, "lost"),
        (3600, "lost"),
    ],
)
def test_compute_staleness(age_seconds: int, expected: str) -> None:
    now = datetime(2026, 5, 14, 18, 0, 0, tzinfo=UTC)
    last_seen = now - timedelta(seconds=age_seconds)
    assert _compute_staleness(last_seen, now) == expected


# === _compute_weather_impact ===


@pytest.mark.parametrize(
    ("category", "expected"),
    [
        ("LIFR", "high"),
        ("IFR", "medium"),
        ("MVFR", "low"),
        ("VFR", "low"),
        (None, "low"),
    ],
)
def test_compute_weather_impact(category: FlightCategory | None, expected: str) -> None:
    assert _compute_weather_impact(category) == expected
