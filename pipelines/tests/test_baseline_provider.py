"""Unit tests for the BaselineProvider abstraction + heuristic impl.

Covers the great-circle math (sanity-checked against a known city pair),
the None-returning edge cases the ``delay`` rule relies on, and the
env-driven factory.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from pipelines.services.baseline_provider import (
    HeuristicBaselineProvider,
    _haversine_nm,
    build_baseline_provider,
)

# A small fixture of real US airport coordinates (lat, lon).
COORDS: dict[str, tuple[float, float]] = {
    "KSFO": (37.6189, -122.3750),
    "KJFK": (40.6398, -73.7789),
    "KLAX": (33.9425, -118.4081),
    "KSEA": (47.4490, -122.3093),
}


def test_haversine_known_distance_sfo_jfk() -> None:
    # SFO→JFK great-circle is ~2,250 nm. Allow a generous tolerance.
    nm = _haversine_nm(*COORDS["KSFO"], *COORDS["KJFK"])
    assert 2100 < nm < 2400


def test_expected_duration_sfo_jfk_is_reasonable() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    dur = provider.expected_duration("KSFO", "KJFK")
    assert dur is not None
    # ~2,250 nm / 450 kt = ~5.0 h + 35 min overhead ≈ 5.5 h. Bracket it.
    assert timedelta(hours=5) < dur < timedelta(hours=6)


def test_unknown_origin_returns_none() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    assert provider.expected_duration("KZZZ", "KJFK") is None


def test_unknown_destination_returns_none() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    assert provider.expected_duration("KSFO", "KZZZ") is None


def test_same_airport_returns_none() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    assert provider.expected_duration("KSFO", "KSFO") is None


def test_empty_icao_returns_none() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    assert provider.expected_duration("", "KJFK") is None
    assert provider.expected_duration("KSFO", "") is None


def test_longer_route_has_longer_duration() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    short = provider.expected_duration("KSFO", "KLAX")  # ~300 nm
    long = provider.expected_duration("KSFO", "KJFK")  # ~2,250 nm
    assert short is not None and long is not None
    assert short < long


def test_custom_cruise_speed_changes_duration() -> None:
    fast = HeuristicBaselineProvider(COORDS, cruise_speed_kt=600.0)
    slow = HeuristicBaselineProvider(COORDS, cruise_speed_kt=300.0)
    d_fast = fast.expected_duration("KSFO", "KJFK")
    d_slow = slow.expected_duration("KSFO", "KJFK")
    assert d_fast is not None and d_slow is not None
    assert d_fast < d_slow


def test_aircraft_type_param_is_accepted_and_ignored() -> None:
    provider = HeuristicBaselineProvider(COORDS)
    with_type = provider.expected_duration("KSFO", "KJFK", aircraft_type="B738")
    without = provider.expected_duration("KSFO", "KJFK")
    assert with_type == without


def test_factory_defaults_to_heuristic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BASELINE_PROVIDER", raising=False)
    provider = build_baseline_provider(COORDS)
    assert isinstance(provider, HeuristicBaselineProvider)


def test_factory_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BASELINE_PROVIDER", "heuristic")
    provider = build_baseline_provider(COORDS)
    assert isinstance(provider, HeuristicBaselineProvider)


def test_factory_explicit_arg_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BASELINE_PROVIDER", "opensky")
    # Explicit arg wins over the env var.
    provider = build_baseline_provider(COORDS, provider_name="heuristic")
    assert isinstance(provider, HeuristicBaselineProvider)


def test_factory_unimplemented_provider_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BASELINE_PROVIDER", raising=False)
    with pytest.raises(NotImplementedError, match="opensky"):
        build_baseline_provider(COORDS, provider_name="opensky")
    with pytest.raises(NotImplementedError, match="parquet"):
        build_baseline_provider(COORDS, provider_name="parquet")
