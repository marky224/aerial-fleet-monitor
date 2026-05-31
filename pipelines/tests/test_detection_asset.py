"""Tests for the case_detector orchestration + its pure helpers.

The pure pieces (nearest-site, enrichment, detect+dedup) are tested
directly. ``run_case_detection`` is exercised with the Postgres/lakehouse
I/O stubbed at the module seam so the detect -> insert flow is verified
without a DB.
"""

from __future__ import annotations

import math
from types import SimpleNamespace

import pandas as pd
from dagster import MaterializeResult, build_asset_context

from pipelines.assets import detection
from pipelines.rules import Anomaly
from pipelines.services.baseline_provider import HeuristicBaselineProvider
from pipelines.tests.rule_helpers import NOW, empty_cases, make_positions, mins

WATCHED = {"KSFO": (37.6189, -122.3750), "KJFK": (40.6398, -73.7789)}


def _empty_flight_plans() -> pd.DataFrame:
    return pd.DataFrame(columns=["icao24", "origin_icao", "destination_icao", "departure_time"])


# -- _nearest_sites -------------------------------------------------------


def test_nearest_site_picks_closest_watched_airport() -> None:
    # A point right at SFO and one at JFK.
    lats = pd.Series([37.62, 40.64])
    lons = pd.Series([-122.37, -73.78])
    icaos, dists = detection._nearest_sites(lats, lons, WATCHED)
    assert icaos == ["KSFO", "KJFK"]
    assert dists[0] is not None and dists[0] < 5  # within a few nm
    assert dists[1] is not None and dists[1] < 5


def test_nearest_site_nan_position_yields_none() -> None:
    lats = pd.Series([math.nan])
    lons = pd.Series([math.nan])
    icaos, dists = detection._nearest_sites(lats, lons, WATCHED)
    assert icaos == [None]
    assert dists == [None]


def test_nearest_site_no_watched_airports() -> None:
    lats = pd.Series([37.0, 38.0])
    lons = pd.Series([-122.0, -121.0])
    icaos, dists = detection._nearest_sites(lats, lons, {})
    assert icaos == [None, None]
    assert dists == [None, None]


# -- enrich_positions -----------------------------------------------------


def test_enrich_joins_flight_plans_and_adds_site_columns() -> None:
    positions = make_positions(
        [{"icao24": "abc123", "lat": 37.62, "lon": -122.37, "ts_polled": NOW}]
    )
    flight_plans = pd.DataFrame(
        [
            {
                "icao24": "abc123",
                "origin_icao": "KJFK",
                "destination_icao": "KSFO",
                "departure_time": NOW - mins(120),
            }
        ]
    )
    out = detection.enrich_positions(positions, flight_plans, WATCHED)
    row = out.iloc[0]
    assert row["origin_icao"] == "KJFK"
    assert row["destination_icao"] == "KSFO"
    assert row["nearest_site_icao"] == "KSFO"
    assert row["nearest_site_distance_nm"] < 5


def test_enrich_with_empty_flight_plans_sets_null_plan_columns() -> None:
    positions = make_positions([{"icao24": "abc123", "lat": 37.62, "lon": -122.37}])
    out = detection.enrich_positions(positions, _empty_flight_plans(), WATCHED)
    row = out.iloc[0]
    assert row["origin_icao"] is None
    assert row["destination_icao"] is None
    assert row["nearest_site_icao"] == "KSFO"


# -- detect_and_dedup -----------------------------------------------------


def test_detect_and_dedup_runs_rules_and_suppresses_existing() -> None:
    positions = make_positions(
        [
            # 32k (not 38k) so the lost_signal severity gradation stays
            # above the skip-on-low floor — see pipelines/rules/lost_signal.py.
            {"icao24": "lost01", "altitude_ft": 32_000, "ts_polled": NOW - mins(14.5)},
            {"icao24": "live01", "ts_polled": NOW},
        ]
    )
    enriched = detection.enrich_positions(positions, _empty_flight_plans(), {})
    baseline = HeuristicBaselineProvider({})
    now = enriched["ts_polled"].max()

    anomalies = detection.detect_and_dedup(
        enriched, {}, empty_cases(), baseline, detection.ALL_RULES, now
    )
    assert [a.rule for a in anomalies] == ["lost_signal"]
    assert anomalies[0].icao24 == "lost01"


# -- run_case_detection (I/O stubbed) -------------------------------------


def _stub_io(monkeypatch, *, watched=None, flight_plans=None, weather=None, existing=None) -> list:
    inserted: list[tuple[str, str]] = []
    ids = (f"CASE-2026-{n:06d}" for n in range(1, 1000))
    monkeypatch.setattr(
        detection, "_load_flight_plans", lambda _pg: flight_plans or _empty_flight_plans()
    )
    monkeypatch.setattr(detection, "_load_watched_coords", lambda _pg: watched or {})
    monkeypatch.setattr(detection, "_load_weather", lambda _pg: weather or {})
    monkeypatch.setattr(
        detection,
        "_load_open_cases",
        lambda _pg: existing if existing is not None else empty_cases(),
    )
    monkeypatch.setattr(detection, "load_airport_coords", lambda _pg: {})
    monkeypatch.setattr(detection, "_next_case_id", lambda _pg: next(ids))
    monkeypatch.setattr(detection, "_load_runbook_refs", lambda _pg, _ct: [])
    monkeypatch.setattr(
        detection, "_insert_case", lambda _pg, a, cid, _rb: inserted.append((cid, a.rule))
    )
    return inserted


def test_run_case_detection_creates_a_case(monkeypatch) -> None:
    positions = make_positions(
        [
            {
                "icao24": "lost01",
                # 32k → lost_signal severity stays "medium", above skip-on-low.
                "altitude_ft": 32_000,
                "customer_region": "west",
                "ts_polled": NOW - mins(14.5),
            },
            {"icao24": "live01", "customer_region": "west", "ts_polled": NOW},
        ]
    )
    fake_lake = SimpleNamespace(read_recent_positions=lambda *_a, **_k: positions)
    inserted = _stub_io(monkeypatch)

    result = detection.run_case_detection(build_asset_context(), object(), fake_lake)

    assert result.cases_created == 1
    assert result.by_rule == {"lost_signal": 1}
    assert inserted[0][1] == "lost_signal"
    assert inserted[0][0].startswith("CASE-2026-")


def test_run_case_detection_empty_positions_noop(monkeypatch) -> None:
    empty = make_positions([])
    fake_lake = SimpleNamespace(read_recent_positions=lambda *_a, **_k: empty)
    inserted = _stub_io(monkeypatch)

    result = detection.run_case_detection(build_asset_context(), object(), fake_lake)

    assert result.cases_created == 0
    assert inserted == []


def test_run_case_detection_filters_out_of_scope(monkeypatch) -> None:
    # All traffic out of scope (customer_region None) → nothing detected.
    positions = make_positions(
        [
            {
                "icao24": "oos01",
                "altitude_ft": 38_000,
                "customer_region": None,
                "ts_polled": NOW - mins(5),
            },
            {"icao24": "oos02", "customer_region": None, "ts_polled": NOW},
        ]
    )
    fake_lake = SimpleNamespace(read_recent_positions=lambda *_a, **_k: positions)
    inserted = _stub_io(monkeypatch)

    result = detection.run_case_detection(build_asset_context(), object(), fake_lake)

    assert result.cases_created == 0
    assert inserted == []


def test_asset_wrapper_returns_materializeresult(monkeypatch) -> None:
    monkeypatch.setattr(
        detection,
        "run_case_detection",
        lambda *_a, **_k: detection.DetectionResult(5, 2, 2, {"lost_signal": 2}),
    )
    out = detection.case_detector(
        build_asset_context(),
        postgres=SimpleNamespace(),  # type: ignore[arg-type]
        lakehouse=SimpleNamespace(),  # type: ignore[arg-type]
    )
    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["cases_created"].value == 2
    assert md["rule.lost_signal"].value == 2


# -- _insert_case SF-push gate --------------------------------------------


class _RecordingCursor:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple) -> None:
        self.calls.append((sql, params))


class _RecordingConn:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    def __enter__(self) -> _RecordingConn:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self.calls)

    def commit(self) -> None:
        pass


class _RecordingPostgres:
    def __init__(self) -> None:
        self.calls: list = []

    def get_conn(self) -> _RecordingConn:
        return _RecordingConn(self.calls)


def test_insert_case_gates_sf_push_to_high_severity() -> None:
    """SF_PUSH_SEVERITIES cases are queued ('pending') for the SF Task;
    everything else is 'skipped' (local-only). Currently {high}; medium/
    low/None fall through to 'skipped'. The sf_sync_status is the last
    bound param of the insert_case statement."""
    for severity, expected in [
        ("high", "pending"),
        ("medium", "skipped"),
        ("low", "skipped"),
        (None, "skipped"),
    ]:
        pg = _RecordingPostgres()
        anomaly = Anomaly(
            rule="lost_signal",
            icao24="abc123",
            site_icao="KSFO",
            customer_region="west",
            detection_facts={"gap_minutes": 16.0},
            severity_hint=severity,
        )
        detection._insert_case(pg, anomaly, "CASE-TEST-1", ["lost-signal-cruise"])  # type: ignore[arg-type]
        insert_sql, params = pg.calls[0]
        assert "INSERT INTO app.cases" in insert_sql
        assert params[-1] == expected, f"severity={severity!r} -> {params[-1]!r}, want {expected!r}"
