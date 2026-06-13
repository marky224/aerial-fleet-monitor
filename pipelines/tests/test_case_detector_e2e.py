"""End-to-end detector tests over recorded OpenSky fixtures (Phase 10).

Replays the committed OpenSky ``/states/all`` snapshots in
``pipelines/tests/fixtures/opensky/`` — built from a real CONUS capture via
``scripts/record_opensky_fixture.py`` with icao24s anonymised — through the
REAL pipeline: OpenSky parse (``OpenSkyResource._row_to_state``) → ingestion
transform (``_convert_states_to_rows``, including watched-airport region
inference) → a temporary Parquet lakehouse → ``run_case_detection``. Only the
Postgres I/O seam is stubbed (no database), exactly as ``test_detection_asset``
does; everything between the OpenSky JSON and the rule engine runs for real.

Each fixture is a list of ticks carrying an ``offset_minutes`` relative to
"now", so temporal scenarios (lost_signal) can stage an aircraft going dark
across polls. The detector reads the lakehouse on a wall-clock window, so the
ticks are written relative to ``datetime.now`` and read back in the same call.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dagster import build_asset_context

from pipelines.assets import detection
from pipelines.assets.ingestion import _convert_states_to_rows
from pipelines.resources.lakehouse import LakehouseResource
from pipelines.resources.opensky import OpenSkyResource
from pipelines.resources.postgres import PostgresResource
from pipelines.resources.watchlist import WatchedAirport, WatchlistResource
from pipelines.rules import AirportConditions
from pipelines.tests.rule_helpers import empty_cases

FIXTURES = Path(__file__).parent / "fixtures" / "opensky"

# Watched airports the e2e tags traffic against — the coordinates the fixtures
# were filtered around (KLAX → west, KJFK → east).
_WATCHED = [
    WatchedAirport(icao="KLAX", lat=33.9416, lon=-118.4085, customer_regions=("west",)),
    WatchedAirport(icao="KJFK", lat=40.6398, lon=-73.7789, customer_regions=("east",)),
]
_WATCHED_COORDS = {a.icao: (a.lat, a.lon) for a in _WATCHED}


def _seeded_watchlist() -> WatchlistResource:
    """A real WatchlistResource with its caches pre-seeded (skips the DB read)."""
    wl = WatchlistResource(postgres=PostgresResource(dsn="postgresql://unused/none"))
    wl._airports_cache = list(_WATCHED)
    wl._coords_cache = np.array([[a.lat, a.lon] for a in _WATCHED], dtype=np.float64)
    return wl


def _empty_flight_plans() -> pd.DataFrame:
    return pd.DataFrame(columns=["icao24", "origin_icao", "destination_icao", "departure_time"])


def _states_from_fixture_rows(raw_rows: list[Any]) -> tuple[Any, ...]:
    """Parse raw /states/all rows via the real OpenSky parser (asset's filter)."""
    return tuple(
        OpenSkyResource._row_to_state(r)
        for r in raw_rows
        if isinstance(r, list) and len(r) >= 17 and isinstance(r[0], str) and r[0]
    )


def _replay_fixture(name: str, lakehouse: LakehouseResource, now: datetime) -> int:
    """Replay every tick of a fixture into the lakehouse. Returns the in-scope row count."""
    fixture = json.loads((FIXTURES / f"{name}.json").read_text())
    watchlist = _seeded_watchlist()
    in_scope = 0
    for tick in fixture["ticks"]:
        polled_at = now + timedelta(minutes=float(tick["offset_minutes"]))
        states = _states_from_fixture_rows(tick["states"])
        rows = _convert_states_to_rows(states, polled_at, watchlist)
        in_scope += sum(1 for r in rows if r["customer_region"] is not None)
        if rows:
            lakehouse.write_positions_snapshot(rows, polled_at)
    return in_scope


def _stub_io(monkeypatch, *, weather=None):  # type: ignore[no-untyped-def]
    """Stub run_case_detection's Postgres seam; capture inserted anomalies."""
    inserted: list = []
    ids = (f"CASE-E2E-{n:05d}" for n in range(1, 1_000_000))
    monkeypatch.setattr(detection, "_load_flight_plans", lambda _pg: _empty_flight_plans())
    monkeypatch.setattr(detection, "_load_watched_coords", lambda _pg: dict(_WATCHED_COORDS))
    monkeypatch.setattr(detection, "_load_weather", lambda _pg: weather or {})
    monkeypatch.setattr(detection, "_load_open_cases", lambda _pg: empty_cases())
    monkeypatch.setattr(detection, "load_airport_coords", lambda _pg: {})
    monkeypatch.setattr(detection, "_next_case_id", lambda _pg: next(ids))
    monkeypatch.setattr(detection, "_load_runbook_refs", lambda _pg, _ct: [])
    monkeypatch.setattr(detection, "_insert_case", lambda _pg, a, _cid, _rb: inserted.append(a))
    return inserted


def _run(monkeypatch, lakehouse, *, weather=None):  # type: ignore[no-untyped-def]
    inserted = _stub_io(monkeypatch, weather=weather)
    result = detection.run_case_detection(build_asset_context(), object(), lakehouse)
    return result, inserted


def test_e2e_normal_day_no_false_positives(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    lake = LakehouseResource(lake_path=str(tmp_path))
    in_scope = _replay_fixture("normal_day", lake, datetime.now(UTC))
    assert in_scope > 0  # region inference tagged real near-airport traffic
    result, inserted = _run(monkeypatch, lake)
    assert result.in_scope_aircraft > 0
    assert result.cases_created == 0, f"normal traffic should fire no cases: {result.by_rule}"
    assert inserted == []


def test_e2e_off_peak_no_cases(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    lake = LakehouseResource(lake_path=str(tmp_path))
    _replay_fixture("off_peak", lake, datetime.now(UTC))
    result, inserted = _run(monkeypatch, lake)
    assert result.cases_created == 0
    assert inserted == []


def test_e2e_holiday_traffic_scale_no_cases(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    lake = LakehouseResource(lake_path=str(tmp_path))
    in_scope = _replay_fixture("holiday_traffic", lake, datetime.now(UTC))
    assert in_scope > 0  # exercises the ingestion transform + region inference at scale
    result, _ = _run(monkeypatch, lake)
    assert result.cases_created == 0


def test_e2e_lost_signal_fires(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    lake = LakehouseResource(lake_path=str(tmp_path))
    _replay_fixture("lost_signal_scenario", lake, datetime.now(UTC))
    result, inserted = _run(monkeypatch, lake)
    assert result.by_rule.get("lost_signal", 0) >= 1
    lost = [a for a in inserted if a.rule == "lost_signal"]
    assert any(a.icao24 == "e9ff01" for a in lost), [a.icao24 for a in lost]


def test_e2e_weather_event_fires(tmp_path, monkeypatch):  # type: ignore[no-untyped-def]
    lake = LakehouseResource(lake_path=str(tmp_path))
    fixture = json.loads((FIXTURES / "weather_event.json").read_text())
    sw = fixture["seed_weather"]
    weather = {
        sw["site_icao"]: AirportConditions(
            site_icao=sw["site_icao"],
            flight_category=sw["flight_category"],
            wind_kt=sw["wind_kt"],
            visibility_sm=sw["visibility_sm"],
            ceiling_ft=sw["ceiling_ft"],
        )
    }
    _replay_fixture("weather_event", lake, datetime.now(UTC))
    result, inserted = _run(monkeypatch, lake, weather=weather)
    assert result.by_rule.get("weather_impact", 0) >= 1
    wx = [a for a in inserted if a.rule == "weather_impact"]
    assert wx and wx[0].severity_hint == "high"  # LIFR → high
