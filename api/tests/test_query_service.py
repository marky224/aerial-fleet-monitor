"""QueryService unit tests with mocked PostgresPool + LakehouseQuery.

Covers each method's happy path plus the error/scope branches:
  - list_live_positions: bbox filter, region scope-violation, empty result,
    safety-ceiling truncation flag
  - get_flight: NotFoundError, ScopeViolation, joined-registry happy path
  - get_flight_trail: interval mapping, scope check via current_positions
  - list_sites: is_in_scope projection, region scope-violation
  - get_site: NotFoundError, ScopeViolation, weather composition
  - get_site_sla: weather_impact derivation
  - list_inbound / list_outbound: flow summaries

Smoke verified the SQL is correct end-to-end; these tests defend the
Python-side logic around it: scope enforcement, parameter wiring,
derived-field composition, and response-shape construction.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.exceptions import NotFoundError, ScopeViolation
from app.models.common import Scope
from app.services import query_service as qs
from app.services.query_service import LIVE_POSITION_CEILING, QueryService


def _position_row(
    icao24: str = "a2024b",
    customer_region: str | None = "east",
    last_seen_at: datetime | None = None,
) -> dict:  # type: ignore[type-arg]
    """Minimal current_positions row matching the SELECT in list_live_positions."""
    return {
        "icao24": icao24,
        "callsign": "N22889",
        "lat": 37.6,
        "lon": -122.4,
        "altitude_ft": 17025,
        "speed_kt": 259,
        "heading_deg": 356,
        "vertical_rate_fpm": 0,
        "on_ground": False,
        "customer_region": customer_region,
        "last_seen_at": last_seen_at or datetime.now(UTC),
    }


# === list_live_positions ===


def test_list_live_positions_happy_path(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = [_position_row(), _position_row(icao24="abc123")]

    result = query_service.list_live_positions(scope=internal_scope)

    assert result.count == 2
    assert result.items[0].icao24 == "a2024b"
    assert result.items[0].staleness in ("fresh", "stale", "lost")


def test_list_live_positions_bbox_adds_params(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []
    query_service.list_live_positions(scope=internal_scope, bbox=(32.0, -125.0, 49.0, -114.0))
    _, params = mock_postgres.fetchall.call_args[0]
    assert params["lat_min"] == 32.0
    assert params["lat_max"] == 49.0
    assert params["lon_min"] == -125.0
    assert params["lon_max"] == -114.0


def test_list_live_positions_bounds_by_recency_window(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """The 'currently airborne' contract (API.md §3.1) requires a recency
    filter — current_positions is an eviction-free last-known store, so
    without this the endpoint returns long-landed traffic."""
    mock_postgres.fetchall.return_value = []
    query_service.list_live_positions(scope=internal_scope)
    sql, _ = mock_postgres.fetchall.call_args[0]
    assert "last_seen_at >= NOW() - INTERVAL '15 minutes'" in sql


def test_list_live_positions_region_override_rejected_for_narrow_scope(
    query_service: QueryService, west_scope: Scope
) -> None:
    with pytest.raises(ScopeViolation, match="cannot request region"):
        query_service.list_live_positions(scope=west_scope, region="east")


def test_list_live_positions_empty_result(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []
    result = query_service.list_live_positions(scope=internal_scope)
    assert result.count == 0


def test_list_live_positions_not_truncated_by_default(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """A normal-sized result is never flagged truncated, and the probe
    LIMIT is ceiling + 1 (one row past the ceiling, to detect a clip)."""
    mock_postgres.fetchall.return_value = [_position_row()]
    result = query_service.list_live_positions(scope=internal_scope)
    assert result.truncated is False
    _, params = mock_postgres.fetchall.call_args[0]
    assert params["ceiling_probe"] == LIVE_POSITION_CEILING + 1


def test_list_live_positions_truncated_when_ceiling_exceeded(
    query_service: QueryService,
    mock_postgres: MagicMock,
    internal_scope: Scope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One row past the ceiling → clipped to the ceiling, the *oldest*
    row dropped (SELECT is ORDER BY last_seen_at DESC), truncated=True."""
    monkeypatch.setattr(qs, "LIVE_POSITION_CEILING", 3)
    now = datetime.now(UTC)
    # 4 rows, newest → oldest, as the DESC-ordered query would return them.
    mock_postgres.fetchall.return_value = [
        _position_row(icao24=f"a0000{i}", last_seen_at=now - timedelta(seconds=i)) for i in range(4)
    ]

    result = query_service.list_live_positions(scope=internal_scope)

    assert result.truncated is True
    assert result.count == 3
    assert [p.icao24 for p in result.items] == ["a00000", "a00001", "a00002"]
    _, params = mock_postgres.fetchall.call_args[0]
    assert params["ceiling_probe"] == 4
    assert result.pipeline_lag_seconds == 0


# === get_flight ===


def test_get_flight_happy_path(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    row = _position_row()
    row.update(
        {
            "origin_icao": None,
            "destination_icao": None,
            "aircraft_type": "B190",
            "registration": "N22889",
            "operator_icao": None,
        }
    )
    mock_postgres.fetchone.return_value = row

    result = query_service.get_flight(scope=internal_scope, icao24="A2024B")

    assert result.icao24 == "a2024b"
    assert result.registration == "N22889"
    assert result.aircraft_type == "B190"
    assert result.eta_minutes is None
    assert result.status_timeline == []


def test_get_flight_not_found(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchone.return_value = None
    with pytest.raises(NotFoundError, match="not seen in the last 30 minutes"):
        query_service.get_flight(scope=internal_scope, icao24="deadbe")


def test_get_flight_scope_violation(
    query_service: QueryService, mock_postgres: MagicMock, west_scope: Scope
) -> None:
    row = _position_row(customer_region="east")
    row.update(
        {
            "origin_icao": None,
            "destination_icao": None,
            "aircraft_type": "B190",
            "registration": None,
            "operator_icao": None,
        }
    )
    mock_postgres.fetchone.return_value = row
    with pytest.raises(ScopeViolation, match="region 'east'"):
        query_service.get_flight(scope=west_scope, icao24="a2024b")


# === get_flight_trail ===


def test_get_flight_trail_uses_correct_interval(
    query_service: QueryService,
    mock_postgres: MagicMock,
    mock_lakehouse: MagicMock,
    internal_scope: Scope,
) -> None:
    mock_postgres.fetchone.return_value = None  # no scope check
    mock_lakehouse.query.return_value = [
        {"ts": datetime.now(UTC), "lat": 37.6, "lon": -122.4, "altitude_ft": 17000, "speed_kt": 250}
    ]

    result = query_service.get_flight_trail(scope=internal_scope, icao24="A2024B", lookback="4h")

    assert result.point_count == 1
    assert result.lookback == "4h"
    sql_arg = mock_lakehouse.query.call_args[0][0]
    assert "'4 hours'" in sql_arg


def test_get_flight_trail_since_takeoff_caps_at_6h(
    query_service: QueryService,
    mock_postgres: MagicMock,
    mock_lakehouse: MagicMock,
    internal_scope: Scope,
) -> None:
    mock_postgres.fetchone.return_value = None
    mock_lakehouse.query.return_value = []
    query_service.get_flight_trail(scope=internal_scope, icao24="a2024b", lookback="since_takeoff")
    sql_arg = mock_lakehouse.query.call_args[0][0]
    assert "'6 hours'" in sql_arg


# === get_flight_trails_batch ===


def test_get_flight_trails_batch_groups_one_response_per_icao24(
    query_service: QueryService,
    mock_postgres: MagicMock,
    mock_lakehouse: MagicMock,
    internal_scope: Scope,
) -> None:
    mock_postgres.fetchall.return_value = []  # none in current_positions → all allowed
    now = datetime.now(UTC)
    # One ordered scan, two aircraft contiguous (ORDER BY icao24, ts_polled).
    mock_lakehouse.query_stream.return_value = iter(
        [
            {
                "icao24": "aaa111",
                "ts": now,
                "lat": 1.0,
                "lon": 2.0,
                "altitude_ft": 100,
                "speed_kt": 9,
            },
            {
                "icao24": "aaa111",
                "ts": now,
                "lat": 1.1,
                "lon": 2.1,
                "altitude_ft": 110,
                "speed_kt": 9,
            },
            {
                "icao24": "bbb222",
                "ts": now,
                "lat": 3.0,
                "lon": 4.0,
                "altitude_ft": 200,
                "speed_kt": 8,
            },
        ]
    )

    out = list(
        query_service.get_flight_trails_batch(
            scope=internal_scope, icao24s=["aaa111", "bbb222"], lookback="4h"
        )
    )

    assert [(t.icao24, t.point_count) for t in out] == [("aaa111", 2), ("bbb222", 1)]
    assert all(t.lookback == "4h" for t in out)
    sql_arg = mock_lakehouse.query_stream.call_args[0][0]
    assert "list_contains($icao24s, icao24)" in sql_arg
    assert "'4 hours'" in sql_arg
    assert mock_lakehouse.query_stream.call_args.kwargs["icao24s"] == ["aaa111", "bbb222"]


def test_get_flight_trails_batch_filters_out_of_scope_not_raises(
    query_service: QueryService,
    mock_postgres: MagicMock,
    mock_lakehouse: MagicMock,
    west_scope: Scope,
) -> None:
    # 'east1' is east-region → filtered for a west scope (no 403); 'westy1'
    # is west → kept; 'agedo1' absent from current_positions → allowed.
    mock_postgres.fetchall.return_value = [
        {"icao24": "east1", "customer_region": "east"},
        {"icao24": "westy1", "customer_region": "west"},
    ]
    mock_lakehouse.query_stream.return_value = iter(
        [
            {
                "icao24": "westy1",
                "ts": datetime.now(UTC),
                "lat": 1.0,
                "lon": 2.0,
                "altitude_ft": None,
                "speed_kt": None,
            }
        ]
    )

    out = list(
        query_service.get_flight_trails_batch(
            scope=west_scope, icao24s=["east1", "westy1", "agedo1"], lookback="2h"
        )
    )

    assert [t.icao24 for t in out] == ["westy1"]
    # The scan is asked only for the in-scope/allowed subset.
    assert sorted(mock_lakehouse.query_stream.call_args.kwargs["icao24s"]) == ["agedo1", "westy1"]


def test_get_flight_trails_batch_no_allowed_skips_scan_entirely(
    query_service: QueryService,
    mock_postgres: MagicMock,
    mock_lakehouse: MagicMock,
    west_scope: Scope,
) -> None:
    mock_postgres.fetchall.return_value = [{"icao24": "east1", "customer_region": "east"}]
    out = list(
        query_service.get_flight_trails_batch(scope=west_scope, icao24s=["east1"], lookback="2h")
    )
    assert out == []
    mock_lakehouse.query_stream.assert_not_called()


# === list_sites ===


def test_list_sites_projects_is_in_scope(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = [
        {
            "icao": "KSFO",
            "iata": "SFO",
            "name": "San Francisco International",
            "state": "CA",
            "customer_regions": ["west"],
        },
        {
            "icao": "KJFK",
            "iata": "JFK",
            "name": "John F Kennedy International",
            "state": "NY",
            "customer_regions": ["east"],
        },
    ]
    result = query_service.list_sites(scope=internal_scope)
    assert result.count == 2
    # region='all' grants in-scope to everything
    assert all(item.is_in_scope for item in result.items)


def test_list_sites_region_override_rejected_for_narrow_scope(
    query_service: QueryService, west_scope: Scope
) -> None:
    with pytest.raises(ScopeViolation):
        query_service.list_sites(scope=west_scope, region="east")


# === get_site ===


def test_get_site_scope_violation_for_out_of_scope_icao(
    query_service: QueryService, west_scope: Scope
) -> None:
    # west_scope has sites_in_scope=['KSFO', 'KLAX']; KJFK is out.
    with pytest.raises(ScopeViolation, match="KJFK"):
        query_service.get_site(scope=west_scope, icao="kjfk")


def test_get_site_not_found(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchone.return_value = None
    with pytest.raises(NotFoundError, match="not found or not watched"):
        query_service.get_site(scope=internal_scope, icao="KZZZ")


def test_get_site_happy_path_with_weather(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    detail_row = {
        "icao": "KSFO",
        "iata": "SFO",
        "name": "San Francisco International",
        "city": "San Francisco",
        "state": "CA",
        "lat": 37.61981,
        "lon": -122.37482,
        "elevation_ft": 13,
        "timezone": None,
        "customer_regions": ["west"],
        "metar_raw": "METAR KSFO 141656Z 06003KT 10SM FEW026 16/11 A3005",
        "flight_category": "VFR",
        "wind_kt": 3,
        "visibility_sm": 10.0,
        "ceiling_ft": None,
        "metar_observed_at": datetime.now(UTC),
    }
    # get_site -> fetchone (detail), then _count_flights twice for inbound/outbound
    mock_postgres.fetchone.side_effect = [detail_row, {"cnt": 5}, {"cnt": 3}]

    result = query_service.get_site(scope=internal_scope, icao="ksfo")

    assert result.icao == "KSFO"
    assert result.weather is not None
    assert result.weather.flight_category == "VFR"
    assert result.inbound_count_60m == 5
    assert result.outbound_count_60m == 3
    assert result.active_case_count == 0  # Phase 05 populates


# === get_site_sla ===


def test_get_site_sla_weather_impact_from_lifr(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchone.return_value = {"flight_category": "LIFR"}
    result = query_service.get_site_sla(scope=internal_scope, icao="KSFO", period="last_24h")
    assert result.weather_impact == "high"
    assert result.flight_category == "LIFR"
    assert result.on_time_arrival_pct is None  # Phase 05 populates


# === list_inbound / list_outbound ===


def test_list_inbound_returns_flight_summaries(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = [
        {
            "icao24": "a2024b",
            "callsign": "N22889",
            "origin_icao": "KLAX",
            "destination_icao": "KSFO",
            "aircraft_type": "B190",
        }
    ]
    result = query_service.list_inbound(scope=internal_scope, icao="KSFO")
    assert result.count == 1
    assert result.items[0].icao24 == "a2024b"
    assert result.items[0].destination_icao == "KSFO"
