"""Unit tests for ``AfmApiClient`` using respx to mock httpx transport."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx
import structlog
from pydantic import ValidationError

from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import (
    FlightDetail,
    PositionsLiveResponse,
    SiteDetail,
    SiteFlightListResponse,
    SiteListResponse,
    SiteSla,
    TrailResponse,
)
from afm_foundry_sync.settings import FoundrySettings

# ---------------------------------------------------------------------------
# Sample payloads (canonical shapes from docs/API.md, trimmed where allowed)
# ---------------------------------------------------------------------------

_POSITION: dict[str, Any] = {
    "icao24": "a12345",
    "callsign": "UAL1234",
    "lat": 37.62,
    "lon": -122.37,
    "altitude_ft": 12000,
    "speed_kt": 300,
    "heading_deg": 270,
    "vertical_rate_fpm": 0,
    "on_ground": False,
    "customer_region": "west",
    "last_seen_at": "2026-05-15T12:00:00Z",
    "staleness": "fresh",
}

_POSITIONS_LIVE: dict[str, Any] = {
    "items": [_POSITION],
    "count": 1,
    "server_time": "2026-05-15T12:00:01Z",
    "pipeline_lag_seconds": 5,
}

_FLIGHT_DETAIL: dict[str, Any] = {
    "icao24": "a12345",
    "callsign": "UAL1234",
    "registration": "N12345",
    "aircraft_type": "B738",
    "operator_icao": "UAL",
    "origin_icao": None,
    "destination_icao": None,
    "customer_region": "west",
    "position": _POSITION,
    "eta_minutes": None,
    "status_timeline": [],
    "open_case_ids": [],
}

_TRAIL: dict[str, Any] = {
    "icao24": "a12345",
    "points": [
        {
            "ts": "2026-05-15T11:30:00Z",
            "lat": 37.5,
            "lon": -122.3,
            "altitude_ft": 10000,
            "speed_kt": 280,
        }
    ],
    "lookback": "2h",
    "point_count": 1,
}

_SITE_LIST: dict[str, Any] = {
    "items": [
        {
            "icao": "KSFO",
            "iata": "SFO",
            "name": "San Francisco Intl",
            "state": "CA",
            "customer_regions": ["west"],
            "is_in_scope": True,
        }
    ],
    "count": 1,
}

_SITE_DETAIL: dict[str, Any] = {
    "icao": "KSFO",
    "iata": "SFO",
    "name": "San Francisco Intl",
    "city": "San Francisco",
    "state": "CA",
    "lat": 37.6188,
    "lon": -122.3754,
    "elevation_ft": 13,
    "timezone": None,
    "weather": None,
    "inbound_count_60m": 4,
    "outbound_count_60m": 2,
    "active_case_count": 0,
    "customer_regions": ["west"],
}

_SITE_SLA: dict[str, Any] = {
    "icao": "KSFO",
    "period": "last_24h",
    "inbound_count": 0,
    "outbound_count": 0,
    "on_time_arrival_pct": None,
    "on_time_departure_pct": None,
    "avg_arrival_delay_min": None,
    "avg_departure_delay_min": None,
    "weather_impact": "low",
    "flight_category": "VFR",
    "active_cases": 0,
    "sparkline_7d": [],
}

_SITE_FLIGHTS: dict[str, Any] = {
    "items": [
        {
            "icao24": "a12345",
            "callsign": "UAL1234",
            "origin_icao": None,
            "destination_icao": None,
            "eta_minutes": None,
            "status": "unknown",
            "aircraft_type": "B738",
        }
    ],
    "count": 1,
}


# ---------------------------------------------------------------------------
# Happy-path coverage: one test per endpoint
# ---------------------------------------------------------------------------


@respx.mock
async def test_fetch_positions_live(settings: FoundrySettings) -> None:
    route = respx.get("http://api.test/v1/positions/live").mock(
        return_value=httpx.Response(200, json=_POSITIONS_LIVE)
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_positions_live()
    assert route.called
    assert isinstance(result, PositionsLiveResponse)
    assert result.count == 1
    assert result.items[0].icao24 == "a12345"
    assert result.pipeline_lag_seconds == 5
    assert result.truncated is False


@respx.mock
async def test_fetch_positions_live_truncated_logs_warning(settings: FoundrySettings) -> None:
    """A truncated upstream snapshot passes the flag through and is
    surfaced as a WARNING — the derived tenant sync is incomplete."""
    respx.get("http://api.test/v1/positions/live").mock(
        return_value=httpx.Response(200, json={**_POSITIONS_LIVE, "truncated": True})
    )
    with structlog.testing.capture_logs() as logs:
        async with AfmApiClient(settings) as client:
            result = await client.fetch_positions_live()
    assert result.truncated is True
    assert any(
        e["event"] == "positions_live_truncated_upstream" and e["log_level"] == "warning"
        for e in logs
    )


@respx.mock
async def test_fetch_flight(settings: FoundrySettings) -> None:
    respx.get("http://api.test/v1/flights/a12345").mock(
        return_value=httpx.Response(200, json=_FLIGHT_DETAIL)
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_flight("a12345")
    assert isinstance(result, FlightDetail)
    assert result.registration == "N12345"
    assert result.position.icao24 == "a12345"


@respx.mock
async def test_fetch_flight_trail_passes_lookback(settings: FoundrySettings) -> None:
    route = respx.get("http://api.test/v1/flights/a12345/trail", params={"lookback": "4h"}).mock(
        return_value=httpx.Response(200, json={**_TRAIL, "lookback": "4h"})
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_flight_trail("a12345", lookback="4h")
    assert route.called
    assert isinstance(result, TrailResponse)
    assert result.lookback == "4h"
    assert result.point_count == 1


@respx.mock
async def test_stream_flight_trails_yields_one_response_per_ndjson_line(
    settings: FoundrySettings,
) -> None:
    # The batched bulk path: POST /v1/flights/trail/batch, body carries the
    # icao24 set + lookback; the server streams one TrailResponse per NDJSON
    # line and the client yields each parsed.
    line_a = json.dumps({**_TRAIL, "icao24": "a12345"})
    line_b = json.dumps({**_TRAIL, "icao24": "b67890", "point_count": 1})
    route = respx.post("http://api.test/v1/flights/trail/batch").mock(
        return_value=httpx.Response(
            200,
            content=(line_a + "\n" + line_b + "\n").encode(),
            headers={"content-type": "application/x-ndjson"},
        )
    )
    async with AfmApiClient(settings) as client:
        got = [t async for t in client.stream_flight_trails(["a12345", "b67890"], "2h")]

    assert route.called
    sent = json.loads(route.calls.last.request.content)
    assert sent == {"icao24s": ["a12345", "b67890"], "lookback": "2h"}
    assert [t.icao24 for t in got] == ["a12345", "b67890"]
    assert all(isinstance(t, TrailResponse) for t in got)
    assert got[0].points[0].lat == 37.5


@respx.mock
async def test_stream_flight_trails_empty_stream_yields_nothing(
    settings: FoundrySettings,
) -> None:
    # No icao24 had positions in the window → empty body → no yields (the
    # caller treats every requested icao24 as an empty trail).
    respx.post("http://api.test/v1/flights/trail/batch").mock(
        return_value=httpx.Response(
            200, content=b"", headers={"content-type": "application/x-ndjson"}
        )
    )
    async with AfmApiClient(settings) as client:
        got = [t async for t in client.stream_flight_trails(["zzzzzz"])]
    assert got == []


@respx.mock
async def test_fetch_sites(settings: FoundrySettings) -> None:
    respx.get("http://api.test/v1/sites").mock(return_value=httpx.Response(200, json=_SITE_LIST))
    async with AfmApiClient(settings) as client:
        result = await client.fetch_sites()
    assert isinstance(result, SiteListResponse)
    assert result.items[0].icao == "KSFO"


@respx.mock
async def test_fetch_site(settings: FoundrySettings) -> None:
    respx.get("http://api.test/v1/sites/KSFO").mock(
        return_value=httpx.Response(200, json=_SITE_DETAIL)
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_site("KSFO")
    assert isinstance(result, SiteDetail)
    assert result.inbound_count_60m == 4


@respx.mock
async def test_fetch_site_sla_passes_period(settings: FoundrySettings) -> None:
    route = respx.get("http://api.test/v1/sites/KSFO/sla", params={"period": "last_7d"}).mock(
        return_value=httpx.Response(200, json={**_SITE_SLA, "period": "last_7d"})
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_site_sla("KSFO", period="last_7d")
    assert route.called
    assert isinstance(result, SiteSla)
    assert result.period == "last_7d"


@respx.mock
async def test_fetch_site_flights_inbound(settings: FoundrySettings) -> None:
    respx.get("http://api.test/v1/sites/KSFO/inbound").mock(
        return_value=httpx.Response(200, json=_SITE_FLIGHTS)
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_site_flights("KSFO", direction="inbound")
    assert isinstance(result, SiteFlightListResponse)
    assert result.items[0].status == "unknown"


# ---------------------------------------------------------------------------
# Retry behavior
# ---------------------------------------------------------------------------


@respx.mock
async def test_retries_on_503_then_succeeds(settings: FoundrySettings) -> None:
    route = respx.get("http://api.test/v1/positions/live").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(200, json=_POSITIONS_LIVE),
        ]
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_positions_live()
    assert route.call_count == 2
    assert result.count == 1


@respx.mock
async def test_retries_on_transport_error_then_succeeds(
    settings: FoundrySettings,
) -> None:
    route = respx.get("http://api.test/v1/positions/live").mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200, json=_POSITIONS_LIVE),
        ]
    )
    async with AfmApiClient(settings) as client:
        result = await client.fetch_positions_live()
    assert route.call_count == 2
    assert result.count == 1


@respx.mock
async def test_does_not_retry_on_404(settings: FoundrySettings) -> None:
    route = respx.get("http://api.test/v1/flights/missing").mock(
        return_value=httpx.Response(404, json={"error": "not found"})
    )
    async with AfmApiClient(settings) as client:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.fetch_flight("missing")
    assert exc_info.value.response.status_code == 404
    assert route.call_count == 1


@respx.mock
async def test_exhausts_retries_on_persistent_503(
    settings: FoundrySettings,
) -> None:
    route = respx.get("http://api.test/v1/positions/live").mock(return_value=httpx.Response(503))
    async with AfmApiClient(settings) as client:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.fetch_positions_live()
    assert exc_info.value.response.status_code == 503
    assert route.call_count == 3


# ---------------------------------------------------------------------------
# Validation: malformed payload → pydantic ValidationError (no retry)
# ---------------------------------------------------------------------------


@respx.mock
async def test_validation_error_on_malformed_payload(
    settings: FoundrySettings,
) -> None:
    route = respx.get("http://api.test/v1/positions/live").mock(
        return_value=httpx.Response(200, json={"items": "not a list", "count": 0})
    )
    async with AfmApiClient(settings) as client:
        with pytest.raises(ValidationError):
            await client.fetch_positions_live()
    assert route.call_count == 1
