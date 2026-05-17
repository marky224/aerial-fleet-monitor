"""Unit tests for the sync orchestration layer.

HTTP is mocked with respx on both sides: the local /v1 API
(``http://api.test``) and the Foundry Action API
(``https://tenant.example.com``). No real tenant or .env is touched —
clients are built from the ``settings`` fixture (conftest).
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
import respx
from pydantic import BaseModel

from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import Position
from afm_foundry_sync.ontology_writers import FoundryWriter
from afm_foundry_sync.settings import FoundrySettings
from afm_foundry_sync.sync_jobs import (
    FoundrySyncSkipped,
    TakeoffDetector,
    _dedupe_latest,
    full_sync_sites,
    guarded_sync,
    incremental_sync_positions,
    synthesize_flight_id,
)

_T0 = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
_T1 = datetime(2026, 5, 15, 12, 0, 30, tzinfo=UTC)

_POS_URL = "http://api.test/v1/positions/live"
_AIRCRAFT_BATCH = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-aircraft/applyBatch"
)
_SITE_BATCH = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-site/applyBatch"
)
_FLIGHT_BATCH = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-flight/applyBatch"
)


def _pos(icao24: str, *, on_ground: bool, seen: datetime, **kw: object) -> Position:
    base = dict(
        icao24=icao24,
        callsign="UAL1",
        lat=37.62,
        lon=-122.37,
        altitude_ft=10000,
        speed_kt=300,
        heading_deg=270,
        vertical_rate_fpm=0,
        on_ground=on_ground,
        customer_region="west",
        last_seen_at=seen,
        staleness="fresh",
    )
    base.update(kw)
    return Position(**base)  # type: ignore[arg-type]


def _positions_payload(positions: list[Position], server_time: datetime) -> dict:
    return {
        "items": [p.model_dump(mode="json") for p in positions],
        "count": len(positions),
        "server_time": server_time.isoformat(),
        "pipeline_lag_seconds": 2,
    }


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_dedupe_latest_keeps_newest_per_icao24() -> None:
    stale = _pos("abc123", on_ground=False, seen=_T0)
    fresh = _pos("abc123", on_ground=False, seen=_T1, callsign="UAL9")
    other = _pos("def456", on_ground=True, seen=_T0)

    result = _dedupe_latest([stale, fresh, other])

    assert len(result) == 2
    by_icao = {p.icao24: p for p in result}
    assert by_icao["abc123"].last_seen_at == _T1
    assert by_icao["abc123"].callsign == "UAL9"


def test_synthesize_flight_id_is_icao_plus_unix_ts() -> None:
    assert synthesize_flight_id("abc123", _T0) == f"abc123-{int(_T0.timestamp())}"


# --------------------------------------------------------------------------- #
# TakeoffDetector
# --------------------------------------------------------------------------- #


def test_first_sighting_seeds_state_no_edge() -> None:
    det = TakeoffDetector()
    assert det.observe([_pos("abc123", on_ground=False, seen=_T0)]) == []


def test_takeoff_edge_detected_on_ground_to_airborne() -> None:
    det = TakeoffDetector()
    det.observe([_pos("abc123", on_ground=True, seen=_T0)])
    takeoffs = det.observe([_pos("abc123", on_ground=False, seen=_T1)])

    assert len(takeoffs) == 1
    assert takeoffs[0].icao24 == "abc123"
    assert takeoffs[0].takeoff_ts == _T1
    assert takeoffs[0].flight_id == synthesize_flight_id("abc123", _T1)


def test_no_edge_when_staying_airborne_or_landing() -> None:
    det = TakeoffDetector()
    det.observe([_pos("abc123", on_ground=False, seen=_T0)])
    assert det.observe([_pos("abc123", on_ground=False, seen=_T1)]) == []  # stays up
    det.observe([_pos("def456", on_ground=False, seen=_T0)])
    assert det.observe([_pos("def456", on_ground=True, seen=_T1)]) == []  # lands


def test_takeoff_detector_seeds_from_prior_state() -> None:
    """A detector seeded with prior on-ground state detects an edge on the
    very first observe (the cross-restart path the asset relies on)."""
    det = TakeoffDetector({"abc123": True})
    takeoffs = det.observe([_pos("abc123", on_ground=False, seen=_T1)])
    assert len(takeoffs) == 1
    assert takeoffs[0].flight_id == synthesize_flight_id("abc123", _T1)


def test_takeoff_detector_does_not_alias_prior_state() -> None:
    prior = {"abc123": True}
    det = TakeoffDetector(prior)
    det.observe([_pos("abc123", on_ground=False, seen=_T1)])
    assert prior == {"abc123": True}  # caller's dict untouched


def test_takeoff_detector_state_for_bounds_to_given_icao24s() -> None:
    det = TakeoffDetector()
    det.observe(
        [
            _pos("abc123", on_ground=True, seen=_T0),
            _pos("def456", on_ground=False, seen=_T0),
        ]
    )
    # Only the requested, known icao24s survive (bounds the persisted blob).
    assert det.state_for(["abc123"]) == {"abc123": True}
    assert det.state_for(["abc123", "def456"]) == {"abc123": True, "def456": False}
    assert det.state_for(["ghost"]) == {}  # absent → dropped, not error


# --------------------------------------------------------------------------- #
# guarded_sync
# --------------------------------------------------------------------------- #


async def test_guarded_sync_maps_validation_error() -> None:
    class _M(BaseModel):
        x: int

    with pytest.raises(FoundrySyncSkipped, match="config absent"):
        async with guarded_sync("positions"):
            _M.model_validate({})  # raises ValidationError


async def test_guarded_sync_maps_http_error() -> None:
    with pytest.raises(FoundrySyncSkipped, match="unreachable"):
        async with guarded_sync("sites"):
            raise httpx.ConnectError("connection refused")


async def test_guarded_sync_lets_real_defects_propagate() -> None:
    with pytest.raises(ValueError, match="bug"):
        async with guarded_sync("positions"):
            raise ValueError("bug")


# --------------------------------------------------------------------------- #
# Job functions
# --------------------------------------------------------------------------- #


@respx.mock
async def test_incremental_sync_dedupes_and_returns_server_time_cursor(
    settings: FoundrySettings,
) -> None:
    positions = [
        _pos("abc123", on_ground=False, seen=_T0),  # stale duplicate
        _pos("abc123", on_ground=False, seen=_T1, callsign="UAL9"),  # newer wins
        _pos("def456", on_ground=False, seen=_T0),
    ]
    respx.get(_POS_URL).mock(
        return_value=httpx.Response(200, json=_positions_payload(positions, _T1))
    )
    batch_route = respx.post(_AIRCRAFT_BATCH).mock(
        return_value=httpx.Response(200, json={})
    )

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await incremental_sync_positions(client, writer, since=_T0)

    assert batch_route.called
    assert result.attempted == 2  # deduped to 2 distinct icao24
    assert result.succeeded == 2
    assert result.cursor == _T1  # cursor = response.server_time
    assert b"abc123" in batch_route.calls.last.request.content


@respx.mock
async def test_takeoff_detected_across_runs_with_caller_owned_detector(
    settings: FoundrySettings,
) -> None:
    """A takeoff edge spans two ticks; the detector is owned by the caller.

    Within one /v1/positions/live batch ``_dedupe_latest`` keeps only the
    newest row per icao24, so an edge is only observable run-to-run — which
    is why the detector state lives with the caller, not in this module.
    """
    batch_route = respx.post(_AIRCRAFT_BATCH).mock(
        return_value=httpx.Response(200, json={})
    )
    flight_route = respx.post(_FLIGHT_BATCH).mock(
        return_value=httpx.Response(200, json={})
    )
    det = TakeoffDetector()

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        # Tick 1: abc123 on the ground — seeds state, no edge.
        respx.get(_POS_URL).mock(
            return_value=httpx.Response(
                200,
                json=_positions_payload(
                    [_pos("abc123", on_ground=True, seen=_T0)], _T0
                ),
            )
        )
        r1 = await incremental_sync_positions(client, writer, detector=det)

        # Tick 2: same aircraft now airborne — on-ground→airborne edge.
        respx.get(_POS_URL).mock(
            return_value=httpx.Response(
                200,
                json=_positions_payload(
                    [_pos("abc123", on_ground=False, seen=_T1)], _T1
                ),
            )
        )
        r2 = await incremental_sync_positions(client, writer, detector=det)

    assert r1.takeoffs_detected == 0
    assert r2.takeoffs_detected == 1
    assert batch_route.call_count == 2
    # Create-only Flight write fired exactly once — only on the edge tick
    # (tick 1 had no takeoff → upsert_flight_batch([]) short-circuits).
    assert flight_route.call_count == 1
    assert r1.flights_written == 0
    assert r2.flights_written == 1
    # Post-run detector state is returned, bounded to the run's batch.
    assert r2.detector_state == {"abc123": False}
    assert b"abc123-" in flight_route.calls.last.request.content  # synthesized PK


@respx.mock
async def test_full_sync_sites_sla_failure_is_non_fatal(
    settings: FoundrySettings,
) -> None:
    respx.get("http://api.test/v1/sites").mock(
        return_value=httpx.Response(
            200,
            json={
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
            },
        )
    )
    respx.get("http://api.test/v1/sites/KSFO").mock(
        return_value=httpx.Response(
            200,
            json={
                "icao": "KSFO",
                "iata": "SFO",
                "name": "San Francisco Intl",
                "city": "San Francisco",
                "state": "CA",
                "lat": 37.62,
                "lon": -122.37,
                "elevation_ft": 13,
                "timezone": "America/Los_Angeles",
                "weather": None,
                "inbound_count_60m": 4,
                "outbound_count_60m": 7,
                "active_case_count": 1,
                "customer_regions": ["west"],
            },
        )
    )
    # SLA endpoint down → must not sink the batch; Site written with null SLA.
    respx.get("http://api.test/v1/sites/KSFO/sla").mock(
        return_value=httpx.Response(503)
    )
    site_route = respx.post(_SITE_BATCH).mock(
        return_value=httpx.Response(200, json={})
    )

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await full_sync_sites(client, writer)

    assert site_route.called
    assert result.attempted == 1
    assert result.succeeded == 1


@respx.mock
async def test_incremental_sync_propagates_skip_on_unreachable_api(
    settings: FoundrySettings,
) -> None:
    respx.get(_POS_URL).mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(FoundrySyncSkipped, match="unreachable"):
        async with (
            guarded_sync("positions"),
            AfmApiClient(settings) as client,
            FoundryWriter(settings) as writer,
        ):
            await incremental_sync_positions(client, writer)
