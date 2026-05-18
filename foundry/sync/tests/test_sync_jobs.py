"""Unit tests for the sync orchestration layer.

HTTP is mocked with respx on both sides: the local /v1 API
(``http://api.test``) and the Foundry Action API
(``https://tenant.example.com``). No real tenant or .env is touched —
clients are built from the ``settings`` fixture (conftest).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest
import respx
from pydantic import BaseModel

from afm_foundry_sync import sync_jobs as _sj
from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import Position
from afm_foundry_sync.ontology_writers import FoundryWriter
from afm_foundry_sync.settings import FoundrySettings
from afm_foundry_sync.sync_jobs import (
    FoundrySyncSkipped,
    TakeoffDetector,
    _dedupe_latest,
    enriched_sync_flights,
    full_sync_sites,
    guarded_sync,
    incremental_sync_positions,
    parse_flight_id,
    reconcile_aircraft,
    synthesize_flight_id,
)

_T0 = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
_T1 = datetime(2026, 5, 15, 12, 0, 30, tzinfo=UTC)

_POS_URL = "http://api.test/v1/positions/live"
_AIRCRAFT_BATCH = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-aircraft/applyBatch"
)
_SITE_BATCH = "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-site/applyBatch"
_FLIGHT_BATCH = "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-flight/applyBatch"


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
    batch_route = respx.post(_AIRCRAFT_BATCH).mock(return_value=httpx.Response(200, json={}))

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
    batch_route = respx.post(_AIRCRAFT_BATCH).mock(return_value=httpx.Response(200, json={}))
    flight_route = respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))
    det = TakeoffDetector()

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        # Tick 1: abc123 on the ground — seeds state, no edge.
        respx.get(_POS_URL).mock(
            return_value=httpx.Response(
                200,
                json=_positions_payload([_pos("abc123", on_ground=True, seen=_T0)], _T0),
            )
        )
        r1 = await incremental_sync_positions(client, writer, detector=det)

        # Tick 2: same aircraft now airborne — on-ground→airborne edge.
        respx.get(_POS_URL).mock(
            return_value=httpx.Response(
                200,
                json=_positions_payload([_pos("abc123", on_ground=False, seen=_T1)], _T1),
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
    respx.get("http://api.test/v1/sites/KSFO/sla").mock(return_value=httpx.Response(503))
    site_route = respx.post(_SITE_BATCH).mock(return_value=httpx.Response(200, json={}))

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


# ---------------------------------------------------------------------------
# reconcile_aircraft (Fix C — tenant-side eviction of departed aircraft)
# ---------------------------------------------------------------------------

_OBJECTS_URL = "https://tenant.example.com/api/v2/ontologies/afm/objects/Aircraft"
_DELETE_BATCH = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/delete-aircraft/applyBatch"
)


@respx.mock
async def test_reconcile_deletes_only_orphans_not_in_live(
    settings: FoundrySettings,
) -> None:
    # live = {abc123, def456}; tenant = those + two departed → orphans = 2.
    respx.get(_POS_URL).mock(
        return_value=httpx.Response(
            200,
            json=_positions_payload(
                [
                    _pos("abc123", on_ground=False, seen=_T1),
                    _pos("def456", on_ground=False, seen=_T1),
                ],
                _T1,
            ),
        )
    )
    respx.get(_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"icao24": x} for x in ("abc123", "def456", "ccc333", "ddd444")]},
        )
    )
    del_route = respx.post(_DELETE_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await reconcile_aircraft(client, writer)

    assert (result.live, result.tenant, result.orphans, result.deleted) == (2, 4, 2, 2)
    assert result.skipped_empty_live is False
    assert del_route.called
    body = json.loads(del_route.calls.last.request.content)
    sent = {r["parameters"]["Aircraft"] for r in body["requests"]}
    assert sent == {"ccc333", "ddd444"}  # only the departed, PascalCase key


@respx.mock
async def test_reconcile_skips_on_empty_live_without_listing_or_deleting(
    settings: FoundrySettings,
) -> None:
    # The safety guard: an empty feed means "fleet unknown", not "fleet
    # gone". Reconciling would delete the whole tenant — so it must bail
    # BEFORE enumerating the tenant and never issue a delete.
    respx.get(_POS_URL).mock(return_value=httpx.Response(200, json=_positions_payload([], _T1)))
    list_route = respx.get(_OBJECTS_URL).mock(return_value=httpx.Response(200, json={"data": []}))
    del_route = respx.post(_DELETE_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await reconcile_aircraft(client, writer)

    assert result.skipped_empty_live is True
    assert (result.live, result.tenant, result.orphans, result.deleted) == (0, 0, 0, 0)
    assert not list_route.called  # bailed before the expensive enumeration
    assert not del_route.called  # and never deleted


@respx.mock
async def test_reconcile_no_orphans_makes_no_delete_call(
    settings: FoundrySettings,
) -> None:
    respx.get(_POS_URL).mock(
        return_value=httpx.Response(
            200,
            json=_positions_payload([_pos("abc123", on_ground=False, seen=_T1)], _T1),
        )
    )
    respx.get(_OBJECTS_URL).mock(
        return_value=httpx.Response(200, json={"data": [{"icao24": "abc123"}]})
    )
    del_route = respx.post(_DELETE_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await reconcile_aircraft(client, writer)

    assert (result.orphans, result.deleted) == (0, 0)
    assert not del_route.called  # empty delete batch → no HTTP call


@respx.mock
async def test_reconcile_paginates_the_tenant_listing(
    settings: FoundrySettings,
) -> None:
    respx.get(_POS_URL).mock(
        return_value=httpx.Response(
            200,
            json=_positions_payload([_pos("abc123", on_ground=False, seen=_T1)], _T1),
        )
    )
    list_route = respx.get(_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"icao24": "abc123"}, {"icao24": "ccc333"}],
                    "nextPageToken": "page2",
                },
            ),
            httpx.Response(200, json={"data": [{"icao24": "ddd444"}]}),  # no token → stop
        ]
    )
    del_route = respx.post(_DELETE_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await reconcile_aircraft(client, writer)

    assert list_route.call_count == 2  # followed nextPageToken
    assert result.tenant == 3  # abc123 + ccc333 + ddd444 across 2 pages
    assert result.orphans == 2  # ccc333, ddd444 absent from live {abc123}
    body = json.loads(del_route.calls.last.request.content)
    assert {r["parameters"]["Aircraft"] for r in body["requests"]} == {"ccc333", "ddd444"}


@respx.mock
async def test_reconcile_propagates_skip_on_unreachable_api(
    settings: FoundrySettings,
) -> None:
    respx.get(_POS_URL).mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(FoundrySyncSkipped, match="unreachable"):
        async with (
            guarded_sync("reconcile"),
            AfmApiClient(settings) as client,
            FoundryWriter(settings) as writer,
        ):
            await reconcile_aircraft(client, writer)


# ---------------------------------------------------------------------------
# Flight enrichment (deferred FlightDetail/trail backfill)
# ---------------------------------------------------------------------------

_FLIGHT_OBJECTS_URL = "https://tenant.example.com/api/v2/ontologies/afm/objects/Flight"


def _flight_detail(icao24: str) -> dict:  # type: ignore[type-arg]
    return {
        "icao24": icao24,
        "callsign": "UAL1234",
        "registration": "N12345",
        "aircraft_type": "B738",
        "operator_icao": "UAL",
        "origin_icao": "KSFO",
        "destination_icao": "KLAX",
        "customer_region": "west",
        "position": {
            "icao24": icao24,
            "callsign": "UAL1234",
            "lat": 37.6,
            "lon": -122.3,
            "altitude_ft": 12000,
            "speed_kt": 300,
            "heading_deg": 270,
            "vertical_rate_fpm": 0,
            "on_ground": False,
            "customer_region": "west",
            "last_seen_at": "2026-05-15T12:00:00Z",
            "staleness": "fresh",
        },
        "eta_minutes": 25,
        "status_timeline": [],
        "open_case_ids": [],
    }


_TRAIL_JSON: dict = {  # type: ignore[type-arg]
    "icao24": "x",
    "points": [],
    "lookback": "2h",
    "point_count": 0,
}


@pytest.mark.parametrize(
    "icao24, ts",
    [("abc123", _T1), ("a0b1c2", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))],
)
def test_parse_flight_id_roundtrips_synthesize(icao24: str, ts: datetime) -> None:
    fid = synthesize_flight_id(icao24, ts)
    got_icao, got_ts = parse_flight_id(fid)
    assert got_icao == icao24
    # synthesize truncates to whole seconds; compare on the unix int.
    assert int(got_ts.timestamp()) == int(ts.timestamp())


@pytest.mark.parametrize("bad", ["noseparator", "-1700", "abc123-", "abc123-notanint"])
def test_parse_flight_id_rejects_malformed(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_flight_id(bad)


@respx.mock
async def test_enriched_sync_flights_enriches_only_latest_per_icao24(
    settings: FoundrySettings,
) -> None:
    # Tenant carries an OLD and a NEW flight_id for abc123 + one for def456
    # + a malformed PK that must be dropped (not crash).
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"flightId": "abc123-1700000000"},  # old
                    {"flightId": "abc123-1700003600"},  # newer → the target
                    {"flightId": "def456-1700000500"},
                    {"flightId": "garbage"},  # malformed → dropped
                ]
            },
        )
    )
    abc_route = respx.get("http://api.test/v1/flights/abc123").mock(
        return_value=httpx.Response(200, json=_flight_detail("abc123"))
    )
    def_route = respx.get("http://api.test/v1/flights/def456").mock(
        return_value=httpx.Response(200, json=_flight_detail("def456"))
    )
    respx.get("http://api.test/v1/flights/abc123/trail").mock(
        return_value=httpx.Response(200, json=_TRAIL_JSON)
    )
    respx.get("http://api.test/v1/flights/def456/trail").mock(
        return_value=httpx.Response(200, json=_TRAIL_JSON)
    )
    upsert_route = respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await enriched_sync_flights(client, writer)

    assert result.tenant_flights == 4
    assert result.candidates == 2  # abc123 (newest only) + def456
    assert result.enriched == 2
    assert (result.skipped_inactive, result.fetch_failed) == (0, 0)
    assert abc_route.call_count == 1  # the OLD abc123 PK was NOT fetched
    assert def_route.call_count == 1
    body = json.loads(upsert_route.calls.last.request.content)
    sent_ids = {r["parameters"]["flightId"] for r in body["requests"]}
    assert sent_ids == {"abc123-1700003600", "def456-1700000500"}
    assert "abc123-1700000000" not in sent_ids  # the superseded PK is untouched


@respx.mock
async def test_enriched_sync_flights_skips_inactive_404(
    settings: FoundrySettings,
) -> None:
    # /v1/flights 404s when the aircraft is outside the recency window —
    # nothing to enrich for that flight; counted, not failed, not upserted.
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(200, json={"data": [{"flightId": "dead01-1700000000"}]})
    )
    respx.get("http://api.test/v1/flights/dead01").mock(return_value=httpx.Response(404))
    upsert_route = respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await enriched_sync_flights(client, writer)

    assert (result.candidates, result.enriched) == (1, 0)
    assert result.skipped_inactive == 1
    assert result.fetch_failed == 0
    assert not upsert_route.called  # empty batch → no HTTP


@respx.mock
async def test_enriched_sync_flights_non_404_status_is_counted_not_fatal(
    settings: FoundrySettings,
) -> None:
    # One icao24 500s after retry; it is counted as fetch_failed and the
    # pass continues so a single bad flight cannot sink the batch.
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"flightId": "bad999-1700000000"}, {"flightId": "ok0001-1700000000"}]},
        )
    )
    respx.get("http://api.test/v1/flights/bad999").mock(return_value=httpx.Response(500))
    respx.get("http://api.test/v1/flights/ok0001").mock(
        return_value=httpx.Response(200, json=_flight_detail("ok0001"))
    )
    respx.get("http://api.test/v1/flights/ok0001/trail").mock(
        return_value=httpx.Response(200, json=_TRAIL_JSON)
    )
    upsert_route = respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await enriched_sync_flights(client, writer)

    assert result.candidates == 2
    assert result.enriched == 1  # only ok0001 made it
    assert result.fetch_failed == 1
    assert upsert_route.called
    body = json.loads(upsert_route.calls.last.request.content)
    assert {r["parameters"]["flightId"] for r in body["requests"]} == {"ok0001-1700000000"}


@respx.mock
async def test_enriched_sync_flights_propagates_skip_on_unreachable_api(
    settings: FoundrySettings,
) -> None:
    # Transport failure (API/Foundry down) is NOT swallowed per-flight — it
    # bubbles to guarded_sync as a clean skip, same discipline as reconcile.
    respx.get(_FLIGHT_OBJECTS_URL).mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(FoundrySyncSkipped, match="unreachable"):
        async with (
            guarded_sync("flight_enrichment"),
            AfmApiClient(settings) as client,
            FoundryWriter(settings) as writer,
        ):
            await enriched_sync_flights(client, writer)


@respx.mock
async def test_enriched_sync_flights_streams_in_chunks_and_aggregates(
    settings: FoundrySettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The OOM fix: candidates are upserted in bounded chunks (not one
    accumulate-all batch), and per-chunk counts aggregate. With chunk=2 and
    3 distinct-icao24 candidates → exactly 2 upsert HTTP calls, enriched=3."""
    monkeypatch.setattr(_sj, "_ENRICHMENT_CHUNK", 2)
    icaos = ["aa1111", "bb2222", "cc3333"]
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200, json={"data": [{"flightId": f"{i}-1700000000"} for i in icaos]}
        )
    )
    for i in icaos:
        respx.get(f"http://api.test/v1/flights/{i}").mock(
            return_value=httpx.Response(200, json=_flight_detail(i))
        )
        respx.get(f"http://api.test/v1/flights/{i}/trail").mock(
            return_value=httpx.Response(200, json=_TRAIL_JSON)
        )
    upsert_route = respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await enriched_sync_flights(client, writer)

    assert result.candidates == 3
    assert result.enriched == 3
    assert (result.skipped_inactive, result.fetch_failed) == (0, 0)
    # 2 chunks (sizes 2 + 1) → 2 separate upsert applyBatch POSTs, proving
    # memory is bounded per-chunk rather than one accumulate-all batch.
    assert upsert_route.call_count == 2
    sent = {
        r["parameters"]["flightId"]
        for call in upsert_route.calls
        for r in json.loads(call.request.content)["requests"]
    }
    assert sent == {f"{i}-1700000000" for i in icaos}


@respx.mock
async def test_enriched_sync_flights_fetches_are_concurrent_but_bounded(
    settings: FoundrySettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runtime fix: per-flight fetches within a chunk run concurrently,
    but never more than ``_ENRICHMENT_CONCURRENCY`` in flight at once. With
    concurrency=2 and a single chunk of 6, the observed peak in-flight count
    is exactly 2 (>1 proves it is not serial; ==2 proves it is bounded)."""
    monkeypatch.setattr(_sj, "_ENRICHMENT_CONCURRENCY", 2)
    monkeypatch.setattr(_sj, "_ENRICHMENT_CHUNK", 100)
    icaos = [f"a{i:05d}" for i in range(6)]
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200, json={"data": [{"flightId": f"{i}-1700000000"} for i in icaos]}
        )
    )

    state = {"in_flight": 0, "peak": 0}

    async def _tracked(_request: httpx.Request) -> httpx.Response:
        state["in_flight"] += 1
        state["peak"] = max(state["peak"], state["in_flight"])
        try:
            await asyncio.sleep(0.02)
        finally:
            state["in_flight"] -= 1
        return httpx.Response(200, json=_flight_detail("a00000"))

    for i in icaos:
        respx.get(f"http://api.test/v1/flights/{i}").mock(side_effect=_tracked)
        respx.get(f"http://api.test/v1/flights/{i}/trail").mock(
            return_value=httpx.Response(200, json=_TRAIL_JSON)
        )
    respx.post(_FLIGHT_BATCH).mock(return_value=httpx.Response(200, json={}))

    async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
        result = await enriched_sync_flights(client, writer)

    assert result.enriched == 6
    assert state["peak"] == 2, f"expected bounded peak 2, saw {state['peak']}"
