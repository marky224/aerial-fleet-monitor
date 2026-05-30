"""Unit tests for ``FoundryWriter`` using respx to mock the Action API."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import httpx
import pytest
import respx

from afm_foundry_sync import ontology_writers
from afm_foundry_sync.models import (
    Aircraft,
    Case,
    Flight,
    FlightStatusEvent,
    Site,
    SparklinePoint,
    TrailPoint,
)
from afm_foundry_sync.ontology_writers import (
    FlightLandedStamp,
    FlightLiveStamp,
    FoundryWriter,
    _camel,
    aircraft_params,
    case_params,
    flight_params,
    site_params,
)
from afm_foundry_sync.settings import FoundrySettings

_AIRCRAFT_URL = (
    "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-aircraft/applyBatch"
)
_SITE_URL = "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-site/applyBatch"
_FLIGHT_URL = "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-flight/applyBatch"

_LAST_SEEN = datetime(2026, 5, 15, 12, 0, 0, tzinfo=UTC)
_TAKEOFF = datetime(2026, 5, 16, 11, 30, 0, tzinfo=UTC)


def _aircraft(icao24: str = "a12345", **overrides: object) -> Aircraft:
    base = dict(
        icao24=icao24,
        callsign="UAL1234",
        lat=37.62,
        lon=-122.37,
        altitude_ft=12000,
        speed_kt=300,
        heading_deg=270,
        vertical_rate_fpm=0,
        on_ground=False,
        customer_region="west",
        last_seen_at=_LAST_SEEN,
        staleness="fresh",
    )
    base.update(overrides)
    return Aircraft(**base)  # type: ignore[arg-type]


def _site(icao: str = "KSFO", **overrides: object) -> Site:
    base = dict(
        icao=icao,
        iata="SFO",
        name="San Francisco Intl",
        city="San Francisco",
        state="CA",
        lat=37.62,
        lon=-122.37,
        elevation_ft=13,
        timezone=None,
        customer_regions=["west", "all"],
        inbound_count_60m=4,
        outbound_count_60m=7,
        active_case_count=0,
        metar_raw="KSFO 151200Z ...",
        metar_plain_english=None,
        wind_kt=12,
        visibility_sm=10.0,
        ceiling_ft=None,
        weather_observed_at=_LAST_SEEN,
        flight_category="VFR",
        sla_period="last_24h",
        sla_inbound_count=0,
        sla_outbound_count=0,
        on_time_arrival_pct=None,
        on_time_departure_pct=None,
        avg_arrival_delay_min=None,
        avg_departure_delay_min=None,
        weather_impact="low",
        sla_sparkline_7d=[],
    )
    base.update(overrides)
    return Site(**base)  # type: ignore[arg-type]


def _flight(flight_id: str = "a12345-1747308600", **overrides: object) -> Flight:
    """Enriched Flight by default (lat/lon set). Pass lat=None, lon=None for
    the takeoff-create shape."""
    base = dict(
        flight_id=flight_id,
        icao24="a12345",
        takeoff_ts=_TAKEOFF,
        landed_at=None,
        callsign="UAL1234",
        registration="N12345",
        aircraft_type="B738",
        operator_icao="UAL",
        customer_region="west",
        origin_icao="KSFO",
        destination_icao="KLAX",
        eta_minutes=42,
        status="enroute",
        current_stage="cruise",
        lat=37.62,
        lon=-122.37,
        open_case_count=0,
        open_case_ids=[],
        status_timeline=[FlightStatusEvent(stage="departed", occurred_at=_TAKEOFF)],
        trail_2h=[],
    )
    base.update(overrides)
    return Flight(**base)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("snake", "expected"),
    [
        # Single-token names are identity (PK, locator, geopoint).
        ("icao24", "icao24"),
        ("icao", "icao"),
        ("aircraft", "aircraft"),
        ("site", "site"),
        ("position", "position"),
        ("location", "location"),
        # Multi-token: must reproduce the recon'd tenant property names verbatim.
        ("last_seen_at", "lastSeenAt"),
        ("on_ground", "onGround"),
        ("vertical_rate_fpm", "verticalRateFpm"),
        ("customer_regions", "customerRegions"),
        ("inbound_count_60m", "inboundCount60m"),
        ("metar_plain_english", "metarPlainEnglish"),
        ("on_time_arrival_pct", "onTimeArrivalPct"),
        ("sla_sparkline_7d", "slaSparkline7d"),
    ],
)
def test_camel_matches_recon_property_names(snake: str, expected: str) -> None:
    assert _camel(snake) == expected


# ---------------------------------------------------------------------------
# Serialization units
# ---------------------------------------------------------------------------


def test_aircraft_params_pk_written_twice_and_geopoint_order() -> None:
    p = aircraft_params(_aircraft())
    assert p["icao24"] == "a12345"
    assert p["aircraft"] == "a12345"  # locator param = bare PK string
    # GeoJSON axis order is [lon, lat].
    assert p["position"] == {"type": "Point", "coordinates": [-122.37, 37.62]}
    # Keys are camelCase (the Foundry action-param contract).
    assert p["lastSeenAt"] == "2026-05-15T12:00:00Z"


def test_aircraft_params_omits_none_optionals() -> None:
    p = aircraft_params(_aircraft(callsign=None, altitude_ft=None, customer_region=None))
    assert "callsign" not in p
    assert "altitudeFt" not in p
    assert "customerRegion" not in p
    # Required params remain.
    assert p["onGround"] is False
    assert p["staleness"] == "fresh"


def test_site_params_arrays_are_json_strings() -> None:
    p = site_params(
        _site(
            sla_sparkline_7d=[
                SparklinePoint(day=date(2026, 5, 15), on_time_pct=91.2, avg_delay_min=7.4)
            ]
        )
    )
    assert p["site"] == "KSFO"
    assert p["customerRegions"] == '["west", "all"]'
    # Struct array serialized as JSON; date rendered ISO via model_dump(mode=json).
    assert p["slaSparkline7d"] == (
        '[{"day": "2026-05-15", "on_time_pct": 91.2, "avg_delay_min": 7.4}]'
    )
    assert p["location"] == {"type": "Point", "coordinates": [-122.37, 37.62]}


def test_site_params_empty_sparkline_is_empty_json_array() -> None:
    p = site_params(_site(sla_sparkline_7d=[]))
    assert p["slaSparkline7d"] == "[]"


def test_site_params_omits_none_optionals_keeps_required() -> None:
    p = site_params(_site(timezone=None, on_time_arrival_pct=None, ceiling_ft=None))
    assert "timezone" not in p
    assert "onTimeArrivalPct" not in p
    assert "ceilingFt" not in p
    assert p["inboundCount60m"] == 4
    assert p["weatherObservedAt"] == "2026-05-15T12:00:00Z"


def test_flight_params_pk_written_twice_and_camel_keys() -> None:
    p = flight_params(_flight())
    assert p["flightId"] == "a12345-1747308600"
    assert p["flight"] == "a12345-1747308600"  # locator param = bare PK string
    assert p["icao24"] == "a12345"
    assert p["takeoffTs"] == "2026-05-16T11:30:00Z"
    assert p["status"] == "enroute"
    assert p["currentStage"] == "cruise"


def test_flight_params_list_fields_are_json_strings() -> None:
    p = flight_params(
        _flight(
            open_case_ids=["CASE-1", "CASE-2"],
            status_timeline=[FlightStatusEvent(stage="departed", occurred_at=_TAKEOFF)],
            trail_2h=[
                TrailPoint(ts=_TAKEOFF, lat=37.7, lon=-122.4, altitude_ft=8000, speed_kt=280)
            ],
        )
    )
    assert p["openCaseIds"] == '["CASE-1", "CASE-2"]'
    assert p["statusTimeline"] == ('[{"stage": "departed", "occurred_at": "2026-05-16T11:30:00Z"}]')
    assert p["trail2h"] == (
        '[{"ts": "2026-05-16T11:30:00Z", "lat": 37.7, "lon": -122.4,'
        ' "altitude_ft": 8000, "speed_kt": 280}]'
    )


def test_flight_params_empty_collections_are_empty_json_arrays() -> None:
    p = flight_params(_flight(open_case_ids=[], status_timeline=[], trail_2h=[]))
    assert p["openCaseIds"] == "[]"
    assert p["statusTimeline"] == "[]"
    assert p["trail2h"] == "[]"


def test_flight_params_geopoint_only_when_both_coords_present() -> None:
    enriched = flight_params(_flight(lat=37.62, lon=-122.37))
    assert enriched["position"] == {"type": "Point", "coordinates": [-122.37, 37.62]}
    # Takeoff-create shape: no lat/lon -> no position/lat/lon keys at all.
    takeoff = flight_params(_flight(lat=None, lon=None))
    assert "position" not in takeoff
    assert "lat" not in takeoff
    assert "lon" not in takeoff
    # Required params still present on the bare create payload.
    assert takeoff["flightId"] == "a12345-1747308600"
    assert takeoff["flight"] == "a12345-1747308600"
    assert takeoff["openCaseCount"] == 0
    # statusTimeline is the seeded "departed" event (takeoff_to_flight never
    # emits an empty timeline) — present and JSON-encoded.
    assert isinstance(takeoff["statusTimeline"], str)
    assert "departed" in takeoff["statusTimeline"]


def test_flight_params_trail_path_linestring_geo_native_projection() -> None:
    # >= 2 trail points → trailPath is a GeoJSON LineString in [lon, lat]
    # order (mirrors position's geopoint), the App 3 §3.3 polyline binding.
    # trail2h (the raw JSON-string points) is still emitted alongside it.
    p = flight_params(
        _flight(
            trail_2h=[
                TrailPoint(ts=_TAKEOFF, lat=37.7, lon=-122.4, altitude_ft=8000, speed_kt=280),
                TrailPoint(ts=_TAKEOFF, lat=37.8, lon=-122.5, altitude_ft=9000, speed_kt=300),
            ]
        )
    )
    assert p["trailPath"] == {
        "type": "LineString",
        "coordinates": [[-122.4, 37.7], [-122.5, 37.8]],
    }
    assert isinstance(p["trail2h"], str)  # raw points still shipped


def test_flight_params_trail_path_omitted_under_two_points() -> None:
    # A LineString needs >= 2 positions; a 0/1-point trail omits trailPath
    # entirely (like position when coords are absent) — never an invalid
    # shape. trail2h still serializes the (possibly empty) points.
    one = flight_params(
        _flight(
            trail_2h=[TrailPoint(ts=_TAKEOFF, lat=1.0, lon=2.0, altitude_ft=None, speed_kt=None)]
        )
    )
    assert "trailPath" not in one
    none = flight_params(_flight(trail_2h=[]))
    assert "trailPath" not in none
    assert none["trail2h"] == "[]"


def test_flight_params_trail_path_omitted_for_degenerate_all_coincident() -> None:
    # A stationary/parked aircraft's 2 h trail can collapse to one repeated
    # coordinate. That yields a zero-length LineString, which Foundry's
    # geoshape validator rejects with 400 INVALID_ARGUMENT — and since
    # applyBatch is all-or-nothing per chunk, that one Flight 400s the
    # whole chunk (run skip-fails, enriched=0). So an all-coincident trail
    # must omit trailPath, exactly like a 0/1-point trail. trail2h still
    # ships every raw point (no data loss). Repro: a889a9-1779125622,
    # coordinates [[-123.4629, 46.1935], [-123.4629, 46.1935]].
    p = flight_params(
        _flight(
            trail_2h=[
                TrailPoint(ts=_TAKEOFF, lat=46.1935, lon=-123.4629, altitude_ft=0, speed_kt=0),
                TrailPoint(ts=_TAKEOFF, lat=46.1935, lon=-123.4629, altitude_ft=0, speed_kt=0),
                TrailPoint(ts=_TAKEOFF, lat=46.1935, lon=-123.4629, altitude_ft=0, speed_kt=0),
            ]
        )
    )
    assert "trailPath" not in p
    assert isinstance(p["trail2h"], str)  # raw points still shipped


def test_flight_params_trail_path_dedupes_consecutive_identical_coords() -> None:
    # Consecutive-identical coordinates are collapsed so the LineString is
    # well-formed; [A, A, B] → [A, B]. A LineString with >= 2 *distinct*
    # positions is valid and emitted.
    p = flight_params(
        _flight(
            trail_2h=[
                TrailPoint(ts=_TAKEOFF, lat=37.7, lon=-122.4, altitude_ft=8000, speed_kt=280),
                TrailPoint(ts=_TAKEOFF, lat=37.7, lon=-122.4, altitude_ft=8000, speed_kt=280),
                TrailPoint(ts=_TAKEOFF, lat=37.8, lon=-122.5, altitude_ft=9000, speed_kt=300),
            ]
        )
    )
    assert p["trailPath"] == {
        "type": "LineString",
        "coordinates": [[-122.4, 37.7], [-122.5, 37.8]],
    }


def test_flight_params_trail_path_omitted_when_two_points_coincide() -> None:
    # Exactly 2 points but identical → after dedup only 1 distinct
    # position remains → omitted (not a 2-point degenerate LineString).
    p = flight_params(
        _flight(
            trail_2h=[
                TrailPoint(ts=_TAKEOFF, lat=1.0, lon=2.0, altitude_ft=None, speed_kt=None),
                TrailPoint(ts=_TAKEOFF, lat=1.0, lon=2.0, altitude_ft=None, speed_kt=None),
            ]
        )
    )
    assert "trailPath" not in p


def test_flight_params_omits_none_optionals_keeps_required() -> None:
    p = flight_params(_flight(callsign=None, landed_at=None, eta_minutes=None, operator_icao=None))
    assert "callsign" not in p
    assert "landedAt" not in p
    assert "etaMinutes" not in p
    assert "operatorIcao" not in p
    # Required params remain.
    assert p["icao24"] == "a12345"
    assert p["openCaseCount"] == 0


def test_flight_params_landed_at_iso_utc_when_present() -> None:
    landed = datetime(2026, 5, 16, 12, 15, 0, tzinfo=UTC)
    p = flight_params(_flight(landed_at=landed))
    assert p["landedAt"] == "2026-05-16T12:15:00Z"


# ---------------------------------------------------------------------------
# applyBatch behavior
# ---------------------------------------------------------------------------


@respx.mock
async def test_upsert_aircraft_batch_posts_expected_envelope(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_AIRCRAFT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.upsert_aircraft_batch([_aircraft("a1"), _aircraft("a2")])

    assert route.called
    assert route.call_count == 1
    sent = route.calls.last.request
    assert sent.headers["authorization"] == "Bearer t-test-token"
    payload = json.loads(sent.content)
    assert [r["parameters"]["icao24"] for r in payload["requests"]] == ["a1", "a2"]
    assert payload["requests"][0]["parameters"]["aircraft"] == "a1"
    assert result == ontology_writers.BatchResult(attempted=2, succeeded=2)


@respx.mock
async def test_upsert_flight_batch_posts_expected_envelope(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.upsert_flight_batch([_flight("f-1"), _flight("f-2")])

    assert route.called
    assert route.call_count == 1
    payload = json.loads(route.calls.last.request.content)
    assert [r["parameters"]["flightId"] for r in payload["requests"]] == ["f-1", "f-2"]
    assert payload["requests"][0]["parameters"]["flight"] == "f-1"
    assert result == ontology_writers.BatchResult(attempted=2, succeeded=2)


@respx.mock
async def test_upsert_flight_empty_batch_makes_no_http_call(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.upsert_flight_batch([])

    assert not route.called
    assert result == ontology_writers.BatchResult(attempted=0, succeeded=0)


@respx.mock
async def test_empty_batch_makes_no_http_call(settings: FoundrySettings) -> None:
    route = respx.post(_AIRCRAFT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.upsert_aircraft_batch([])

    assert not route.called
    assert result == ontology_writers.BatchResult(attempted=0, succeeded=0)


@respx.mock
async def test_chunking_splits_into_multiple_posts(
    settings: FoundrySettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ontology_writers, "_MAX_BATCH", 2)
    route = respx.post(_SITE_URL).mock(return_value=httpx.Response(200, json={}))

    async with FoundryWriter(settings) as w:
        result = await w.upsert_site_batch([_site(f"K{i:03d}") for i in range(5)])

    # 5 items, chunk size 2 -> 3 POSTs (2 + 2 + 1).
    assert route.call_count == 3
    assert result == ontology_writers.BatchResult(attempted=5, succeeded=5)


@respx.mock
async def test_retries_on_503_then_succeeds(settings: FoundrySettings) -> None:
    route = respx.post(_AIRCRAFT_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={})]
    )
    async with FoundryWriter(settings) as w:
        result = await w.upsert_aircraft_batch([_aircraft()])

    assert route.call_count == 2
    assert result.succeeded == 1


@respx.mock
async def test_does_not_retry_on_400(settings: FoundrySettings) -> None:
    route = respx.post(_AIRCRAFT_URL).mock(
        return_value=httpx.Response(400, json={"errorCode": "INVALID_ARGUMENT"})
    )
    async with FoundryWriter(settings) as w:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await w.upsert_aircraft_batch([_aircraft()])

    assert exc_info.value.response.status_code == 400
    assert route.call_count == 1  # 4xx is a real signal, never retried


@respx.mock
async def test_exhausts_retries_on_persistent_503(settings: FoundrySettings) -> None:
    route = respx.post(_SITE_URL).mock(return_value=httpx.Response(503))
    async with FoundryWriter(settings) as w:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await w.upsert_site_batch([_site()])

    assert exc_info.value.response.status_code == 503
    assert route.call_count == 3


# ---------------------------------------------------------------------------
# Tenant reconcile primitives (Fix C): list_aircraft_pks / delete_aircraft_batch
# ---------------------------------------------------------------------------

_OBJECTS_URL = "https://tenant.example.com/api/v2/ontologies/afm/objects/Aircraft"
_DELETE_URL = "https://tenant.example.com/api/v2/ontologies/afm/actions/delete-aircraft/applyBatch"


@respx.mock
async def test_list_aircraft_pks_follows_pagination(settings: FoundrySettings) -> None:
    route = respx.get(_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"icao24": "abc123"}, {"icao24": "def456"}],
                    "nextPageToken": "p2",
                },
            ),
            httpx.Response(200, json={"data": [{"icao24": "ghi789"}]}),  # no token
        ]
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_aircraft_pks()

    assert pks == {"abc123", "def456", "ghi789"}
    assert route.call_count == 2  # stopped when nextPageToken absent


@respx.mock
async def test_list_aircraft_pks_falls_back_to_primary_key(
    settings: FoundrySettings,
) -> None:
    respx.get(_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"__primaryKey": "aaa111"}, {"icao24": "bbb222"}]},
        )
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_aircraft_pks()

    assert pks == {"aaa111", "bbb222"}


@respx.mock
async def test_delete_aircraft_batch_uses_pascalcase_key_and_chunks(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_DELETE_URL).mock(return_value=httpx.Response(200, json={}))
    icao24s = [f"a{i:05d}" for i in range(250)]  # 3 chunks at _MAX_BATCH=100

    async with FoundryWriter(settings) as w:
        result = await w.delete_aircraft_batch(icao24s)

    assert result.attempted == 250
    assert result.succeeded == 250
    assert route.call_count == 3
    body = json.loads(route.calls[0].request.content)
    # PascalCase "Aircraft" key (delete contract), NOT the lowercase locator.
    assert body["requests"][0]["parameters"] == {"Aircraft": "a00000"}


@respx.mock
async def test_delete_aircraft_batch_empty_makes_no_http_call(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_DELETE_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.delete_aircraft_batch([])

    assert (result.attempted, result.succeeded) == (0, 0)
    assert not route.called


@respx.mock
async def test_list_aircraft_pks_retries_on_503_then_succeeds(
    settings: FoundrySettings,
) -> None:
    route = respx.get(_OBJECTS_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={"data": []})]
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_aircraft_pks()

    assert pks == set()
    assert route.call_count == 2  # transient_retry covers the GET too


# ---------------------------------------------------------------------------
# Landing stamp (Phase A2): partial upsert that must NOT clobber enrichment
# ---------------------------------------------------------------------------


@respx.mock
async def test_stamp_flight_landed_batch_sends_minimal_partial_upsert(
    settings: FoundrySettings,
) -> None:
    """The landing stamp goes through the upsert-flight (modify-or-create)
    Action carrying ONLY identity + the three lifecycle fields. The
    clobber-avoidance contract: it must NOT send status_timeline / trail_2h /
    open_case_* (which flight_params always emits) — those stay as enrichment
    left them (verified against the live tenant 2026-05-29). camelCase keys,
    PK sent twice (flightId param + flight locator)."""
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    stamp = FlightLandedStamp(
        flight_id="abc123-1748480400",
        icao24="abc123",
        takeoff_ts=datetime(2026, 5, 29, 1, 0, 0, tzinfo=UTC),
        landed_at=datetime(2026, 5, 29, 3, 30, 0, tzinfo=UTC),
    )
    async with FoundryWriter(settings) as w:
        result = await w.stamp_flight_landed_batch([stamp])

    assert (result.attempted, result.succeeded) == (1, 1)
    params = json.loads(route.calls.last.request.content)["requests"][0]["parameters"]
    assert params == {
        "flightId": "abc123-1748480400",
        "flight": "abc123-1748480400",
        "icao24": "abc123",
        "takeoffTs": "2026-05-29T01:00:00Z",
        "landedAt": "2026-05-29T03:30:00Z",
        "status": "landed",
        "currentStage": "landed",
    }
    # Clobber-avoidance: enrichment fields are absent, so the modify path
    # leaves them unchanged.
    for clobberable in ("statusTimeline", "trail2h", "trailPath", "openCaseCount", "openCaseIds"):
        assert clobberable not in params


@respx.mock
async def test_stamp_flight_landed_batch_empty_makes_no_http_call(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.stamp_flight_landed_batch([])

    assert (result.attempted, result.succeeded) == (0, 0)
    assert not route.called


# ---------------------------------------------------------------------------
# Flight.isLive liveness flag (Tier 2-lite)
# ---------------------------------------------------------------------------


def test_flight_params_omits_islive_by_default() -> None:
    """The flag is off by default (emit_is_live=False), so `isLive` is never
    sent — even on a Flight that carries a value. This is the pre-provisioning
    safety: an unknown param would 400 the whole applyBatch."""
    assert "isLive" not in flight_params(_flight(is_live=True))


def test_flight_params_emits_islive_when_enabled() -> None:
    """With emit_is_live=True (the writer threads its islive_enabled flag), a
    non-None is_live is sent as camelCase `isLive`."""
    assert flight_params(_flight(is_live=True), emit_is_live=True)["isLive"] is True
    assert flight_params(_flight(is_live=False), emit_is_live=True)["isLive"] is False


def test_flight_params_omits_islive_none_even_when_enabled() -> None:
    """None is_live is omitted even when emitting is enabled — so an enrichment
    re-upsert (which carries is_live=None) never clobbers the sweep-maintained
    value (the modify path leaves omitted params unchanged)."""
    assert "isLive" not in flight_params(_flight(is_live=None), emit_is_live=True)


@respx.mock
async def test_upsert_flight_batch_threads_islive_flag(settings_islive: FoundrySettings) -> None:
    """upsert_flight_batch passes the writer's islive_enabled flag into
    flight_params, so a live takeoff create carries isLive=true when the flag
    is on."""
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings_islive) as w:
        await w.upsert_flight_batch([_flight(lat=None, lon=None, is_live=True)])
    params = json.loads(route.calls.last.request.content)["requests"][0]["parameters"]
    assert params["isLive"] is True


@respx.mock
async def test_upsert_flight_batch_omits_islive_when_flag_off(settings: FoundrySettings) -> None:
    """Default settings keep the flag off → no isLive on the wire even if the
    Flight carries one (deploy-before-provision safety)."""
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        await w.upsert_flight_batch([_flight(lat=None, lon=None, is_live=True)])
    params = json.loads(route.calls.last.request.content)["requests"][0]["parameters"]
    assert "isLive" not in params


@respx.mock
async def test_stamp_flight_landed_batch_sets_islive_false_when_enabled(
    settings_islive: FoundrySettings,
) -> None:
    """With the flag on, the landing stamp also flips isLive=false (a landing
    ends the live leg) — added to the same minimal partial upsert, still
    omitting every enrichment field."""
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    stamp = FlightLandedStamp(
        flight_id="abc123-1748480400",
        icao24="abc123",
        takeoff_ts=datetime(2026, 5, 29, 1, 0, 0, tzinfo=UTC),
        landed_at=datetime(2026, 5, 29, 3, 30, 0, tzinfo=UTC),
    )
    async with FoundryWriter(settings_islive) as w:
        await w.stamp_flight_landed_batch([stamp])
    params = json.loads(route.calls.last.request.content)["requests"][0]["parameters"]
    assert params["isLive"] is False
    for clobberable in ("statusTimeline", "trail2h", "openCaseCount"):
        assert clobberable not in params


@respx.mock
async def test_set_flight_live_batch_sends_minimal_partial_upsert(
    settings: FoundrySettings,
) -> None:
    """The sweep's writer goes through upsert-flight carrying ONLY identity +
    isLive — never the enrichment fields (clobber-avoidance), camelCase keys,
    PK twice. Carries the target value verbatim (true or false)."""
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    stamps = [
        FlightLiveStamp(
            flight_id="abc123-1748480400",
            icao24="abc123",
            takeoff_ts=datetime(2026, 5, 29, 1, 0, 0, tzinfo=UTC),
            is_live=True,
        ),
        FlightLiveStamp(
            flight_id="def456-1748470000",
            icao24="def456",
            takeoff_ts=datetime(2026, 5, 28, 22, 6, 40, tzinfo=UTC),
            is_live=False,
        ),
    ]
    async with FoundryWriter(settings) as w:
        result = await w.set_flight_live_batch(stamps)

    assert (result.attempted, result.succeeded) == (2, 2)
    reqs = json.loads(route.calls.last.request.content)["requests"]
    assert reqs[0]["parameters"] == {
        "flightId": "abc123-1748480400",
        "flight": "abc123-1748480400",
        "icao24": "abc123",
        "takeoffTs": "2026-05-29T01:00:00Z",
        "isLive": True,
    }
    assert reqs[1]["parameters"]["isLive"] is False
    for clobberable in ("statusTimeline", "trail2h", "trailPath", "landedAt", "status"):
        assert clobberable not in reqs[0]["parameters"]


@respx.mock
async def test_set_flight_live_batch_empty_makes_no_http_call(settings: FoundrySettings) -> None:
    route = respx.post(_FLIGHT_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.set_flight_live_batch([])

    assert (result.attempted, result.succeeded) == (0, 0)
    assert not route.called


# ---------------------------------------------------------------------------
# Flight enrichment primitive: list_flight_pks (mirrors list_aircraft_pks)
# ---------------------------------------------------------------------------

_FLIGHT_OBJECTS_URL = "https://tenant.example.com/api/v2/ontologies/afm/objects/Flight"


@respx.mock
async def test_list_flight_pks_follows_pagination(settings: FoundrySettings) -> None:
    route = respx.get(_FLIGHT_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"flightId": "abc123-1700"}, {"flightId": "def456-1800"}],
                    "nextPageToken": "p2",
                },
            ),
            httpx.Response(200, json={"data": [{"flightId": "ghi789-1900"}]}),
        ]
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_flight_pks()

    assert pks == {"abc123-1700", "def456-1800", "ghi789-1900"}
    assert route.call_count == 2  # stopped when nextPageToken absent


@respx.mock
async def test_list_flight_pks_falls_back_to_primary_key(settings: FoundrySettings) -> None:
    respx.get(_FLIGHT_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"__primaryKey": "aaa111-1700"}, {"flightId": "bbb222-1800"}]},
        )
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_flight_pks()

    assert pks == {"aaa111-1700", "bbb222-1800"}


@respx.mock
async def test_list_flight_pks_with_completion_flags_landed_and_live(
    settings: FoundrySettings,
) -> None:
    """The scan returns ALL pks plus, from the SAME paginated pass (no extra
    round-trips): the completed subset (non-null ``landedAt``, the Phase-B
    reconcile's protected set) and the currently-live subset (``isLive`` is
    exactly ``true``). An explicit null landedAt is NOT completed; ``isLive``
    false/null/absent is NOT live."""
    route = respx.get(_FLIGHT_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [
                        {"flightId": "f-airborne-1700", "isLive": True},
                        {
                            "flightId": "f-landed-1800",
                            "landedAt": "2026-05-16T12:00:00Z",
                            "isLive": False,
                        },
                    ],
                    "nextPageToken": "p2",
                },
            ),
            httpx.Response(
                200,
                json={"data": [{"flightId": "f-null-landed-1900", "landedAt": None}]},
            ),
        ]
    )
    async with FoundryWriter(settings) as w:
        all_pks, completed, live = await w.list_flight_pks_with_completion()

    assert all_pks == {"f-airborne-1700", "f-landed-1800", "f-null-landed-1900"}
    assert completed == {"f-landed-1800"}
    assert live == {"f-airborne-1700"}  # isLive=false and absent are NOT live
    assert route.call_count == 2  # paginates exactly like the PK-only scan


@respx.mock
async def test_iter_completed_flights_streams_only_landed_across_pages(
    settings: FoundrySettings,
) -> None:
    """The archive read streams the full objects of completed flights (landedAt
    present + non-null) page by page; stubs and explicit-null landedAt are
    skipped, and empty pages are not yielded."""
    route = respx.get(_FLIGHT_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [
                        {"flightId": "stub-1700"},  # no landedAt -> skipped
                        {
                            "flightId": "done-1800",
                            "landedAt": "2026-05-16T12:00:00Z",
                            "trail2h": "[]",
                        },
                    ],
                    "nextPageToken": "p2",
                },
            ),
            httpx.Response(
                200,
                json={
                    "data": [
                        {"flightId": "done-1900", "landedAt": "2026-05-16T13:00:00Z"},
                        {"flightId": "null-2000", "landedAt": None},  # explicit null -> skipped
                    ]
                },
            ),
        ]
    )
    async with FoundryWriter(settings) as w:
        pages = [page async for page in w.iter_completed_flights()]

    flat = [obj["flightId"] for page in pages for obj in page]
    assert flat == ["done-1800", "done-1900"]
    # full objects ride through (the archive needs the trail), not just the PK.
    assert pages[0][0]["trail2h"] == "[]"
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# Cases (Phase 05 task #5)
# ---------------------------------------------------------------------------


_CASE_URL = "https://tenant.example.com/api/v2/ontologies/afm/actions/upsert-case/applyBatch"
_CASE_DELETE_URL = "https://tenant.example.com/api/v2/ontologies/afm/actions/delete-case/applyBatch"
_CASE_OBJECTS_URL = "https://tenant.example.com/api/v2/ontologies/afm/objects/Case"

_CREATED_AT = datetime(2026, 5, 24, 10, 0, 0, tzinfo=UTC)
_UPDATED_AT = datetime(2026, 5, 24, 10, 5, 0, tzinfo=UTC)


def _case(case_id: str = "CASE-2026-000001", **overrides: object) -> Case:
    base = dict(
        case_id=case_id,
        salesforce_id="500X000000abc",
        case_type="lost_signal",
        status="open",
        severity="high",
        customer_region="west",
        site_icao="KSFO",
        flight_id="a12345-1747308600",
        subject="Lost signal during cruise — UAL1234 near KSFO",
        summary=None,
        severity_justification=None,
        detection_facts={"callsign": "UAL1234", "gap_minutes": 12},
        runbook_refs=["lost-signal-cruise"],
        created_at=_CREATED_AT,
        updated_at=_UPDATED_AT,
        resolved_at=None,
    )
    base.update(overrides)
    return Case(**base)  # type: ignore[arg-type]


def test_case_params_pk_via_locator_only_no_case_id_param() -> None:
    """Unique vs Aircraft/Site/Flight: the upsert-case action has no separate
    PK string param — the `case` object-locator alone handles the PK."""
    p = case_params(_case())
    assert p["case"] == "CASE-2026-000001"  # locator = bare PK string
    assert "caseId" not in p  # NO separate PK param (verified live 2026-05-24)


def test_case_params_camel_keys_and_iso_timestamps() -> None:
    p = case_params(_case())
    # camelCase keys per the established tenant contract.
    assert p["caseType"] == "lost_signal"
    assert p["customerRegion"] == "west"
    assert p["siteIcao"] == "KSFO"
    assert p["flightId"] == "a12345-1747308600"
    assert p["createdAt"] == "2026-05-24T10:00:00Z"
    assert p["updatedAt"] == "2026-05-24T10:05:00Z"


def test_case_params_dict_and_list_fields_are_json_strings() -> None:
    """detection_facts (dict) and runbook_refs (list) → JSON-encoded strings
    (same precedent as Site.customer_regions / Flight.trail_2h)."""
    p = case_params(
        _case(
            detection_facts={"callsign": "UAL1234", "gap_minutes": 12},
            runbook_refs=["lost-signal-cruise", "satcom-outage"],
        )
    )
    assert json.loads(p["detectionFacts"]) == {"callsign": "UAL1234", "gap_minutes": 12}
    assert json.loads(p["runbookRefs"]) == ["lost-signal-cruise", "satcom-outage"]


def test_case_params_empty_collections_serialize_to_empty_json() -> None:
    p = case_params(_case(detection_facts={}, runbook_refs=[]))
    assert p["detectionFacts"] == "{}"
    assert p["runbookRefs"] == "[]"


def test_case_params_omits_none_optionals_keeps_required() -> None:
    p = case_params(
        _case(
            salesforce_id=None,
            subject=None,
            summary=None,
            severity_justification=None,
            resolved_at=None,
        )
    )
    assert "salesforceId" not in p
    assert "subject" not in p
    assert "summary" not in p
    assert "severityJustification" not in p
    assert "resolvedAt" not in p
    # Required ones survive.
    assert p["caseType"] == "lost_signal"
    assert p["status"] == "open"
    assert p["severity"] == "high"


def test_case_params_resolved_at_iso_z_format_when_set() -> None:
    resolved = datetime(2026, 5, 24, 11, 0, 0, tzinfo=UTC)
    p = case_params(_case(resolved_at=resolved))
    assert p["resolvedAt"] == "2026-05-24T11:00:00Z"


@respx.mock
async def test_upsert_case_batch_posts_to_upsert_case_action(
    settings: FoundrySettings,
) -> None:
    route = respx.post(_CASE_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.upsert_case_batch([_case("CASE-A"), _case("CASE-B")])
    assert route.call_count == 1
    body = json.loads(route.calls[0].request.content)
    assert len(body["requests"]) == 2
    assert body["requests"][0]["parameters"]["case"] == "CASE-A"
    assert body["requests"][1]["parameters"]["case"] == "CASE-B"
    assert result.attempted == 2 and result.succeeded == 2


async def test_upsert_case_batch_empty_is_noop(settings: FoundrySettings) -> None:
    """Empty list → no HTTP call (mirrors aircraft/site/flight upsert contracts)."""
    async with FoundryWriter(settings) as w:
        result = await w.upsert_case_batch([])
    assert result.attempted == 0 and result.succeeded == 0


@respx.mock
async def test_delete_case_batch_uses_pascal_case_object_key(
    settings: FoundrySettings,
) -> None:
    """delete-case mirrors delete-aircraft: the single param is the PascalCase
    object-type name ``Case`` (NOT routed through _camel)."""
    route = respx.post(_CASE_DELETE_URL).mock(return_value=httpx.Response(200, json={}))
    async with FoundryWriter(settings) as w:
        result = await w.delete_case_batch(["CASE-1", "CASE-2"])
    assert route.call_count == 1
    body = json.loads(route.calls[0].request.content)
    assert body["requests"] == [
        {"parameters": {"Case": "CASE-1"}},
        {"parameters": {"Case": "CASE-2"}},
    ]
    assert result.succeeded == 2


async def test_delete_case_batch_empty_is_noop(settings: FoundrySettings) -> None:
    async with FoundryWriter(settings) as w:
        result = await w.delete_case_batch([])
    assert result.attempted == 0 and result.succeeded == 0


@respx.mock
async def test_list_case_pks_paginates_and_pulls_case_id(settings: FoundrySettings) -> None:
    route = respx.get(_CASE_OBJECTS_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"caseId": "CASE-1"}, {"caseId": "CASE-2"}],
                    "nextPageToken": "p2",
                },
            ),
            httpx.Response(200, json={"data": [{"caseId": "CASE-3"}]}),
        ]
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_case_pks()
    assert pks == {"CASE-1", "CASE-2", "CASE-3"}
    assert route.call_count == 2


@respx.mock
async def test_list_case_pks_falls_back_to_primary_key(settings: FoundrySettings) -> None:
    respx.get(_CASE_OBJECTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"__primaryKey": "CASE-ALPHA"}, {"caseId": "CASE-BETA"}]},
        )
    )
    async with FoundryWriter(settings) as w:
        pks = await w.list_case_pks()
    assert pks == {"CASE-ALPHA", "CASE-BETA"}
