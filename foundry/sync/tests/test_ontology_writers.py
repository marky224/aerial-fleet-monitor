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
    Flight,
    FlightStatusEvent,
    Site,
    SparklinePoint,
    TrailPoint,
)
from afm_foundry_sync.ontology_writers import (
    FoundryWriter,
    _camel,
    aircraft_params,
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
