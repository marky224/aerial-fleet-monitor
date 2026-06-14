"""Unit tests for the OpenSky REST resource (Phase 10).

Exercises the parser + auth/retry layer of ``OpenSkyResource`` without a
network:

* the pure ``_parse_response`` / ``_parse_flights_response`` / ``_row_to_state``
  static methods get a hand-built fake ``requests.Response``; and
* ``fetch_states`` / ``fetch_flight_history`` / ``_get_with_retry`` /
  ``_fetch_token`` get a scripted stand-in for ``requests.get`` /
  ``requests.post`` (monkeypatched on the module) so every status-code branch —
  200, 401-refresh, 429, 5xx-retry, network-error-retry, 404 — runs offline.

``time.sleep`` is stubbed module-wide so the 2-second retry backoffs don't slow
the suite. Token-dependent paths pre-seed the private token cache so they don't
also have to script the token POST.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from pipelines.resources import opensky as opensky_mod
from pipelines.resources.opensky import (
    OpenSkyAuthError,
    OpenSkyError,
    OpenSkyParseError,
    OpenSkyRateLimited,
    OpenSkyResource,
    OpenSkyServerError,
)


def _state_row(icao24: str = "abc123", callsign: str | None = "UAL245 ") -> list[Any]:
    """A valid 17-field ``/states/all`` row in OpenSky's native column order."""
    return [
        icao24,  # 0  icao24
        callsign,  # 1  callsign (space-padded in the API)
        "United States",  # 2  origin_country
        1_700_000_000,  # 3  time_position
        1_700_000_005,  # 4  last_contact
        -118.4,  # 5  lon
        33.9,  # 6  lat
        10000.0,  # 7  baro_altitude_m
        False,  # 8  on_ground
        220.0,  # 9  velocity_ms
        90.0,  # 10 true_track
        0.0,  # 11 vertical_rate
        None,  # 12 sensors (unused)
        10500.0,  # 13 geo_altitude_m
        "1200",  # 14 squawk
        False,  # 15 spi
        0,  # 16 position_source
    ]


def _flight_dict(**overrides: Any) -> dict[str, Any]:
    """A valid ``/flights/aircraft`` row dict."""
    base: dict[str, Any] = {
        "icao24": "ABC123",
        "firstSeen": 1_700_000_000,
        "lastSeen": 1_700_003_600,
        "callsign": "UAL245 ",
        "estDepartureAirport": "KSFO",
        "estArrivalAirport": "KJFK",
        "departureAirportCandidatesCount": 2,
        "arrivalAirportCandidatesCount": 3,
    }
    base.update(overrides)
    return base


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` (only what the code reads)."""

    def __init__(
        self,
        *,
        status_code: int = 200,
        json_data: Any = None,
        json_exc: Exception | None = None,
        headers: dict[str, str] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self._json_exc = json_exc
        self.headers = headers or {}
        self.text = text

    def json(self) -> Any:
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_data


class _ScriptedHTTP:
    """Callable replacing ``requests.get``/``requests.post``; pops scripted outcomes.

    Each outcome is either a ``_FakeResponse`` to return or a ``BaseException``
    to raise (to simulate a network failure on that attempt).
    """

    def __init__(self, outcomes: list[Any]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, *_args: Any, **kwargs: Any) -> _FakeResponse:
        self.calls.append(kwargs)
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """The retry paths call ``time.sleep(2.0)``; make them instant."""
    monkeypatch.setattr(opensky_mod.time, "sleep", lambda _s: None)


def _resource() -> OpenSkyResource:
    return OpenSkyResource(client_id="cid", client_secret="secret")


def _resource_with_token() -> OpenSkyResource:
    """A resource with a pre-cached, non-expiring token (skips the token POST)."""
    res = _resource()
    res._cached_token = "cached-token"
    res._cached_token_expires_at = opensky_mod.time.time() + 3600
    return res


# ---------------------------------------------------------------------------
# _parse_response (pure)
# ---------------------------------------------------------------------------


def test_parse_response_happy() -> None:
    resp = _FakeResponse(
        json_data={"time": 1_700_000_010, "states": [_state_row()]},
        headers={"X-Rate-Limit-Remaining": "3950"},
    )
    parsed = OpenSkyResource._parse_response(resp)
    assert parsed.api_time == 1_700_000_010
    assert parsed.credits_used == 4
    assert parsed.rate_limit_remaining == 3950
    assert parsed.http_status == 200
    assert len(parsed.states) == 1
    state = parsed.states[0]
    assert state.icao24 == "abc123"
    assert state.callsign == "UAL245"  # trimmed
    assert state.on_ground is False
    assert state.squawk == "1200"


@pytest.mark.parametrize(
    "header,expected",
    [("3950", 3950), ("-5", -5), ("notanumber", None), (None, None)],
)
def test_parse_response_rate_limit_header(header: str | None, expected: int | None) -> None:
    headers = {"X-Rate-Limit-Remaining": header} if header is not None else {}
    resp = _FakeResponse(json_data={"time": 1, "states": []}, headers=headers)
    assert OpenSkyResource._parse_response(resp).rate_limit_remaining == expected


def test_parse_response_skips_malformed_rows() -> None:
    rows = [
        _state_row("aaa111"),  # good
        ["short", "row"],  # too few fields → skipped
        "not-a-list",  # non-list → skipped
        _state_row(""),  # empty icao24 → skipped
        [None, *_state_row()[1:]],  # non-str icao24 → skipped
    ]
    parsed = OpenSkyResource._parse_response(_FakeResponse(json_data={"time": 1, "states": rows}))
    assert [s.icao24 for s in parsed.states] == ["aaa111"]


def test_parse_response_blank_callsign_becomes_none() -> None:
    resp = _FakeResponse(json_data={"time": 1, "states": [_state_row(callsign="   ")]})
    assert OpenSkyResource._parse_response(resp).states[0].callsign is None


@pytest.mark.parametrize(
    "resp",
    [
        _FakeResponse(json_exc=ValueError("bad json")),  # non-JSON body
        _FakeResponse(json_data=["not", "a", "dict"]),  # payload not a dict
        _FakeResponse(json_data={"states": []}),  # missing "time"
        _FakeResponse(json_data={"time": "not-int", "states": []}),  # time → ValueError
        _FakeResponse(json_data={"time": None, "states": []}),  # time → TypeError
        _FakeResponse(json_data={"time": 1, "states": {"not": "a list"}}),  # states not a list
    ],
)
def test_parse_response_malformed_raises(resp: _FakeResponse) -> None:
    with pytest.raises(OpenSkyParseError):
        OpenSkyResource._parse_response(resp)


# ---------------------------------------------------------------------------
# _parse_flights_response (pure)
# ---------------------------------------------------------------------------


def test_parse_flights_response_happy() -> None:
    flights = OpenSkyResource._parse_flights_response(_FakeResponse(json_data=[_flight_dict()]))
    assert len(flights) == 1
    flight = flights[0]
    assert flight.icao24 == "abc123"  # lowercased
    assert flight.callsign == "UAL245"  # trimmed
    assert flight.est_departure_airport == "KSFO"
    assert flight.est_arrival_airport == "KJFK"
    assert flight.departure_airport_candidates_count == 2
    assert flight.arrival_airport_candidates_count == 3


def test_parse_flights_response_defaults_and_filtering() -> None:
    rows = [
        _flight_dict(
            callsign=None,
            estDepartureAirport=None,
            estArrivalAirport=None,
            departureAirportCandidatesCount="x",  # non-numeric → 0
            arrivalAirportCandidatesCount=None,  # missing-ish → 0
        ),
        "not-a-dict",  # skipped
        _flight_dict(icao24=""),  # empty icao24 → skipped
        _flight_dict(firstSeen="nope"),  # non-numeric firstSeen → skipped
        _flight_dict(lastSeen=None),  # non-numeric lastSeen → skipped
    ]
    flights = OpenSkyResource._parse_flights_response(_FakeResponse(json_data=rows))
    assert len(flights) == 1
    flight = flights[0]
    assert flight.callsign is None
    assert flight.est_departure_airport is None
    assert flight.est_arrival_airport is None
    assert flight.departure_airport_candidates_count == 0
    assert flight.arrival_airport_candidates_count == 0


def test_parse_flights_response_empty_list() -> None:
    assert OpenSkyResource._parse_flights_response(_FakeResponse(json_data=[])) == ()


@pytest.mark.parametrize(
    "resp",
    [
        _FakeResponse(json_exc=ValueError("bad")),  # non-JSON
        _FakeResponse(json_data={"not": "a list"}),  # payload not a list
    ],
)
def test_parse_flights_response_malformed_raises(resp: _FakeResponse) -> None:
    with pytest.raises(OpenSkyParseError):
        OpenSkyResource._parse_flights_response(resp)


# ---------------------------------------------------------------------------
# _get_token / _fetch_token (mocked requests.post)
# ---------------------------------------------------------------------------


def test_get_token_caches_after_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    post = _ScriptedHTTP([_FakeResponse(json_data={"access_token": "tok-1", "expires_in": 3600})])
    monkeypatch.setattr(opensky_mod.requests, "post", post)
    res = _resource()
    assert res._get_token() == "tok-1"
    assert res._get_token() == "tok-1"  # cached → no second POST
    assert len(post.calls) == 1


@pytest.mark.parametrize("status", [400, 401, 403])
def test_fetch_token_bad_credentials(monkeypatch: pytest.MonkeyPatch, status: int) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "post", _ScriptedHTTP([_FakeResponse(status_code=status, text="no")])
    )
    with pytest.raises(OpenSkyAuthError):
        _resource()._get_token()


def test_fetch_token_server_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "post", _ScriptedHTTP([_FakeResponse(status_code=500, text="boom")])
    )
    with pytest.raises(OpenSkyServerError):
        _resource()._get_token()


def test_fetch_token_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "post", _ScriptedHTTP([requests.RequestException("conn reset")])
    )
    with pytest.raises(OpenSkyServerError):
        _resource()._get_token()


def test_fetch_token_non_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "post", _ScriptedHTTP([_FakeResponse(json_exc=ValueError("nope"))])
    )
    with pytest.raises(OpenSkyParseError):
        _resource()._get_token()


@pytest.mark.parametrize(
    "body",
    [
        {"expires_in": 3600},  # missing access_token
        {"access_token": "", "expires_in": 3600},  # empty access_token
        {"access_token": "tok"},  # missing expires_in
        {"access_token": "tok", "expires_in": 0},  # non-positive expires_in
        {"access_token": "tok", "expires_in": "x"},  # non-numeric expires_in
    ],
)
def test_fetch_token_malformed_payload(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "post", _ScriptedHTTP([_FakeResponse(json_data=body)])
    )
    with pytest.raises(OpenSkyParseError):
        _resource()._get_token()


# ---------------------------------------------------------------------------
# _get_with_retry (mocked requests.get; token pre-seeded)
# ---------------------------------------------------------------------------


def _states_ok() -> _FakeResponse:
    return _FakeResponse(json_data={"time": 1, "states": []})


def test_get_with_retry_200(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(opensky_mod.requests, "get", _ScriptedHTTP([_states_ok()]))
    resp = _resource_with_token()._get_with_retry("http://x/states/all", params={})
    assert resp.status_code == 200


@pytest.mark.parametrize(
    "status,exc",
    [(401, OpenSkyAuthError), (403, OpenSkyAuthError), (429, OpenSkyRateLimited)],
)
def test_get_with_retry_auth_and_ratelimit(
    monkeypatch: pytest.MonkeyPatch, status: int, exc: type[OpenSkyError]
) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(status_code=status, text="x")])
    )
    with pytest.raises(exc):
        _resource_with_token()._get_with_retry("http://x", params={})


def test_get_with_retry_5xx_then_200(monkeypatch: pytest.MonkeyPatch) -> None:
    http = _ScriptedHTTP([_FakeResponse(status_code=503, text="x"), _states_ok()])
    monkeypatch.setattr(opensky_mod.requests, "get", http)
    resp = _resource_with_token()._get_with_retry("http://x", params={})
    assert resp.status_code == 200
    assert len(http.calls) == 2  # retried after the 503


def test_get_with_retry_5xx_twice_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests,
        "get",
        _ScriptedHTTP([_FakeResponse(status_code=500), _FakeResponse(status_code=502)]),
    )
    with pytest.raises(OpenSkyServerError):
        _resource_with_token()._get_with_retry("http://x", params={})


def test_get_with_retry_network_then_200(monkeypatch: pytest.MonkeyPatch) -> None:
    http = _ScriptedHTTP([requests.RequestException("reset"), _states_ok()])
    monkeypatch.setattr(opensky_mod.requests, "get", http)
    resp = _resource_with_token()._get_with_retry("http://x", params={})
    assert resp.status_code == 200


def test_get_with_retry_network_twice_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests,
        "get",
        _ScriptedHTTP([requests.RequestException("a"), requests.RequestException("b")]),
    )
    with pytest.raises(OpenSkyServerError):
        _resource_with_token()._get_with_retry("http://x", params={})


def test_get_with_retry_unexpected_status_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(status_code=418, text="teapot")])
    )
    with pytest.raises(OpenSkyError):
        _resource_with_token()._get_with_retry("http://x", params={})


def test_get_with_retry_404_not_found_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(status_code=404)])
    )
    resp = _resource_with_token()._get_with_retry("http://x", params={}, not_found_ok=True)
    assert resp.status_code == 404


def test_get_with_retry_404_without_not_found_ok_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(status_code=404, text="gone")])
    )
    with pytest.raises(OpenSkyError):
        _resource_with_token()._get_with_retry("http://x", params={})


# ---------------------------------------------------------------------------
# fetch_states / fetch_flight_history (full path; token pre-seeded)
# ---------------------------------------------------------------------------


def test_fetch_states_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests,
        "get",
        _ScriptedHTTP([_FakeResponse(json_data={"time": 7, "states": [_state_row()]})]),
    )
    out = _resource_with_token().fetch_states()
    assert out.api_time == 7
    assert len(out.states) == 1


def test_fetch_states_401_then_refresh_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mid-flight 401 clears the cached token, re-fetches it, and retries once."""
    res = _resource_with_token()
    get = _ScriptedHTTP(
        [
            _FakeResponse(status_code=401, text="expired"),
            _FakeResponse(json_data={"time": 9, "states": []}),
        ]
    )
    post = _ScriptedHTTP([_FakeResponse(json_data={"access_token": "fresh", "expires_in": 3600})])
    monkeypatch.setattr(opensky_mod.requests, "get", get)
    monkeypatch.setattr(opensky_mod.requests, "post", post)
    out = res.fetch_states()
    assert out.api_time == 9
    assert res._cached_token == "fresh"  # token was refreshed after the 401
    assert len(post.calls) == 1  # only the post-clear refresh hit the token endpoint


def test_fetch_flight_history_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(json_data=[_flight_dict()])])
    )
    flights = _resource_with_token().fetch_flight_history("ABC123", 1_700_000_000, 1_700_003_600)
    assert len(flights) == 1
    assert flights[0].icao24 == "abc123"


def test_fetch_flight_history_404_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        opensky_mod.requests, "get", _ScriptedHTTP([_FakeResponse(status_code=404)])
    )
    assert _resource_with_token().fetch_flight_history("abc123", 1, 2) == ()
