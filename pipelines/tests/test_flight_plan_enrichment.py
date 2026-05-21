"""Unit tests for flight_plan_enrichment.

The asset's job is to (1) pick stale icao24s from Postgres, (2) fetch
flight history per icao24 from OpenSky, (3) UPSERT into app.flight_plans
with the right ``fetch_status``, and (4) surface per-status counts as
metadata. These tests stub Postgres + OpenSky at the seam (module-level
helpers + a fake opensky object) so the cycle logic — fetch caps, rate
limit short-circuit, error/not_found classification, most-recent
selection — is exercised without DB or network.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from dagster import MaterializeResult, build_asset_context

from pipelines.resources.opensky import (
    OpenSkyAuthError,
    OpenSkyFlight,
    OpenSkyRateLimited,
    OpenSkyServerError,
)

# The package re-exports the `flight_plan_enrichment` *asset* under the
# same name as this *module*, so `from pipelines.assets import
# flight_plan_enrichment` (or `import ... as`) resolves to the asset
# object and shadows the module. import_module returns the real module
# from sys.modules so we can monkeypatch its helpers.
fpe = importlib.import_module("pipelines.assets.flight_plan_enrichment")


def _flight(
    icao24: str = "abc123",
    *,
    first_seen: int = 1_700_000_000,
    last_seen: int = 1_700_010_000,
    dep: str | None = "KSFO",
    arr: str | None = "KJFK",
    callsign: str | None = "UAL245",
) -> OpenSkyFlight:
    return OpenSkyFlight(
        icao24=icao24,
        first_seen=first_seen,
        last_seen=last_seen,
        callsign=callsign,
        est_departure_airport=dep,
        est_arrival_airport=arr,
        departure_airport_candidates_count=1,
        arrival_airport_candidates_count=1,
    )


class _FakeOpenSky:
    """Records calls and returns a per-icao24 scripted response."""

    def __init__(self, responses: dict[str, object]) -> None:
        # values: tuple[OpenSkyFlight, ...] or an Exception instance
        self._responses = responses
        self.calls: list[tuple[str, int, int]] = []

    def fetch_flight_history(
        self,
        icao24: str,
        begin: int,
        end: int,
    ) -> tuple[OpenSkyFlight, ...]:
        self.calls.append((icao24, begin, end))
        outcome = self._responses[icao24]
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, tuple)
        return outcome


@pytest.fixture
def stub_postgres(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Capture upserts; tests provide stale-icao24 list per-call."""
    upserts: list[tuple[str, str]] = []

    def _upsert(_pg: object, icao24: str, _flight: object, fetch_status: str) -> None:
        upserts.append((icao24, fetch_status))

    monkeypatch.setattr(fpe, "_upsert_flight_plan", _upsert)
    return upserts


def _patch_stale(monkeypatch: pytest.MonkeyPatch, icao24s: list[str]) -> None:
    def _select(_pg: object) -> list[str]:
        return icao24s

    monkeypatch.setattr(fpe, "_select_stale_icao24s", _select)


def test_happy_path_success_for_each_stale_icao24(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111", "bbb222"])
    opensky = _FakeOpenSky(
        {
            "aaa111": (_flight("aaa111", dep="KSFO", arr="KJFK"),),
            "bbb222": (_flight("bbb222", dep="KLAX", arr="KSEA"),),
        }
    )

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    assert result.candidates == 2
    assert result.fetch_attempts == 2
    assert result.fetched_success == 2
    assert result.fetched_not_found == 0
    assert result.fetched_error == 0
    assert result.deferred == 0
    assert result.rate_limited is False
    assert stub_postgres == [("aaa111", "success"), ("bbb222", "success")]


def test_empty_stale_set_makes_no_fetches(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, [])
    opensky = _FakeOpenSky({})

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    assert result.candidates == 0
    assert result.fetch_attempts == 0
    assert opensky.calls == []
    assert stub_postgres == []


def test_opensky_404_cached_as_not_found(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111"])
    opensky = _FakeOpenSky({"aaa111": ()})  # 404 surfaces as empty tuple

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    assert result.fetched_not_found == 1
    assert result.fetched_success == 0
    assert stub_postgres == [("aaa111", "not_found")]


def test_transient_opensky_error_cached_as_error_and_cycle_continues(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111", "bbb222"])
    opensky = _FakeOpenSky(
        {
            "aaa111": OpenSkyServerError("boom"),
            "bbb222": (_flight("bbb222"),),
        }
    )

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    assert result.fetched_error == 1
    assert result.fetched_success == 1
    assert stub_postgres == [("aaa111", "error"), ("bbb222", "success")]


def test_rate_limit_breaks_cycle_and_defers_remaining(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111", "bbb222", "ccc333"])
    opensky = _FakeOpenSky(
        {
            "aaa111": (_flight("aaa111"),),
            "bbb222": OpenSkyRateLimited("429"),
            "ccc333": (_flight("ccc333"),),  # should never be called
        }
    )

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    # First succeeded, second 429 short-circuits, third never tried.
    assert result.rate_limited is True
    assert result.fetched_success == 1
    assert result.fetch_attempts == 1
    assert [c[0] for c in opensky.calls] == ["aaa111", "bbb222"]
    assert stub_postgres == [("aaa111", "success")]


def test_auth_error_raises_loudly_does_not_cache(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111"])
    opensky = _FakeOpenSky({"aaa111": OpenSkyAuthError("bad creds")})

    with pytest.raises(OpenSkyAuthError):
        fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    # Caching a misconfigured client every cycle would mask the real
    # failure — the asset must surface a real run failure instead.
    assert stub_postgres == []


def test_per_cycle_cap_defers_excess(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    # MAX_FETCHES_PER_CYCLE = 30. Hand it 35 and assert 5 deferred.
    stale = [f"{i:06x}" for i in range(35)]
    _patch_stale(monkeypatch, stale)
    opensky = _FakeOpenSky({h: (_flight(h),) for h in stale})

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)

    assert result.candidates == 35
    assert result.fetch_attempts == fpe.MAX_FETCHES_PER_CYCLE
    assert result.deferred == 35 - fpe.MAX_FETCHES_PER_CYCLE


def test_most_recent_flight_wins_when_multiple_returned(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    _patch_stale(monkeypatch, ["aaa111"])
    older = _flight("aaa111", first_seen=1_700_000_000, last_seen=1_700_010_000, dep="KORD")
    newer = _flight("aaa111", first_seen=1_700_020_000, last_seen=1_700_030_000, dep="KSFO")
    opensky = _FakeOpenSky({"aaa111": (older, newer)})  # newer last_seen

    chosen = fpe._pick_most_recent((older, newer))
    assert chosen is newer  # sanity-check the helper directly

    result = fpe.run_flight_plan_enrichment(build_asset_context(), object(), opensky)
    assert result.fetched_success == 1
    assert stub_postgres == [("aaa111", "success")]


def test_asset_returns_materializeresult_with_metadata(
    monkeypatch: pytest.MonkeyPatch, stub_postgres: list[tuple[str, str]]
) -> None:
    # Confirm the @asset wrapper assembles metadata correctly. We bypass
    # the resource injection by stubbing run_flight_plan_enrichment.
    fake_result = fpe.EnrichmentResult(
        candidates=5,
        fetch_attempts=3,
        fetched_success=2,
        fetched_not_found=1,
        fetched_error=0,
        rate_limited=False,
        deferred=2,
    )
    monkeypatch.setattr(fpe, "run_flight_plan_enrichment", lambda *_a, **_kw: fake_result)

    out = fpe.flight_plan_enrichment(
        build_asset_context(),
        postgres=SimpleNamespace(),  # type: ignore[arg-type]
        opensky=SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["candidates"].value == 5
    assert md["fetched_success"].value == 2
    assert md["fetched_not_found"].value == 1
    assert md["deferred"].value == 2
    assert md["rate_limited"].value is False
