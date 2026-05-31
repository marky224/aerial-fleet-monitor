"""Tests for the sf_case_push asset wrapper.

The HTTP call (``push_pending_cases``) is stubbed at the module seam so
the asset's metadata mapping + skip-on-unreachable behaviour are verified
without a live API.
"""

from __future__ import annotations

import httpx
from dagster import AssetCheckSeverity, MaterializeResult, build_asset_context

from pipelines.assets import sync


def test_sf_case_push_reports_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        sync,
        "push_pending_cases",
        lambda _base, _limit: {"attempted": 3, "synced": 2, "retrying": 1, "failed": 0},
    )
    out = sync.sf_case_push(build_asset_context())
    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["skipped"].value is False
    assert md["attempted"].value == 3
    assert md["synced"].value == 2
    assert md["retrying"].value == 1
    assert md["failed"].value == 0


def test_sf_case_push_skips_when_api_unreachable(monkeypatch) -> None:
    def boom(_base: str, _limit: int) -> dict[str, int]:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(sync, "push_pending_cases", boom)
    out = sync.sf_case_push(build_asset_context())
    md = out.metadata or {}
    assert md["skipped"].value is True
    assert "connection refused" in md["skip_reason"].value


# -- sf_push_not_failing asset check --------------------------------------


class _FakeCursor:
    def __init__(self, row: tuple[int, int]) -> None:
        self._row = row

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False

    def execute(self, *_a: object, **_k: object) -> None:
        pass

    def fetchone(self) -> tuple[int, int]:
        return self._row


class _FakeConn:
    def __init__(self, row: tuple[int, int]) -> None:
        self._row = row

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_a: object) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._row)


class _FakePostgres:
    """Returns a fixed (failed, synced) row for the check's COUNT query."""

    def __init__(self, failed: int, synced: int) -> None:
        self._row = (failed, synced)

    def get_conn(self) -> _FakeConn:
        return _FakeConn(self._row)


def test_sf_push_check_passes_when_healthy() -> None:
    res = sync.sf_push_not_failing(_FakePostgres(failed=0, synced=100))  # type: ignore[arg-type]
    assert res.passed is True
    assert res.metadata["fail_rate"].value == 0.0


def test_sf_push_check_fails_on_high_failure_rate() -> None:
    res = sync.sf_push_not_failing(_FakePostgres(failed=50, synced=0))  # type: ignore[arg-type]
    assert res.passed is False
    assert res.severity == AssetCheckSeverity.ERROR


def test_sf_push_check_ignores_small_sample() -> None:
    # below _HEALTH_MIN_SAMPLE: a tiny window must not flap red even at 100%.
    res = sync.sf_push_not_failing(_FakePostgres(failed=5, synced=0))  # type: ignore[arg-type]
    assert res.passed is True


def test_sf_push_check_passes_below_threshold() -> None:
    # 30/100 = 30% < 50% fail-rate threshold.
    res = sync.sf_push_not_failing(_FakePostgres(failed=30, synced=70))  # type: ignore[arg-type]
    assert res.passed is True
