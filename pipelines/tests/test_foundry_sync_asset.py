"""Dagster wiring tests for the Foundry sync assets.

Verifies the independent-failure-domain contract: ``FoundrySyncSkipped``
becomes a *successful* materialization carrying ``skip_reason`` (not a
failed run), and a normal result surfaces its counts + cursor as metadata.
The sync layer itself is unit-tested in ``foundry/sync``; here we only
test the asset boundary, so ``run_*_sync`` is stubbed.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from afm_foundry_sync.sync_jobs import FoundrySyncSkipped, SyncResult
from dagster import MaterializeResult, build_asset_context

from pipelines.assets import foundry_sync


def test_positions_sync_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise(**_kw: object) -> SyncResult:
        raise FoundrySyncSkipped("positions: foundry config absent")

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _raise)

    result = foundry_sync.foundry_positions_sync(build_asset_context())

    assert isinstance(result, MaterializeResult)
    md = result.metadata or {}
    assert md["skip_reason"].value == "positions: foundry config absent"
    assert md["attempted"].value == 0
    assert md["succeeded"].value == 0


def test_sites_sync_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise() -> SyncResult:
        raise FoundrySyncSkipped("sites: foundry/api unreachable: boom")

    monkeypatch.setattr(foundry_sync, "run_sites_sync", _raise)

    result = foundry_sync.foundry_sites_sync(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert (result.metadata or {})["skip_reason"].value.startswith("sites: foundry/api unreachable")


def test_positions_sync_success_surfaces_counts_and_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = datetime(2026, 5, 15, 12, 0, 30, tzinfo=UTC)

    async def _ok(**_kw: object) -> SyncResult:
        return SyncResult(attempted=3, succeeded=3, failed=0, cursor=cursor)

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _ok)

    result = foundry_sync.foundry_positions_sync(build_asset_context())

    assert isinstance(result, MaterializeResult)
    md = result.metadata or {}
    assert md["attempted"].value == 3
    assert md["succeeded"].value == 3
    assert "skip_reason" not in md
    assert md["cursor"].value == cursor.isoformat()


def test_real_defect_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-skip exception (a real bug) must propagate, not become a skip."""

    async def _bug(**_kw: object) -> SyncResult:
        raise RuntimeError("malformed action payload")

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _bug)

    with pytest.raises(RuntimeError, match="malformed action payload"):
        foundry_sync.foundry_positions_sync(build_asset_context())
