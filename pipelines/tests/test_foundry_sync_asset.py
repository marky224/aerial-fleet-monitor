"""Dagster wiring tests for the Foundry sync assets.

Verifies the independent-failure-domain contract: ``FoundrySyncSkipped``
becomes a *successful* materialization carrying ``skip_reason`` (not a
failed run), and a normal result surfaces its counts + cursor as metadata.
The sync layer itself is unit-tested in ``foundry/sync``; here we only
test the asset boundary, so ``run_*_sync`` is stubbed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from afm_foundry_sync.sync_jobs import (
    FlightEnrichmentResult,
    FoundrySyncSkipped,
    ReconcileResult,
    SyncResult,
    TakeoffDetector,
)
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


def test_positions_sync_surfaces_flights_written_and_detector_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _ok(**_kw: object) -> SyncResult:
        return SyncResult(
            attempted=5,
            succeeded=5,
            cursor=datetime(2026, 5, 16, 1, 0, 0, tzinfo=UTC),
            takeoffs_detected=2,
            flights_written=2,
            detector_state={"abc123": False, "def456": True},
        )

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _ok)

    md = foundry_sync.foundry_positions_sync(build_asset_context()).metadata or {}
    assert md["takeoffs_detected"].value == 2
    assert md["flights_written"].value == 2
    # detector_state persisted as a JSON string (same access path as cursor).
    assert json.loads(md["detector_state"].value) == {"abc123": False, "def456": True}


def test_positions_sync_seeds_a_detector_into_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The asset must construct + pass a TakeoffDetector (seeded from prior
    metadata) so cross-tick edges are observable."""
    captured: dict[str, object] = {}

    async def _capture(**kw: object) -> SyncResult:
        captured.update(kw)
        return SyncResult(attempted=0, succeeded=0)

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _capture)

    foundry_sync.foundry_positions_sync(build_asset_context())

    assert isinstance(captured.get("detector"), TakeoffDetector)


def test_sites_sync_does_not_emit_detector_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Site sync has no detector — detector_state must be absent (the
    metadata writer guards on None)."""

    async def _ok() -> SyncResult:
        return SyncResult(attempted=1, succeeded=1)

    monkeypatch.setattr(foundry_sync, "run_sites_sync", _ok)

    md = foundry_sync.foundry_sites_sync(build_asset_context()).metadata or {}
    assert "detector_state" not in md
    assert md["flights_written"].value == 0


def test_reconcile_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise() -> ReconcileResult:
        raise FoundrySyncSkipped("reconcile: foundry config absent")

    monkeypatch.setattr(foundry_sync, "run_aircraft_reconcile", _raise)

    result = foundry_sync.foundry_aircraft_reconcile(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert (result.metadata or {})["skip_reason"].value == "reconcile: foundry config absent"


def test_reconcile_success_surfaces_diff_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _ok() -> ReconcileResult:
        return ReconcileResult(live=100, tenant=140, orphans=40, deleted=40)

    monkeypatch.setattr(foundry_sync, "run_aircraft_reconcile", _ok)

    md = foundry_sync.foundry_aircraft_reconcile(build_asset_context()).metadata or {}
    assert md["live"].value == 100
    assert md["tenant"].value == 140
    assert md["orphans"].value == 40
    assert md["deleted"].value == 40
    assert md["skipped_empty_live"].value is False


def test_reconcile_empty_live_skip_surfaces_in_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _empty() -> ReconcileResult:
        return ReconcileResult(live=0, tenant=0, orphans=0, deleted=0, skipped_empty_live=True)

    monkeypatch.setattr(foundry_sync, "run_aircraft_reconcile", _empty)

    md = foundry_sync.foundry_aircraft_reconcile(build_asset_context()).metadata or {}
    assert md["skipped_empty_live"].value is True
    assert md["deleted"].value == 0


def test_real_defect_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-skip exception (a real bug) must propagate, not become a skip."""

    async def _bug(**_kw: object) -> SyncResult:
        raise RuntimeError("malformed action payload")

    monkeypatch.setattr(foundry_sync, "run_positions_sync", _bug)

    with pytest.raises(RuntimeError, match="malformed action payload"):
        foundry_sync.foundry_positions_sync(build_asset_context())


def test_flight_enrichment_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise() -> FlightEnrichmentResult:
        raise FoundrySyncSkipped("flight_enrichment: foundry config absent")

    monkeypatch.setattr(foundry_sync, "run_flight_enrichment", _raise)

    result = foundry_sync.foundry_flight_enrichment(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert (result.metadata or {})["skip_reason"].value == (
        "flight_enrichment: foundry config absent"
    )


def test_flight_enrichment_success_surfaces_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _ok() -> FlightEnrichmentResult:
        return FlightEnrichmentResult(
            tenant_flights=50,
            candidates=30,
            enriched=27,
            skipped_inactive=2,
            fetch_failed=1,
        )

    monkeypatch.setattr(foundry_sync, "run_flight_enrichment", _ok)

    md = foundry_sync.foundry_flight_enrichment(build_asset_context()).metadata or {}
    assert md["tenant_flights"].value == 50
    assert md["candidates"].value == 30
    assert md["enriched"].value == 27
    assert md["skipped_inactive"].value == 2
    assert md["fetch_failed"].value == 1


def test_flight_enrichment_real_defect_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _bug() -> FlightEnrichmentResult:
        raise RuntimeError("malformed flight upsert payload")

    monkeypatch.setattr(foundry_sync, "run_flight_enrichment", _bug)

    with pytest.raises(RuntimeError, match="malformed flight upsert payload"):
        foundry_sync.foundry_flight_enrichment(build_asset_context())
