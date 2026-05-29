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
from types import SimpleNamespace

import pytest
from afm_foundry_sync.sync_jobs import (
    CaseSyncResult,
    FlightEnrichmentResult,
    FlightReconcileResult,
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


def test_flight_enrichment_overlap_guard_skips_when_sibling_run_in_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The hourly schedule must not stack a second run on a slow one: if
    another run of the job is in progress, the tick is a coalesced skip
    (surfaced via skip_reason) and the enrichment body never runs."""

    async def _must_not_run() -> FlightEnrichmentResult:
        raise AssertionError("enrichment ran despite an in-progress sibling")

    monkeypatch.setattr(foundry_sync, "run_flight_enrichment", _must_not_run)
    ctx = build_asset_context()
    sibling = SimpleNamespace(dagster_run=SimpleNamespace(run_id="a-different-run"))
    monkeypatch.setattr(ctx.instance, "get_run_records", lambda *a, **k: [sibling])

    result = foundry_sync.foundry_flight_enrichment(ctx)

    assert isinstance(result, MaterializeResult)
    assert "already in progress" in (result.metadata or {})["skip_reason"].value


def test_flight_enrichment_overlap_guard_ignores_own_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard must exclude the current run itself (which is STARTED while
    executing) — otherwise every run would skip itself."""

    async def _ok() -> FlightEnrichmentResult:
        return FlightEnrichmentResult(
            tenant_flights=1,
            candidates=1,
            enriched=1,
            skipped_inactive=0,
            fetch_failed=0,
        )

    monkeypatch.setattr(foundry_sync, "run_flight_enrichment", _ok)
    ctx = build_asset_context()
    own = SimpleNamespace(dagster_run=SimpleNamespace(run_id=ctx.run_id))
    monkeypatch.setattr(ctx.instance, "get_run_records", lambda *a, **k: [own])

    md = foundry_sync.foundry_flight_enrichment(ctx).metadata or {}
    assert md["enriched"].value == 1  # proceeded — own run is not "another"


# ---------------------------------------------------------------------------
# foundry_flight_reconcile (Phase A — Flight-side eviction)
# ---------------------------------------------------------------------------


def test_flight_reconcile_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise() -> FlightReconcileResult:
        raise FoundrySyncSkipped("flight_reconcile: foundry config absent")

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _raise)

    result = foundry_sync.foundry_flight_reconcile(build_asset_context())

    assert isinstance(result, MaterializeResult)
    assert (result.metadata or {})["skip_reason"].value == "flight_reconcile: foundry config absent"


def test_flight_reconcile_success_surfaces_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _ok() -> FlightReconcileResult:
        return FlightReconcileResult(
            live_airborne=4890,
            tenant=85000,
            keep=3700,
            orphans=81300,
            deleted=5000,
            remaining=76300,
        )

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _ok)

    md = foundry_sync.foundry_flight_reconcile(build_asset_context()).metadata or {}
    assert md["live_airborne"].value == 4890
    assert md["tenant"].value == 85000
    assert md["keep"].value == 3700
    assert md["orphans"].value == 81300
    assert md["deleted"].value == 5000
    assert md["remaining"].value == 76300
    assert md["skipped_empty_live"].value is False


def test_flight_reconcile_empty_live_skip_surfaces_in_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _empty() -> FlightReconcileResult:
        return FlightReconcileResult(
            live_airborne=0, tenant=0, keep=0, orphans=0, deleted=0, skipped_empty_live=True
        )

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _empty)

    md = foundry_sync.foundry_flight_reconcile(build_asset_context()).metadata or {}
    assert md["skipped_empty_live"].value is True
    assert md["deleted"].value == 0


def test_flight_reconcile_real_defect_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _bug() -> FlightReconcileResult:
        raise RuntimeError("malformed delete-flight payload")

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _bug)

    with pytest.raises(RuntimeError, match="malformed delete-flight payload"):
        foundry_sync.foundry_flight_reconcile(build_asset_context())


def test_flight_reconcile_overlap_guard_skips_when_sibling_run_in_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The per-run drain cap means the first reconcile spans several hourly
    runs; a tick must not stack on a still-draining sibling."""

    async def _must_not_run() -> FlightReconcileResult:
        raise AssertionError("reconcile ran despite an in-progress sibling")

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _must_not_run)
    ctx = build_asset_context()
    sibling = SimpleNamespace(dagster_run=SimpleNamespace(run_id="a-different-run"))
    monkeypatch.setattr(ctx.instance, "get_run_records", lambda *a, **k: [sibling])

    result = foundry_sync.foundry_flight_reconcile(ctx)

    assert isinstance(result, MaterializeResult)
    assert "already in progress" in (result.metadata or {})["skip_reason"].value


def test_flight_reconcile_overlap_guard_ignores_own_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard must exclude the current run itself (STARTED while executing)."""

    async def _ok() -> FlightReconcileResult:
        return FlightReconcileResult(
            live_airborne=10, tenant=20, keep=12, orphans=8, deleted=8, remaining=0
        )

    monkeypatch.setattr(foundry_sync, "run_flight_reconcile", _ok)
    ctx = build_asset_context()
    own = SimpleNamespace(dagster_run=SimpleNamespace(run_id=ctx.run_id))
    monkeypatch.setattr(ctx.instance, "get_run_records", lambda *a, **k: [own])

    md = foundry_sync.foundry_flight_reconcile(ctx).metadata or {}
    assert md["deleted"].value == 8  # proceeded — own run is not "another"


# ---------------------------------------------------------------------------
# foundry_cases_sync (Phase 05 task #5)
# ---------------------------------------------------------------------------


def test_cases_sync_skip_is_a_materialization_not_a_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _raise(**_kw: object) -> CaseSyncResult:
        raise FoundrySyncSkipped("cases: foundry/api unreachable: boom")

    monkeypatch.setattr(foundry_sync, "run_cases_sync", _raise)

    result = foundry_sync.foundry_cases_sync(build_asset_context())

    assert isinstance(result, MaterializeResult)
    md = result.metadata or {}
    assert md["skip_reason"].value.startswith("cases: foundry/api unreachable")
    assert md["attempted"].value == 0
    assert md["succeeded"].value == 0


def test_cases_sync_success_surfaces_counts_and_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = datetime(2026, 5, 24, 10, 5, 0, tzinfo=UTC)

    async def _ok(**_kw: object) -> CaseSyncResult:
        return CaseSyncResult(attempted=3, succeeded=3, failed=0, cursor=cursor)

    monkeypatch.setattr(foundry_sync, "run_cases_sync", _ok)

    md = foundry_sync.foundry_cases_sync(build_asset_context()).metadata or {}
    assert md["attempted"].value == 3
    assert md["succeeded"].value == 3
    assert md["failed"].value == 0
    assert "skip_reason" not in md
    # Cursor persisted under the same key ``_prior_cursor`` reads, so the
    # next tick picks it up as ``since`` automatically.
    assert md["cursor"].value == cursor.isoformat()


def test_cases_sync_empty_run_omits_cursor_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty initial run (no prior cursor, no new rows) → no cursor metadata
    (the writer guards on None so the next tick still seeds from None)."""

    async def _ok(**_kw: object) -> CaseSyncResult:
        return CaseSyncResult(attempted=0, succeeded=0, failed=0, cursor=None)

    monkeypatch.setattr(foundry_sync, "run_cases_sync", _ok)

    md = foundry_sync.foundry_cases_sync(build_asset_context()).metadata or {}
    assert "cursor" not in md
    assert md["attempted"].value == 0


def test_cases_sync_passes_prior_cursor_as_since(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The asset must seed `since` from the prior materialization's cursor."""
    captured: dict[str, object] = {}

    async def _capture(**kw: object) -> CaseSyncResult:
        captured.update(kw)
        return CaseSyncResult(attempted=0, succeeded=0)

    monkeypatch.setattr(foundry_sync, "run_cases_sync", _capture)
    ctx = build_asset_context()
    # Stub _prior_cursor to return a known value (same indirection the
    # positions tests would use — keeps this test free of instance-DB setup).
    prior = datetime(2026, 5, 24, 9, 30, 0, tzinfo=UTC)
    monkeypatch.setattr(foundry_sync, "_prior_cursor", lambda _ctx: prior)

    foundry_sync.foundry_cases_sync(ctx)

    assert captured.get("since") == prior


def test_cases_sync_real_defect_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-skip exception (a real bug) must propagate, not become a skip."""

    async def _bug(**_kw: object) -> CaseSyncResult:
        raise RuntimeError("malformed case upsert payload")

    monkeypatch.setattr(foundry_sync, "run_cases_sync", _bug)

    with pytest.raises(RuntimeError, match="malformed case upsert payload"):
        foundry_sync.foundry_cases_sync(build_asset_context())
