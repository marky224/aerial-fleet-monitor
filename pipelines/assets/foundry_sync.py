"""Foundry sync assets — push local /v1 state into the Foundry Ontology.

``foundry_positions_sync`` (every 30 s, via a sensor — Dagster schedules
are minute-resolution): reads ``/v1/positions/live`` and upserts the
``Aircraft`` Ontology object. ``foundry_sites_sync`` (every 5 min): full
refresh of the ``Site`` object from ``/v1/sites`` + per-site SLA.

Both assets are an **independent failure domain** from the rest of the
pipeline: AFM's local stack must run with Foundry creds absent or Foundry
unreachable. The orchestration layer (``afm_foundry_sync.sync_jobs``)
raises ``FoundrySyncSkipped`` for exactly those conditions; here it is
translated to a *successful* materialization carrying a ``skip_reason``
(same convention as ``opensky_positions`` handling ``OpenSkyError``), not
a failed run. A genuine defect (e.g. a malformed Action payload) is not a
``FoundrySyncSkipped`` and so still fails the asset loudly.

Cursor: the positions response ``server_time`` is recorded in the asset's
materialization metadata each run and read back as ``since`` on the next
run. The v1 API returns the full live snapshot (no server-side delta), so
``since`` is advisory/logged for now — the mechanism is wired so a future
delta endpoint needs no asset change.

Takeoff detection (``afm_foundry_sync.sync_jobs.TakeoffDetector``) is
intentionally **not** wired here yet: the ``Flight`` Ontology object is
unprovisioned so its write is deferred, and a per-run detector cannot see
cross-run on-ground→airborne edges. The detector and its cross-run state
persistence are co-delivered with the Flight Ontology work.
"""

import asyncio
from datetime import datetime

from afm_foundry_sync.sync_jobs import (
    FoundrySyncSkipped,
    SyncResult,
    run_positions_sync,
    run_sites_sync,
)
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

_CURSOR_KEY = "cursor"


def _prior_cursor(context: AssetExecutionContext) -> datetime | None:
    """Read the previous run's ``server_time`` cursor from this asset's
    latest materialization metadata. Any miss (first run, no metadata,
    unparseable) returns None — the sync then runs without a ``since``.
    """
    try:
        event = context.instance.get_latest_materialization_event(context.asset_key)
        if event is None or event.asset_materialization is None:
            return None
        entry = event.asset_materialization.metadata.get(_CURSOR_KEY)
        if entry is None:
            return None
        return datetime.fromisoformat(str(entry.value))
    except (ValueError, AttributeError) as exc:
        context.log.warning("could not read prior cursor: %s", exc)
        return None


def _skipped(context: AssetExecutionContext, reason: str) -> MaterializeResult:
    context.log.warning("foundry sync skipped: %s", reason)
    return MaterializeResult(
        metadata={
            "attempted": MetadataValue.int(0),
            "succeeded": MetadataValue.int(0),
            "failed": MetadataValue.int(0),
            "skip_reason": MetadataValue.text(reason),
        }
    )


def _result_metadata(result: SyncResult) -> dict[str, MetadataValue]:
    meta: dict[str, MetadataValue] = {
        "attempted": MetadataValue.int(result.attempted),
        "succeeded": MetadataValue.int(result.succeeded),
        "failed": MetadataValue.int(result.failed),
        "takeoffs_detected": MetadataValue.int(result.takeoffs_detected),
    }
    if result.cursor is not None:
        meta[_CURSOR_KEY] = MetadataValue.text(result.cursor.isoformat())
    return meta


@asset(
    group_name="foundry_sync",
    description="Upserts the Aircraft Ontology object from /v1/positions/live.",
    metadata={"target": "Foundry Ontology: Aircraft", "cadence": "30s"},
)
def foundry_positions_sync(context: AssetExecutionContext) -> MaterializeResult:
    since = _prior_cursor(context)
    try:
        result = asyncio.run(run_positions_sync(since=since))
    except FoundrySyncSkipped as exc:
        return _skipped(context, exc.reason)
    return MaterializeResult(metadata=_result_metadata(result))


@asset(
    group_name="foundry_sync",
    description="Full-refresh upsert of the Site Ontology object from /v1/sites + SLA.",
    metadata={"target": "Foundry Ontology: Site", "cadence": "5min"},
)
def foundry_sites_sync(context: AssetExecutionContext) -> MaterializeResult:
    try:
        result = asyncio.run(run_sites_sync())
    except FoundrySyncSkipped as exc:
        return _skipped(context, exc.reason)
    return MaterializeResult(metadata=_result_metadata(result))
