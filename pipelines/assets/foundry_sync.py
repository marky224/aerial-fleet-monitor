"""Foundry sync assets — push local /v1 state into the Foundry Ontology.

``foundry_positions_sync`` (every 30 s, via a sensor — Dagster schedules
are minute-resolution): reads ``/v1/positions/live`` and upserts the
``Aircraft`` Ontology object. ``foundry_sites_sync`` (every 5 min): full
refresh of the ``Site`` object from ``/v1/sites`` + per-site SLA.
``foundry_aircraft_reconcile`` (hourly, Fix C): the positions sync is
upsert-only and never deletes, so departed aircraft persist in the
Ontology forever — this evicts the Aircraft objects whose icao24 is no
longer in the live feed (with an empty-live safety guard; see
``afm_foundry_sync.sync_jobs.reconcile_aircraft``). It mirrors the
``prune_stale_positions`` (Fix B) pattern on the Foundry side.
``foundry_flight_enrichment`` (hourly, offset to :30 so it does not
collide with the top-of-hour reconcile): the takeoff path writes Flight
objects with only their synthesized identity, so this backfills
route/operator/registration/status + 2h trail from ``/v1/flights`` for
the latest flight per icao24 (see
``afm_foundry_sync.sync_jobs.enriched_sync_flights``).

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
wired into ``foundry_positions_sync``: a detected on-ground→airborne edge
triggers a create-only ``Flight`` upsert. The detector is stateful across
ticks but a per-process instance would reset on restart and miss every
cross-restart edge, so its on-ground map is persisted to this asset's
materialization metadata (same mechanism as the cursor, stored as a JSON
string) and a fresh ``TakeoffDetector`` is seeded from it each run.
FlightDetail enrichment runs out-of-band in ``foundry_flight_enrichment``
(the per-icao24 ``/v1/flights`` fanout — out of scope for the 30s tick).
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta

from afm_foundry_sync.sync_jobs import (
    CaseSyncResult,
    FlightEnrichmentResult,
    FoundrySyncSkipped,
    ReconcileResult,
    SyncResult,
    TakeoffDetector,
    run_aircraft_reconcile,
    run_cases_sync,
    run_flight_enrichment,
    run_positions_sync,
    run_sites_sync,
)
from dagster import (
    AssetExecutionContext,
    DagsterRunStatus,
    MaterializeResult,
    MetadataValue,
    RunsFilter,
    asset,
)

_CURSOR_KEY = "cursor"
_DETECTOR_STATE_KEY = "detector_state"

# Overlap guard liveness window. A real enrichment run emits step events
# every few seconds, so its ``update_timestamp`` stays fresh; a STARTED
# row whose update_timestamp is older than this is a zombie left by a
# killed process (container restart, OOM, host reboot) and must not be
# allowed to coalesce every subsequent hourly tick. Set well above the
# observed worst-case runtime (~56 min, see PR #19 trail-batch handoff).
_ENRICHMENT_LIVENESS_WINDOW = timedelta(minutes=90)


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


def _prior_detector_state(context: AssetExecutionContext) -> dict[str, bool] | None:
    """Read the previous run's TakeoffDetector on-ground map (JSON string)
    from this asset's latest materialization metadata. Any miss returns
    None — the detector then starts empty (first sightings only seed).
    """
    try:
        event = context.instance.get_latest_materialization_event(context.asset_key)
        if event is None or event.asset_materialization is None:
            return None
        entry = event.asset_materialization.metadata.get(_DETECTOR_STATE_KEY)
        if entry is None:
            return None
        raw = json.loads(str(entry.value))
        if not isinstance(raw, dict):
            return None
        return {str(k): bool(v) for k, v in raw.items()}
    except (ValueError, AttributeError) as exc:
        context.log.warning("could not read prior detector state: %s", exc)
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
        "flights_written": MetadataValue.int(result.flights_written),
    }
    if result.cursor is not None:
        meta[_CURSOR_KEY] = MetadataValue.text(result.cursor.isoformat())
    if result.detector_state is not None:
        # JSON string (not MetadataValue.json) so read-back uses the same
        # entry.value access path as the cursor — no Dagster-version risk.
        meta[_DETECTOR_STATE_KEY] = MetadataValue.text(json.dumps(result.detector_state))
    return meta


@asset(
    group_name="foundry_sync",
    description="Upserts the Aircraft Ontology object from /v1/positions/live.",
    metadata={"target": "Foundry Ontology: Aircraft", "cadence": "30s"},
)
def foundry_positions_sync(context: AssetExecutionContext) -> MaterializeResult:
    since = _prior_cursor(context)
    detector = TakeoffDetector(_prior_detector_state(context))
    try:
        result = asyncio.run(run_positions_sync(since=since, detector=detector))
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


def _reconcile_metadata(result: ReconcileResult) -> dict[str, MetadataValue]:
    return {
        "live": MetadataValue.int(result.live),
        "tenant": MetadataValue.int(result.tenant),
        "orphans": MetadataValue.int(result.orphans),
        "deleted": MetadataValue.int(result.deleted),
        "skipped_empty_live": MetadataValue.bool(result.skipped_empty_live),
    }


@asset(
    group_name="foundry_sync",
    description=(
        "Evicts Aircraft Ontology objects no longer in /v1/positions/live "
        "(Fix C — the upsert-only positions sync never deletes)."
    ),
    metadata={"target": "Foundry Ontology: Aircraft", "cadence": "hourly"},
)
def foundry_aircraft_reconcile(context: AssetExecutionContext) -> MaterializeResult:
    try:
        result = asyncio.run(run_aircraft_reconcile())
    except FoundrySyncSkipped as exc:
        return _skipped(context, exc.reason)
    if result.skipped_empty_live:
        context.log.warning("reconcile skipped: live feed empty (fleet unknown — not evicting)")
    else:
        context.log.info(
            "reconcile: live=%d tenant=%d orphans=%d deleted=%d",
            result.live,
            result.tenant,
            result.orphans,
            result.deleted,
        )
    return MaterializeResult(metadata=_reconcile_metadata(result))


def _enrichment_metadata(result: FlightEnrichmentResult) -> dict[str, MetadataValue]:
    return {
        "tenant_flights": MetadataValue.int(result.tenant_flights),
        "candidates": MetadataValue.int(result.candidates),
        "enriched": MetadataValue.int(result.enriched),
        "skipped_inactive": MetadataValue.int(result.skipped_inactive),
        "fetch_failed": MetadataValue.int(result.fetch_failed),
    }


def _cases_metadata(result: CaseSyncResult) -> dict[str, MetadataValue]:
    meta: dict[str, MetadataValue] = {
        "attempted": MetadataValue.int(result.attempted),
        "succeeded": MetadataValue.int(result.succeeded),
        "failed": MetadataValue.int(result.failed),
    }
    # Persist the cursor under the same ``_CURSOR_KEY`` the positions asset
    # uses so ``_prior_cursor`` round-trips it on the next tick (the helper
    # is asset-key scoped — same code path, per-asset state).
    if result.cursor is not None:
        meta[_CURSOR_KEY] = MetadataValue.text(result.cursor.isoformat())
    return meta


@asset(
    group_name="foundry_sync",
    description=(
        "Backfills route/operator/registration/status + 2h trail onto the "
        "create-only takeoff Flight objects from /v1/flights (latest flight "
        "per icao24)."
    ),
    metadata={"target": "Foundry Ontology: Flight", "cadence": "hourly"},
)
def foundry_flight_enrichment(context: AssetExecutionContext) -> MaterializeResult:
    # Overlap guard: the hourly schedule must never stack a second run on a
    # slow one (on a bad-upstream hour enrichment can still run long). If
    # another run of this job is already in progress, skip this tick — a
    # coalesced no-op, surfaced via the same ``skip_reason`` contract as a
    # FoundrySyncSkipped so verification treats it identically.
    in_progress = context.instance.get_run_records(
        RunsFilter(
            job_name="foundry_flight_enrichment_job",
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
                DagsterRunStatus.CANCELING,
            ],
        )
    )
    # Liveness check on the guard: drop any record whose update_timestamp is
    # older than the window. Without this, a single killed run (status stuck
    # at STARTED with no live process) wedges the schedule forever — see the
    # 2026-05-21 incident where run 0b5b282d froze enrichment for 5 days.
    # ``update_timestamp`` comes back naive (postgres `timestamp without
    # time zone`), so strip tzinfo to compare apples-to-apples.
    now = datetime.now(UTC).replace(tzinfo=None)
    live_in_progress = [
        r
        for r in in_progress
        if r.dagster_run.run_id != context.run_id
        and (now - r.update_timestamp) < _ENRICHMENT_LIVENESS_WINDOW
    ]
    if live_in_progress:
        return _skipped(
            context,
            "another foundry_flight_enrichment run is already in progress "
            "(coalesced — the previous run is still draining)",
        )
    try:
        result = asyncio.run(run_flight_enrichment())
    except FoundrySyncSkipped as exc:
        return _skipped(context, exc.reason)
    context.log.info(
        "flight enrichment: tenant=%d candidates=%d enriched=%d "
        "skipped_inactive=%d fetch_failed=%d",
        result.tenant_flights,
        result.candidates,
        result.enriched,
        result.skipped_inactive,
        result.fetch_failed,
    )
    return MaterializeResult(metadata=_enrichment_metadata(result))


@asset(
    group_name="foundry_sync",
    description=(
        "Mirrors app.cases into the Foundry Case ontology so App 1 renders "
        "real cases (Phase 05 task #5). Incremental on updated_at — the "
        "cursor is persisted in this asset's materialization metadata and "
        "passed back as ``since`` next tick."
    ),
    metadata={"target": "Foundry Ontology: Case", "cadence": "60s"},
)
def foundry_cases_sync(context: AssetExecutionContext) -> MaterializeResult:
    """Drain `/v1/cases/all-for-sync` since the prior cursor → upsert-case batch.

    Upsert-only: resolved cases stop advancing ``updated_at`` and fall out
    of the moving window naturally; App 1's Cases panel applies a
    ``status`` filter at display time so leaving resolved Cases in the
    tenant is harmless. Cursor is persisted post-write so a transient
    Foundry failure (``FoundrySyncSkipped``) leaves the watermark
    untouched and the next tick re-reads the same window.
    """
    since = _prior_cursor(context)
    try:
        result = asyncio.run(run_cases_sync(since=since))
    except FoundrySyncSkipped as exc:
        return _skipped(context, exc.reason)
    context.log.info(
        "cases sync: attempted=%d succeeded=%d failed=%d cursor=%s",
        result.attempted,
        result.succeeded,
        result.failed,
        result.cursor.isoformat() if result.cursor else None,
    )
    return MaterializeResult(metadata=_cases_metadata(result))
