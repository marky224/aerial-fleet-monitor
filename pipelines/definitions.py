"""Top-level Dagster Definitions for AFM.

Loaded by the user-code gRPC server (`dagster api grpc -m pipelines.definitions`)
and discovered by the webserver/daemon via `workspace.yaml`.

Assets, schedules, sensors, and resources are added phase by phase:

  Phase 01 — Reference data + OpenSky + NOAA ingestion assets, schedules,
             and an OpenSky sensor (Dagster schedules are minute-resolution
             so the spec's 30s OpenSky cadence is a sensor, not a schedule).
  Phase 05 — Case detector asset and rule-engine resources.
  Phase 07 — Daily brief asset.
  Phase 08 — Runbook → Notion sync asset.

Schedules and the sensor default to RUNNING so the stack ticks
immediately on startup — matches the local-first MVP "flip the switch
and watch it work" intent. Users can still pause via the Dagster UI.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    define_asset_job,
    sensor,
)

from pipelines.assets import (
    archive_and_tenant_disjoint,
    archive_retention_enforced,
    case_detector,
    flight_plan_enrichment,
    foundry_aircraft_reconcile,
    foundry_cases_sync,
    foundry_flight_archive,
    foundry_flight_archive_purge,
    foundry_flight_enrichment,
    foundry_flight_reconcile,
    foundry_positions_sync,
    foundry_sites_sync,
    noaa_weather,
    opensky_positions,
    prune_stale_positions,
    sf_case_push,
    sf_case_sync,
    static_reference,
)
from pipelines.resources import (
    LakehouseResource,
    NoaaResource,
    OpenSkyResource,
    PostgresResource,
    WatchlistResource,
)

postgres = PostgresResource(dsn=EnvVar("DATABASE_URL"))
watchlist = WatchlistResource(postgres=postgres)


# Per-asset jobs. Each schedule/sensor targets one asset; the asset
# graph is shallow enough in Phase 01 that a single combined
# "ingestion_job" would just hide the per-asset run history.

opensky_positions_job = define_asset_job(
    name="opensky_positions_job",
    selection=AssetSelection.assets(opensky_positions),
)

noaa_weather_job = define_asset_job(
    name="noaa_weather_job",
    selection=AssetSelection.assets(noaa_weather),
)

static_reference_job = define_asset_job(
    name="static_reference_job",
    selection=AssetSelection.assets(static_reference),
)

foundry_positions_sync_job = define_asset_job(
    name="foundry_positions_sync_job",
    selection=AssetSelection.assets(foundry_positions_sync),
)

foundry_sites_sync_job = define_asset_job(
    name="foundry_sites_sync_job",
    selection=AssetSelection.assets(foundry_sites_sync),
)

prune_stale_positions_job = define_asset_job(
    name="prune_stale_positions_job",
    selection=AssetSelection.assets(prune_stale_positions),
)

foundry_aircraft_reconcile_job = define_asset_job(
    name="foundry_aircraft_reconcile_job",
    selection=AssetSelection.assets(foundry_aircraft_reconcile),
)

foundry_flight_enrichment_job = define_asset_job(
    name="foundry_flight_enrichment_job",
    selection=AssetSelection.assets(foundry_flight_enrichment),
)


foundry_flight_reconcile_job = define_asset_job(
    name="foundry_flight_reconcile_job",
    selection=AssetSelection.assets(foundry_flight_reconcile),
)


foundry_flight_archive_job = define_asset_job(
    name="foundry_flight_archive_job",
    selection=AssetSelection.assets(foundry_flight_archive),
)


foundry_flight_archive_purge_job = define_asset_job(
    name="foundry_flight_archive_purge_job",
    selection=AssetSelection.assets(foundry_flight_archive_purge),
)


foundry_cases_sync_job = define_asset_job(
    name="foundry_cases_sync_job",
    selection=AssetSelection.assets(foundry_cases_sync),
)


flight_plan_enrichment_job = define_asset_job(
    name="flight_plan_enrichment_job",
    selection=AssetSelection.assets(flight_plan_enrichment),
)


sf_case_push_job = define_asset_job(
    name="sf_case_push_job",
    selection=AssetSelection.assets(sf_case_push),
)


sf_case_sync_job = define_asset_job(
    name="sf_case_sync_job",
    selection=AssetSelection.assets(sf_case_sync),
)


case_detector_job = define_asset_job(
    name="case_detector_job",
    selection=AssetSelection.assets(case_detector),
)


# OpenSky `/states/all` empirically costs 4 cr/call at the CONUS bbox
# (verified 2026-05-26: 999/999 consecutive paid polls drained
# `X-Rate-Limit-Remaining` by exactly 4). The poll interval is the sole
# OpenSky-credit lever (the Foundry syncs below read the local cache, not
# OpenSky). Burn ladder vs the 4,000-cr/day authenticated budget:
#   30s  x 4 cr = 11,520 cr/day — exhausted ~08:51 UTC (the old regime).
#   120s x 4 cr =  2,880 cr/day — 1,120 cr headroom.
#   300s x 4 cr =  1,152 cr/day — 2,848 cr headroom  ← current.
# Raised 120s→300s on 2026-05-31 for OpenSky headroom (user-directed; the
# 28%-headroom-is-enough guidance from the 2026-05-28 handoff was
# explicitly overridden). Spec PIPELINES.md §5's "30s mandated" is
# superseded by this constraint.
#
# COUPLED TO ANOMALY DETECTION — this is not a free knob. The poll is the
# position track the case detector runs on, so the interval is load-bearing
# for the rules:
#   * lost_signal — gap-based; the 14-min floor is an absolute coverage-hole
#     duration (cadence-independent), but at 300s the 5-min quantization
#     forces an effective 15-min floor. Live data (2026-05-31) showed only
#     ~5.7% of fires fall in [14,15) and are lost — 94.3% survive. See
#     LONG_GAP_THRESHOLD in pipelines/rules/lost_signal.py (raised 15→20 to
#     keep the severity gradation from force-promoting every fire at 300s).
#   * go_around — needs >=3 near-field snapshots to trace its valley; at
#     300s an aircraft is sampled ~1-2x within 10 nm, so the rule is
#     effectively DORMANT (structural, not tunable). Accepted trade-off.
# Revisit both rules if this interval changes again.
OPENSKY_POLL_INTERVAL_SECONDS = 300


@sensor(
    job=opensky_positions_job,
    name="opensky_positions_sensor",
    minimum_interval_seconds=OPENSKY_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
    description="Fires opensky_positions every 300s (rate-budget constrained — see comment above).",
)
def opensky_positions_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


# Foundry Aircraft sync mirrors the upstream `opensky_positions` cadence
# (it consumes the same /v1/positions/live snapshot — no point running
# faster than fresh data lands). Independent failure domain: a
# Foundry-unreachable run materializes as a skip, not a failure, so this
# never blocks the local pipeline.
@sensor(
    job=foundry_positions_sync_job,
    name="foundry_positions_sync_sensor",
    minimum_interval_seconds=OPENSKY_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
    description="Upserts Aircraft to the Foundry Ontology every 300s (matches opensky_positions cadence).",
)
def foundry_positions_sync_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


# Pushes pending app.cases rows to Salesforce. A sensor (not a schedule)
# for the ~60s cadence the dashboard expects (build-doc §5/§7) — finer than
# Dagster's minute-resolution cron. Each fire launches a fresh run
# (run_key=None), and each run re-scans whatever is still pending, so a case
# left pending by a transient SF failure is retried on the next tick — this
# sensor IS the case-sync retry mechanism (build-doc §8).
@sensor(
    job=sf_case_push_job,
    name="case_sync_retry_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Pushes pending cases to Salesforce every ~60s; re-scan provides retry.",
)
def case_sync_retry_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


# Pulls Salesforce-modified Cases back into app.cases (PIPELINES.md §3.5).
# A sensor at ~60s for the same reason as the push: the spec's 60s cadence is
# finer than Dagster's minute-resolution cron, and a fresh RunRequest each
# fire launches a run. The pull is watermark-driven + idempotent, so an
# overlapping run is harmless; an API/SF outage materializes as a skip with
# the watermark untouched, so the next tick re-reads the same window.
@sensor(
    job=sf_case_sync_job,
    name="sf_case_sync_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Mirrors Salesforce Case changes into app.cases every ~60s (watermark-driven).",
)
def sf_case_sync_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


# Mirrors app.cases → Foundry Case ontology (Phase 05 task #5). 60s cadence
# matches sf_case_sync_sensor so the end-to-end SF→PG→Foundry latency is
# ~120s worst case. Watermark-driven + idempotent (upsert keyed on case_id),
# so an overlapping run is harmless; a Foundry-unreachable run materializes
# as a skip with the cursor untouched (the standalone contract).
@sensor(
    job=foundry_cases_sync_job,
    name="foundry_cases_sync_sensor",
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description="Mirrors app.cases into the Foundry Case ontology every ~60s (cursor-driven).",
)
def foundry_cases_sync_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


noaa_weather_schedule = ScheduleDefinition(
    name="noaa_weather_schedule",
    job=noaa_weather_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Fires noaa_weather every 5 minutes (PIPELINES.md §5).",
)


static_reference_schedule = ScheduleDefinition(
    name="static_reference_schedule",
    job=static_reference_job,
    cron_schedule="0 2 * * SUN",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Refreshes ref.airports + ref.aircraft_registry weekly (Sun 02:00 UTC).",
)


foundry_sites_sync_schedule = ScheduleDefinition(
    name="foundry_sites_sync_schedule",
    job=foundry_sites_sync_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Full-refresh upsert of Site to the Foundry Ontology every 5 minutes.",
)


prune_stale_positions_schedule = ScheduleDefinition(
    name="prune_stale_positions_schedule",
    job=prune_stale_positions_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Hourly: evict app.current_positions rows older than the retention window.",
)


foundry_aircraft_reconcile_schedule = ScheduleDefinition(
    name="foundry_aircraft_reconcile_schedule",
    job=foundry_aircraft_reconcile_job,
    # */5 to follow the 300s opensky_positions poll — no point evicting
    # faster than the live feed refreshes (the reconcile reads the local
    # current_positions cache, so this is host/Foundry-write relief, not an
    # OpenSky-credit change).
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Every 5 min (Fix C): evict Foundry Aircraft objects outside the "
        "in-scope live feed so the tenant stays continuously equal to the "
        "live set (overlap-guarded; deletes capped per run)."
    ),
)


foundry_flight_reconcile_schedule = ScheduleDefinition(
    name="foundry_flight_reconcile_schedule",
    job=foundry_flight_reconcile_job,
    # :45 past the hour — after the :30 flight enrichment so the Flight set it
    # reads is post-enrichment-stable, and offset from the :00 aircraft
    # reconcile/prune. The per-run delete cap means the first reconcile drains
    # the one-time backlog over several hourly runs; the asset's overlap guard
    # prevents a new tick stacking on a still-draining one.
    cron_schedule="45 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly at :45 (Phase A): evict Foundry Flight objects outside the "
        "live working set (latest-per-airborne-icao24 plus within-TTL) — the "
        "Flight-side mirror of foundry_aircraft_reconcile."
    ),
)


foundry_flight_archive_schedule = ScheduleDefinition(
    name="foundry_flight_archive_schedule",
    job=foundry_flight_archive_job,
    # :50 past the hour — after the :45 reconcile (which leaves completed
    # flights in the tenant for exactly this asset), so it reads a stable
    # post-reconcile Flight set, and offset from every other foundry job.
    cron_schedule="50 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly at :50 (Phase B): archive completed Foundry Flights (landed_at "
        "older than the grace window) to the flights_archive/ cold store, then "
        "delete them from Foundry — the sole path that removes completed "
        "flights, archive-before-delete."
    ),
)


foundry_flight_archive_purge_schedule = ScheduleDefinition(
    name="foundry_flight_archive_purge_schedule",
    job=foundry_flight_archive_purge_job,
    # Daily at 03:15 UTC — off-peak, and offset from the hourly :50 archive so
    # a purge never overlaps an archive write. Retention is day-granular, so a
    # daily cadence is ample.
    cron_schedule="15 3 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Daily at 03:15 UTC (Phase B): drop flights_archive/ day-partition "
        "directories older than 30 days (directory unlink, never DELETE+VACUUM)."
    ),
)


foundry_flight_enrichment_schedule = ScheduleDefinition(
    name="foundry_flight_enrichment_schedule",
    job=foundry_flight_enrichment_job,
    # Every 5 min so the Flight ``trailPath`` tracks the Aircraft markers —
    # since the 2026-05-31 poll move to 300s the markers also refresh every
    # 5 min, so trail head and live dot now advance 1:1. Reads the local lake
    # / AFM API only (no OpenSky cost); the overlap guard stops a slow run
    # from stacking. Coinciding with the */5 aircraft reconcile or the :45
    # flight reconcile is safe — different object types, and enrichment writes
    # disjoint fields (trailPath / route) from the reconcile's isLive /
    # landedAt, so the modify-per-field upserts don't clobber.
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Every 5 min: backfill route/operator/registration/status + 2h trail "
        "onto the create-only takeoff Flight objects from /v1/flights, keeping "
        "trailPath aligned with the live 5-min Aircraft markers."
    ),
)


flight_plan_enrichment_schedule = ScheduleDefinition(
    name="flight_plan_enrichment_schedule",
    job=flight_plan_enrichment_job,
    # :15 past the hour — staggered between the :00 reconcile/prune and
    # the :30 Foundry flight enrichment to avoid contending on OpenSky
    # rate-limit credits or the API host's HTTP capacity.
    cron_schedule="15 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly at :15: refresh origin/destination for active icao24s "
        "via OpenSky /flights/aircraft. 12h cache TTL in app.flight_plans."
    ),
)


case_detector_schedule = ScheduleDefinition(
    name="case_detector_schedule",
    job=case_detector_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Every 5 minutes: run the anomaly rule engine over the last hour of "
        "positions and insert detected cases into app.cases (pending). The SF "
        "write is decoupled — case_sync_retry_sensor pushes pending rows."
    ),
)


defs = Definitions(
    assets=[
        noaa_weather,
        opensky_positions,
        static_reference,
        foundry_positions_sync,
        foundry_sites_sync,
        foundry_aircraft_reconcile,
        foundry_flight_enrichment,
        foundry_flight_reconcile,
        foundry_flight_archive,
        foundry_flight_archive_purge,
        foundry_cases_sync,
        flight_plan_enrichment,
        sf_case_push,
        sf_case_sync,
        case_detector,
        prune_stale_positions,
    ],
    jobs=[
        opensky_positions_job,
        noaa_weather_job,
        static_reference_job,
        foundry_positions_sync_job,
        foundry_sites_sync_job,
        foundry_aircraft_reconcile_job,
        foundry_flight_enrichment_job,
        foundry_flight_reconcile_job,
        foundry_flight_archive_job,
        foundry_flight_archive_purge_job,
        foundry_cases_sync_job,
        flight_plan_enrichment_job,
        sf_case_push_job,
        sf_case_sync_job,
        case_detector_job,
        prune_stale_positions_job,
    ],
    schedules=[
        noaa_weather_schedule,
        static_reference_schedule,
        foundry_sites_sync_schedule,
        foundry_aircraft_reconcile_schedule,
        foundry_flight_enrichment_schedule,
        foundry_flight_reconcile_schedule,
        foundry_flight_archive_schedule,
        foundry_flight_archive_purge_schedule,
        flight_plan_enrichment_schedule,
        case_detector_schedule,
        prune_stale_positions_schedule,
    ],
    sensors=[
        opensky_positions_sensor,
        foundry_positions_sync_sensor,
        case_sync_retry_sensor,
        sf_case_sync_sensor,
        foundry_cases_sync_sensor,
    ],
    asset_checks=[
        archive_and_tenant_disjoint,
        archive_retention_enforced,
    ],
    resources={
        "postgres": postgres,
        "watchlist": watchlist,
        "opensky": OpenSkyResource(
            client_id=EnvVar("OPENSKY_CLIENT_ID"),
            client_secret=EnvVar("OPENSKY_CLIENT_SECRET"),
        ),
        "lakehouse": LakehouseResource(lake_path=EnvVar("AFM_LAKE_PATH")),
        "noaa": NoaaResource(),
    },
)
