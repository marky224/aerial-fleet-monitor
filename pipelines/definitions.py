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
    case_detector,
    flight_plan_enrichment,
    foundry_aircraft_reconcile,
    foundry_cases_sync,
    foundry_flight_enrichment,
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


# Dagster's cron schedules are minute-resolution at the finest. OpenSky's
# spec-mandated 30s cadence (PIPELINES.md §5) needs a sensor with
# minimum_interval_seconds=30 returning a RunRequest every fire. Returning
# a fresh RunRequest each time (no run_key dedupe) means every fire
# launches a run — exactly the every-30s behavior we want.
@sensor(
    job=opensky_positions_job,
    name="opensky_positions_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="Fires opensky_positions every 30s (sub-minute cadence not supported by Dagster schedules).",
)
def opensky_positions_sensor(_context: SensorEvaluationContext) -> RunRequest:
    return RunRequest(run_key=None)


# Foundry Aircraft sync mirrors OpenSky's 30s cadence (it consumes the same
# /v1/positions/live snapshot), so it's a sensor for the same sub-minute
# reason. Independent failure domain: a Foundry-unreachable run materializes
# as a skip, not a failure, so this never blocks the local pipeline.
@sensor(
    job=foundry_positions_sync_job,
    name="foundry_positions_sync_sensor",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    description="Upserts Aircraft to the Foundry Ontology every 30s.",
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
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly (Fix C): evict Foundry Aircraft objects no longer in the "
        "live feed — mirrors prune_stale_positions on the Ontology side."
    ),
)


foundry_flight_enrichment_schedule = ScheduleDefinition(
    name="foundry_flight_enrichment_schedule",
    job=foundry_flight_enrichment_job,
    # :30 past the hour — offset from the top-of-hour reconcile + prune so
    # the per-icao24 /v1/flights fanout never runs alongside the eviction
    # pass (and the tenant Flight set it reads is post-reconcile-stable).
    cron_schedule="30 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Hourly at :30: backfill route/operator/registration/status + 2h "
        "trail onto the create-only takeoff Flight objects from /v1/flights."
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
