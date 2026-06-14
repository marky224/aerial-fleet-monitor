"""Smoke + registration tests for the top-level Dagster ``Definitions`` (Phase 10).

``pipelines.definitions`` wires every asset, job, schedule, sensor, asset check,
and resource the stack runs on. These tests (1) force Dagster to resolve the
whole definition graph via ``get_repository_def()`` — which validates job
selections, resource-key requirements, and name uniqueness in one shot — and
(2) assert the expected names are registered, so an accidental drop or rename
fails loudly here instead of at deploy time. Pure import + introspection: no DB,
no network, no env vars (the resource ``EnvVar`` references resolve lazily).
"""

from __future__ import annotations

from dagster import RunRequest, build_sensor_context

from pipelines.definitions import (
    case_sync_retry_sensor,
    defs,
    foundry_cases_sync_sensor,
    foundry_positions_sync_sensor,
    opensky_positions_sensor,
    sf_case_sync_sensor,
)

EXPECTED_JOBS = {
    "opensky_positions_job",
    "noaa_weather_job",
    "static_reference_job",
    "foundry_positions_sync_job",
    "foundry_sites_sync_job",
    "foundry_aircraft_reconcile_job",
    "foundry_flight_enrichment_job",
    "foundry_flight_reconcile_job",
    "foundry_flight_archive_job",
    "foundry_flight_archive_purge_job",
    "foundry_cases_sync_job",
    "foundry_cases_reconcile_job",
    "flight_plan_enrichment_job",
    "sf_case_push_job",
    "sf_case_sync_job",
    "case_detector_job",
    "prune_stale_positions_job",
}

EXPECTED_SCHEDULES = {
    "noaa_weather_schedule",
    "static_reference_schedule",
    "foundry_sites_sync_schedule",
    "foundry_aircraft_reconcile_schedule",
    "foundry_cases_reconcile_schedule",
    "foundry_flight_enrichment_schedule",
    "foundry_flight_reconcile_schedule",
    "foundry_flight_archive_schedule",
    "foundry_flight_archive_purge_schedule",
    "flight_plan_enrichment_schedule",
    "case_detector_schedule",
    "prune_stale_positions_schedule",
}

EXPECTED_SENSORS = {
    "opensky_positions_sensor",
    "foundry_positions_sync_sensor",
    "case_sync_retry_sensor",
    "sf_case_sync_sensor",
    "foundry_cases_sync_sensor",
}

EXPECTED_RESOURCES = {"postgres", "watchlist", "opensky", "lakehouse", "noaa"}


def test_definitions_resolve_into_a_repository() -> None:
    """The full graph wires: job selections, resource keys, and unique names validate."""
    repo = defs.get_repository_def()
    assert repo is not None


def test_expected_jobs_registered() -> None:
    # get_all_job_defs() also surfaces Dagster's implicit ``__ASSET_JOB`` — assert
    # our named jobs are a subset rather than equal so that implicit job is fine.
    names = {job.name for job in defs.get_all_job_defs()}
    assert names >= EXPECTED_JOBS


def test_expected_schedules_registered() -> None:
    repo = defs.get_repository_def()
    assert {sched.name for sched in repo.schedule_defs} == EXPECTED_SCHEDULES


def test_expected_sensors_registered() -> None:
    repo = defs.get_repository_def()
    assert {sen.name for sen in repo.sensor_defs} == EXPECTED_SENSORS


def test_expected_resource_keys_present() -> None:
    assert set(defs.resources) == EXPECTED_RESOURCES


def test_sensors_request_a_run() -> None:
    """Each ingestion/sync sensor yields a fresh run (run_key=None → never deduped)."""
    for sensor_def in (
        opensky_positions_sensor,
        foundry_positions_sync_sensor,
        case_sync_retry_sensor,
        sf_case_sync_sensor,
        foundry_cases_sync_sensor,
    ):
        result = sensor_def(build_sensor_context())
        assert isinstance(result, RunRequest)
        assert result.run_key is None
