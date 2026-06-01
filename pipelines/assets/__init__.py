"""Dagster assets for AFM pipelines."""

from pipelines.assets.detection import case_detector
from pipelines.assets.flight_archival import (
    archive_and_tenant_disjoint,
    archive_retention_enforced,
    foundry_flight_archive,
    foundry_flight_archive_purge,
)
from pipelines.assets.flight_plan_enrichment import flight_plan_enrichment
from pipelines.assets.foundry_sync import (
    foundry_aircraft_reconcile,
    foundry_cases_reconcile,
    foundry_cases_sync,
    foundry_flight_enrichment,
    foundry_flight_reconcile,
    foundry_positions_sync,
    foundry_sites_sync,
)
from pipelines.assets.ingestion import noaa_weather, opensky_positions
from pipelines.assets.maintenance import prune_stale_positions
from pipelines.assets.reference import static_reference
from pipelines.assets.sync import sf_case_push, sf_case_sync, sf_push_not_failing

__all__ = [
    "archive_and_tenant_disjoint",
    "archive_retention_enforced",
    "case_detector",
    "flight_plan_enrichment",
    "foundry_aircraft_reconcile",
    "foundry_cases_reconcile",
    "foundry_cases_sync",
    "foundry_flight_archive",
    "foundry_flight_archive_purge",
    "foundry_flight_enrichment",
    "foundry_flight_reconcile",
    "foundry_positions_sync",
    "foundry_sites_sync",
    "noaa_weather",
    "opensky_positions",
    "prune_stale_positions",
    "sf_case_push",
    "sf_case_sync",
    "sf_push_not_failing",
    "static_reference",
]
