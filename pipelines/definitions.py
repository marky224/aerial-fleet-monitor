"""Top-level Dagster Definitions for AFM.

Loaded by the user-code gRPC server (`dagster api grpc -m pipelines.definitions`)
and discovered by the webserver/daemon via `workspace.yaml`.

Assets, schedules, sensors, and resources are added phase by phase:

  Phase 01 — Reference data + OpenSky + NOAA ingestion assets, schedules.
  Phase 05 — Case detector asset and rule-engine resources.
  Phase 07 — Daily brief asset.
  Phase 08 — Runbook → Notion sync asset.
"""

from __future__ import annotations

from dagster import Definitions, EnvVar

from pipelines.assets import noaa_weather, opensky_positions, static_reference
from pipelines.resources import (
    LakehouseResource,
    NoaaResource,
    OpenSkyResource,
    PostgresResource,
    WatchlistResource,
)

postgres = PostgresResource(dsn=EnvVar("DATABASE_URL"))
watchlist = WatchlistResource(postgres=postgres)

defs = Definitions(
    assets=[noaa_weather, opensky_positions, static_reference],
    schedules=[],
    sensors=[],
    jobs=[],
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
