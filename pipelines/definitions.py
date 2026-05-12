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

from pipelines.assets import static_reference
from pipelines.resources import PostgresResource

defs = Definitions(
    assets=[static_reference],
    schedules=[],
    sensors=[],
    jobs=[],
    resources={
        "postgres": PostgresResource(dsn=EnvVar("DATABASE_URL")),
    },
)
