"""Top-level Dagster Definitions for AFM.

Loaded by the user-code gRPC server (`dagster api grpc -m pipelines.definitions`)
and discovered by the webserver/daemon via `workspace.yaml`.

Phase 00 ships an empty Definitions object — enough for the webserver to
start, render an empty workspace, and pass health checks. Assets,
schedules, sensors, and resources are added phase by phase:

  Phase 01 — OpenSky + NOAA ingestion assets, schedules.
  Phase 05 — Case detector asset and rule-engine resources.
  Phase 07 — Daily brief asset.
  Phase 08 — Runbook → Notion sync asset.
"""

from __future__ import annotations

from dagster import Definitions

defs = Definitions(
    assets=[],
    schedules=[],
    sensors=[],
    jobs=[],
    resources={},
)
