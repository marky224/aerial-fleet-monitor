# Aerial Fleet Monitor — Pipelines

The full pipelines specification documents AFM's Dagster asset-oriented pipeline architecture: ingestion, anomaly detection, two-way Salesforce sync, analytical aggregation, daily brief generation, runbook indexing, and lifecycle management.

## Topics covered in the full specification

- Why Dagster (asset model rationale, asset-graph-as-documentation pattern)
- Asset graph (visual representation of data flow across all assets)
- Asset-by-asset detail:
  - `opensky_positions` — 30-second polling, US bounding box, Parquet snapshot + Postgres upsert
  - `noaa_weather` — 5-minute batch fetch for watched airports
  - `static_reference` — weekly airport + aircraft-registry refresh
  - `case_detector` — 5-minute rules engine across 6 anomaly types; dedups and writes new cases to Postgres only (marked `pending`)
  - `sf_case_push` — ~60-second sensor-driven push that drains `pending` cases into Salesforce (decoupled from detection; idempotent, single-flighted, retrying)
  - `sf_case_sync` — 60-second SF → Postgres mirror with persistent watermark
  - `site_metrics_refresh` — 15-minute SLA recomputation
  - `daily_brief_generator` — per-region 7am-local brief synthesis
  - `runbook_index_sync` — markdown → Notion sync, with sensor-driven webhook trigger
  - `partition_lifecycle` — daily Parquet retention sweep
- Resources (12 dependency-injected clients including the swappable `BaselineProvider`)
- Schedules (cadence per asset, including timezone-aware per-region brief schedules)
- Sensors (GitHub webhook, case-sync retry, disk-usage threshold)
- Jobs (asset groupings for orchestration)
- Observability hooks (custom Prometheus metrics emitted per asset)
- Local development workflow (Dagster UI as cockpit, manual asset materialization)

The full pipelines specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the full asset definitions, resource configurations, dedup-window tables, and rule-engine logic, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
