# Aerial Fleet Monitor — Pipelines

The full pipelines specification documents AFM's Dagster asset-oriented pipeline architecture: ingestion, anomaly detection, two-way Salesforce sync, Foundry sync, daily brief generation, and maintenance.

## Topics covered in the full specification

- Why Dagster (asset model rationale, asset-graph-as-documentation pattern)
- Asset graph (visual representation of data flow across all assets)
- Asset-by-asset detail:
  - `opensky_positions` — 300-second polling (a sensor), US bounding box, Parquet snapshot + Postgres upsert
  - `noaa_weather` — 5-minute batch fetch for watched airports
  - `static_reference` — weekly airport + aircraft-registry refresh
  - `case_detector` — 5-minute rules engine across 6 anomaly types; dedups and writes new cases to Postgres only (marked `pending` when high severity, `skipped` otherwise)
  - `sf_case_push` — ~60-second sensor-driven push that drains `pending` cases into Salesforce (decoupled from detection; idempotent, single-flighted, retrying)
  - `sf_case_sync` — 60-second SF → Postgres mirror with persistent watermark
  - `daily_brief_west` / `daily_brief_east` / `daily_brief_all` — per-region 7am-local brief synthesis (cases → Salesforce Models API via the `AFM_BriefSynthesis` Apex action → Chatter group + `app.briefs`)
  - `flight_plan_enrichment` — hourly origin/destination lookup for in-scope aircraft, cached in `app.flight_plans`
  - `prune_stale_positions` — hourly prune of `app.current_positions` rows older than the retention window
  - `foundry_*` (nine assets) — sync to the Foundry Ontology: positions (300-second sensor), sites and flight enrichment (every 5 minutes), cases (~60-second sensor), the aircraft, flight and case reconciles, and the flight archive with its 30-day purge
- Resources (five: Postgres, lakehouse, OpenSky, NOAA, watchlist) and the swappable `BaselineProvider` service
- Schedules (cadence per asset, including timezone-aware per-region brief schedules)
- Sensors (OpenSky poll, Foundry positions sync, case-sync retry, Salesforce case pull, Foundry cases sync)
- Jobs (asset groupings for orchestration)
- Observability (run counts, duration and freshness per pipeline, read by the API from Dagster's run store)
- Local development workflow (Dagster UI as cockpit, manual asset materialization)

The full pipelines specification is available on request.

---

This stub exists so readers know this scope is documented. For the complete specification, including the full asset definitions, resource configurations, dedup-window tables, and rule-engine logic, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
