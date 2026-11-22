# Aerial Fleet Monitor — Observability

The full observability specification documents AFM's self-hosted Loki + Promtail + Prometheus + Grafana stack, the structured-log shape, the custom-metric catalogue, and the five Grafana dashboards.

## Topics covered in the full specification

- What we observe and why (logs vs metrics vs traces — and why no traces in v1)
- Stack components (Loki for logs, Promtail for shipping, Prometheus for metrics, Grafana as the unified UI)
- Exposure model (Grafana and Dagster UIs are private-network only — not publicly reachable; the metrics endpoint is network-scoped and never forwarded by the public reverse tunnel)
- Log shape standard (structured JSON via `structlog`, dotted event names)
- Metrics catalogue:
  - AFM gauges read from Postgres on each scrape (`afm_active_aircraft`, `afm_tracked_aircraft`, `afm_pipeline_lag_seconds`, `afm_cases`, `afm_cases_created_24h`, `afm_sf_sync_backlog`, etc.)
  - Dagster run gauges read from the run store (`afm_dagster_runs_24h`, `afm_dagster_job_last_duration_seconds`, `afm_dagster_job_last_age_seconds`)
  - Standard FastAPI metrics via `prometheus-fastapi-instrumentator`
- Grafana dashboards (5 provisioned at startup):
  - Fleet Ops Overview
  - Salesforce Health
  - Pipeline Health
  - Sites
  - Case Detector Tuning
- Alerting (none configured in v1: no alert rules, no notification channels)
- Local development (same stack runs locally with the same dashboards)
- What we deliberately don't observe in v1 (tracing, real-user monitoring, synthetic uptime, distributed log correlation)
- Reading guide for reviewers (recommended dashboard order for cold inspection)

The full observability specification is available on request.

---

This stub exists so readers know this scope is documented. For the complete specification, including the dashboard JSON, the Loki/Promtail configs, and the metric label conventions, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
