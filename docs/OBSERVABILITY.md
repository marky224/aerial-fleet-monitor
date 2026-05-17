# Aerial Fleet Monitor — Observability

The full observability specification documents AFM's self-hosted Loki + Promtail + Prometheus + Grafana stack, the structured-log shape, the custom-metric catalogue, the five Grafana dashboards, and the deliberately minimal alerting philosophy.

## Topics covered in the full specification

- What we observe and why (logs vs metrics vs traces — and why no traces in v1)
- Stack components (Loki for logs, Promtail for shipping, Prometheus for metrics, Grafana as the unified UI)
- Exposure model (Grafana and Dagster UIs are private-network only — not publicly reachable; the metrics endpoint is network-scoped and never forwarded by the public reverse tunnel)
- Log shape standard (structured JSON via `structlog`, dotted event names, `request_id` correlation across components)
- Metrics catalogue:
  - Counters (`afm_opensky_credits_used_total`, `afm_cases_created_total`, `afm_sf_sync_failures_total`, etc.)
  - Histograms (`afm_sf_sync_duration_seconds`, `afm_anthropic_call_duration_seconds`, etc.)
  - Gauges (`afm_pipeline_lag_seconds`, `afm_open_cases_count`, `afm_lakehouse_disk_usage_bytes`, etc.)
  - Standard FastAPI metrics via `prometheus-fastapi-instrumentator`
- Grafana dashboards (5 provisioned at startup):
  - Fleet Ops Overview
  - Salesforce Integration Health
  - Pipeline Health
  - Per-Airport SLA Trends
  - Case Detector Tuning
- Alerting (deliberately minimal — only OpenSky credit cap and lakehouse disk fill, both via Slack)
- Local development (same stack runs locally with the same dashboards)
- What we deliberately don't observe in v1 (tracing, real-user monitoring, synthetic uptime, distributed log correlation)
- Reading guide for reviewers (recommended dashboard order for cold inspection)

The full observability specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the dashboard JSON, the Loki/Promtail configs, and the metric label conventions, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
