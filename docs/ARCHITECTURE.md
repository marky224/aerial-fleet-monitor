# Aerial Fleet Monitor — Architecture

> **Audience:** Technical reviewers, hiring managers, contributors.
> **Status:** v1 architectural shape. Detailed implementation sequencing is held privately.

---

## 1. System topology

AFM is a hybrid edge/local system. The frontend runs on AWS edge infrastructure for global reach and zero-cost-at-rest hosting. The data plane runs on a single self-hosted Linux box using `docker-compose`, exposed publicly via Cloudflare Tunnel. Salesforce is a third independent plane reached over the public internet via OAuth and REST.

```
                 ┌────────────────────────────────────────────────┐
                 │                  PUBLIC INTERNET                │
                 └────────────────────────────────────────────────┘
                                       │
            ┌──────────────────────────┼─────────────────────────────┐
            │                          │                             │
            ▼                          ▼                             ▼
   ┌──────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
   │  AWS CloudFront  │      │  Cloudflare Tunnel   │      │  Salesforce DE Org   │
   │       +S3        │      │  (api.afm.…)         │      │  (Agentforce)        │
   │  React frontend  │      │  (grafana.afm.…)     │      │                      │
   └────────┬─────────┘      └──────────┬───────────┘      └──────────┬───────────┘
            │                           │                              │
            │ HTTPS                     │ tunnelled                    │ OAuth + REST
            ▼                           ▼                              ▼
   ┌──────────────────┐      ┌─────────────────────────────────────────────────────┐
   │  Browser client  │◀────▶│             openclaw-pc (Ubuntu 24.04)              │
   │  ArcGIS SDK,     │ REST │                                                     │
   │  shadcn/ui,      │  &   │  ┌──────────────────────────────────────────────┐  │
   │  Tailwind        │ WSS  │  │ docker-compose stack:                        │  │
   └──────────────────┘      │  │   FastAPI · Dagster · Postgres 16            │  │
                             │  │   Parquet lakehouse + DuckDB                 │  │
                             │  │   Loki · Prometheus · Grafana                │  │
                             │  │   cloudflared tunnel connector               │  │
                             │  └──────────────────────────────────────────────┘  │
                             └─────────────────────────────────────────────────────┘
```

The hybrid topology is a deliberate cost/realism tradeoff. All-AWS at all-US scale runs ~$15–25/month; the local marginal cost is ~$1–3/month. The lakehouse-on-local-NVMe pattern (Postgres OLTP + Parquet + DuckDB) also better mirrors the operational/analytical separation found in production fleet ops platforms.

## 2. Component inventory

### Frontend plane (AWS)

React + ArcGIS Maps SDK + Tailwind + shadcn/ui, built with Vite, served from S3 behind CloudFront with TLS terminating at the edge. Pure static hosting; no runtime backend dependency on AWS.

### Data plane (self-hosted)

A single `docker-compose` stack runs the operational backbone:

- **FastAPI** — REST and (reserved) WebSocket; auth; Salesforce sync
- **Dagster** — asset-oriented pipeline orchestrator; webserver, daemon, user-code containers
- **Postgres 16** — operational store (cases, timelines, site metrics, briefs, audit, reference data)
- **Parquet lakehouse on NVMe** — historical position snapshots, queried in-process via DuckDB
- **Loki + Promtail + Prometheus + Grafana** — observability stack
- **cloudflared** — tunnel connector exposing the public API and Grafana subdomains

### CRM plane (Salesforce Agentforce DE org)

A Connected App for OAuth, custom objects (`AFM_Site__c`, `AFM_Flight__c`), custom Case fields, Permission Sets and custom permissions, an Apex consolidated controller for the agent's actions, a Lightning Web Component embedded on the Case record page, and the Agentforce agent itself with a Record-Triggered Flow invoking it on Case insert.

## 3. Data flow (high level)

Three primary flows operate continuously:

1. **Live ingestion.** A Dagster schedule polls OpenSky every 30 seconds for current US ADS-B positions, filters sensitive operators, writes each cycle as a Parquet snapshot partitioned by hour, and upserts the latest position per aircraft into a hot Postgres table for low-latency reads. A parallel five-minute schedule fetches METAR/TAF for the watched airport set.

2. **Anomaly detection.** Every five minutes, a rules engine reads the last hour of position snapshots via DuckDB plus current weather, applies six anomaly rules (lost signal, diversion, excessive holding, weather impact, go-around, delay), de-duplicates against open cases, and writes new cases to both Postgres (for fast dashboard reads) and Salesforce (as the system of record). The Salesforce Case insert fires a Record-Triggered Flow that invokes the Fleet Anomaly Triage Agentforce agent.

3. **Authentication.** A cold visitor receives an auto-issued read-only `internal-ops` session JWT, rendering the dashboard immediately. Switching into a customer view kicks off a real Salesforce OAuth Web Server Flow; on callback, the backend reads the user's custom permissions from Salesforce, derives a region scope, signs a new AFM JWT, and sets it as an HttpOnly cookie. Subsequent requests carry the cookie automatically; every backend query filters by the JWT's scope claim.

## 4. Network topology

| Hostname | Purpose |
|---|---|
| `aerial-fleet-monitor.markandrewmarquez.com` | AFM frontend (S3 + CloudFront) |
| `api.aerial-fleet-monitor.markandrewmarquez.com` | AFM backend API (Cloudflare Tunnel) |
| `grafana.aerial-fleet-monitor.markandrewmarquez.com` | Observability UI (Cloudflare Access protected) |
| `dagster.aerial-fleet-monitor.markandrewmarquez.com` | Dagster UI (Cloudflare Access protected) |
| `alerts.markandrewmarquez.com` | Email sender domain (Resend) |

All public hostnames TLS-terminated at CloudFront or Cloudflare. No direct port exposure from the self-hosted box — only Cloudflare's tunnel connector reaches in.

## 5. Runtime characteristics

| Property | Target |
|---|---|
| Position poll latency budget | ≤2 seconds per poll cycle |
| Case detection cadence | every 5 minutes |
| Live position freshness shown to UI | typically ≤30 seconds, ≤60 seconds worst case |
| Case appears in dashboard after detection | ≤10 seconds |
| Case appears in Salesforce | ≤3 seconds |
| Agentforce agent completes its action sequence | ≤30 seconds |
| Critical case alert email sent | ≤60 seconds from case open |
| LWC telemetry refresh | every 30 seconds |
| Daily brief generation | 7am customer-local |
| Site metrics refresh | every 15 minutes |

## 6. Service boundaries

```
React frontend ──► FastAPI ──► QueryService ──► Postgres + Parquet/DuckDB

Dagster pipelines ──► External APIs (OpenSky, NOAA, Anthropic, Salesforce, Notion, Resend, Slack)
                  ──► Postgres + Parquet
                  ──► Salesforce Cases ──► SF Flow ──► Agentforce agent ──► Apex controller

Salesforce LWC ──► Apex controller ──► Named Credential ──► AFM API
```

Single-direction dependencies make the system tractable to debug. The frontend never talks to Salesforce directly; the Salesforce LWC never talks to Postgres directly; both go through the AFM API.

## 7. Storage layout

Postgres holds operational state across two schemas: `app` (cases, case timeline, site metrics, briefs, audit logs, user sessions, sync watermarks) and `ref` (airports, aircraft registry, runbook index). Migrations are managed by Alembic.

The Parquet lakehouse uses Hive-style partitioning on `year/month/day/hour` so DuckDB can prune efficiently when reading historical positions. Each polling cycle writes one file (≈5,000 rows compressed via zstd to ~500 KB on average). A daily lifecycle job drops partitions older than 365 days; disk usage target stays under 500 GB sustained.

## 8. Deployment process

**Backend** is built into a Docker image by GitHub Actions, pushed to GHCR, and pulled by the openclaw-pc compose stack on each merge to `main`. Database migrations run before the API container starts.

**Frontend** is built with Vite, synced to S3 by GitHub Actions, and the CloudFront distribution is invalidated to flush the edge cache.

**Salesforce metadata** is deployed via `sf project deploy start --target-org afm-dev`. Agent script changes trigger a separate workflow that runs `sf agent validate && sf agent publish && sf agent activate`.

---

## Full specification

This document presents AFM's architectural shape for portfolio review. Detailed implementation notes — exact failure-mode handling, deployment sequencing, observability metric catalog, complete component port and dependency inventory, and the per-phase build instructions — are held in private working documentation.

For a full architectural walkthrough or technical evaluation conversation, reach out: **Mark Andrew Marquez** · `mark@markandrewmarquez.com`
