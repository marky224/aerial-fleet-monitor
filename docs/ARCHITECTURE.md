# Aerial Fleet Monitor — Architecture

> **Audience:** Technical reviewers, hiring managers, contributors.
> **Status:** v1 architectural shape. Detailed implementation sequencing is held privately.

---

## 1. System topology

AFM is a hybrid system. The dashboard plane is hosted on Palantir Foundry (developer tier) — Workshop apps over an Ontology populated by sync from the local data plane. The data plane runs on a single self-hosted Linux box using `docker-compose`, with **only the API** exposed publicly via a self-hosted reverse tunnel; the observability UIs and Postgres stay on the private network. Salesforce is a third independent plane reached over the public internet via OAuth and REST.

```
                 ┌────────────────────────────────────────────────┐
                 │                  PUBLIC INTERNET                │
                 └────────────────────────────────────────────────┘
                                       │
            ┌──────────────────────────┼─────────────────────────────┐
            │                          │                             │
            ▼                          ▼                             ▼
   ┌──────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
   │ Palantir Foundry │      │  Reverse tunnel      │      │  Salesforce DE Org   │
   │  Workshop apps,  │      │  (public API only)   │      │  (Agentforce)        │
   │  Ontology,       │      │                      │      │                      │
   │  AIP Logic       │      │                      │      │                      │
   └────────▲─────────┘      └──────────┬───────────┘      └──────────┬───────────┘
            │                           │                              │
            │ Foundry sync              │ tunnelled                    │ OAuth + REST
            │ (Dagster, every 30s)      ▼                              ▼
            │                  ┌─────────────────────────────────────────────────────┐
            └──────────────────│        self-hosted Linux box (Ubuntu 24.04)         │
                               │                                                     │
                               │  ┌──────────────────────────────────────────────┐  │
                               │  │ docker-compose stack:                        │  │
                               │  │   FastAPI · Dagster · Postgres 16            │  │
                               │  │   Parquet lakehouse + DuckDB                 │  │
                               │  │   Loki · Prometheus · Grafana (private)      │  │
                               │  │   reverse-tunnel connector (public API only) │  │
                               │  └──────────────────────────────────────────────┘  │
                               └─────────────────────────────────────────────────────┘
```

The hybrid topology is a deliberate cost/realism tradeoff. All-AWS at all-US scale runs ~$15–25/month; the local marginal cost is ~$1–3/month. The lakehouse-on-local-NVMe pattern (Postgres OLTP + Parquet + DuckDB) also better mirrors the operational/analytical separation found in production fleet ops platforms.

## 2. Component inventory

### Dashboard plane (Foundry)

Palantir Foundry developer-tier tenant, hosting:

- **Workshop apps** — Fleet Overview (live aircraft map), Site Drilldown (per-airport SLA + weather), Flight Detail (telemetry + trail). Bound to the AFM Ontology.
- **Ontology** — `Aircraft`, `Flight`, `Site`, `Operator`, `Case` objects with link types. Sourced from local DuckDB marts via Foundry sync (a Dagster asset on the self-hosted box).
- **AIP Logic** — natural-language fleet Q&A functions over the Ontology, complementing the Anthropic-Haiku-driven AFM-internal LLM path.

Foundry hosts the dashboard; there is no separate React frontend or AWS hosting. The local stack remains authoritative for data and runs standalone if Foundry is unreachable (sync simply pauses).

### Data plane (self-hosted)

A single `docker-compose` stack runs the operational backbone:

- **FastAPI** — REST and (reserved) WebSocket; auth; Salesforce sync
- **Dagster** — asset-oriented pipeline orchestrator; webserver, daemon, user-code containers
- **Postgres 16** — operational store (cases, timelines, site metrics, briefs, audit, reference data)
- **Parquet lakehouse on NVMe** — historical position snapshots, queried in-process via DuckDB
- **Loki + Promtail + Prometheus + Grafana** — observability stack
- **Reverse-tunnel connector** — exposes the public API only; observability UIs and Postgres remain network-scoped (not publicly reachable)

### CRM plane (Salesforce Agentforce DE org)

A Connected App for OAuth, custom objects (`AFM_Site__c`, `AFM_Flight__c`), custom Case fields, Permission Sets and custom permissions, an Apex consolidated controller for the agent's actions, a Lightning Web Component embedded on the Case record page, and the Agentforce agent itself with a Record-Triggered Flow invoking it on Case insert.

## 3. Data flow (high level)

Three primary flows operate continuously:

1. **Live ingestion.** A Dagster schedule polls OpenSky every 30 seconds for current US ADS-B positions, filters sensitive operators, writes each cycle as a Parquet snapshot partitioned by hour, and upserts the latest position per aircraft into a hot Postgres table for low-latency reads. A parallel five-minute schedule fetches METAR/TAF for the watched airport set.

2. **Anomaly detection.** Every five minutes, a rules engine reads the last hour of position snapshots via DuckDB plus current weather, applies six anomaly rules (lost signal, diversion, excessive holding, weather impact, go-around, delay), de-duplicates against open cases, and writes new cases to both Postgres (for fast dashboard reads) and Salesforce (as the system of record). The Salesforce Case insert fires a Record-Triggered Flow that invokes the Fleet Anomaly Triage Agentforce agent.

3. **Authentication.** A user logs into the Foundry workspace; Foundry handles SSO. View-mode and customer-region scoping flows through Foundry's permissions on Ontology objects (replaces the original Salesforce-OAuth-cookie-on-React-frontend chain — see Phase 04 re-plan). Salesforce remains the IdP for the LWC↔API path (Salesforce LWC → Apex → Named Credential → AFM API), and the AFM API still validates per-request scope claims as a defense-in-depth layer.

## 4. Network topology

Concrete hostnames are kept out of the public tree per scrub-infra discipline; endpoints are described by role.

| Endpoint | Purpose | Exposure |
|---|---|---|
| Foundry tenant | AFM dashboard (Workshop apps) | Foundry-hosted (its own TLS) |
| AFM backend API | `/v1/*` for Salesforce + Foundry sync | public, via the self-hosted reverse tunnel |
| Observability UI (Grafana) | dashboards/alerts | private network only — not publicly reachable |
| Dagster UI | pipeline ops | private network only — not publicly reachable |
| Transactional email | outbound alert/notification sender | provider-hosted |

The public API is TLS-terminated by the reverse tunnel; Foundry handles its own tenant TLS. No direct port exposure from the self-hosted box — only the tunnel connector reaches in, and it forwards the API alone. Observability UIs, Dagster, and Postgres are reachable only on the private network.

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
Foundry sync (Dagster asset) ──► FastAPI ──► QueryService ──► Postgres + Parquet/DuckDB
                              ──► Foundry Action API (HTTPS) ──► Foundry Ontology
                                                          │
                                                          ▼
                                         Foundry Workshop apps + AIP Logic

Dagster pipelines ──► External APIs (OpenSky, NOAA, Anthropic, Salesforce, Notion, Resend, Slack)
                  ──► Postgres + Parquet
                  ──► Salesforce Cases ──► SF Flow ──► Agentforce agent ──► Apex controller

Salesforce LWC ──► Apex controller ──► Named Credential ──► AFM API
```

Single-direction dependencies make the system tractable to debug. The Foundry sync never talks to Salesforce directly; the Salesforce LWC never talks to Postgres directly; both go through the AFM API.

## 7. Storage layout

Postgres holds operational state across two schemas: `app` (cases, case timeline, site metrics, briefs, audit logs, user sessions, sync watermarks) and `ref` (airports, aircraft registry, runbook index). Migrations are managed by Alembic.

The Parquet lakehouse uses Hive-style partitioning on `year/month/day/hour` so DuckDB can prune efficiently when reading historical positions. Each polling cycle writes one file (≈5,000 rows compressed via zstd to ~500 KB on average). A daily lifecycle job drops partitions older than 365 days; disk usage target stays under 500 GB sustained.

## 8. Deployment process

**Backend** is built into a Docker image by GitHub Actions, pushed to GHCR, and pulled by the self-hosted compose stack on each merge to `main`. Database migrations run before the API container starts.

**Foundry assets** (Ontology object definitions, Workshop app exports) are version-controlled in `foundry/ontology/*.yaml` and `foundry/workshop/*.json`. Tenant updates are applied via the Foundry CLI; CI verifies Ontology shapes match the local API contract.

**Salesforce metadata** is deployed via `sf project deploy start --target-org afm-dev`. Agent script changes trigger a separate workflow that runs `sf agent validate && sf agent publish && sf agent activate`.

---

## Full specification

This document presents AFM's architectural shape for portfolio review. Detailed implementation notes — exact failure-mode handling, deployment sequencing, observability metric catalog, complete component port and dependency inventory, and the per-phase build instructions — are held in private working documentation.

For a full architectural walkthrough or technical evaluation conversation, reach out: **Mark Andrew Marquez** · `mark@markandrewmarquez.com`
