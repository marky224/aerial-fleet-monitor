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
            │ (Dagster, 1–5 min)        ▼                              ▼
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
- **Ontology** — `Aircraft`, `Flight`, `Site`, `Case` objects with link types. Populated by the Foundry sync: Dagster assets on the self-hosted box that read the AFM API and write to the Ontology.
- **AIP Logic** — natural-language fleet Q&A functions over the Ontology.

Foundry hosts the dashboard; there is no separate React frontend or AWS hosting. The local stack remains authoritative for data and runs standalone if Foundry is unreachable (sync simply pauses).

### Data plane (self-hosted)

A single `docker-compose` stack runs the operational backbone:

- **FastAPI** — REST and (reserved) WebSocket; Salesforce sync
- **Dagster** — asset-oriented pipeline orchestrator; webserver, daemon, user-code containers
- **Postgres 16** — operational store (cases, timelines, site metrics, briefs, audit, reference data)
- **Parquet lakehouse on NVMe** — historical position snapshots, queried in-process via DuckDB
- **Loki + Promtail + Prometheus + Grafana** — observability stack
- **Reverse-tunnel connector** — exposes the public API only; observability UIs and Postgres remain network-scoped (not publicly reachable)

### CRM plane (Salesforce Agentforce DE org)

A Connected App for OAuth, custom objects (`AFM_Site__c`, `AFM_Flight__c`), custom Case fields, Permission Sets and custom permissions, an Apex trigger on Case insert that runs deterministic triage (investigation Tasks, runbook Files, escalation) and then a Salesforce Models API call for the severity justification and customer narrative, the on-demand Fleet Anomaly Triage Agentforce agent with one consolidated Apex action, and an Apex REST action that writes the daily brief and posts it to Chatter.

## 3. Data flow (high level)

Three primary flows operate continuously:

1. **Live ingestion.** A Dagster sensor polls OpenSky every 300 seconds for current US ADS-B positions, filters sensitive operators, writes each cycle as a Parquet snapshot partitioned by hour, and upserts the latest position per aircraft into a hot Postgres table for low-latency reads. A parallel five-minute schedule fetches METAR/TAF for the watched airport set.

2. **Anomaly detection and Case sync.** Every five minutes, a rules engine reads the last 30 minutes of position snapshots via DuckDB plus current weather, applies six anomaly rules (lost signal, diversion, excessive holding, weather impact, go-around, delay), de-duplicates against open cases, and writes each new case **to Postgres only**, marked `pending` when its severity is high and `skipped` otherwise. The Salesforce write is intentionally *decoupled*: a separate sync path (the `POST /v1/cases/sync-pending` endpoint, driven by a ~60-second pipeline sensor) drains `pending` cases into Salesforce — so a Salesforce outage degrades sync but can never block detection. The push is idempotent and single-flighted (a duplicate external id reconciles to the existing record rather than failing), and retries transient failures while parking permanent ones. Salesforce remains the Case system of record: the Case insert fires an Apex trigger that runs deterministic triage and queues a Salesforce Models API call for the severity justification and customer narrative, and a reverse `sf_case_sync` pull (~60 seconds, watermarked on `SystemModstamp`) mirrors Salesforce-side changes — status, priority, description and the severity justification — back into Postgres for fast dashboard reads.

3. **Authentication.** A user logs into the Foundry workspace; Foundry handles sign-in. Customer-region scoping in the Workshop apps is a filter widget; no per-user region scoping is built. The AFM API has no login in v1: it serves every request as one internal, all-regions identity (`GET /v1/auth/me` returns that stub), and it authenticates to Salesforce with OAuth 2.0 Client Credentials.

## 4. Network topology

Concrete hostnames are kept out of the public tree per scrub-infra discipline; endpoints are described by role.

| Endpoint | Purpose | Exposure |
|---|---|---|
| Foundry tenant | AFM dashboard (Workshop apps) | Foundry-hosted (its own TLS) |
| AFM backend API | `/v1/*` for Salesforce + Foundry sync | public, via the self-hosted reverse tunnel |
| Observability UI (Grafana) | dashboards | private network only — not publicly reachable |
| Dagster UI | pipeline ops | private network only — not publicly reachable |

The public API is TLS-terminated by the reverse tunnel; Foundry handles its own tenant TLS. No direct port exposure from the self-hosted box — only the tunnel connector reaches in, and it forwards the API alone. Observability UIs, Dagster, and Postgres are reachable only on the private network.

## 5. Runtime characteristics

| Property | Target |
|---|---|
| Position poll latency budget | ≤2 seconds per poll cycle |
| Case detection cadence | every 5 minutes |
| Live position refresh | OpenSky poll and Foundry positions sync every 300 seconds each |
| Case appears in dashboard after detection | within about a minute (Foundry cases sync, ~60-second sensor) |
| Case appears in Salesforce | within about a minute, high-severity cases only (~60-second push sensor) |
| Agentforce agent completes its triage action | ≤30 seconds |
| Daily brief generation | 7am customer-local |

## 6. Service boundaries

```
Foundry sync (Dagster asset) ──► FastAPI ──► QueryService ──► Postgres + Parquet/DuckDB
                              ──► Foundry Action API (HTTPS) ──► Foundry Ontology
                                                          │
                                                          ▼
                                         Foundry Workshop apps + AIP Logic

Dagster pipelines ──► External APIs (OpenSky, NOAA)
                  ──► Postgres + Parquet
                  ──► AFM API ──► Salesforce Cases ──► Apex trigger ──► triage (Tasks, runbook Files) + Models API text

Agentforce agent (on demand) ──► Apex action ──► the same triage
```

Single-direction dependencies make the system tractable to debug. The Foundry sync never talks to Salesforce, and the pipelines reach Salesforce only through the AFM API.

## 7. Storage layout

Postgres holds operational state across two schemas: `app` (cases, case timeline, site metrics, briefs, audit logs, user sessions, sync watermarks) and `ref` (airports, aircraft registry, runbook index). Migrations are managed by Alembic.

The Parquet lakehouse uses Hive-style partitioning on `year/month/day/hour` so DuckDB can prune efficiently when reading historical positions. Each polling cycle writes one file (≈5,000 rows compressed via zstd to ~500 KB on average). No job prunes the position partitions in v1; the only lakehouse retention sweep is `foundry_flight_archive_purge`, which drops `flights_archive/` day partitions older than 30 days.

## 8. Deployment process

**Backend** images are built on the host by `docker compose` from `api/Dockerfile` and `pipelines/Dockerfile`; no workflow builds or publishes an image. Database migrations are applied with `make db-migrate`.

**Foundry assets**: the Ontology object definitions are version-controlled in `foundry/ontology/*.yaml` as the specification for provisioning the object types in the tenant. The Workshop apps are built in the tenant and cannot be exported as files (`foundry/workshop/README.md`). No CI job checks the Ontology.

**Salesforce metadata** is deployed with `make sf-deploy` (`sf project deploy start`). The `sf-agent-deploy` workflow runs a check-only deploy with the Apex tests on each pull request that touches Salesforce paths; on merge it deploys, runs the Apex tests, seeds the runbook records and Files, and publishes and activates the agent when its authoring bundle changed.

---

## Full specification

This document presents AFM's architectural shape for portfolio review. Detailed implementation notes — exact failure-mode handling, deployment sequencing, observability metric catalog, complete component port and dependency inventory, and the per-phase build instructions — are held in private working documentation.

For a full architectural walkthrough or technical evaluation conversation, reach out: **Mark Andrew Marquez** · `mark@markandrewmarquez.com`
