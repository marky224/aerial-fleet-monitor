# Aerial Fleet Monitor

> Real-time fleet operations and customer-success console — built on public US aviation data as a stand-in for private fleet telemetry.

[![Build](https://github.com/marky224/aerial-fleet-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/marky224/aerial-fleet-monitor/actions)
[![Coverage](https://codecov.io/gh/marky224/aerial-fleet-monitor/branch/main/graph/badge.svg)](https://codecov.io/gh/marky224/aerial-fleet-monitor)
[![License: Proprietary](https://img.shields.io/badge/license-Proprietary-red)](./LICENSE.md)

---

## What it is

AFM is a unified fleet operations console plus a deeply integrated Salesforce Service Cloud back-end. It ingests live US aircraft positions, weather data, and reference airport data; detects six categories of operational anomalies in real time; opens Salesforce Cases for each anomaly; runs an Agentforce-driven triage workflow; and surfaces everything to internal advocates and customer ops managers through region-scoped dashboards.

The project demonstrates a complete Mission Success / Success Systems toolchain — telemetry visualization, anomaly detection, CRM-backed case management, AI agent triage, customer-facing notifications, runbook-driven operational consistency, and self-hosted observability.

It uses public aviation data because real drone telemetry isn't available to me, and aviation data has the same shape (geospatial telemetry, multi-asset, real-time, with anomalies that map cleanly onto fleet-ops concerns). The project copy is aviation-only; the architecture and patterns are fleet-agnostic.

## Architecture at a glance

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Frontend plane (AWS edge)                                              │
│    React + ArcGIS Maps SDK + Tailwind + shadcn/ui                       │
│    S3 + CloudFront                                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                  │ HTTPS
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Data plane (self-hosted on openclaw-pc, exposed via Cloudflare Tunnel) │
│    FastAPI  ←→  Postgres 16  ←→  Parquet lakehouse (DuckDB)             │
│    Dagster orchestration                                                │
│    Loki + Prometheus + Grafana (observability)                          │
└─────────────────────────────────────────────────────────────────────────┘
                                  │ REST + OAuth
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  CRM plane (Salesforce Agentforce DE)                                   │
│    Custom objects: AFM_Site__c, AFM_Flight__c                           │
│    Custom Case fields, Permission Sets, custom permissions              │
│    Apex consolidated controller (5 @InvocableMethod actions)            │
│    Lightning Web Component on Case page                                 │
│    Agentforce agent: Fleet Anomaly Triage                               │
│    Record-Triggered Flow → async agent invocation                       │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │  External integrations      │
                    │  • OpenSky Network (positions)
                    │  • NOAA AWC (weather)       │
                    │  • Anthropic (LLM)          │
                    │  • Notion (runbooks)        │
                    │  • Resend (email)           │
                    │  • Slack (case events)      │
                    └─────────────────────────────┘
```

Full architecture detail available on request.

## Tech stack

**Frontend:** React 18 · TypeScript · Vite · Tailwind CSS · shadcn/ui · TanStack Query · Zustand · ArcGIS Maps SDK 4.x

**Backend:** FastAPI · Pydantic v2 · Postgres 16 · DuckDB over Parquet · Alembic migrations · structlog

**Orchestration:** Dagster (asset-oriented pipelines, schedules, sensors)

**CRM:** Salesforce Agentforce Developer Edition · Apex · Lightning Web Components · Agentforce agents · SFDX

**AI:** Anthropic Claude (Sonnet for case triage rationale, Haiku for case summaries and daily briefs); Agentforce Atlas LLM for the triage agent itself

**Observability:** Loki · Promtail · Prometheus · Grafana (5 dashboards: Fleet Ops Overview, Salesforce Health, Pipeline Health, Per-Airport SLA Trends, Case Detector Tuning)

**Infrastructure:** Docker Compose on Ubuntu 24.04 · Cloudflare Tunnel · Cloudflare Access · AWS S3 + CloudFront for frontend · Route 53

**Testing:** pytest · Vitest · Playwright · schemathesis · Apex test framework · GitHub Actions CI

## Local development

```bash
git clone https://github.com/marky224/aerial-fleet-monitor.git
cd aerial-fleet-monitor

cp .env.example .env             # fill in API keys
make install                     # Python venvs + pnpm
make db-migrate                  # Postgres schema
make db-seed                     # reference airport data

make dev                         # docker compose up -d + frontend dev server
```

Then:
- Dashboard: [http://localhost:5173](http://localhost:5173)
- API: [http://localhost:8000/v1/docs](http://localhost:8000/v1/docs) (Swagger UI)
- Dagster: [http://localhost:3000](http://localhost:3000)
- Grafana: [http://localhost:3001](http://localhost:3001) (login `admin` / your `.env` password)

For Salesforce: a separate Agentforce Developer Edition org is required.

Tests:
```bash
make test-unit           # ~30 seconds
make test-integration    # ~3 minutes (needs SF dev org credentials)
make test-contract       # ~1 minute
make test-e2e            # ~5 minutes (Playwright against staging)
```

## Documentation

Detailed design docs (architecture, data model, API contracts, Salesforce setup, pipelines, frontend patterns, runbooks, observability, testing strategy) are maintained privately. Available on request for technical evaluation — `mark@markandrewmarquez.com`.

## License

This project is **proprietary** — all rights reserved. The repository is publicly visible for portfolio review and technical evaluation. You can read the code, clone it locally for evaluation, and reference it in conversations. You cannot use it commercially, deploy it as a service, redistribute it, or build derivative works.

## Contact

Mark Andrew Marquez · [markandrewmarquez.com](https://markandrewmarquez.com) · mark@markandrewmarquez.com

For commercial licensing inquiries, hiring conversations, or technical questions about the architecture, use the email above.

---

Built on public data. Designed to look like production. Architected to absorb the move to real fleet telemetry without redesign.
