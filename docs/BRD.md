# Aerial Fleet Monitor — Business Requirements Document

> **Document type:** BRD, v1.0 (public summary)
> **Audience:** Portfolio reviewers, hiring managers, stakeholders evaluating the system's design and intent.

---

## Executive summary

Aerial Fleet Monitor (AFM) is a real-time fleet operations and customer-success console that demonstrates the architecture and integration patterns required to surface live telemetry, detect operational anomalies, and triage them through a connected CRM. The system uses public US aviation data as a stand-in for private fleet telemetry, and is built to mirror the patterns a fleet operations team would use to monitor a fleet of autonomous aerial assets and proactively communicate with the customers whose operations depend on those assets.

The project demonstrates competencies across:

- **Salesforce Service Cloud architecture** — Connected App design, OAuth Web Server Flow, custom data modeling, Permission Sets and custom permissions, Lightning Web Components, Agentforce agents
- **Customer experience design** — internal-vs-customer view modes, scoped data access via real Salesforce sharing, region-tailored notifications
- **Data engineering** — operational/analytical storage split, real-time ingestion pipelines, lakehouse-pattern analytics
- **AI integration** — agent-driven case triage with multi-action workflows; LLM-powered case summarization and operational digests
- **Production discipline** — observability, automated testing, CI/CD, knowledge management via runbooks

## Problem statement

Operating a fleet of autonomous aerial assets at scale produces continuous streams of telemetry across thousands of independent assets serving multiple customers, each with different operational sites and SLA expectations. The fleet operations function is responsible for:

1. Detecting operational anomalies in real time before they become customer-impacting incidents
2. Triaging cases with consistent severity assessment and runbook-aligned response
3. Communicating with customers through their preferred channels and at appropriate detail level
4. Surfacing operational insight to internal teams and external stakeholders through dashboards and proactive notifications
5. Maintaining the institutional knowledge that enables consistent triage and customer communication

Without a unified system, this work fragments across disconnected tools — telemetry in one platform, cases in another, customer communication in a third, runbooks in yet a fourth. Every fragmentation introduces latency, inconsistency, and customer experience regressions.

## Solution overview

AFM is a single console plus a deeply integrated Service Cloud back-end. It provides:

- A real-time map-based dashboard showing all flights in the United States, with airport drill-down, single-flight telemetry detail, and dotted flight-path trails
- Real-time anomaly detection across six rule types (lost signal, diversion, excessive holding, weather impact, go-around, delay), generating Salesforce Cases with structured detection facts and runbook references
- An Agentforce-driven triage workflow that automatically sets case severity, creates investigation tasks, drafts customer notifications, and escalates engineering on critical events
- A Lightning Web Component embedded on the Salesforce Case page, surfacing live telemetry inside the Service Console where support advocates work
- Region-scoped customer views authenticated via real Salesforce OAuth + Permission Sets, demonstrating the same pattern used in production Experience Cloud deployments
- Daily operations briefs generated per-region in customer-local time, posted to Salesforce Chatter and delivered via email
- A markdown-based runbook library version-controlled in GitHub and synced to Notion, with each runbook structured for both human readers and the Agentforce agent's reasoning
- Self-hosted observability (Loki + Prometheus + Grafana) demonstrating operational maturity and quantifiable system health

## Stakeholder roles

| Role | Interaction with AFM |
|---|---|
| **Internal-ops advocate** (fleet operations team) | Primary user. Logs into the Service Console; works Cases as they're triaged by Agentforce. Reviews per-airport SLA scorecards. Reads daily ops brief in Chatter. |
| **Customer ops manager** (West Coast or East Coast) | Authenticated user via Salesforce OAuth. Sees only their region's data. Receives customer-friendly notifications and daily briefs. |
| **Engineering on-call** | Recipient of escalation Tasks for critical cases. May extend or tune detection rules. |
| **Product / leadership** | Reviews Grafana dashboards for system health and operational trends. Approves runbook changes via PR review. |
| **Hiring manager** (portfolio reviewer) | Evaluates the system as evidence of relevant capabilities. Cold-visits the demo, walks the cases workflow, reads the BRD and the architecture doc. |

## Quality attributes

- **Freshness.** Live aircraft positions stay no more than 60 seconds stale under normal operation; staleness is explicitly labeled in the UI rather than silently presented as fresh.
- **Latency.** A new anomaly results in a Salesforce Case within 30 seconds of detection; the dashboard reflects the case within 60 seconds.
- **Availability.** The Foundry-hosted dashboard remains accessible when the local backend is degraded; cached Ontology data continues to render with a sync-staleness indicator. The system tolerates one upstream component being down without cascading failure.
- **Cost discipline.** Ongoing operating cost stays at ~$5/month, by deliberate architectural choice (self-hosted data plane plus free Foundry developer-tier tenant for the dashboard).
- **Scope enforcement.** Customer-region scoping is enforced at every layer — UI, API, and Salesforce-side — not just one.
- **Auditability.** Every case state change produces a structured timeline event with `request_id` correlation; every Salesforce write produces a structured log line.
- **Reproducibility.** The entire system (excluding the Salesforce dev org) is reproducible from a fresh `git clone` with a single `make install && docker compose up -d`.

## Architecture summary

The system spans three planes:

- **Dashboard plane** — Palantir Foundry (developer tier): Workshop apps + Ontology + AIP Logic, replacing what would otherwise be a custom React frontend
- **Data plane** — FastAPI + Dagster + Postgres + Parquet lakehouse, running in Docker Compose on a self-hosted Linux server, with the API exposed publicly via a self-hosted reverse tunnel (observability UIs and Postgres stay private)
- **CRM plane** — Salesforce Agentforce Developer Edition with custom data model, Apex callouts, Lightning Web Components, Agentforce agent, Record-Triggered Flow

Auth flows through Salesforce as the IdP via OAuth 2.0 Web Server Flow. Region scoping flows from real Salesforce custom permissions through to the AFM JWT and into every backend query.

## Success metrics

The system is successful when:

- An authorized user logs into the Foundry workspace, opens the Fleet Overview Workshop app, understands what they're seeing within 30 seconds, and successfully switches view-mode (internal-ops vs customer scope) within 60 seconds.
- A real anomaly triggers a Case in Salesforce, the Agentforce agent runs all five actions, and the resulting state is visible in both the dashboard and the Service Console within 90 seconds end-to-end.
- A reviewer reading the BRD, README, and one technical doc can answer: what does it do, who is it for, why does it exist, how does it work, what does success look like, what's next.
- A hiring manager opening Grafana sees five dashboards with real data, a pipeline lag under 30 seconds, and a Salesforce sync queue at zero. The system looks like it's running at production discipline.

## Roadmap

### v1 (this release)

Real-time visibility, anomaly detection, Agentforce triage, scoped customer views, LWC inside Service Console, daily briefs, runbook library, full observability stack, full test pyramid, two demo customer regions.

### v2 (post-deployment)

- **NL chat ("Ask the console")** — LangGraph + Anthropic tool-use over the lakehouse, exposing the existing `QueryService` as LLM-callable tools. v1 architecture deliberately accommodates this without refactor.
- **Additional customer regions** — license headroom exists; mainly a config and permissions change.
- **Read-write Salesforce sync via Platform Events** — replaces the polling loop with sub-second sync.
- **Predictive delay model** — small ML model trained on rolling Parquet history.
- **Mobile-tuned Workshop layouts** — dashboard at tablet and phone widths (Foundry default is desktop).

### Beyond v2

- **Real fleet telemetry integration** — repointing the ingestion source from public aviation data to a real platform's telemetry stream. The architecture is designed to absorb this change.
- **Multi-tenant Salesforce deployment** — moving from a DE org to a packaged AppExchange-style deployment.

## Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| OpenSky API rate-limited | medium | medium | Polling cadence stays well under daily credit cap; Grafana alerts at 90%; fallback to slower polling |
| Salesforce DE org storage fills | low | medium | Test cleanup discipline; lifecycle on `app_logs`; 5MB storage is plenty for a demo |
| Agentforce LLM latency | medium | low | Async Flow invocation; user doesn't wait on agent latency for the Case creation |
| Reverse tunnel disconnects | low | low | Tunnel connector auto-reconnects; Docker `restart: unless-stopped` |
| Case detector false positives | medium | low | Each rule has explicit dedup window; Grafana dashboard highlights rule-level rates; runbooks explicitly note "not all triggers are issues" |

## Out-of-scope and explicit non-goals

The system is **not**:

- A flight tracking app (FlightAware, FlightRadar24 already exist; AFM is not competitive)
- A flight-safety or flight-planning system (zero operational impact on real flights)
- A military or sensitive-flight observability tool (denylist filters these at ingestion)
- A general-purpose Salesforce package or AppExchange listing
- A multi-tenant SaaS product (single-tenant demo only)
- A customer-facing Salesforce UI (v1 customers interact with AFM via the dashboard only; the Service Console, the `afmFlightTelemetry` LWC, and direct Salesforce record access are internal-ops only)

---

## Full specification

This document presents AFM's high-level requirements and intent for portfolio review. Detailed functional and non-functional requirements (FR-1 through FR-10, NFR-1 through NFR-7), the integration map detail, deployment specifics, and the per-phase build instructions are held in private working documentation.

For a full architectural or requirements walkthrough, reach out: **Mark Andrew Marquez** · `mark@markandrewmarquez.com`
