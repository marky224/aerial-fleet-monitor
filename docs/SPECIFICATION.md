# Aerial Fleet Monitor — Specification

The full specification is the project's locked v1 constitution: scope, architectural decisions, demo-account model, the 12-phase build sequence, and the criteria that define v1 as complete.

## Topics covered in the full specification

- Project overview and one-sentence pitch
- Goals and non-goals (in-scope for v1, deferred to v2, explicit "never do")
- Key decisions, with rationale for each:
  - Hybrid edge/local architecture
  - Lakehouse pattern (Postgres OLTP + Parquet/DuckDB analytical)
  - Salesforce as IdP via OAuth Web Server Flow
  - Foundry-hosted dashboard (Workshop apps + Ontology) replacing a local React/ArcGIS frontend
  - BaselineProvider abstraction (OpenSky vs local Parquet baseline swap point)
  - LLM strategy (Anthropic Haiku for AFM, Agentforce default for the SF agent, Foundry default for AIP Logic)
  - Notification channel separation (in-app, Salesforce, out-of-band)
  - Self-hosted observability stack
  - License and visibility decisions
- Demo accounts and customer scoping (3 Salesforce users, Permission Sets, watched-airport tagging)
- Build phases (12 phases, each documented separately)
- Success criteria for v1 completion
- Open questions, v2 reservations, and items to revisit during build

The full specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the per-phase build sequence, the decision rationales, and the locked demo-account model, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
