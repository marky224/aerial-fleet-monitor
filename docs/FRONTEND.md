# Aerial Fleet Monitor — Dashboard

The full dashboard specification documents AFM's Foundry-hosted operator UI: the Ontology layer that models Aircraft / Flight / Site / Operator / Case objects, the Workshop apps that render the operator UX, the local→Foundry sync service that keeps data fresh, and the AIP Logic functions that surface natural-language Q&A over the fleet.

## Topics covered in the full specification

- Foundry workspace setup (developer-tier tenant, OAuth client, OSDK installation)
- Ontology object shapes (`Aircraft`, `Flight`, `Site`, `Operator`, `Case`) and link types
- Local→Foundry sync architecture:
  - Dagster assets (`foundry_positions_sync` every 30s, `foundry_sites_sync` every 5min)
  - Service-to-service OAuth credentials grant with in-memory token cache
  - Independent failure-domain design (Foundry unreachable → sync skip, never breaks local pipeline)
- Workshop app inventory:
  - **Fleet Overview** — Map widget bound to `Aircraft` Ontology, KPI strip, cases panel
  - **Site Drilldown** — site selector, SLA scorecard, weather panel, inbound/outbound tables
  - **Flight Detail** — telemetry readout, trail polyline, ETA, open cases
- Map visual language (heading-rotated markers, altitude-band colors matching the Salesforce LWC palette)
- AIP Logic surfaces (`flightStatusSummary` starter function; broader NL-Q&A deferred)
- View-mode handling (internal-ops vs customer view via Salesforce-OAuth scope claims propagated to Foundry)
- Time and timezone handling (UTC API, user-tz display in Workshop)
- Testing approach (sync unit tests + Dagster asset tests; Workshop app tests via Foundry's own framework)
- Standalone-guarantee invariant (the local stack must run end-to-end without Foundry being reachable)
- Explicitly out of v1 (mobile-tuned Workshop layouts, i18n, real-time push beyond 30s polling)

The full dashboard specification is available on request.

---

This stub exists so that automated reviewers and human readers know this scope is documented. For the complete specification, including the Ontology schemas, sync service implementation, and Workshop app definitions, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
