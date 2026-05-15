# Foundry Workshop apps

This directory is the version-controlled home for AFM's three Foundry
Workshop dashboard apps:

- **Fleet Overview** — Map widget bound to the `Aircraft` Ontology object
  (heading-rotated markers, altitude-band colors), KPI strip, cases panel.
- **Site Drilldown** — site selector → SLA scorecard → weather panel →
  inbound/outbound counts, bound to the `Site` Ontology object.
- **Flight Detail** — aircraft telemetry readout, trail polyline, ETA, and
  open cases; opened by clicking an aircraft on Fleet Overview.

## No exported app definitions are committed here

Workshop app definitions are **not exportable as files on the AFM
developer-tier tenant** (the same tier constraint that defers OSDK and
service-account OAuth — the sync writes via the Foundry Action
`applyBatch` API with a user-scoped bearer token, no SDK).

Because the tenant emits nothing to commit, the apps are reproduced by
following a step-by-step provisioning walkthrough rather than by
re-importing `*.json`. The walkthrough is the authoritative,
version-controlled artifact for these apps: widget bindings, KPI
definitions, the shared altitude color palette, click-through wiring,
and per-app verification steps.

The walkthrough lives outside the public tree (it captures manual
tenant-side steps). Maintainers: see `_private/docs/foundry/WORKSHOP_APPS.md`.
