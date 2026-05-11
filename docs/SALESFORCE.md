# Aerial Fleet Monitor — Salesforce Implementation

The full Salesforce specification covers org provisioning, OAuth Connected App configuration, the custom data model, the Permission Set design, the Apex consolidated-controller pattern, the Lightning Web Component, the Agentforce agent script, the Record-Triggered Flow, and the dev workflow.

## Topics covered in the full specification

- Org provisioning (Agentforce DE setup, My Domain, default timezone)
- Connected App configuration (OAuth scopes, callback URLs for prod and local dev, refresh-token policy, secret-management policy)
- Demo users (3 users — internal-ops, west-coast-ops, east-coast-ops — with Profile + Permission Set + custom permission assignment)
- Custom data model summary (cross-references the data model spec)
- Permission Sets and custom permissions (3 perm sets, 3 custom perms, OWD-vs-backend-filter sharing rationale)
- Apex classes:
  - `AFM_AgentActionsController` (consolidated `@InvocableMethod` actions wired to the agent)
  - `AFM_TelemetryController` (LWC's bridge to the AFM API via Named Credential)
  - `AFM_TestDataFactory` (`@IsTest` helpers)
  - `*_Test` class patterns
- Lightning Web Component `afmFlightTelemetry` (HTML template, JS implementation, JS-meta config, Case Lightning Record Page placement, Jest tests)
- Agentforce agent: agent-script structure, action invocation sequence, severity-decision criteria, deployment via the `sf agent` CLI
- Record-Triggered Flow (Case insert → async agent invocation)
- Two-way sync: AFM → SF via REST, SF → AFM via 60s polling, the centralized region-value translation convention
- Dev workflow (retrieve, validate, deploy, run-tests, open-org commands)

The full Salesforce implementation specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the full Apex source, LWC files, agent script, and metadata layout, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
