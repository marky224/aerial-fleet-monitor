# Aerial Fleet Monitor — Salesforce Implementation

The full Salesforce specification covers org provisioning, OAuth Connected App configuration, the custom data model, the Permission Set design, the Apex trigger and consolidated triage action, the Agentforce agent script, and the dev workflow.

## Topics covered in the full specification

- Org provisioning (Agentforce DE setup, My Domain, default timezone)
- Connected App configuration (OAuth 2.0 Client Credentials for the API, JWT for CI, secret-management policy)
- Demo users (3 users — internal-ops, west-coast-ops, east-coast-ops — with Profile + Permission Set + custom permission assignment)
- Custom data model summary (cross-references the data model spec)
- Permission Sets and custom permissions (5 perm sets, 3 custom perms, OWD-vs-backend-filter sharing rationale)
- Apex classes:
  - `AFM_TriageCase` (the one consolidated `@InvocableMethod` action the agent calls)
  - `AFM_CaseTriageService` (deterministic triage: investigation Tasks, escalation, customer-notification Task, runbook File links)
  - `AFM_CaseTriggerHandler` and the `AFM_CaseTrigger` trigger (auto-triage on Case insert)
  - `AFM_TriageNarrative` (Salesforce Models API text for the severity justification and customer narrative)
  - `AFM_RunbookFileSeeder` (seeds the runbook Files in one transaction: a new File, a new version when the content changed, or nothing; called by `scripts/seed_runbook_files.sh`)
  - `AFM_TestDataFactory` (`@IsTest` helpers)
  - `AFM_BriefSynthesis` (Phase 11 — `@RestResource` daily-brief action: Salesforce Models API synthesis + Chatter post to the "Fleet Operations Daily" group)
  - `*_Test` class patterns
- Agentforce agent: agent-script structure, the single triage action, deployment via the `sf agent` CLI
- Case trigger (Case insert → synchronous triage, then an asynchronous Models API call)
- Two-way sync: AFM → SF via REST, SF → AFM via 60s polling, the centralized region-value translation convention
- Dev workflow (retrieve, validate, deploy, run-tests, open-org commands)

The full Salesforce implementation specification is available on request.

---

This stub exists so readers know this scope is documented. For the complete specification, including the full Apex source, agent script, and metadata layout, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
