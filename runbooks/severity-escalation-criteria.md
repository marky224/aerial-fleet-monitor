---
id: severity-escalation-criteria
title: "Severity Escalation Criteria"
case_types:
  - lost_signal
  - diversion
  - excessive_hold
  - weather_impact
  - go_around
  - delay
severity_floor: low
tags:
  - severity
  - cross-cutting
  - reference
salesforce_record_type: Case
---

# Severity Escalation Criteria

## When this fires

Cross-cutting reference. Referenced from every case to document the rules under which a case's severity should escalate above the runbook's `severity_floor`. Linked automatically by the agent on every case.

## Triage steps

The Agentforce agent uses these criteria when calling `setCaseSeverity`. Human reviewers can use them to validate or override agent decisions.

### Promote to MEDIUM if any of the following

- Aircraft is within 30 nm of a customer-watched site.
- More than 2 simultaneously-active cases at the same site.
- Detection facts include `customer_impact: true`.

### Promote to HIGH if any of the following

- Lost signal aged >10 minutes with no recovery.
- Diversion to a non-customer site for a customer-watched flight.
- 3+ inbound flights to a customer site holding simultaneously during IFR conditions.
- Severity_floor in the runbook was already MEDIUM.

### Promote to CRITICAL if any of the following

- Lost signal aged >30 minutes for an aircraft above 10,000 ft (no recovery).
- Detection facts include `escalation_required: true` (set by rules under specific conditions).
- Three or more HIGH-severity cases active at the same customer site.
- Aircraft squawking 7500/7600/7700 (emergency codes) — note: AFM does not detect or surface these per the project's data scope; this row is included for completeness only.

## Customer communication

Severity changes are not communicated to customers directly. Severity drives internal routing and visibility (CRITICAL cases page on-call; HIGH appear at the top of customer notification panels).

## Resolution criteria

N/A — this is a reference runbook.
