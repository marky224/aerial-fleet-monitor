---
id: go-around-investigation
title: "Go-Around Investigation"
case_types:
  - go_around
severity_floor: low
tags:
  - flight-status
  - approach
salesforce_record_type: Case
---

# Go-Around Investigation

## When this fires

An aircraft descended below 2,000 ft AGL within 5 nm of a runway threshold, then climbed back through 2,000 ft AGL while still in the airport's vicinity. This signature usually indicates an aborted landing approach.

## Triage steps

1. **[Auto]** Severity set to LOW. Single go-arounds are routine and rarely indicate a customer-impacting issue.
2. Check if multiple aircraft at the same site have gone around in the past 60 minutes — if so, escalate to MEDIUM and link to any active weather case for the site.
3. Track whether the affected aircraft completes a successful approach within 30 minutes (auto-tracked).
4. If a second go-around occurs on the same flight, escalate to MEDIUM and add a new timeline event.

## Customer communication

Single go-arounds do not warrant proactive customer communication. If a pattern emerges (≥3 go-arounds at the same site in 60 minutes), the underlying weather or operational case (if open) carries the customer notification — this case stays internal.

## Resolution criteria

- Auto-resolve when the aircraft successfully lands at the original destination.
- Auto-resolve when the case is older than 2 hours and no further go-around behavior is detected.
- Resolution timeline event records: go-around count for the flight, eventual outcome (landed, diverted, etc.).
