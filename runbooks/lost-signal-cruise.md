---
id: lost-signal-cruise
title: "Lost Signal During Cruise"
case_types:
  - lost_signal
severity_floor: medium
tags:
  - telemetry
  - comms
  - fleet-health
salesforce_record_type: Case
salesforce_deeplink: "/lightning/r/Case/{case_id}/view"
related_runbooks:
  - severity-escalation-criteria
---

# Lost Signal During Cruise

## When this fires

An aircraft was airborne above 10,000 ft with normal telemetry, then stopped reporting position for >2 minutes. Detector waits 2 minutes after the last update before opening a case to allow for transient gaps.

## Triage steps

1. **[Auto]** Severity set to MEDIUM by default (HIGH if the gap exceeds 10 minutes or if the aircraft was within 30 nm of a customer site).
2. Confirm last known position, altitude, and heading from the case detection facts.
3. Check ADS-B receiver coverage map for the area — coverage gaps over remote terrain are common and not necessarily indicative of an actual issue.
4. Cross-reference any secondary feed if available (FlightAware, FlightRadar24) to confirm whether the signal loss is universal or specific to the OpenSky feed.
5. Wait at least 5 minutes after the last reported position before treating as a sustained outage.
6. **[Auto]** If the case ages past 10 minutes without a position update, severity is escalated to HIGH and an engineering Task is created.

## Customer communication

If the affected flight serves a watched customer site (`AFM_Customer_Region__c` is not 'All'), surface a notification in the customer's view using neutral language: "Telemetry interruption detected for flight {callsign}. Position will be restored when the signal recovers." Do not speculate on the cause. Do not contact the customer directly.

If the case escalates to HIGH and an actual operational disruption is confirmed (e.g., diversion follows), draft a customer email using the standard "Telemetry Interruption — Operational Impact" template (`salesforce_template_id`: TBD).

## Resolution criteria

- Auto-resolve when the aircraft's signal recovers and reaches its original destination within 60 minutes of the original ETA.
- Manual resolve if signal does not recover but flight outcome is confirmed via external sources.
- Final resolution timeline event must record: last known position, signal-recovery position (if any), and total signal-loss duration.

## See also

- `severity-escalation-criteria.md` — for the rules governing MEDIUM → HIGH escalation
