---
id: excessive-holding
title: "Excessive Holding Pattern"
case_types:
  - excessive_hold
severity_floor: medium
tags:
  - flight-status
  - delay
salesforce_record_type: Case
related_runbooks:
  - weather-driven-arrival-backups
---

# Excessive Holding Pattern

## When this fires

An aircraft has loitered within 15 nm of a watched airport at under 15,000 ft for at least 30 minutes while its heading sweeps through 6 or more of the 8 compass sectors — a circling/holding pattern rather than transiting through. Aircraft transiting or sequencing 20+ nm out are deliberately excluded (the radius was tightened from 40 nm to 15 nm to cut transit false positives). Sustained holding indicates arrival sequencing or weather-related delay.

## Triage steps

1. **[Auto]** Severity set to MEDIUM.
2. Check destination airport's current METAR (`AFM_Site_Icao__c`) — IFR or LIFR conditions warrant escalation context.
3. Check whether other inbound flights to the same site are also holding (count of currently-holding aircraft inbound to the destination — available via the `holding_count` field in detection facts).
4. If holding extends past 30 minutes total, escalate to HIGH and re-evaluate whether the flight is likely to divert.
5. **[Auto]** If holding clears (aircraft resumes direct path to destination), case auto-resolves on next refresh.

## Customer communication

Notify the customer view of the affected destination with: "Inbound traffic into {site} is being sequenced; expect arrival delays of approximately {minutes} minutes." Use the holding count and the rolling 60-minute average delay to phrase the impact specifically rather than abstractly.

## Resolution criteria

- Auto-resolve when the aircraft exits the holding pattern and proceeds to land or divert (the latter would open a separate diversion case).
- Resolution timeline event records: total holding duration, peak number of concurrently-holding aircraft, and whether the case was correlated with weather.

## See also

- `weather-driven-arrival-backups.md`
