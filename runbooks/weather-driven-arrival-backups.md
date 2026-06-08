---
id: weather-driven-arrival-backups
title: "Weather-Driven Arrival Backups"
case_types:
  - weather_impact
severity_floor: medium
tags:
  - weather
  - customer-impact
  - site-level
salesforce_record_type: Case
related_runbooks:
  - excessive-holding
  - diversion-to-alternate
  - customer-communication-ifr-operations
---

# Weather-Driven Arrival Backups

## When this fires

A watched site reports IFR (or worse) conditions in METAR while three or more aircraft are inbound within the next 30 minutes. The case is opened against the site, not against any single flight.

## Triage steps

1. **[Auto]** Severity set to MEDIUM. Escalates to HIGH when conditions are LIFR or when more than 8 inbound flights are affected.
2. Read the current METAR and any TAF forecast covering the next 2 hours. Note whether conditions are improving or deteriorating.
3. Check for related cases: are inbound flights diverting or holding? If yes, link those cases via `related_runbooks` references.
4. **[Auto]** Customer notification is drafted for the site's customer region (West Coast or East Coast). Notification includes current flight category and TAF-derived expected duration.

## Customer communication

The notification template should explicitly name the weather conditions and give a TAF-grounded duration estimate where available. Tone: factual and specific — avoid "we are monitoring" boilerplate. Sample structure:

> "**KSFO operating in IFR conditions** (ceiling {ft} ft, visibility {sm} sm). TAF forecasts conditions to lift after {time}. Expect arrival sequencing delays during this period. {N} flights currently inbound. We will update when conditions improve."

## Resolution criteria

- Auto-resolve when the site's flight category returns to MVFR or VFR for at least 30 consecutive minutes.
- Resolution timeline event records: peak severity, total duration of impact, count of related diversion/hold cases.

## See also

- `excessive-holding.md`
- `diversion-to-alternate.md`
- `customer-communication-ifr-operations.md`
