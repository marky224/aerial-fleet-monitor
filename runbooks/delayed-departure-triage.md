---
id: delayed-departure-triage
title: "Delayed Departure Triage"
case_types:
  - delay
severity_floor: low
tags:
  - delay
  - customer-impact
  - departure
salesforce_record_type: Case
---

# Delayed Departure Triage

## When this fires

A flight's actual time aloft has exceeded 1.5x the baseline duration for the city pair (per the active `BaselineProvider`), or the flight has remained on the ground past its expected departure window by more than 30 minutes when the origin airport is on the customer watchlist.

## Triage steps

1. **[Auto]** Severity set to LOW for departures delayed <60 min, MEDIUM for >60 min.
2. Cross-reference the origin airport's METAR — weather-driven delays warrant linking to any active weather case.
3. Check whether the operator has multiple delayed departures from the same site (operator-wide issue vs. flight-specific).
4. **[Auto]** Customer notification draft is created if the destination is on the customer's watchlist (since arrival times will be affected).

## Customer communication

For customer-watched destinations, mention specific expected arrival impact: "Flight {callsign} delayed at origin {origin}. Revised arrival at {dest}: {time}." Do not speculate on cause unless cause is documented in detection facts.

## Resolution criteria

- Auto-resolve when the flight departs.
- If the flight is canceled (no departure within 4 hours of expected), close as "canceled" with appropriate timeline event.
