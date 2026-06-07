---
id: customer-communication-ifr-operations
title: "Customer Communication: IFR Operations"
case_types:
  - weather_impact
  - diversion
severity_floor: low
tags:
  - communication
  - templates
  - cross-cutting
salesforce_record_type: Case
---

# Customer Communication: IFR Operations

## When this fires

This runbook is referenced from any case where customer-facing weather communication is required. It does not fire on its own — it's linked from weather-impact and diversion cases as a communication standard reference.

## Triage steps

This runbook does not have triage steps in the operational sense. It is referenced for content guidance.

### Recommended message structure for IFR conditions

1. **Lead with the condition.** Name the airport and the flight category explicitly. Don't say "weather is impacting"; say "IFR conditions" or "LIFR conditions" with ceiling and visibility numbers.
2. **Give a duration estimate.** Cite the TAF forecast time window. Avoid open-ended phrasing like "until conditions improve."
3. **State the operational impact in concrete terms.** Number of flights affected, expected delay range, whether diversions are likely.
4. **Close with a commitment.** Specify when the next update will arrive ("we will send an update at the top of each hour while conditions persist" or "we will notify you when conditions clear").

### Don't

- Don't speculate on root cause beyond what's in the METAR/TAF data.
- Don't apologize gratuitously — this is an information channel, not a customer service one.
- Don't reference internal case IDs in the customer-facing message.
- Don't promise resolution timing that isn't in the TAF.

## Customer communication

(See above — this is the canonical reference.)

## Resolution criteria

N/A — this is a reference runbook.
