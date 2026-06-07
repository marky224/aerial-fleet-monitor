---
id: diversion-to-alternate
title: "Diversion to Alternate"
case_types:
  - diversion
severity_floor: medium
tags:
  - flight-status
  - customer-impact
salesforce_record_type: Case
salesforce_deeplink: "/lightning/r/Case/{case_id}/view"
related_runbooks:
  - weather-driven-arrival-backups
  - customer-communication-ifr-operations
---

# Diversion to Alternate

## When this fires

A flight's destination changed mid-flight, indicated by the aircraft's heading deviating substantially from the great-circle path to its filed destination, then stabilizing on a path to an alternate airport. Confirmed when the aircraft has been within 30 nm of the alternate for at least 5 minutes.

## Triage steps

1. **[Auto]** Severity set to MEDIUM. Severity escalates to HIGH if the diversion was caused by weather at the original destination and at least 3 inbound flights are also affected.
2. Identify the alternate airport from detection facts. Verify it's a reasonable choice (within fuel range, has appropriate runway length, is within the operating region).
3. Check whether the original destination has active weather or operational issues (open weather-impact cases for the same site).
4. Confirm whether the alternate is on the customer's watchlist; if not, the customer's "site visibility" of this flight ends at diversion.
5. **[Auto]** Investigation Tasks are created for each of these checks, due within 30 minutes.

## Customer communication

If the original destination is a customer site, notify the customer view with: "Flight {callsign} has diverted to {alternate}. Original destination was {original}. Diversion may indicate operational conditions affecting your site." If multiple flights divert from the same site within an hour, consider rolling them up into a single site-level notification.

## Resolution criteria

- Auto-resolve when the aircraft lands at the alternate or returns to its original destination.
- Resolution timeline event records: original destination, alternate, duration of diversion, and whether the diversion was attributable to weather (cross-references active weather-impact case if any).

## See also

- `weather-driven-arrival-backups.md`
- `customer-communication-ifr-operations.md`
