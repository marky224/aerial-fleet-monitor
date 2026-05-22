# Aerial Fleet Monitor — Runbook Library

> **Audience:** Claude Code creating runbooks; reviewers evaluating operational maturity; future maintainers.
> **Status:** v1, locked. Adding new runbooks is encouraged. Modifying frontmatter schema requires spec update.
> **Companion docs:** `DATA_MODEL.md` §3.3 for `runbook_index` schema; `PIPELINES.md` §3.8 for sync mechanics.

---

## 1. Why runbooks exist in this project

Runbooks codify operational response procedures for the anomalies the case detector flags. They serve three roles:

1. **Triage guide for support advocates.** When a Case opens, the linked runbook tells whoever picks it up *what to check, in what order, and what to escalate.* This is the same role runbooks play at any operations-heavy company.
2. **Customer communication consistency.** Each runbook specifies recommended customer-facing language for the anomaly type, ensuring the same situation gets described the same way to every customer. Critical for trust at scale.
3. **Code-as-documentation evidence.** Runbooks are markdown in the repo. They get versioned, reviewed, and synced to Notion. This proves operational knowledge is treated with the same rigor as code.

The runbook content is aviation-flavored because the demo data is aviation. The runbook *structure* is universal.

## 2. Frontmatter schema

Every runbook starts with YAML frontmatter. This is the contract Claude Code must honor when authoring or editing runbooks.

```yaml
---
id: kebab-case-id                       # required, unique, matches filename minus .md
title: "Title Case Title"               # required, human-readable
case_types:                             # required, ≥1 entry, must match enum
  - lost_signal | diversion | excessive_hold | weather_impact | go_around | delay
severity_floor: low | medium | high | critical    # required, the agent uses this as minimum
tags:                                   # optional, freeform
  - telemetry
  - comms
  - fleet-health
  - customer-impact
  - weather
salesforce_record_type: Case            # default: Case
salesforce_template_id: null            # optional Salesforce email template ID
salesforce_deeplink: "/lightning/r/Case/{case_id}/view"   # template; {case_id} is substituted
related_runbooks:                       # optional, soft links
  - other-runbook-id
---
```

Field semantics:

| Field | Purpose |
|---|---|
| `id` | Stable identifier referenced by case rules and linked from cases. Never rename without a migration. |
| `title` | Display title in the in-app viewer, Notion, and Salesforce ContentDocument links. |
| `case_types` | Which anomaly types this runbook applies to. The case detector uses this to suggest runbook references when a new case fires. |
| `severity_floor` | Minimum severity the agent should set when this runbook applies. May be overridden upward based on detection facts. |
| `tags` | Search/grouping hints. Surfaced in the in-app viewer. |
| `salesforce_record_type` | Almost always `Case`. Reserved for future use if other Salesforce records (e.g., WorkOrder) become AFM record types. |
| `salesforce_template_id` | If set, the agent's `draftCustomerNotification` action uses this template instead of the generic one. |
| `salesforce_deeplink` | Template for Lightning deeplink. `{case_id}` and `{external_id}` placeholders supported. |
| `related_runbooks` | Soft links to other runbooks for context (rendered as "see also" in the in-app viewer). |

## 3. Body content structure

After the frontmatter, every runbook follows the same five-section structure. Section headers are exact strings — sync logic and indexing depend on them.

```markdown
# {Title}

## When this fires

[1–3 sentences describing the rule conditions that trigger this case type. Include
specific thresholds where they exist (e.g., "above 10,000 ft for >2 minutes"). This
section answers the question: "Why am I seeing this case?"]

## Triage steps

[Numbered list of investigation steps in execution order. Each step is short,
imperative, and verifiable. Steps that produce structured outputs (e.g., "set
severity to HIGH") are the actions the Agentforce agent will take automatically;
mark these with **[Auto]** prefix so a human reading the runbook understands what's
already been done by the agent and what's left for them.]

1. [Step]
2. [Step]
3. [Step]

## Customer communication

[Recommended language and tone for the customer-facing notification. Include
explicit "do" and "don't" guidance. Specify the EmailMessage template to draft
(referencing salesforce_template_id from frontmatter if set). State whether the
customer should be notified proactively or only on inquiry.]

## Resolution criteria

[Clear, testable conditions for closing the case. Include both auto-resolution
paths (e.g., "signal recovers and aircraft completes flight") and manual-resolution
paths. Specify what the timeline event for resolution should record.]

## See also

[Links to related runbooks, internal docs, or external references. May be omitted
if there are none.]
```

The format is rigid by design. A runbook that follows the format is auto-discoverable, auto-syncable, and auto-rendered correctly in every surface (in-app, Notion, Salesforce).

## 4. Initial library — eight runbooks

The v1 library covers all six case types plus two cross-cutting procedures. Each entry below shows the full file content that should land in `runbooks/` at first commit.

### 4.1 `runbooks/lost-signal-cruise.md`

```markdown
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
```

### 4.2 `runbooks/diversion-to-alternate.md`

```markdown
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
```

### 4.3 `runbooks/excessive-holding.md`

```markdown
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
```

### 4.4 `runbooks/weather-driven-arrival-backups.md`

```markdown
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
```

### 4.5 `runbooks/go-around-investigation.md`

```markdown
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
```

### 4.6 `runbooks/delayed-departure-triage.md`

```markdown
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
```

### 4.7 `runbooks/customer-communication-ifr-operations.md`

```markdown
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
```

### 4.8 `runbooks/severity-escalation-criteria.md`

```markdown
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
```

## 5. Notion sync mechanics

The sync job (`runbook_index_sync` in `PIPELINES.md`) maintains a one-way sync from `runbooks/` (source of truth) to a Notion database.

**Notion database structure:** one page per runbook. Properties:
- `Title` (Notion title, from runbook `title`)
- `ID` (rich text, from runbook `id`)
- `Case Types` (multi-select, from runbook `case_types`)
- `Severity Floor` (select, from runbook `severity_floor`)
- `Tags` (multi-select, from runbook `tags`)
- `Last Synced` (date, set by sync job)
- `Git SHA` (rich text, set by sync job)

**Page body:** the runbook's body markdown converted to Notion blocks. The conversion uses the `notion-client` Python library (or `tomark` + manual block construction). Code blocks, headings, lists, tables all preserved.

**Deletion handling:** If a runbook is removed from `runbooks/`, the sync job archives the corresponding Notion page (using `archived: true` in the API). It does not delete — preserves an audit trail.

**Sync trigger:** GitHub webhook on push to `main` calls the AFM API's `/v1/webhooks/github` endpoint, which forwards to Dagster's GraphQL endpoint to launch `runbook_index_sync` immediately. Failsafe: a 5-minute scheduled run picks up any missed pushes.

## 6. Discovery and indexing

The case detector consults `ref.runbook_index` when creating a case to populate `runbook_refs`. Matching logic (in `pipelines/assets/detection.py`):

```python
def runbooks_for(case_type: str, site_icao: str | None) -> list[str]:
    candidates = postgres.query(
        "SELECT runbook_id FROM ref.runbook_index WHERE %s = ANY(case_types)",
        case_type,
    )
    # Always append the cross-cutting severity runbook
    candidates.append("severity-escalation-criteria")
    # Always append IFR communication runbook for weather-related cases
    if case_type in ("weather_impact", "diversion"):
        candidates.append("customer-communication-ifr-operations")
    return list(dict.fromkeys(candidates))   # de-dup, preserve order
```

Result is written to `app.cases.runbook_refs` and to Salesforce `AFM_Runbook_Refs__c` as comma-separated IDs.

## 7. Authoring guidance

When adding a new runbook (Claude Code or human):

1. Pick a unique kebab-case `id`. Verify by `grep -r "^id: " runbooks/`.
2. Copy the structure of a similar runbook in §4. Adjust `case_types`, `severity_floor`, body content.
3. Run `make lint-runbooks` (a small Python script that validates frontmatter against schema; tests for required sections in body).
4. Commit. The Notion sync runs automatically.

When modifying an existing runbook:
- Never change `id` (breaks references). To rename conceptually, create a new file and remove the old one (the sync archives the old in Notion).
- Severity floor changes are real semantic changes — flag in the PR description.
- Body content changes are routine and don't require additional review unless they alter triage steps or resolution criteria.
