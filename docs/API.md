# Aerial Fleet Monitor — API Specification

> **Audience:** Developers working on the FastAPI service and the Foundry sync client; reviewers of the integration contract.
> **Status:** v1, locked. Endpoint additions are allowed; breaking changes require a `/v2/*` prefix.
> **Companion docs:** `DATA_MODEL.md` for the underlying schemas, `FRONTEND.md` for dashboard (Foundry) patterns.

---

## 1. Conventions

### 1.1 Base URL and versioning

```
https://api.example.com
```
(Public base URL is environment-specific and kept out of the public tree per scrub-infra discipline; `api.example.com` is a placeholder.)

All endpoints are prefixed `/v1/`. v1 stays stable for the lifetime of the project. Breaking changes go to `/v2/`. Non-breaking additions (new fields, new endpoints) ship under `/v1/` without warning.

### 1.2 Status codes

- `200 OK` — successful read
- `400 Bad Request` — malformed input
- `403 Forbidden` — the request's scope does not cover the requested region or site
- `404 Not Found` — resource doesn't exist
- `409 Conflict` — duplicate write (e.g., same external ID)
- `422 Unprocessable Entity` — Pydantic validation failure
- `500 Internal Server Error` — unexpected
- `501 Not Implemented` — reserved path (`/v1/positions/stream`)
- `503 Service Unavailable` — upstream (OpenSky/Salesforce/etc.) failure surfaced

### 1.3 Standard error envelope

All non-2xx responses return:

```json
{
  "error": {
    "code": "scope_insufficient",
    "message": "User scope 'west' cannot access site 'KJFK' (region: east)",
    "request_id": "req_01H2K3M4N5P6Q7R8S9T0",
    "details": {}
  }
}
```

`code` is a stable string; `message` is human-readable but may evolve. Clients should switch on `code`, not `message`.

### 1.4 Request/response models

All request bodies and response payloads are described as Pydantic v2 models. The OpenAPI spec is auto-generated and served at `/openapi.json`, with Swagger UI at `/docs` and ReDoc at `/redoc`, only when `EXPOSE_DOCS=true`; the default is `false`, and the API has no auth layer that gates these routes.

### 1.5 Pagination

No endpoint takes a `cursor`. List responses return the whole in-scope set as `items` plus `count` (the number of items); `/v1/positions/live` and `/v1/cases` add `truncated`, set when the 50,000-row safety ceiling clips the result (§3.1, §6.1). The one paged read is the system-internal `/v1/cases/all-for-sync`, which pages by a `since` timestamp (§6.5).

```json
{
  "items": [...],
  "count": 50
}
```

### 1.6 Timestamps

All timestamps are ISO-8601 in UTC with explicit `Z` suffix: `2026-05-09T14:30:00.123Z`. Clients convert to user timezone for display (Workshop apps handle this via Foundry's timezone widget config).

### 1.7 Auth

No login is built in v1. Every request is served as one internal identity — `user_handle: "internal-ops"`, `region: "all"`, no custom permissions — with `sites_in_scope` taken from the live watched-airports list (`get_scope` in `api/app/dependencies.py`). There is no session cookie, JWT or CSRF handling, and no route returns `401`.

The scope checks are built: a query for a region or site outside the caller's scope is refused with `403 scope_insufficient`. With the single all-regions identity they pass every request.

## 2. Auth endpoints

### 2.1 `GET /v1/auth/me`

Return the current session identity and scope.

**Response 200:**
```python
class MeResponse(BaseModel):
    user_handle: str                       # 'internal-ops' | 'west-coast-ops' | …
    salesforce_user_id: str | None
    region: Literal['west', 'east', 'all']
    custom_perms: list[str]
    read_only: bool
    expires_at: datetime
    sites_in_scope: list[str]              # ICAO codes the user can read
```

Every caller gets the same response: the internal-ops identity from §1.7, with `salesforce_user_id: null`, `read_only: true` and `expires_at` 24 hours ahead. There is no `401`.

### 2.2 `POST /v1/admin/sf-test-case` (dev-only)

Salesforce write smoke (Phase 04 acceptance #9). Creates a Case in the
connected dev org populating **every** `AFM_*__c` custom field plus the
`Fleet_Operations` record type, then deletes it. Returns the cross-system
id pair and the exact field map sent (after `SalesforceService`'s region
and field-name translation), so the AFM→SF write path can be verified
end-to-end without the case detector.

Mounted **only when `ENVIRONMENT=dev`** — the route does not exist (404)
in any other environment, which is the access control. Authenticates to
Salesforce via OAuth 2.0 Client Credentials (Connected App "Run As" user).

**Response 200:**
```python
class SfTestCaseResult(BaseModel):
    created: SalesforceCaseRef               # {salesforce_id, external_id}
    deleted: bool                            # True if the smoke Case was cleaned up
    sf_fields_sent: dict[str, Any]           # post-translation SF field map
```

**Response 503** (`upstream_unavailable`) if Salesforce is unconfigured or unreachable.

## 3. Positions

### 3.1 `GET /v1/positions/live`

Return all currently airborne aircraft within the caller's scope.
"Currently airborne" = aircraft observed within the last **15 minutes**
(`last_seen_at >= now() - 15m`). The backing `current_positions` store
keeps a last-known row per aircraft indefinitely, so this recency bound
is what makes the result "live"; without it the endpoint returns
long-landed traffic. Each returned row still carries a `staleness`
bucket (`fresh` < 60 s, `stale` < 5 min, `lost` otherwise) so the
recently-lost tail inside the window is distinguishable client-side.

The endpoint returns the **complete** in-scope set (it is a snapshot, not
a paginated list; see §1.5).
A server-side safety ceiling of **50,000** rows bounds memory and response
size; this is far above realistic airborne traffic (single-digit
thousands). If the in-scope set ever exceeds the ceiling, the freshest
50,000 rows (ordered by `last_seen_at` desc) are returned and
`truncated: true` is set so the clip is observable rather than silent.

**Query params:**
- `bbox` (optional): `lat_min,lon_min,lat_max,lon_max`. If omitted, returns all in scope.
- `region` (optional): override scope to a specific region (rejected with 403 if the caller's scope is narrower than the requested region).

**Response 200:**
```python
class Position(BaseModel):
    icao24: str
    callsign: str | None
    lat: float
    lon: float
    altitude_ft: int | None
    speed_kt: int | None
    heading_deg: int | None
    vertical_rate_fpm: int | None
    on_ground: bool
    customer_region: Literal['west', 'east', 'all', None]
    last_seen_at: datetime
    staleness: Literal['fresh', 'stale', 'lost']  # fresh < 60s, stale < 5min, lost otherwise

class PositionsLiveResponse(BaseModel):
    items: list[Position]
    count: int
    server_time: datetime
    pipeline_lag_seconds: int                       # last successful poll lag
    truncated: bool = False                         # True if clipped at the 50k safety ceiling
```

Polling cadence from the dashboard sync: every 300 s (the `foundry_positions_sync` sensor).

### 3.2 `WebSocket /v1/positions/stream`

**Reserved for v2 — the path answers a plain `GET` with 501 Not Implemented; no WebSocket handler is mounted.**

The v1 dashboard surface (Foundry positions sync) polls `/v1/positions/live` every 300 s, which is sufficient for portfolio-scale read patterns. The WebSocket surface is kept on the spec so the v2 reservation is explicit, mirroring the `/v1/chat/*` reservation in §11.

When implemented, design intent is: plain-JSON server-pushed snapshots every 30s with the same payload shape as `/v1/positions/live`, 25-second `{"type":"ping"}` / `{"type":"pong"}` heartbeat, no binary frames, no per-message compression.

## 4. Flights

### 4.1 `GET /v1/flights/{icao24}`

Return current state and metadata for a single flight.

**Response 200:**
```python
class FlightDetail(BaseModel):
    icao24: str
    callsign: str | None
    registration: str | None
    aircraft_type: str | None
    operator_icao: str | None
    origin_icao: str | None
    destination_icao: str | None
    customer_region: Literal['west', 'east', 'all', None]
    position: Position
    eta_minutes: int | None                          # if destination known and computable
    status_timeline: list[FlightStatusEvent]         # taxi/climb/cruise/descent/landed
    open_case_ids: list[str]                         # AFM case IDs

class FlightStatusEvent(BaseModel):
    stage: Literal['departed', 'climb', 'cruise', 'descent', 'approach', 'landed']
    occurred_at: datetime
```

**Response 404** if `icao24` not seen in the last 30 minutes.

### 4.2 `GET /v1/flights/{icao24}/trail`

Return historical positions for a flight in the lookback window.

**Query params:**
- `lookback`: `1h` | `2h` | `4h` | `since_takeoff`. Default `2h`.

**Response 200:**
```python
class TrailPoint(BaseModel):
    ts: datetime
    lat: float
    lon: float
    altitude_ft: int | None
    speed_kt: int | None

class TrailResponse(BaseModel):
    icao24: str
    points: list[TrailPoint]
    lookback: Literal['1h', '2h', '4h', 'since_takeoff']
    point_count: int
```

For `since_takeoff`, the server caps at 6 hours of history to bound query cost.

### 4.3 `POST /v1/flights/trail/batch`

Bulk trail fetch for many aircraft in a **single** lakehouse scan,
streamed as NDJSON. The per-flight `4.2` endpoint scans the lookback
window once per `icao24` (the `icao24` predicate does not reduce the
scan — positions are written time-ordered), so a fan-out over thousands
of aircraft re-reads the same window thousands of times. This endpoint
collapses that to one scan. `4.2` is unchanged for single-flight use.

**Request body:**
```python
class TrailBatchRequest(BaseModel):
    icao24s: list[str]   # 1..20000; lowercased + hex-validated + de-duped
    lookback: Literal['1h', '2h', '4h', 'since_takeoff'] = '2h'
```

**Response 200** — `Content-Type: application/x-ndjson`, one
`TrailResponse` (see `4.2`) JSON object per line, ordered by `icao24`:
```
{"icao24":"a1b2c3","points":[...],"lookback":"2h","point_count":42}
{"icao24":"d4e5f6","points":[...],"lookback":"2h","point_count":7}
```

- `icao24`s with no positions in the window are **omitted** from the
  stream (treat an absent `icao24` as an empty trail).
- Out-of-scope `icao24`s are **filtered**, not an error — a bulk request
  is never 403'd because one member is out of scope.
- The body is streamed lazily; a lakehouse IO error mid-scan ends the
  stream after a `200` (a bulk caller must tolerate a short stream).

## 5. Sites

### 5.1 `GET /v1/sites`

List all watched sites, optionally filtered.

**Query params:**
- `region`: `west` | `east` | `all` (defaults to caller's scope, can't broaden it)

**Response 200:**
```python
class SiteListItem(BaseModel):
    icao: str
    iata: str | None
    name: str
    state: str | None
    customer_regions: list[str]
    is_in_scope: bool

class SiteListResponse(BaseModel):
    items: list[SiteListItem]
    count: int
```

### 5.2 `GET /v1/sites/{icao}`

Single-site detail.

**Response 200:**
```python
class SiteDetail(BaseModel):
    icao: str
    iata: str | None
    name: str
    city: str | None
    state: str | None
    lat: float
    lon: float
    elevation_ft: int | None
    timezone: str | None
    weather: SiteWeather | None
    inbound_count_60m: int
    outbound_count_60m: int
    active_case_count: int
    customer_regions: list[str]

class SiteWeather(BaseModel):
    metar_raw: str
    metar_plain_english: str | None                  # always null in v1
    flight_category: Literal['VFR', 'MVFR', 'IFR', 'LIFR']
    wind_kt: int | None
    visibility_sm: float | None
    ceiling_ft: int | None
    observed_at: datetime
```

**Response 403** if `icao` is not in the caller's scope.

### 5.3 `GET /v1/sites/{icao}/sla`

SLA scorecard for a site.

**Query params:**
- `period`: `last_24h` | `last_7d`. Default `last_24h`.

**Response 200:**
```python
class SiteSla(BaseModel):
    icao: str
    period: Literal['last_24h', 'last_7d']
    inbound_count: int
    outbound_count: int
    on_time_arrival_pct: float | None
    on_time_departure_pct: float | None
    avg_arrival_delay_min: float | None
    avg_departure_delay_min: float | None
    weather_impact: Literal['low', 'medium', 'high']
    flight_category: Literal['VFR', 'MVFR', 'IFR', 'LIFR']
    active_cases: int
    sparkline_7d: list[SparklinePoint]               # for trend rendering

class SparklinePoint(BaseModel):
    day: date
    on_time_pct: float | None
    avg_delay_min: float | None
```

### 5.4 `GET /v1/sites/{icao}/inbound` and `/outbound`

Live arrivals/departures within 60 minutes.

**Response 200:**
```python
class SiteFlightListResponse(BaseModel):
    items: list[FlightSummary]
    count: int

class FlightSummary(BaseModel):
    icao24: str
    callsign: str | None
    origin_icao: str | None
    destination_icao: str | None
    eta_minutes: int | None
    status: Literal['scheduled', 'departed', 'enroute', 'approaching', 'landed', 'unknown']
    aircraft_type: str | None
```

## 6. Cases

### 6.1 `GET /v1/cases`

List cases visible in caller's scope. Scope filter: a narrow-region caller
sees rows where `customer_region IN (own_region, 'all')` so cross-region
('all'-tagged) cases surface to both east and west — matches the Workshop
App-1 panel.

**Query params:**
- `status`: filter by case status; repeatable
  (`?status=open&status=in_progress`). Default omits `resolved` (open +
  acknowledged + in_progress), matching the Workshop App-1 panel.
- `severity`: filter by severity (`low`/`medium`/`high`/`critical`).
- `site`: filter by site ICAO (uppercased server-side).
- `region`: override scope to a specific region. Rejected with `403` if
  the caller's scope is narrower than the requested region.

**Pagination:** none. Single bounded page (50k safety ceiling). The
response carries `truncated=True` if the ceiling is hit (oldest rows
dropped, since `ORDER BY created_at DESC`). Tenant case volume is
~1-2k at Phase-05 close, so 50k is ~30-50× headroom. Follows the same
shape as `/v1/positions/live` (§3.1) so consumers have one pagination
story to learn.

**Response 200:**
```python
class CaseListItem(BaseModel):
    case_id: str
    salesforce_id: str | None
    salesforce_url: str | None    # Lightning deeplink; null until pushed
    case_type: str
    status: str
    severity: str
    customer_region: str          # included so the App-1 region dropdown
                                  # can render per-row counts without a
                                  # second round-trip
    site_icao: str
    flight_id: str
    summary: str | None
    created_at: datetime
    updated_at: datetime

class CaseListResponse(BaseModel):
    items: list[CaseListItem]
    count: int
    truncated: bool               # True when the page hit the 50k ceiling
```

### 6.2 `GET /v1/cases/{case_id}`

Single case detail. `404 not_found` if absent; `403 scope_insufficient`
if the caller's region scope doesn't cover the case's `customer_region`
(`'all'`-tagged cases are visible to every region).

**Response 200:**
```python
class CaseDetail(BaseModel):
    case_id: str
    salesforce_id: str | None
    salesforce_url: str | None
    case_type: str
    status: str
    severity: str
    severity_justification: str | None
    customer_region: str
    site_icao: str
    flight_id: str
    summary: str | None
    detection_facts: dict
    runbook_refs: list[str]
    timeline: list[CaseTimelineEvent]      # ASC by occurred_at, event_id
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None

class CaseTimelineEvent(BaseModel):
    event_type: str
    detail: dict
    source: str
    actor: str | None
    occurred_at: datetime
```

`salesforce_url` is the Lightning deeplink to the Case; it is null until the push has set `salesforce_id`, or when `SALESFORCE_INSTANCE_URL` is unset.

**Not built:** `related_tasks` — Salesforce Task sync isn't built.

### 6.3 `POST /v1/cases/sync-pending`

System-internal push trigger for the decoupled AFM→Salesforce write path
(Phase 05). The case detector writes local cases as
`sf_sync_status='pending'` and never touches Salesforce; this endpoint
drains up to `limit` pending rows into the connected org through
`SalesforceService` and reconciles `app.cases` (`salesforce_id`, `sf_sync_status`)
+ `app.case_timeline`. The pipelines `sf_case_push` asset polls it on a
~60s cadence; each call also retries cases left `pending` by a prior
transient failure (so the endpoint is the retry surface too).

Failure handling per row: a transient Salesforce failure (`503`) leaves
the case `pending` for the next pass (until an attempts cap parks it
`failed`); a permanent failure (`400`/`409`) parks it `failed` at once.

The call is safe to repeat and to overlap: it is single-flighted (a
concurrent call returns a no-op `attempted: 0`) and idempotent on the
unique case external id (a row whose Case already exists reconciles to it
and is marked `synced` rather than failing as a duplicate).

**Query params:**
- `limit`: max pending cases to push this pass (default `50`, 1–500).

**Response 200:**
```python
class CaseSyncSummary(BaseModel):
    attempted: int                           # pending cases pulled this pass
    synced: int                              # created in SF + marked synced
    retrying: int                            # transient failure; left pending
    failed: int                              # permanent failure / max attempts
```

**Response 503** (`upstream_unavailable`) if Salesforce is unconfigured or unreachable.

### 6.4 `POST /v1/cases/sync-from-sf`

System-internal pull trigger for the SF→Postgres mirror (Phase 05), the
inverse of §6.3. Reads the persisted `sf_case_sync` watermark, fetches up
to `limit` `Fleet_Operations` Cases with `SystemModstamp` greater than the
watermark through `SalesforceService`, mirrors each onto its `app.cases` row
(matched by `salesforce_id`, falling back to the external id), writes
`app.case_timeline` events for material changes (`status_changed`,
`severity_changed`, `resolved`), and advances the watermark to the maximum
`SystemModstamp` observed (leaving it untouched when nothing changed). The
pipelines `sf_case_sync` asset polls it on a ~60s cadence. SF→AFM
translation (Status/Priority/Description → `app.cases` status/severity/
summary) lives only in `SalesforceService`.

**Query params:**
- `limit`: max modified Cases to pull this pass (default `200`, 1–200).

**Response 200:**
```python
class CasePullSummary(BaseModel):
    fetched: int                             # cases returned by SF since the watermark
    updated: int                             # matched app.cases rows updated
    unmatched: int                           # SF cases with no local row (skipped)
    watermark: datetime | None               # new watermark; null if nothing changed
```

**Response 503** (`upstream_unavailable`) if Salesforce is unconfigured or unreachable.

### 6.5 `GET /v1/cases/all-for-sync`

System-internal read for the Foundry sync (Phase 05). Returns a paginated
snapshot of `app.cases` consumed by two Foundry-sync pipelines assets:
`foundry_cases_sync` (incremental upsert into the Case ontology) and
`foundry_cases_reconcile` (the full snapshot is its keep-set — Case
objects absent from it are evicted from the ontology as orphans, so the
upsert-only sync never leaks deleted Cases). No scope filter — this
endpoint is server-to-server. The customer-facing scope-gated reads
(§6.1, §6.2) are a separate slice.

Rows are ordered by `(updated_at, case_id)` ASC so a `since`-cursor walk
is deterministic. Resolved cases are included (App 1's panel applies its
own status filter); once a Case resolves, its `updated_at` stops
advancing, so it falls out of the moving window naturally.

The subject field is derived at response time from `case_type` +
`detection_facts` by the same formatter the SF push uses
(`CaseSyncService._format_subject`).

**Query params:**
- `since`: timestamp; return rows with `updated_at > since`. Omit for the first page.
- `limit`: max rows in this page (default `200`, 1–1000).

**Response 200:**
```python
class CaseForSync(BaseModel):
    case_id: str
    salesforce_id: str | None
    salesforce_url: str | None
    case_type: str
    status: str                                  # open/acknowledged/in_progress/resolved
    severity: str                                # low/medium/high/critical
    customer_region: str                         # west/east/all
    site_icao: str
    flight_id: str                               # 'WX-{site_icao}' for site-level cases
    subject: str                                 # derived from case_type + facts
    summary: str | None
    severity_justification: str | None
    detection_facts: dict
    runbook_refs: list[str]
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None

class CasesForSyncPage(BaseModel):
    items: list[CaseForSync]
    next_cursor: datetime | None                 # max(updated_at) in this page
    truncated: bool                              # True when len(items) == limit
```

The caller persists `next_cursor` and passes it as `since` on the next
call; pages until `truncated=False`. Foundry-side upserts are idempotent
on `case_id`, so a boundary tie at `updated_at == since` that re-ships a
previous page's tail row is harmless.

## 7. Briefs

### 7.1 `POST /v1/briefs/generate`

System-internal trigger for the daily-brief feature (Phase 11). Composes the
prior-local-day operations brief for `region` from `app.cases`, synthesizes it
via the Salesforce Models API (the `AFM_BriefSynthesis` Apex action), posts
it to the Fleet Operations Chatter group, and upserts `app.briefs`. No scope
filter — server-to-server, like §6.3–6.5. The three per-region `daily_brief_*`
Dagster schedules fire it at 07:00 in each region's local timezone.

`region` is `west` | `east` | `all` (`all` is fleet-wide — no region filter, not
the `customer_region='all'` partition). The window is the region's prior *local*
calendar day (west→`America/Los_Angeles`, east→`America/New_York`,
all→`America/Chicago`).

Idempotent on `(region, brief_date)`: a brief already generated for the day is a
no-op (`status: "already_posted"` — no second LLM call or Chatter post) unless
`force=true`. The outward Chatter post is separately gated by
`SALESFORCE_CHATTER_BRIEF_GROUP_ID`; unset, the brief still generates and persists
but is not posted (`chatter_post_id: null`).

**Request body:**
```python
class BriefGenerateRequest(BaseModel):
    region: Literal["west", "east", "all"]
```

**Query params:**
- `force`: regenerate even if this region's brief for the day exists (default `false`).

**Response 200:**
```python
class BriefGenerateSummary(BaseModel):
    region: str
    brief_date: date                         # the prior local day the brief covers
    status: Literal["posted", "already_posted"]
    chatter_post_id: str | None              # Chatter FeedItem id, when posted
    opened: int                              # cases opened in the window
    resolved: int                            # cases resolved in the window
    brief_id: int | None                     # app.briefs row id
```

**Response 503** (`upstream_unavailable`) if Salesforce is unconfigured or unreachable.

The persisted `app.briefs` row carries the synthesized text (`summary_md`), the
composed facts (`key_metrics`), the notable case ids (`notable_cases`), the
region's `timezone`, and `chatter_post_id`. A read-side `GET /v1/briefs`
list/detail API is intentionally not built — the brief's consumer is the
Salesforce Chatter feed, not an AFM-hosted UI.

## 8. Runbooks

Runbooks are not served by the API: they are the markdown files in `runbooks/`, and they reach Salesforce as `AFM_Runbook__mdt` records and Files through `make sf-seed-runbooks` (`RUNBOOKS.md` §5).

## 9. Health and ops endpoints

### 9.1 `GET /v1/health`

Liveness check. Returns 200 if the process is up.

```json
{ "status": "ok", "service": "afm-api", "version": "1.0.0" }
```

### 9.2 `GET /v1/metrics`

Prometheus scrape endpoint. Standard `text/plain; version=0.0.4` format. Not authenticated — it is network-scoped (reachable only on the private network, never forwarded by the public reverse tunnel).

### 9.3 `GET /v1/metrics/extras`

Second Prometheus scrape endpoint: AFM gauges read from Postgres on each scrape (active and tracked aircraft, pipeline lag, case counts by severity, status, region, rule and site, the Salesforce sync backlog) plus Dagster run counts, duration and age read from the run store. Same format and exposure as §9.2; like `/v1/metrics`, it is not in the OpenAPI schema.

## 10. Endpoint summary table

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/auth/me` | current identity (internal-ops stub) |
| POST | `/v1/admin/sf-test-case` | SF write smoke (dev-only, acceptance #9) |
| GET | `/v1/positions/live` | all current positions in scope |
| GET | `/v1/positions/stream` | reserved for a v2 stream; returns 501 |
| GET | `/v1/flights/{icao24}` | single flight detail |
| GET | `/v1/flights/{icao24}/trail` | flight trail |
| POST | `/v1/flights/trail/batch` | bulk flight trails (one scan, NDJSON stream) |
| GET | `/v1/sites` | list sites |
| GET | `/v1/sites/{icao}` | site detail |
| GET | `/v1/sites/{icao}/sla` | SLA scorecard |
| GET | `/v1/sites/{icao}/inbound` | inbound list |
| GET | `/v1/sites/{icao}/outbound` | outbound list |
| GET | `/v1/cases` | case list |
| GET | `/v1/cases/{case_id}` | case detail |
| POST | `/v1/cases/sync-pending` | push pending cases to SF (system; ~60s) |
| POST | `/v1/cases/sync-from-sf` | pull SF Case changes into app.cases (system; ~60s) |
| GET | `/v1/cases/all-for-sync` | paginated cases snapshot for Foundry sync (system; ~60s) |
| POST | `/v1/briefs/generate` | generate + post the daily brief for a region (system; daily 07:00 local) |
| GET | `/v1/health` | liveness |
| GET | `/v1/metrics` | Prometheus metrics |
| GET | `/v1/metrics/extras` | Prometheus gauges from Postgres and the Dagster run store |

No endpoint requires a login in v1 (§1.7). `/v1/admin/sf-test-case` exists only when `ENVIRONMENT=dev`; the two metrics endpoints are for the Prometheus scraper (§9).

## 11. Architectural reservation for v2 NL chat

`/v1/chat/*` is intentionally unspecified in v1 but reserved. The NL chat endpoint will:

- Accept `{ "query": "...", "session_id": "..." }`
- Call into `QueryService` through an LLM tool layer
- Return `{ "answer": "...", "sources": [...], "trace": [...] }`

The Pydantic request/response models on every existing endpoint double as tool definitions for the chat layer. No v1 endpoint should hardcode response shapes outside Pydantic models — doing so would force a v2 rewrite when adding chat tool exposure.
