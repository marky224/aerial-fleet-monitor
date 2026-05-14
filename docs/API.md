# Aerial Fleet Monitor — API Specification

> **Audience:** Claude Code building the FastAPI service and the React client; reviewers of the integration contract.
> **Status:** v1, locked. Endpoint additions are allowed; breaking changes require a `/v2/*` prefix.
> **Companion docs:** `DATA_MODEL.md` for the underlying schemas, `FRONTEND.md` for client patterns.

---

## 1. Conventions

### 1.1 Base URL and versioning

```
https://api.aerial-fleet-monitor.markandrewmarquez.com
```

All endpoints are prefixed `/v1/`. v1 stays stable for the lifetime of the project. Breaking changes go to `/v2/`. Non-breaking additions (new fields, new endpoints) ship under `/v1/` without warning.

### 1.2 Status codes

- `200 OK` — successful read
- `201 Created` — successful write (rarely used; POST endpoints are minimal in v1)
- `204 No Content` — successful action with no payload
- `400 Bad Request` — malformed input
- `401 Unauthorized` — missing or invalid `afm_session` cookie
- `403 Forbidden` — authenticated but scope insufficient (e.g., West Coast user requesting an East Coast site)
- `404 Not Found` — resource doesn't exist
- `409 Conflict` — duplicate write (e.g., same external ID)
- `422 Unprocessable Entity` — Pydantic validation failure
- `429 Too Many Requests` — rate-limited (rare in v1)
- `500 Internal Server Error` — unexpected
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

`code` is a stable string; `message` is human-readable but may evolve. Frontend should switch on `code`, not `message`.

### 1.4 Request/response models

All request bodies and response payloads are described as Pydantic v2 models. The OpenAPI spec is auto-generated and exposed at `/v1/openapi.json` and `/v1/docs` (Swagger UI in development; protected in production).

### 1.5 Pagination

List endpoints accept `cursor` (opaque string) and `limit` (default 50, max 200). Responses include `next_cursor` (null if no more) and `count` (number of items in this page).

```json
{
  "items": [...],
  "count": 50,
  "next_cursor": "eyJpZCI6ICJDQVNF..."
}
```

### 1.6 Timestamps

All timestamps are ISO-8601 in UTC with explicit `Z` suffix: `2026-05-09T14:30:00.123Z`. Frontend converts to user timezone for display.

### 1.7 Auth

Session is a signed JWT in `afm_session` HttpOnly cookie. JWT carries:

```json
{
  "sub": "west-coast-ops",
  "salesforce_user_id": "0058z000ABCDEFG",
  "region": "west",
  "custom_perms": ["AFM_Region_West"],
  "exp": 1714000000,
  "iat": 1713996400,
  "iss": "afm.markandrewmarquez.com"
}
```

Anonymous visitors get an auto-issued JWT with `sub: "internal-ops"`, `region: "all"`, `read_only: true` (read-only mode prevents any state-changing endpoints). Read-only mode is the default for cold visits and never requires Salesforce authentication.

CSRF: Use `SameSite=Lax` cookies plus `Origin` header validation on all non-GET endpoints. Demo doesn't include separate CSRF tokens.

Note: `/v1/auth/callback` is a GET, so it is exempt from Origin validation by definition (the redirect originates from Salesforce, not the AFM frontend, so the Origin header would always be wrong). CSRF protection on the callback path uses the OAuth `state` parameter instead — generated at `/v1/auth/login`, persisted in a short-lived `oauth_state` cookie, verified on callback, rejected with 400 on mismatch.

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

**Response 401** if no cookie. (Frontend handles by issuing the auto-internal-ops session.)

### 2.2 `GET /v1/auth/login`

Initiate Salesforce OAuth flow.

**Query params:**
- `as` (optional): demo user handle to pre-select. One of `west-coast-ops`, `east-coast-ops`. Sent as `login_hint` to Salesforce.
- `return_to` (optional): post-auth redirect path within the dashboard. Defaults to `/`.

**Response 302** to Salesforce login URL.

### 2.3 `GET /v1/auth/callback`

Salesforce OAuth callback. Exchange code, fetch userinfo + permissions, mint AFM JWT, set cookie.

**Query params:** `code`, `state` (CSRF anti-replay token, validated against the cookie set during `/login`)

**Response 302** to the original `return_to`.

### 2.4 `POST /v1/auth/logout`

Clear the cookie. If the cookie holds a Salesforce-issued session, also call SF's revoke endpoint (cleans up server-side).

**Response 204.**

## 3. Positions

### 3.1 `GET /v1/positions/live`

Return all currently airborne aircraft within the caller's scope.

**Query params:**
- `bbox` (optional): `lat_min,lon_min,lat_max,lon_max`. If omitted, returns all in scope.
- `region` (optional): override scope to a specific region (rejected with 403 if user lacks `AFM_All_Regions`).

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
```

Polling cadence from frontend: every 30s.

### 3.2 `WebSocket /v1/positions/stream`

**Reserved for v2 — currently returns 501 Not Implemented.**

The v1 frontend uses 30s polling against `/v1/positions/live` (see `FRONTEND.md` §4.1), which is sufficient for portfolio-scale read patterns. The WebSocket surface is kept on the spec so the v2 reservation is explicit, mirroring the `/v1/chat/*` reservation in §11.

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
    lookback: str
    point_count: int
```

For `since_takeoff`, the server caps at 6 hours of history to bound query cost.

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
    state: str
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
    state: str
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
    metar_plain_english: str | None                  # Anthropic-generated, cached
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
    period: str
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

List cases visible in caller's scope.

**Query params:**
- `status`: filter by case status (default: open + acknowledged + in_progress)
- `severity`: filter by severity
- `site`: filter by site ICAO
- `cursor`, `limit`: pagination

**Response 200:**
```python
class CaseListItem(BaseModel):
    case_id: str
    salesforce_id: str | None
    case_type: str
    status: str
    severity: str
    site_icao: str
    flight_id: str
    summary: str | None
    created_at: datetime
    updated_at: datetime

class CaseListResponse(BaseModel):
    items: list[CaseListItem]
    count: int
    next_cursor: str | None
```

### 6.2 `GET /v1/cases/{case_id}`

Single case detail.

**Response 200:**
```python
class CaseDetail(BaseModel):
    case_id: str
    salesforce_id: str | None
    salesforce_url: str | None                       # Lightning deeplink
    case_type: str
    status: str
    severity: str
    severity_justification: str | None
    site_icao: str
    flight_id: str
    customer_region: str
    summary: str | None
    detection_facts: dict
    runbook_refs: list[str]
    timeline: list[CaseTimelineEvent]
    related_tasks: list[CaseTask]                    # synced from SF
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None

class CaseTimelineEvent(BaseModel):
    event_type: str
    detail: dict
    source: str
    actor: str | None
    occurred_at: datetime

class CaseTask(BaseModel):
    salesforce_id: str
    subject: str
    status: str
    due_date: date | None
    assigned_to: str | None
```

### 6.3 `GET /v1/cases/{case_id}/runbooks`

Returns the linked runbooks (markdown bodies) inline. Convenience for the case detail UI.

**Response 200:**
```python
class CaseRunbooksResponse(BaseModel):
    runbooks: list[Runbook]
```

(See §8 for `Runbook` model.)

## 7. Briefs

### 7.1 `GET /v1/briefs`

List briefs visible in scope.

**Query params:** `region` (optional, defaults to caller's), `limit` (default 30).

**Response 200:**
```python
class BriefListItem(BaseModel):
    brief_id: int
    region: str
    brief_date: date
    timezone: str
    generated_at: datetime
    chatter_post_url: str | None

class BriefListResponse(BaseModel):
    items: list[BriefListItem]
    count: int
```

### 7.2 `GET /v1/briefs/{brief_id}`

Single brief content.

**Response 200:**
```python
class BriefDetail(BaseModel):
    brief_id: int
    region: str
    brief_date: date
    timezone: str
    generated_at: datetime
    summary_md: str
    key_metrics: dict                                # rendered as cards
    notable_cases: list[CaseListItem]
    chatter_post_url: str | None
```

## 8. Runbooks

### 8.1 `GET /v1/runbooks`

List all runbooks.

**Response 200:**
```python
class RunbookListItem(BaseModel):
    runbook_id: str
    title: str
    case_types: list[str]
    severity_floor: str
    tags: list[str]
    notion_url: str | None

class RunbookListResponse(BaseModel):
    items: list[RunbookListItem]
    count: int
```

### 8.2 `GET /v1/runbooks/{runbook_id}`

Single runbook with rendered body.

**Response 200:**
```python
class Runbook(BaseModel):
    runbook_id: str
    title: str
    case_types: list[str]
    severity_floor: str
    tags: list[str]
    body_markdown: str
    body_html: str                                   # server-rendered for in-app viewer
    salesforce_record_type: str | None
    salesforce_template_id: str | None
    salesforce_deeplink: str | None
    notion_url: str | None
    last_synced_at: datetime
```

## 9. Health and ops endpoints

### 9.1 `GET /v1/health`

Liveness check. Returns 200 if the process is up.

```json
{ "status": "ok", "service": "afm-api", "version": "1.0.0" }
```

### 9.2 `GET /v1/health/deep`

Deeper check: Postgres reachable, Parquet lake mounted, Salesforce token valid, Anthropic client healthy. Used by Cloudflare's tunnel health probes.

**Response 200** if all green; **503** if any are degraded.

```python
class DeepHealthResponse(BaseModel):
    status: Literal['ok', 'degraded']
    components: dict[str, Literal['ok', 'degraded', 'down']]
    pipeline_lag_seconds: int
    last_successful_opensky_poll_at: datetime | None
    last_successful_noaa_poll_at: datetime | None
```

### 9.3 `GET /v1/metrics`

Prometheus scrape endpoint. Standard `text/plain; version=0.0.4` format. Not authenticated (it's behind Cloudflare Access for the Grafana subdomain only).

## 10. Endpoint summary table

| Method | Path | Purpose | Auth required |
|---|---|---|---|
| GET | `/v1/auth/me` | current session | yes |
| GET | `/v1/auth/login` | start OAuth | no |
| GET | `/v1/auth/callback` | OAuth callback | no |
| POST | `/v1/auth/logout` | clear session | yes |
| GET | `/v1/positions/live` | all current positions in scope | yes |
| WS | `/v1/positions/stream` | streamed positions (501 in v1, reserved for v2) | yes |
| GET | `/v1/flights/{icao24}` | single flight detail | yes |
| GET | `/v1/flights/{icao24}/trail` | flight trail | yes |
| GET | `/v1/sites` | list sites | yes |
| GET | `/v1/sites/{icao}` | site detail | yes |
| GET | `/v1/sites/{icao}/sla` | SLA scorecard | yes |
| GET | `/v1/sites/{icao}/inbound` | inbound list | yes |
| GET | `/v1/sites/{icao}/outbound` | outbound list | yes |
| GET | `/v1/cases` | case list | yes |
| GET | `/v1/cases/{case_id}` | case detail | yes |
| GET | `/v1/cases/{case_id}/runbooks` | linked runbooks | yes |
| GET | `/v1/briefs` | brief list | yes |
| GET | `/v1/briefs/{brief_id}` | brief content | yes |
| GET | `/v1/runbooks` | runbook list | yes |
| GET | `/v1/runbooks/{runbook_id}` | single runbook | yes |
| GET | `/v1/health` | liveness | no |
| GET | `/v1/health/deep` | deep health | no |
| GET | `/v1/metrics` | Prometheus metrics | no (network-protected) |

All `yes` rows accept the auto-issued internal-ops session for cold visitors. Endpoints that require a real Salesforce-backed session (none in v1) would explicitly check `read_only == false`.

## 11. Architectural reservation for v2 NL chat

`/v1/chat/*` is intentionally unspecified in v1 but reserved. The NL chat endpoint will:

- Accept `{ "query": "...", "session_id": "..." }`
- Call into `QueryService` via Anthropic tool-use
- Return `{ "answer": "...", "sources": [...], "trace": [...] }`

The Pydantic request/response models on every existing endpoint double as tool definitions for the chat layer. No v1 endpoint should hardcode response shapes outside Pydantic models — doing so would force a v2 rewrite when adding chat tool exposure.
