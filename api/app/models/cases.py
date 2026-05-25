"""Case-facing models.

Phase 05 task #5 lands one model pair: `CaseForSync` / `CasesForSyncPage`
for the system-internal `GET /v1/cases/all-for-sync` endpoint the Foundry
sync (`afm_foundry_sync.api_readers.fetch_cases_for_sync`) consumes.

Customer-facing read models (`CaseListItem`, `CaseDetail`,
`CaseTimelineEvent`) land here with Phase 05 task #4 (`GET /v1/cases`,
`GET /v1/cases/{id}`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CaseForSync(BaseModel):
    """One `app.cases` row in the shape the Foundry sync ingests.

    Server-to-server snapshot — no scope filtering, includes resolved
    cases (Foundry mirrors the full lifecycle; App 1's panel applies its
    own status filter at display time). Mirrors `app.cases` columns
    one-for-one minus the `sf_sync_*` internals, plus a derived `subject`
    (formatted by `CaseSyncService._format_subject` — the same formatter
    the SF push uses, kept in one place).
    """

    case_id: str = Field(description="AFM PK; also Salesforce `AFM_External_Id__c`.")
    salesforce_id: str | None = Field(
        default=None,
        description="SF Case Id once the push has succeeded; NULL while pending.",
    )
    case_type: str = Field(description="Rule id (lost_signal/diversion/excessive_hold/...).")
    status: str = Field(description="AFM lifecycle: open/acknowledged/in_progress/resolved.")
    severity: str = Field(description="AFM severity: low/medium/high/critical.")
    customer_region: str = Field(description="west/east/all.")
    site_icao: str = Field(description="Nearest site or, for site-level rules, the site itself.")
    flight_id: str = Field(
        description=(
            "Aircraft-level: synthesized Flight PK `{icao24}-{unix_takeoff_ts}` "
            "(FK to Flight). Site-level (weather_impact): `WX-{site_icao}` sentinel."
        ),
    )
    subject: str = Field(
        description=(
            "Human-readable subject derived from case_type + detection_facts via "
            "`CaseSyncService._format_subject` (same formatter as the SF push)."
        ),
    )
    summary: str | None = Field(
        default=None,
        description="Free-text summary from SF Description (Phase 07 Agentforce-authored).",
    )
    severity_justification: str | None = Field(
        default=None,
        description="Mirrors SF `AFM_Severity_Justification__c`; NULL until set in SF.",
    )
    detection_facts: dict[str, Any] = Field(
        default_factory=dict,
        description="Rule-specific facts the detector emitted; written verbatim.",
    )
    runbook_refs: list[str] = Field(
        default_factory=list,
        description="Runbook slugs from `lookup_runbooks(rule, site_icao)`.",
    )
    created_at: datetime = Field(description="UTC time the detector inserted the row.")
    updated_at: datetime = Field(
        description="UTC time of the last mutation; drives the Foundry sync's incremental cursor.",
    )
    resolved_at: datetime | None = Field(
        default=None,
        description="UTC time the case transitioned to status=resolved; NULL while open.",
    )


class CasesForSyncPage(BaseModel):
    """One page of cases returned by `GET /v1/cases/all-for-sync`.

    `next_cursor` is the max(updated_at) across `items`; the Foundry sync
    persists it as the watermark and passes it back as `since` next call.
    `truncated=True` when the page filled the requested `limit`, so the
    caller should keep paging until it sees `False`. An empty page leaves
    `next_cursor=None` (watermark untouched).
    """

    items: list[CaseForSync] = Field(default_factory=list)
    next_cursor: datetime | None = Field(
        default=None,
        description="max(updated_at) in this page; pass as `since` on the next call.",
    )
    truncated: bool = Field(
        description="True when len(items) == limit; more rows likely available.",
    )
