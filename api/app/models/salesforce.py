"""Salesforce-facing models for the AFM→SF write path (Phase 04 Half-A).

`CaseCreateInput` is the API-side (snake_case, Postgres-region-lowercase)
shape. `SalesforceService` is the *only* place region values are
translated to Salesforce TitleCase and snake_case fields are mapped to
`AFM_*__c` API names (SALESFORCE.md §10.1) — these models never carry SF
field names so the translation stays centralized.

Interactive OAuth token/userinfo models are deliberately absent: the
user-facing login flow is the Half-B re-plan (no React frontend under
the Foundry-as-dashboard pivot) — see docs/build/04_salesforce_setup.md.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.common import CustomerRegion

# 6 AFM rule types — API value = rule id (snake_case) to match the
# Phase-05 detector and the AFM_Case_Type__c restricted picklist.
AfmCaseType = str


class CaseCreateInput(BaseModel):
    """A Case to create in Salesforce. Region is Postgres-lowercase here;
    `SalesforceService` applies `REGION_TO_SF` before the REST write."""

    external_id: str = Field(description="CASE-YYYY-NNNNNN. → AFM_External_Id__c (unique).")
    subject: str = Field(description="Templated Case subject. → standard Subject.")
    status: str = Field(default="New", description="→ standard Status (Fleet_Operations process).")
    priority: str | None = Field(
        default=None,
        description="→ standard Priority. Carries AFM severity (see build-doc decision log).",
    )
    flight_id: str | None = Field(default=None, description="ICAO24 hex. → AFM_Flight_Id__c.")
    site_icao: str | None = Field(default=None, description="Affected airport. → AFM_Site_Icao__c.")
    customer_region: CustomerRegion = Field(
        default=None, description="west/east/all (lowercase). → AFM_Customer_Region__c (TitleCase)."
    )
    case_type: AfmCaseType | None = Field(
        default=None, description="One of the 6 rule ids. → AFM_Case_Type__c."
    )
    detection_facts: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured rule output. JSON-encoded → AFM_Detection_Facts__c.",
    )
    severity_justification: str | None = Field(
        default=None, description="Agent rationale. → AFM_Severity_Justification__c."
    )
    runbook_refs: list[str] = Field(
        default_factory=list, description="Runbook ids. Comma-joined → AFM_Runbook_Refs__c."
    )
    internal_url: str | None = Field(
        default=None, description="AFM dashboard deeplink. → AFM_Internal_Url__c."
    )


class SalesforceCaseRef(BaseModel):
    """Result of a Case write — both sides of the cross-system id pair."""

    salesforce_id: str = Field(description="Salesforce Case Id (15/18-char).")
    external_id: str = Field(description="AFM_External_Id__c (== Postgres cases.case_id).")


class CaseSyncSummary(BaseModel):
    """Outcome of a `POST /v1/cases/sync-pending` push pass."""

    attempted: int = Field(description="Pending cases pulled this pass.")
    synced: int = Field(description="Created in Salesforce + marked synced.")
    retrying: int = Field(description="Transient failure; left pending for the next pass.")
    failed: int = Field(description="Permanent failure (or max attempts); parked failed.")


class CaseSyncRecord(BaseModel):
    """One Salesforce Case translated to the AFM (Postgres) side.

    Produced by `SalesforceService.query_cases_modified_since` — the single
    place SF→AFM translation happens (SALESFORCE.md §10.1, the mirror of
    `CaseCreateInput`'s AFM→SF direction). `status`/`severity` are already
    AFM-lowercase here; this model never carries an SF picklist value or an
    `AFM_*__c` field name."""

    salesforce_id: str = Field(description="Salesforce Case Id. Match key into app.cases.")
    external_id: str = Field(
        description="AFM_External_Id__c (== app.cases.case_id). Fallback match key."
    )
    status: str = Field(description="AFM status: open|acknowledged|in_progress|resolved|closed.")
    severity: str | None = Field(
        default=None, description="AFM severity from standard Priority (low|medium|high)."
    )
    summary: str | None = Field(
        default=None, description="From standard Description (Agentforce-authored)."
    )
    severity_justification: str | None = Field(
        default=None, description="From AFM_Severity_Justification__c."
    )
    runbook_refs: list[str] = Field(
        default_factory=list, description="From AFM_Runbook_Refs__c (comma-split)."
    )
    resolved_at: datetime | None = Field(default=None, description="From standard ClosedDate.")
    system_modstamp: datetime = Field(description="SF SystemModstamp — drives the sync watermark.")


class CasePullSummary(BaseModel):
    """Outcome of a `POST /v1/cases/sync-from-sf` pull pass."""

    fetched: int = Field(description="Cases returned by SF since the watermark.")
    updated: int = Field(description="Matched app.cases rows updated.")
    unmatched: int = Field(description="SF Cases with no local app.cases row (skipped).")
    watermark: datetime | None = Field(
        default=None, description="New watermark (max SystemModstamp); None if nothing changed."
    )


class SfTestCaseResult(BaseModel):
    """Round-trip result of the dev-only `POST /v1/admin/sf-test-case`."""

    created: SalesforceCaseRef
    deleted: bool = Field(description="True if the smoke Case was cleaned up.")
    sf_fields_sent: dict[str, Any] = Field(
        description="Exact field map sent to Salesforce (post region/format translation)."
    )
