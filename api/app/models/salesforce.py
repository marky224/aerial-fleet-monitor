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


class SfTestCaseResult(BaseModel):
    """Round-trip result of the dev-only `POST /v1/admin/sf-test-case`."""

    created: SalesforceCaseRef
    deleted: bool = Field(description="True if the smoke Case was cleaned up.")
    sf_fields_sent: dict[str, Any] = Field(
        description="Exact field map sent to Salesforce (post region/format translation)."
    )
