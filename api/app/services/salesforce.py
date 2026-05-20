"""SalesforceService — the AFM→Salesforce write path (Phase 04 Half-A).

`simple_salesforce` is synchronous; every method that touches the REST
client is wrapped in `asyncio.to_thread` so the service is safe to call
from FastAPI's event loop (SALESFORCE.md §"Async wrapping convention").

Auth is OAuth 2.0 Client Credentials against the org's My Domain token
endpoint (no stored user password; the Connected App's "Run As" user is
the effective principal). The token is fetched lazily and cached, then
refreshed once on an auth failure.

This module is the single place region values are translated to
Salesforce TitleCase and snake_case inputs are mapped to `AFM_*__c` API
names (SALESFORCE.md §10.1). Nothing else performs that translation.

Interactive user OAuth (authorization-code, userinfo, JWT-cookie scope)
is the Half-B re-plan and intentionally not implemented here. The one
read method that survives into Half-A, `get_user_custom_perms`, is
provided + unit-tested but is not yet wired into request scope (the
Phase-02 auth stub still owns `get_scope`).
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, cast

from simple_salesforce import Salesforce  # type: ignore[attr-defined]

from app.exceptions import BadRequest, UpstreamUnavailable
from app.logging import get_logger
from app.models.common import Region
from app.models.salesforce import CaseCreateInput, SalesforceCaseRef
from app.settings import Settings

log = get_logger(__name__)

# Region translation — the ONE place it happens (SALESFORCE.md §10.1).
REGION_TO_SF: dict[str, str] = {"west": "West", "east": "East", "all": "All"}
REGION_FROM_SF: dict[str, Region] = {"West": "west", "East": "east", "All": "all"}

_SF_API_VERSION = "62.0"  # REST data API; independent of metadata sourceApiVersion
_TOKEN_TIMEOUT_S = 20


class SalesforceService:
    """REST client wrapper for AFM's Case-write + scope-read concerns."""

    def __init__(self, settings: Settings) -> None:
        self._instance_url = settings.salesforce_instance_url
        self._client_id = settings.salesforce_client_id
        self._client_secret = settings.salesforce_client_secret
        self._record_type_dev_name = settings.salesforce_case_record_type
        # simple_salesforce's client is dynamically typed; keep it Any so
        # `.Case.create/.update/.delete` + .query don't fight loose stubs.
        self._sf: Any = None
        self._case_record_type_id: str | None = None

    # -- auth -------------------------------------------------------------

    def _require_config(self) -> tuple[str, str, str]:
        if not (self._instance_url and self._client_id and self._client_secret):
            raise UpstreamUnavailable(
                "Salesforce is not configured",
                details={"missing": "SALESFORCE_INSTANCE_URL/CLIENT_ID/CLIENT_SECRET"},
            )
        return self._instance_url, self._client_id, self._client_secret

    def _fetch_client(self) -> Any:
        """Client-credentials token exchange → authed simple_salesforce client."""
        instance_url, client_id, client_secret = self._require_config()
        body = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        ).encode()
        req = urllib.request.Request(
            f"{instance_url.rstrip('/')}/services/oauth2/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=_TOKEN_TIMEOUT_S) as resp:
                tok = json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            raise UpstreamUnavailable(
                "Salesforce token request failed",
                details={"status": exc.code},
            ) from exc
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            raise UpstreamUnavailable("Salesforce token request failed") from exc

        access_token = tok.get("access_token")
        token_instance = tok.get("instance_url", instance_url)
        if not access_token:
            raise UpstreamUnavailable(
                "Salesforce token response missing access_token",
                details={"error": tok.get("error", "unknown")},
            )
        log.info("salesforce.token_ok", instance=token_instance)
        return Salesforce(
            instance_url=token_instance,
            session_id=access_token,
            version=_SF_API_VERSION,
        )

    def _client(self) -> Any:
        if self._sf is None:
            self._sf = self._fetch_client()
        return self._sf

    def _reauth(self) -> Any:
        self._sf = None
        self._case_record_type_id = None
        return self._client()

    # -- field mapping ----------------------------------------------------

    def _case_record_type(self) -> str:
        if self._case_record_type_id is None:
            soql = (
                "SELECT Id FROM RecordType WHERE SobjectType='Case' "
                f"AND DeveloperName='{self._record_type_dev_name}'"
            )
            res = self._client().query(soql)
            recs = res.get("records", [])
            if not recs:
                raise UpstreamUnavailable(
                    "Case RecordType not found in org",
                    details={"developer_name": self._record_type_dev_name},
                )
            self._case_record_type_id = cast(str, recs[0]["Id"])
        return self._case_record_type_id

    def to_sf_fields(self, payload: CaseCreateInput) -> dict[str, Any]:
        """Map the API-side payload to Salesforce field names + values.

        Region → TitleCase here and nowhere else. None-valued fields are
        omitted so we never blank a field with an explicit null.
        """
        region_sf: str | None = None
        if payload.customer_region is not None:
            try:
                region_sf = REGION_TO_SF[payload.customer_region]
            except KeyError as exc:
                raise BadRequest(
                    "Unknown customer_region",
                    details={"customer_region": payload.customer_region},
                ) from exc

        fields: dict[str, Any] = {
            "Subject": payload.subject,
            "Status": payload.status,
            "RecordTypeId": self._case_record_type(),
            "AFM_External_Id__c": payload.external_id,
            "Priority": payload.priority,
            "AFM_Flight_Id__c": payload.flight_id,
            "AFM_Site_Icao__c": payload.site_icao,
            "AFM_Customer_Region__c": region_sf,
            "AFM_Case_Type__c": payload.case_type,
            "AFM_Detection_Facts__c": (
                json.dumps(payload.detection_facts) if payload.detection_facts else None
            ),
            "AFM_Severity_Justification__c": payload.severity_justification,
            "AFM_Runbook_Refs__c": (
                ",".join(payload.runbook_refs) if payload.runbook_refs else None
            ),
            "AFM_Internal_Url__c": payload.internal_url,
        }
        return {k: v for k, v in fields.items() if v is not None}

    # -- writes -----------------------------------------------------------

    def _create_case_sync(self, payload: CaseCreateInput) -> SalesforceCaseRef:
        sf_fields = self.to_sf_fields(payload)
        result = self._client().Case.create(sf_fields)
        if not result.get("success"):
            raise UpstreamUnavailable(
                "Salesforce Case create failed",
                details={"errors": result.get("errors", [])},
            )
        return SalesforceCaseRef(
            salesforce_id=cast(str, result["id"]),
            external_id=payload.external_id,
        )

    async def create_case(self, payload: CaseCreateInput) -> SalesforceCaseRef:
        """Insert a Case. Returns both cross-system ids (DATA_MODEL §6)."""
        return await asyncio.to_thread(self._create_case_sync, payload)

    async def update_case(self, case_id: str, updates: dict[str, Any]) -> None:
        """Patch a Case by Salesforce Id. `updates` uses SF field names."""
        await asyncio.to_thread(self._client().Case.update, case_id, updates)

    async def delete_case(self, case_id: str) -> None:
        """Hard-delete a Case by Salesforce Id (used by the dev smoke test)."""
        await asyncio.to_thread(self._client().Case.delete, case_id)

    # -- scope read (Half-A: provided + tested, not yet wired) ------------

    def _user_custom_perms_sync(self, user_id: str) -> list[str]:
        # Two queries — SOQL allows only ONE level of semi-join nesting (a
        # three-level IN(...IN(...IN(...))) is rejected as MALFORMED_QUERY),
        # and CustomPermission's API-name column is `DeveloperName` not `Name`.
        # The mock-based unit test never exercised real SOQL; the Phase-04
        # integration suite (test_salesforce_integration.py) caught both.
        client = self._client()
        access_soql = (
            "SELECT SetupEntityId FROM SetupEntityAccess "
            "WHERE SetupEntityType='CustomPermission' AND ParentId IN ("
            "SELECT PermissionSetId FROM PermissionSetAssignment "
            f"WHERE AssigneeId = '{user_id}')"
        )
        access_recs = client.query(access_soql).get("records", [])
        cp_ids = [cast(str, r["SetupEntityId"]) for r in access_recs]
        if not cp_ids:
            return []
        id_list = ", ".join(f"'{i}'" for i in cp_ids)
        cp_recs = client.query(
            f"SELECT DeveloperName FROM CustomPermission WHERE Id IN ({id_list})"
        ).get("records", [])
        return [cast(str, r["DeveloperName"]) for r in cp_recs]

    async def get_user_custom_perms(self, user_id: str) -> list[str]:
        """Custom-permission API names assigned to a user (scope source).

        Wired into request scope in the Half-B re-plan; here so the SF
        query is implemented + integration-testable now.
        """
        return await asyncio.to_thread(self._user_custom_perms_sync, user_id)
