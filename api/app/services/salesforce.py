"""SalesforceService — the AFM→Salesforce write path (Phase 04 Half-A).

`simple_salesforce` is synchronous; every method that touches the REST
client is wrapped in `asyncio.to_thread` so the service is safe to call
from FastAPI's event loop (SALESFORCE.md §"Async wrapping convention").

Auth is OAuth 2.0 Client Credentials against the org's My Domain token
endpoint (no stored user password; the Connected App's "Run As" user is
the effective principal). The token is fetched lazily and cached;
``_with_session_retry`` drops the cached client and re-fetches once on
a 401 ``INVALID_SESSION_ID`` (token TTL expired between requests on
this app-wide singleton), and ``_translate_sf_error`` maps a residual
401 to a transient ``UpstreamUnavailable`` so the push retries the
row rather than parking it permanently ``failed``.

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
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from simple_salesforce import Salesforce  # type: ignore[attr-defined]
from simple_salesforce.exceptions import SalesforceError

from app.exceptions import AFMException, BadRequest, UpstreamUnavailable
from app.logging import get_logger
from app.models.common import Region
from app.models.salesforce import CaseCreateInput, CaseSyncRecord, SalesforceCaseRef
from app.settings import Settings

log = get_logger(__name__)

# Region translation — the ONE place it happens (SALESFORCE.md §10.1).
REGION_TO_SF: dict[str, str] = {"west": "West", "east": "East", "all": "All"}
REGION_FROM_SF: dict[str, Region] = {"West": "west", "East": "east", "All": "all"}

# SF→AFM translation for the pull half (mirror of the AFM→SF write maps).
# Status: SF Fleet_Operations picklist (New/Working/Escalated/Closed) → AFM
# cases.status (open|acknowledged|in_progress|resolved|closed). Crosswalk
# locked by user 2026-05-21: 'acknowledged' is unused (no SF equivalent in
# the 4-value picklist); 'Closed' maps to 'resolved' and carries ClosedDate.
STATUS_FROM_SF: dict[str, str] = {
    "New": "open",
    "Working": "in_progress",
    "Escalated": "in_progress",
    "Closed": "resolved",
}
# Defensive default if the org adds a Status value we don't map yet — never
# silently drop a Case, just treat the unknown state as open.
_DEFAULT_AFM_STATUS = "open"

# Priority carries AFM severity (CaseCreateInput); reverse of _SEVERITY_TO_PRIORITY
# in case_sync.py. Note 'critical' collapsed to High on the way out, so it
# round-trips back as 'high' — lossy by design (build-doc decision log).
SEVERITY_FROM_SF: dict[str, str] = {"Low": "low", "Medium": "medium", "High": "high"}

_SF_API_VERSION = "62.0"  # REST data API; independent of metadata sourceApiVersion
_TOKEN_TIMEOUT_S = 20

# How far back to look on the very first pull (no watermark row yet).
_FIRST_PULL_LOOKBACK = timedelta(hours=1)
# SystemModstamp is monotonic but a `>` query never re-reads the boundary
# row; LIMIT bounds one cycle's batch (PIPELINES.md §3.5 uses 200).
_PULL_PAGE_LIMIT = 200


def _parse_sf_datetime(value: str | None) -> datetime | None:
    """Parse a Salesforce datetime ('2026-05-21T22:00:00.000+0000') to aware UTC.

    Python 3.11+ `fromisoformat` accepts the millisecond + `+0000` offset SF
    emits. Returns None for null/empty (e.g. an open Case's ClosedDate)."""
    if not value:
        return None
    return datetime.fromisoformat(value)


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

    def _with_session_retry(self, fn: Callable[[], Any]) -> Any:
        """Run ``fn``; on a 401 INVALID_SESSION_ID, drop the cached client
        and retry once. The Connected App's client-credentials access token
        has a finite TTL (org rotation policy) and the SalesforceService
        instance is an app-wide singleton, so an expired token would
        otherwise 401 every subsequent call until process restart. A
        second 401 indicates a real auth problem (revoked creds, IP
        block) and is left to propagate — `_translate_sf_error` will
        surface it as a transient `UpstreamUnavailable` so the push
        retries the row rather than parking it `failed`."""
        try:
            return fn()
        except SalesforceError as exc:
            if getattr(exc, "status", None) != 401:
                raise
            self._reauth()
            return fn()

    # -- field mapping ----------------------------------------------------

    def _case_record_type(self) -> str:
        if self._case_record_type_id is None:
            soql = (
                "SELECT Id FROM RecordType WHERE SobjectType='Case' "
                f"AND DeveloperName='{self._record_type_dev_name}'"
            )
            res = self._with_session_retry(lambda: self._client().query(soql))
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

    @staticmethod
    def _translate_sf_error(exc: SalesforceError, message: str) -> AFMException:
        """Map a ``simple_salesforce`` HTTP error to AFM's error hierarchy.

        ``simple_salesforce`` *raises* a ``SalesforceError`` subclass on a
        non-2xx before returning the result dict, so callers that only check
        ``result["success"]`` never see a 4xx (e.g. a restricted-picklist
        rejection → ``INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST``, HTTP 400).
        Left untranslated it bubbles out as an unhandled 500 and aborts the
        whole push batch. The split mirrors ``push_pending``'s contract: a
        4xx is the request's own fault → permanent ``BadRequest`` (case
        parked ``failed``); a 5xx / network / unknown status is degradation
        → transient ``UpstreamUnavailable`` (case stays ``pending`` to retry).
        """
        status = getattr(exc, "status", None)
        details = {"sf_status": status, "sf_errors": getattr(exc, "content", None)}
        # 401 = session/auth, treated as transient so the push retries the row
        # (`_with_session_retry` already attempts one in-process refresh before
        # this point; a residual 401 here usually means revoked creds or an IP
        # block that may clear on its own).
        if status == 401:
            return UpstreamUnavailable(message, details=details)
        if isinstance(status, int) and 400 <= status < 500:
            return BadRequest(message, details=details)
        return UpstreamUnavailable(message, details=details)

    @staticmethod
    def _is_duplicate_external_id(exc: SalesforceError) -> bool:
        """True if the error is a DUPLICATE_VALUE on the unique external id.

        The push is at-least-once (a retry, or a concurrent pass, can
        re-submit a row whose Case already exists). AFM_External_Id__c is
        unique, so the second insert fails DUPLICATE_VALUE rather than
        creating a twin — we treat that as "already created" and reconcile.
        """
        content = getattr(exc, "content", None)
        if not isinstance(content, list):
            return False
        return any(isinstance(e, dict) and e.get("errorCode") == "DUPLICATE_VALUE" for e in content)

    def _find_case_id_by_external_id_sync(self, external_id: str) -> str | None:
        safe = external_id.replace("\\", "\\\\").replace("'", "\\'")
        soql = f"SELECT Id FROM Case WHERE AFM_External_Id__c = '{safe}' LIMIT 1"
        records = self._with_session_retry(lambda: self._client().query(soql)).get("records", [])
        return cast(str, records[0]["Id"]) if records else None

    def _create_case_sync(self, payload: CaseCreateInput) -> SalesforceCaseRef:
        sf_fields = self.to_sf_fields(payload)
        try:
            result = self._with_session_retry(lambda: self._client().Case.create(sf_fields))
        except SalesforceError as exc:
            # Idempotent recovery: a Case already exists for this external id
            # (retry / concurrent push) → adopt the existing record instead of
            # failing a row that is, in fact, synced.
            if self._is_duplicate_external_id(exc):
                existing = self._find_case_id_by_external_id_sync(payload.external_id)
                if existing is not None:
                    return SalesforceCaseRef(
                        salesforce_id=existing, external_id=payload.external_id
                    )
            raise self._translate_sf_error(exc, "Salesforce Case create failed") from exc
        if not result.get("success"):
            # 2xx with success=False is rare but carries validation errors —
            # treat as a permanent bad request, not transient degradation.
            raise BadRequest(
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

    def _update_case_sync(self, case_id: str, updates: dict[str, Any]) -> None:
        self._with_session_retry(lambda: self._client().Case.update(case_id, updates))

    async def update_case(self, case_id: str, updates: dict[str, Any]) -> None:
        """Patch a Case by Salesforce Id. `updates` uses SF field names."""
        await asyncio.to_thread(self._update_case_sync, case_id, updates)

    def _delete_case_sync(self, case_id: str) -> None:
        self._with_session_retry(lambda: self._client().Case.delete(case_id))

    async def delete_case(self, case_id: str) -> None:
        """Hard-delete a Case by Salesforce Id (used by the dev smoke test)."""
        await asyncio.to_thread(self._delete_case_sync, case_id)

    # -- scope read (Half-A: provided + tested, not yet wired) ------------

    def _user_custom_perms_sync(self, user_id: str) -> list[str]:
        # Two queries — SOQL allows only ONE level of semi-join nesting (a
        # three-level IN(...IN(...IN(...))) is rejected as MALFORMED_QUERY),
        # and CustomPermission's API-name column is `DeveloperName` not `Name`.
        # The mock-based unit test never exercised real SOQL; the Phase-04
        # integration suite (test_salesforce_integration.py) caught both.
        access_soql = (
            "SELECT SetupEntityId FROM SetupEntityAccess "
            "WHERE SetupEntityType='CustomPermission' AND ParentId IN ("
            "SELECT PermissionSetId FROM PermissionSetAssignment "
            f"WHERE AssigneeId = '{user_id}')"
        )
        access_recs = self._with_session_retry(lambda: self._client().query(access_soql)).get(
            "records", []
        )
        cp_ids = [cast(str, r["SetupEntityId"]) for r in access_recs]
        if not cp_ids:
            return []
        id_list = ", ".join(f"'{i}'" for i in cp_ids)
        cp_soql = f"SELECT DeveloperName FROM CustomPermission WHERE Id IN ({id_list})"
        cp_recs = self._with_session_retry(lambda: self._client().query(cp_soql)).get("records", [])
        return [cast(str, r["DeveloperName"]) for r in cp_recs]

    async def get_user_custom_perms(self, user_id: str) -> list[str]:
        """Custom-permission API names assigned to a user (scope source).

        Wired into request scope in the Half-B re-plan; here so the SF
        query is implemented + integration-testable now.
        """
        return await asyncio.to_thread(self._user_custom_perms_sync, user_id)

    # -- reads: the SF→Postgres pull (Phase 05) ---------------------------

    @staticmethod
    def default_pull_watermark() -> datetime:
        """Watermark to use on the first pull (no `sync_watermarks` row yet)."""
        return datetime.now(tz=UTC) - _FIRST_PULL_LOOKBACK

    def _to_sync_record(self, rec: dict[str, Any]) -> CaseSyncRecord:
        """Translate one SF Case query row to the AFM-side `CaseSyncRecord`.

        The ONE place SF→AFM Status/Priority translation happens (the mirror
        of `to_sf_fields`). Unknown Status → open (never drop a Case); unknown
        Priority → None (leaves the local severity untouched downstream)."""
        refs_raw = cast("str | None", rec.get("AFM_Runbook_Refs__c")) or ""
        runbook_refs = [r.strip() for r in refs_raw.split(",") if r.strip()]
        return CaseSyncRecord(
            salesforce_id=cast(str, rec["Id"]),
            external_id=cast(str, rec.get("AFM_External_Id__c") or ""),
            status=STATUS_FROM_SF.get(cast(str, rec.get("Status") or ""), _DEFAULT_AFM_STATUS),
            severity=SEVERITY_FROM_SF.get(cast(str, rec.get("Priority") or "")),
            summary=cast("str | None", rec.get("Description")),
            severity_justification=cast("str | None", rec.get("AFM_Severity_Justification__c")),
            runbook_refs=runbook_refs,
            resolved_at=_parse_sf_datetime(cast("str | None", rec.get("ClosedDate"))),
            system_modstamp=cast(datetime, _parse_sf_datetime(cast(str, rec["SystemModstamp"]))),
        )

    def _query_cases_modified_since_sync(
        self, watermark: datetime, limit: int
    ) -> list[CaseSyncRecord]:
        # SOQL datetime literals are UNQUOTED and need a UTC `Z`/offset form.
        soql_ts = watermark.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        soql = (
            "SELECT Id, AFM_External_Id__c, Status, Priority, Description, "
            "AFM_Severity_Justification__c, AFM_Runbook_Refs__c, ClosedDate, SystemModstamp "
            "FROM Case "
            f"WHERE RecordType.DeveloperName = '{self._record_type_dev_name}' "
            f"AND SystemModstamp > {soql_ts} "
            "ORDER BY SystemModstamp ASC "
            f"LIMIT {int(limit)}"
        )
        records = self._with_session_retry(lambda: self._client().query(soql)).get("records", [])
        return [self._to_sync_record(r) for r in records]

    async def query_cases_modified_since(
        self, watermark: datetime, limit: int = _PULL_PAGE_LIMIT
    ) -> list[CaseSyncRecord]:
        """Fleet_Operations Cases with SystemModstamp > watermark, oldest first.

        Returns AFM-translated rows; `system_modstamp` drives the caller's
        watermark advance (PIPELINES.md §3.5)."""
        return await asyncio.to_thread(self._query_cases_modified_since_sync, watermark, limit)
