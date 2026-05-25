"""Unit tests for SalesforceService (Phase 04 Half-A).

No network, no DB: simple_salesforce + the urllib token call are
monkeypatched. Covers the §10.1 region translation, field mapping,
client-credentials token exchange, and the Case-write / scope-read
methods' success and failure paths.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from simple_salesforce.exceptions import (
    SalesforceExpiredSession,
    SalesforceGeneralError,
    SalesforceMalformedRequest,
)

from app.exceptions import BadRequest, UpstreamUnavailable
from app.models.salesforce import CaseCreateInput
from app.services.salesforce import (
    REGION_FROM_SF,
    REGION_TO_SF,
    SEVERITY_FROM_SF,
    STATUS_FROM_SF,
    SalesforceService,
)
from app.settings import Settings


def _settings(**kw: Any) -> Settings:
    base: dict[str, Any] = {
        "salesforce_instance_url": "https://example.my.salesforce.com",
        "salesforce_client_id": "cid",
        "salesforce_client_secret": "csec",
    }
    base.update(kw)
    return Settings(**base)


class _FakeResp:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._p = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._p

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *a: object) -> bool:
        return False


def test_region_maps_roundtrip() -> None:
    assert REGION_TO_SF == {"west": "West", "east": "East", "all": "All"}
    for lo, ti in REGION_TO_SF.items():
        assert REGION_FROM_SF[ti] == lo


def test_require_config_missing_raises() -> None:
    svc = SalesforceService(_settings(salesforce_client_id=None))
    with pytest.raises(UpstreamUnavailable):
        svc._require_config()


def test_to_sf_fields_maps_and_translates(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012RT000000AAA")
    payload = CaseCreateInput(
        external_id="CASE-2026-000001",
        subject="subj",
        customer_region="west",
        case_type="lost_signal",
        detection_facts={"a": 1},
        runbook_refs=["r1", "r2"],
        flight_id="abc123",
    )
    f = svc.to_sf_fields(payload)
    assert f["AFM_Customer_Region__c"] == "West"
    assert f["AFM_External_Id__c"] == "CASE-2026-000001"
    assert json.loads(f["AFM_Detection_Facts__c"]) == {"a": 1}
    assert f["AFM_Runbook_Refs__c"] == "r1,r2"
    assert f["RecordTypeId"] == "012RT000000AAA"
    assert f["Subject"] == "subj"
    # None-valued optionals are omitted, never sent as an explicit null.
    assert "AFM_Site_Icao__c" not in f
    assert "AFM_Internal_Url__c" not in f


def test_to_sf_fields_unknown_region_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    bad = CaseCreateInput.model_construct(
        external_id="x",
        subject="s",
        status="New",
        priority=None,
        flight_id=None,
        site_icao=None,
        customer_region="north",  # invalid on purpose
        case_type=None,
        detection_facts={},
        severity_justification=None,
        runbook_refs=[],
        internal_url=None,
    )
    with pytest.raises(BadRequest):
        svc.to_sf_fields(bad)


def test_fetch_client_builds_salesforce(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_sf(**kw: Any) -> MagicMock:
        captured.update(kw)
        return MagicMock(name="sf")

    monkeypatch.setattr("app.services.salesforce.Salesforce", fake_sf)
    monkeypatch.setattr(
        "app.services.salesforce.urllib.request.urlopen",
        lambda req, timeout=0: _FakeResp(
            {"access_token": "TOK", "instance_url": "https://i.my.salesforce.com"}
        ),
    )
    svc = SalesforceService(_settings())
    assert svc._client() is not None
    assert captured["session_id"] == "TOK"
    assert captured["instance_url"] == "https://i.my.salesforce.com"


def test_fetch_client_missing_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.services.salesforce.urllib.request.urlopen",
        lambda req, timeout=0: _FakeResp({"error": "invalid_client"}),
    )
    svc = SalesforceService(_settings())
    with pytest.raises(UpstreamUnavailable):
        svc._client()


async def test_create_case_success(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.Case.create.return_value = {"success": True, "id": "500XYZ"}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    ref = await svc.create_case(CaseCreateInput(external_id="CASE-1", subject="s"))
    assert ref.salesforce_id == "500XYZ"
    assert ref.external_id == "CASE-1"
    fake.Case.create.assert_called_once()


async def test_create_case_success_false_raises_bad_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A 2xx with success=False carries validation errors → permanent BadRequest,
    # so push_pending parks the row `failed` rather than retrying it forever.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.Case.create.return_value = {"success": False, "errors": ["bad"]}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(BadRequest):
        await svc.create_case(CaseCreateInput(external_id="C", subject="s"))


async def test_create_case_4xx_translates_to_bad_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The real push-blocker: a restricted-picklist rejection is HTTP 400.
    # simple_salesforce *raises* before returning a dict; untranslated it
    # 500s the whole batch. It must surface as a permanent BadRequest so the
    # one bad row parks `failed` and the batch continues.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    err = SalesforceMalformedRequest(
        url="https://x/Case",
        status=400,
        resource_name="Case",
        content=[
            {
                "message": "bad value for restricted picklist field: weather_impact",
                "errorCode": "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST",
                "fields": ["AFM_Case_Type__c"],
            }
        ],
    )
    fake.Case.create.side_effect = err
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(BadRequest) as exc_info:
        await svc.create_case(CaseCreateInput(external_id="C", subject="s"))
    assert exc_info.value.details["sf_status"] == 400


async def test_create_case_duplicate_external_id_reconciles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Idempotency: a retry / concurrent push re-submits a row whose Case
    # already exists. The unique AFM_External_Id__c bounces it DUPLICATE_VALUE;
    # we adopt the existing record rather than failing a row that IS synced.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.Case.create.side_effect = SalesforceMalformedRequest(
        url="https://x/Case",
        status=400,
        resource_name="Case",
        content=[
            {
                "message": "duplicate value found: AFM_External_Id__c duplicates value "
                "on record with id: 500EXISTING",
                "errorCode": "DUPLICATE_VALUE",
                "fields": [],
            }
        ],
    )
    fake.query.return_value = {"records": [{"Id": "500EXISTING"}]}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    ref = await svc.create_case(CaseCreateInput(external_id="CASE-1", subject="s"))
    assert ref.salesforce_id == "500EXISTING"
    assert ref.external_id == "CASE-1"
    soql = fake.query.call_args[0][0]
    assert "AFM_External_Id__c" in soql and "CASE-1" in soql


async def test_create_case_duplicate_but_lookup_empty_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # DUPLICATE_VALUE but the lookup finds nothing (shouldn't happen, but be
    # safe) → fall through to the normal permanent-failure translation.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.Case.create.side_effect = SalesforceMalformedRequest(
        url="https://x/Case",
        status=400,
        resource_name="Case",
        content=[{"message": "duplicate value found", "errorCode": "DUPLICATE_VALUE"}],
    )
    fake.query.return_value = {"records": []}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(BadRequest):
        await svc.create_case(CaseCreateInput(external_id="CASE-1", subject="s"))


async def test_create_case_5xx_translates_to_upstream_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A 5xx is SF-side degradation → transient; the row stays pending to retry.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    err = SalesforceGeneralError(
        url="https://x/Case",
        status=500,
        resource_name="Case",
        content=[{"message": "server error"}],
    )
    fake.Case.create.side_effect = err
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(UpstreamUnavailable):
        await svc.create_case(CaseCreateInput(external_id="C", subject="s"))


async def test_create_case_401_reauths_and_retries_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The cached access token expires (TTL) → SF returns 401 INVALID_SESSION_ID.
    # Without the retry the whole push batch parks `failed` until process
    # restart; `_with_session_retry` must drop the cached client and retry once.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    expired = SalesforceExpiredSession(
        url="https://x/Case",
        status=401,
        resource_name="Case",
        content=[{"message": "Session expired or invalid", "errorCode": "INVALID_SESSION_ID"}],
    )
    fake.Case.create.side_effect = [expired, {"id": "500NEW", "success": True}]
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    reauth_calls: list[int] = []
    real_reauth = svc._reauth

    def tracked_reauth() -> Any:
        reauth_calls.append(1)
        return real_reauth()

    monkeypatch.setattr(svc, "_reauth", tracked_reauth)
    ref = await svc.create_case(CaseCreateInput(external_id="CASE-X", subject="s"))
    assert ref.salesforce_id == "500NEW"
    assert len(reauth_calls) == 1, "expected exactly one reauth attempt"
    assert fake.Case.create.call_count == 2, "expected one retry after the 401"


async def test_create_case_persistent_401_translates_to_upstream_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Both attempts 401 → real auth problem (revoked creds, IP block). Must
    # surface as `UpstreamUnavailable` (transient) so the case stays `pending`
    # and the next push pass retries it; never `BadRequest` which would park
    # it permanently `failed`.
    svc = SalesforceService(_settings())
    fake = MagicMock()
    expired = SalesforceExpiredSession(
        url="https://x/Case",
        status=401,
        resource_name="Case",
        content=[{"message": "Session expired or invalid", "errorCode": "INVALID_SESSION_ID"}],
    )
    fake.Case.create.side_effect = expired
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(UpstreamUnavailable) as exc_info:
        await svc.create_case(CaseCreateInput(external_id="C", subject="s"))
    assert exc_info.value.details["sf_status"] == 401


async def test_get_user_custom_perms(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    fake = MagicMock()
    # Two SOQL calls: (1) SetupEntityAccess → SetupEntityIds (CustomPermission ids),
    # (2) CustomPermission → DeveloperName. One level of semi-join nesting only;
    # CustomPermission's API-name column is DeveloperName, not Name.
    fake.query.side_effect = [
        {
            "records": [
                {"SetupEntityId": "0PSCP01"},
                {"SetupEntityId": "0PSCP02"},
            ]
        },
        {
            "records": [
                {"DeveloperName": "AFM_Region_West"},
                {"DeveloperName": "AFM_All_Regions"},
            ]
        },
    ]
    monkeypatch.setattr(svc, "_client", lambda: fake)
    perms = await svc.get_user_custom_perms("005xx")
    assert perms == ["AFM_Region_West", "AFM_All_Regions"]
    first_soql, second_soql = (c.args[0] for c in fake.query.call_args_list)
    assert "SetupEntityAccess" in first_soql
    assert "SetupEntityType='CustomPermission'" in first_soql
    assert "PermissionSetAssignment" in first_soql
    assert "005xx" in first_soql
    assert "DeveloperName" in second_soql
    assert "0PSCP01" in second_soql and "0PSCP02" in second_soql


async def test_get_user_custom_perms_no_assignments(monkeypatch: pytest.MonkeyPatch) -> None:
    """User with no Permission Set assignments → no second query, returns []."""
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.query.return_value = {"records": []}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    perms = await svc.get_user_custom_perms("005nope")
    assert perms == []
    assert fake.query.call_count == 1  # short-circuited before the CP lookup


# -- SF→AFM pull translation (Phase 05 sf_case_sync) ----------------------


def test_status_and_severity_crosswalk_locked() -> None:
    # Locked by user 2026-05-21 — the SF picklist → AFM enum mapping.
    assert STATUS_FROM_SF == {
        "New": "open",
        "Working": "in_progress",
        "Escalated": "in_progress",
        "Closed": "resolved",
    }
    # Reverse of the push's severity→Priority; 'critical' is lossy (→high).
    assert SEVERITY_FROM_SF == {"Low": "low", "Medium": "medium", "High": "high"}


def test_to_sync_record_translates_all_fields() -> None:
    svc = SalesforceService(_settings())
    rec = svc._to_sync_record(
        {
            "Id": "500AAA",
            "AFM_External_Id__c": "CASE-2026-000001",
            "Status": "Closed",
            "Priority": "High",
            "Description": "agent summary",
            "AFM_Severity_Justification__c": "because reasons",
            "AFM_Runbook_Refs__c": "lost-signal-cruise, diversion-ops ",
            "ClosedDate": "2026-05-21T22:30:00.000+0000",
            "SystemModstamp": "2026-05-21T22:31:05.000+0000",
        }
    )
    assert rec.salesforce_id == "500AAA"
    assert rec.external_id == "CASE-2026-000001"
    assert rec.status == "resolved"  # Closed → resolved
    assert rec.severity == "high"
    assert rec.summary == "agent summary"  # standard Description
    assert rec.severity_justification == "because reasons"
    assert rec.runbook_refs == ["lost-signal-cruise", "diversion-ops"]  # comma-split + trimmed
    assert rec.resolved_at == datetime(2026, 5, 21, 22, 30, tzinfo=UTC)
    assert rec.system_modstamp == datetime(2026, 5, 21, 22, 31, 5, tzinfo=UTC)


def test_to_sync_record_open_case_defaults() -> None:
    svc = SalesforceService(_settings())
    rec = svc._to_sync_record(
        {
            "Id": "500BBB",
            "AFM_External_Id__c": "CASE-2026-000002",
            "Status": "New",
            "Priority": None,
            "Description": None,
            "AFM_Severity_Justification__c": None,
            "AFM_Runbook_Refs__c": None,
            "ClosedDate": None,
            "SystemModstamp": "2026-05-21T10:00:00.000+0000",
        }
    )
    assert rec.status == "open"
    assert rec.severity is None  # unknown/empty Priority → None (won't blank local)
    assert rec.summary is None
    assert rec.runbook_refs == []
    assert rec.resolved_at is None


def test_to_sync_record_unknown_status_defaults_open() -> None:
    svc = SalesforceService(_settings())
    rec = svc._to_sync_record(
        {"Id": "500C", "Status": "Reopened", "SystemModstamp": "2026-05-21T10:00:00.000+0000"}
    )
    assert rec.status == "open"  # never drop a Case on an unmapped status


async def test_query_cases_modified_since_builds_soql(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings(salesforce_case_record_type="Fleet_Operations"))
    fake = MagicMock()
    fake.query.return_value = {
        "records": [
            {"Id": "500A", "Status": "Working", "SystemModstamp": "2026-05-21T12:00:00.000+0000"}
        ]
    }
    monkeypatch.setattr(svc, "_client", lambda: fake)
    watermark = datetime(2026, 5, 21, 11, 0, 0, tzinfo=UTC)

    out = await svc.query_cases_modified_since(watermark, limit=200)

    soql = fake.query.call_args.args[0]
    assert "RecordType.DeveloperName = 'Fleet_Operations'" in soql
    # SOQL datetime literals are UNQUOTED with a Z suffix.
    assert "SystemModstamp > 2026-05-21T11:00:00Z" in soql
    assert "'2026-05-21T11:00:00Z'" not in soql
    assert "ORDER BY SystemModstamp ASC" in soql
    assert "LIMIT 200" in soql
    assert len(out) == 1 and out[0].status == "in_progress"
