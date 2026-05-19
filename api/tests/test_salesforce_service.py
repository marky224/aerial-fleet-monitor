"""Unit tests for SalesforceService (Phase 04 Half-A).

No network, no DB: simple_salesforce + the urllib token call are
monkeypatched. Covers the §10.1 region translation, field mapping,
client-credentials token exchange, and the Case-write / scope-read
methods' success and failure paths.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.exceptions import BadRequest, UpstreamUnavailable
from app.models.salesforce import CaseCreateInput
from app.services.salesforce import REGION_FROM_SF, REGION_TO_SF, SalesforceService
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


async def test_create_case_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.Case.create.return_value = {"success": False, "errors": ["bad"]}
    monkeypatch.setattr(svc, "_client", lambda: fake)
    monkeypatch.setattr(svc, "_case_record_type", lambda: "012")
    with pytest.raises(UpstreamUnavailable):
        await svc.create_case(CaseCreateInput(external_id="C", subject="s"))


async def test_get_user_custom_perms(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SalesforceService(_settings())
    fake = MagicMock()
    fake.query.return_value = {
        "records": [{"Name": "AFM_Region_West"}, {"Name": "AFM_All_Regions"}]
    }
    monkeypatch.setattr(svc, "_client", lambda: fake)
    perms = await svc.get_user_custom_perms("005xx")
    assert perms == ["AFM_Region_West", "AFM_All_Regions"]
    soql = fake.query.call_args[0][0]
    assert "PermissionSetAssignment" in soql
    assert "005xx" in soql
