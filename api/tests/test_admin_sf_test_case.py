"""Unit tests for the dev-only SF write-smoke endpoint (acceptance #9).

Direct coroutine calls (no TestClient/DB — matches the suite's style).
The router is mounted only when environment == 'dev'; the default test
environment is 'dev', so the mount assertion exercises that wiring.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from app.exceptions import UpstreamUnavailable
from app.models.salesforce import SalesforceCaseRef
from app.routers.admin import sf_test_case


def _mock_sf() -> MagicMock:
    sf = MagicMock()
    sf.to_sf_fields = MagicMock(return_value={"Subject": "x", "AFM_Customer_Region__c": "West"})
    sf.create_case = AsyncMock(
        return_value=SalesforceCaseRef(salesforce_id="500A", external_id="CASE-TEST-x")
    )
    sf.delete_case = AsyncMock(return_value=None)
    return sf


async def test_sf_test_case_roundtrip() -> None:
    sf = _mock_sf()
    res = await sf_test_case(sf=sf)
    assert res.created.salesforce_id == "500A"
    assert res.deleted is True
    assert res.sf_fields_sent["AFM_Customer_Region__c"] == "West"
    sf.create_case.assert_awaited_once()
    sf.delete_case.assert_awaited_once_with("500A")


async def test_sf_test_case_delete_failure_sets_flag() -> None:
    sf = _mock_sf()
    sf.delete_case = AsyncMock(side_effect=UpstreamUnavailable("delete boom"))
    res = await sf_test_case(sf=sf)
    assert res.created.salesforce_id == "500A"
    assert res.deleted is False


def test_admin_router_mounted_in_dev() -> None:
    from app.main import app

    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/v1/admin/sf-test-case" in paths
