"""Integration tests against the live afm-dev Salesforce org (Phase 04 Half-A acceptance #8).

Run with ``make test-integration`` (or ``pytest -m integration`` from
``api/``). The whole module is marked ``integration`` and is auto-skipped
when ``SALESFORCE_INSTANCE_URL``/``CLIENT_ID``/``CLIENT_SECRET`` are not
configured — so unit-test environments and CI runners without Connected
App creds stay green.

Independence discipline: we never trust a 200 / "Succeeded". Every write
that goes through ``SalesforceService`` is verified by a *separate*
client-credentials token + REST ``query``/``queryAll`` round-trip via
:func:`_verifier_query`. This mirrors the #9 write-smoke verification
path — the bug it caught (and would re-catch) is a service that returns
``success=True`` without actually persisting.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from app.models.salesforce import CaseCreateInput
from app.services.salesforce import SalesforceService
from app.settings import Settings

# Single Settings load — credentials, instance URL, and the case
# RecordType DeveloperName all flow from here, exactly like prod.
_settings = Settings()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (
            _settings.salesforce_instance_url
            and _settings.salesforce_client_id
            and _settings.salesforce_client_secret
        ),
        reason=(
            "Salesforce env vars not configured "
            "(SALESFORCE_INSTANCE_URL / SALESFORCE_CLIENT_ID / SALESFORCE_CLIENT_SECRET)"
        ),
    ),
]


# --- independent verifier (separate client-creds path, by design) ----


def _verifier_token() -> tuple[str, str]:
    """Fetch a fresh client-credentials token via plain ``urllib``.

    Deliberately bypasses ``SalesforceService`` so the assertions verify
    the service against an *independent* signal (the rule locked in for
    acceptance #9). Returns ``(access_token, instance_url)``.
    """
    assert _settings.salesforce_instance_url is not None  # narrowed by skipif
    assert _settings.salesforce_client_id is not None
    assert _settings.salesforce_client_secret is not None
    inst = _settings.salesforce_instance_url.rstrip("/")
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": _settings.salesforce_client_id,
            "client_secret": _settings.salesforce_client_secret,
        }
    ).encode()
    req = urllib.request.Request(
        inst + "/services/oauth2/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        tok = json.loads(resp.read().decode())
    return tok["access_token"], tok.get("instance_url", inst)


def _verifier_query(soql: str, *, query_all: bool = False) -> list[dict[str, Any]]:
    """Run SOQL via an independent REST token. ``query_all`` includes deleted rows."""
    access, base = _verifier_token()
    endpoint = "queryAll" if query_all else "query"
    url = f"{base}/services/data/v62.0/{endpoint}/?q={urllib.parse.quote(soql)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access}"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return list(json.loads(resp.read().decode())["records"])


def _verifier_userinfo() -> dict[str, Any]:
    access, base = _verifier_token()
    req = urllib.request.Request(
        base + "/services/oauth2/userinfo",
        headers={"Authorization": f"Bearer {access}"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return dict(json.loads(resp.read().decode()))


# --- service-under-test fixture --------------------------------------


@pytest.fixture(scope="module")
def sf() -> SalesforceService:
    """One ``SalesforceService`` for the whole module so the token cache is reused."""
    return SalesforceService(_settings)


# --- tests ------------------------------------------------------------


async def test_token_grant_runas_is_system_admin(sf: SalesforceService) -> None:
    """Connected App + Run-As wiring proven end-to-end (acceptance #8).

    The independent verifier identifies the Run-As principal and SOQL
    confirms its Profile is ``System Administrator`` — the spec
    requirement for the integration user, not merely "creds are valid".
    """
    # Force the service to lazily authenticate at least once (also exercises
    # _fetch_client → simple_salesforce construction). A trivial SOQL call.
    rt_id: Any = await sf.get_user_custom_perms("000000000000000")  # nonexistent id → []
    assert rt_id == []

    info = _verifier_userinfo()
    assert info.get("preferred_username"), "userinfo missing preferred_username"

    safe_username = info["preferred_username"].replace("'", "\\'")
    [row] = _verifier_query(
        "SELECT Profile.Name, IsActive " f"FROM User WHERE Username='{safe_username}'"
    )
    assert row["IsActive"] is True
    assert row["Profile"]["Name"] == "System Administrator", (
        f"Connected App Run-As is {row['Profile']['Name']!r}, "
        "expected 'System Administrator' (SALESFORCE.md §2 — integration user)"
    )


async def test_case_round_trip_with_region_translation(sf: SalesforceService) -> None:
    """Create → update → delete a Case via the service; verify each step independently.

    Exercises the §10.1 region translation (``customer_region='west'`` →
    ``AFM_Customer_Region__c='West'``), the ``Fleet_Operations`` record-type
    resolution, every ``AFM_*__c`` field map, and that ``update_case`` /
    ``delete_case`` actually persist (never trust the 200).
    """
    external_id = f"CASE-INT-{uuid4().hex[:12]}"
    payload = CaseCreateInput(
        external_id=external_id,
        subject=f"AFM integration smoke (acceptance #8) — {external_id}",
        status="New",
        priority="Medium",
        flight_id="abc123",
        site_icao="KSFO",
        customer_region="west",
        case_type="lost_signal",
        detection_facts={
            "rule": "lost_signal",
            "generated_at": datetime.now(UTC).isoformat(),
        },
        severity_justification="Phase-04 acceptance #8; auto-deleted.",
        runbook_refs=["lost-signal-cruise", "diversion-divert"],
        internal_url=f"https://example.invalid/afm/cases/{external_id}",
    )

    ref = await sf.create_case(payload)
    try:
        # Independently verify the Case was really created with every field.
        [rec] = _verifier_query(
            "SELECT Id, AFM_External_Id__c, AFM_Flight_Id__c, AFM_Site_Icao__c, "
            "AFM_Customer_Region__c, AFM_Case_Type__c, AFM_Severity_Justification__c, "
            "AFM_Runbook_Refs__c, AFM_Internal_Url__c, "
            "Priority, Status, RecordType.Name "
            f"FROM Case WHERE Id='{ref.salesforce_id}'"
        )
        assert rec["AFM_External_Id__c"] == external_id
        assert rec["AFM_Flight_Id__c"] == "abc123"
        assert rec["AFM_Site_Icao__c"] == "KSFO"
        assert rec["AFM_Customer_Region__c"] == "West", "§10.1 region xlate (west→West)"
        assert rec["AFM_Case_Type__c"] == "lost_signal"
        assert rec["AFM_Runbook_Refs__c"] == "lost-signal-cruise,diversion-divert"
        assert rec["AFM_Internal_Url__c"].endswith(external_id)
        assert rec["Priority"] == "Medium"
        assert rec["Status"] == "New"
        assert rec["RecordType"]["Name"] == "Fleet Operations"

        # update_case persists.
        await sf.update_case(ref.salesforce_id, {"Priority": "High"})
        [after_update] = _verifier_query(
            f"SELECT Priority FROM Case WHERE Id='{ref.salesforce_id}'"
        )
        assert after_update["Priority"] == "High"
    finally:
        # delete_case persists (Case moves to the Recycle Bin → IsDeleted=true).
        await sf.delete_case(ref.salesforce_id)

    [after_delete] = _verifier_query(
        f"SELECT IsDeleted FROM Case WHERE Id='{ref.salesforce_id}'",
        query_all=True,
    )
    assert after_delete["IsDeleted"] is True


@pytest.mark.parametrize(
    ("username", "expected_perm"),
    [
        ("internal-ops@aerialfleet.demo", "AFM_All_Regions"),
        ("west-coast-ops@aerialfleet.demo", "AFM_Region_West"),
        ("east-coast-ops@aerialfleet.demo", "AFM_Region_East"),
    ],
)
async def test_get_user_custom_perms_resolves_demo_users(
    sf: SalesforceService, username: str, expected_perm: str
) -> None:
    """Each demo user's Permission Set bundles the matching ``AFM_Region_*`` custom perm.

    This is the read path the Half-B JWT scope claim will use
    (SALESFORCE.md §5.3 — "PS exists primarily to populate the AFM JWT
    scope claim"). It's wired and testable now even though Half-A
    doesn't enforce it on requests.
    """
    safe_username = username.replace("'", "\\'")
    [u] = _verifier_query(f"SELECT Id FROM User WHERE Username='{safe_username}' AND IsActive=true")
    perms = await sf.get_user_custom_perms(u["Id"])
    assert (
        expected_perm in perms
    ), f"{username}: expected {expected_perm} via Permission Set, got {perms}"
