"""Customer-facing `GET /v1/cases` + `GET /v1/cases/{case_id}` (Phase 05 #4).

Tests target the QueryService methods directly, mocking PostgresPool —
matches the rest of the suite (test_query_service.py / test_case_sync.py
style: no FastAPI TestClient, no live DB). The scope-isolation tests
exercise the only real security boundary in this slice: that a narrow
scope cannot see another region's rows, and that the WHERE clause sent
to Postgres reflects that.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.exceptions import NotFoundError, ScopeViolation
from app.models.common import Scope
from app.services import query_service as qs
from app.services.query_service import (
    CASE_LIST_CEILING,
    DEFAULT_CASE_STATUSES,
    QueryService,
)

# --- helpers --------------------------------------------------------------


@pytest.fixture
def east_scope() -> Scope:
    """Narrow east-region scope — mirror of west_scope from conftest."""
    return Scope(
        user_handle="east-coast-ops",
        region="east",
        custom_perms=["AFM_Region_East"],
        sites_in_scope=["KJFK", "KATL"],
    )


def _case_row(
    case_id: str = "CASE-2026-000001",
    customer_region: str = "west",
    severity: str = "high",
    status: str = "open",
    site_icao: str = "KSFO",
    created_at: datetime | None = None,
) -> dict:  # type: ignore[type-arg]
    """app.cases projection used by list_cases."""
    now = created_at or datetime.now(UTC)
    return {
        "case_id": case_id,
        "salesforce_id": "500X" if status == "open" else None,
        "case_type": "lost_signal",
        "status": status,
        "severity": severity,
        "customer_region": customer_region,
        "site_icao": site_icao,
        "flight_id": "abc123",
        "summary": None,
        "created_at": now,
        "updated_at": now,
    }


def _detail_row(
    case_id: str = "CASE-2026-000001",
    customer_region: str = "west",
) -> dict:  # type: ignore[type-arg]
    """app.cases projection used by get_case (adds the detail columns)."""
    row = _case_row(case_id=case_id, customer_region=customer_region)
    row.update(
        {
            "severity_justification": "single-source fix",
            "detection_facts": {"callsign": "SWA1"},
            "runbook_refs": ["lost-signal-cruise"],
            "resolved_at": None,
        }
    )
    return row


# === list_cases — scope filter ===========================================


def test_list_cases_internal_scope_sees_all_regions(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """region='all' caller: WHERE has no customer_region filter."""
    mock_postgres.fetchall.return_value = [_case_row(customer_region="east")]

    result = query_service.list_cases(scope=internal_scope)

    assert result.count == 1
    sql, params = mock_postgres.fetchall.call_args[0]
    # `customer_region` legitimately appears in the SELECT list; assert on
    # the WHERE-side filter binding instead.
    assert "customer_region = ANY" not in sql
    assert "regions" not in params


def test_list_cases_narrow_scope_filters_to_own_region_plus_all(
    query_service: QueryService, mock_postgres: MagicMock, east_scope: Scope
) -> None:
    """An east-scoped caller gets `customer_region IN ('east', 'all')`."""
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=east_scope)

    sql, params = mock_postgres.fetchall.call_args[0]
    assert "customer_region = ANY(%(regions)s)" in sql
    assert params["regions"] == ["east", "all"]


def test_list_cases_west_scope_same_shape(
    query_service: QueryService, mock_postgres: MagicMock, west_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=west_scope)

    _, params = mock_postgres.fetchall.call_args[0]
    assert params["regions"] == ["west", "all"]


def test_list_cases_region_override_accepted_for_internal_scope(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope, region="east")

    _, params = mock_postgres.fetchall.call_args[0]
    assert params["regions"] == ["east", "all"]


def test_list_cases_region_override_rejected_for_narrow_scope(
    query_service: QueryService, east_scope: Scope
) -> None:
    with pytest.raises(ScopeViolation, match="cannot request region"):
        query_service.list_cases(scope=east_scope, region="west")


# === list_cases — status default + filters ===============================


def test_list_cases_default_status_omits_resolved(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """No explicit status → DEFAULT_CASE_STATUSES (no 'resolved')."""
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope)

    _, params = mock_postgres.fetchall.call_args[0]
    assert params["statuses"] == list(DEFAULT_CASE_STATUSES)
    assert "resolved" not in params["statuses"]


def test_list_cases_explicit_status_can_include_resolved(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope, status=["resolved"])

    _, params = mock_postgres.fetchall.call_args[0]
    assert params["statuses"] == ["resolved"]


def test_list_cases_severity_filter(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope, severity="critical")

    sql, params = mock_postgres.fetchall.call_args[0]
    assert "severity = %(severity)s" in sql
    assert params["severity"] == "critical"


def test_list_cases_site_filter_uppercased(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """`site` is uppercased server-side so a lowercase query still matches."""
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope, site="ksfo")

    sql, params = mock_postgres.fetchall.call_args[0]
    assert "site_icao = %(site)s" in sql
    assert params["site"] == "KSFO"


def test_list_cases_orders_by_created_at_desc_then_case_id(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = []

    query_service.list_cases(scope=internal_scope)

    sql, _ = mock_postgres.fetchall.call_args[0]
    assert "ORDER BY created_at DESC, case_id DESC" in sql


# === list_cases — truncated flag =========================================


def test_list_cases_not_truncated_by_default(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchall.return_value = [_case_row()]

    result = query_service.list_cases(scope=internal_scope)

    assert result.truncated is False
    _, params = mock_postgres.fetchall.call_args[0]
    assert params["ceiling_probe"] == CASE_LIST_CEILING + 1


def test_list_cases_truncated_when_ceiling_exceeded(
    query_service: QueryService,
    mock_postgres: MagicMock,
    internal_scope: Scope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CEILING+1 rows → clipped to CEILING, oldest dropped, truncated=True."""
    monkeypatch.setattr(qs, "CASE_LIST_CEILING", 3)
    now = datetime.now(UTC)
    # ORDER BY created_at DESC → caller gets newest first; the dropped tail
    # is the oldest.
    mock_postgres.fetchall.return_value = [
        _case_row(case_id=f"CASE-{i}", created_at=now - timedelta(seconds=i)) for i in range(4)
    ]

    result = query_service.list_cases(scope=internal_scope)

    assert result.truncated is True
    assert result.count == 3
    assert [c.case_id for c in result.items] == ["CASE-0", "CASE-1", "CASE-2"]


# === get_case ============================================================


def test_get_case_happy_path(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    """One detail fetch + one timeline fetch → CaseDetail with ordered events."""
    case = _detail_row()
    event_a = {
        "event_type": "created",
        "detail": {"by": "detector"},
        "source": "detector",
        "actor": None,
        "occurred_at": datetime(2026, 5, 25, 10, 0, tzinfo=UTC),
    }
    event_b = {
        "event_type": "sf_synced",
        "detail": {"salesforce_id": "500X"},
        "source": "sf_sync",
        "actor": None,
        "occurred_at": datetime(2026, 5, 25, 10, 5, tzinfo=UTC),
    }
    mock_postgres.fetchone.return_value = case
    mock_postgres.fetchall.return_value = [event_a, event_b]

    result = query_service.get_case(scope=internal_scope, case_id=case["case_id"])

    assert result.case_id == case["case_id"]
    assert result.detection_facts == {"callsign": "SWA1"}
    assert result.runbook_refs == ["lost-signal-cruise"]
    assert [e.event_type for e in result.timeline] == ["created", "sf_synced"]

    timeline_sql, timeline_params = mock_postgres.fetchall.call_args[0]
    assert "ORDER BY occurred_at ASC, event_id ASC" in timeline_sql
    assert timeline_params == {"case_id": case["case_id"]}


def test_get_case_not_found(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchone.return_value = None

    with pytest.raises(NotFoundError, match="not found"):
        query_service.get_case(scope=internal_scope, case_id="CASE-2026-999999")


def test_get_case_scope_violation_when_region_mismatch(
    query_service: QueryService, mock_postgres: MagicMock, east_scope: Scope
) -> None:
    """east-scoped caller fetching a west-tagged case → 403."""
    mock_postgres.fetchone.return_value = _detail_row(case_id="CASE-WEST", customer_region="west")

    with pytest.raises(ScopeViolation, match="region 'west'"):
        query_service.get_case(scope=east_scope, case_id="CASE-WEST")


def test_get_case_all_region_visible_to_narrow_scope(
    query_service: QueryService, mock_postgres: MagicMock, east_scope: Scope
) -> None:
    """A case tagged customer_region='all' surfaces to any region scope."""
    mock_postgres.fetchone.return_value = _detail_row(case_id="CASE-ALL", customer_region="all")
    mock_postgres.fetchall.return_value = []  # empty timeline

    result = query_service.get_case(scope=east_scope, case_id="CASE-ALL")

    assert result.case_id == "CASE-ALL"
    assert result.customer_region == "all"


def test_get_case_internal_scope_sees_any_region(
    query_service: QueryService, mock_postgres: MagicMock, internal_scope: Scope
) -> None:
    mock_postgres.fetchone.return_value = _detail_row(customer_region="east")
    mock_postgres.fetchall.return_value = []

    # No raise.
    result = query_service.get_case(scope=internal_scope, case_id="CASE-EAST")
    assert result.customer_region == "east"


# === salesforce_url integration =========================================
#
# Per-row Lightning deeplink composition (relies on
# `app.services._lightning.case_lightning_url`; the helper itself has
# direct unit tests in test_lightning.py — these confirm the URL flows
# through `list_cases` + `get_case` end-to-end).


def _query_service_with_sf(
    mock_postgres: MagicMock, mock_lakehouse: MagicMock, instance_url: str | None
) -> QueryService:
    """QueryService variant with `salesforce_instance_url` configured."""
    return QueryService(
        postgres=mock_postgres,
        lakehouse=mock_lakehouse,
        salesforce_instance_url=instance_url,
    )


def test_list_cases_populates_salesforce_url_when_configured(
    mock_postgres: MagicMock, mock_lakehouse: MagicMock, internal_scope: Scope
) -> None:
    """Synced row + instance URL configured → Lightning deeplink in response."""
    row = _case_row(case_id="CASE-A", customer_region="east")
    row["salesforce_id"] = "500X000000ABCDE"
    mock_postgres.fetchall.return_value = [row]

    svc = _query_service_with_sf(
        mock_postgres, mock_lakehouse, "https://orgfarm-12345.my.salesforce.com"
    )
    result = svc.list_cases(scope=internal_scope)

    assert result.items[0].salesforce_url == (
        "https://orgfarm-12345.my.salesforce.com/lightning/r/Case/500X000000ABCDE/view"
    )


def test_list_cases_salesforce_url_null_when_pending_push(
    mock_postgres: MagicMock, mock_lakehouse: MagicMock, internal_scope: Scope
) -> None:
    """Row without salesforce_id (pre-push) → salesforce_url is None."""
    row = _case_row(case_id="CASE-PEND")
    row["salesforce_id"] = None
    mock_postgres.fetchall.return_value = [row]

    svc = _query_service_with_sf(
        mock_postgres, mock_lakehouse, "https://orgfarm-12345.my.salesforce.com"
    )
    result = svc.list_cases(scope=internal_scope)

    assert result.items[0].salesforce_url is None


def test_get_case_populates_salesforce_url_when_configured(
    mock_postgres: MagicMock, mock_lakehouse: MagicMock, internal_scope: Scope
) -> None:
    row = _detail_row(case_id="CASE-DETAIL", customer_region="east")
    row["salesforce_id"] = "500X000000XYZAB"
    mock_postgres.fetchone.return_value = row
    mock_postgres.fetchall.return_value = []

    svc = _query_service_with_sf(
        mock_postgres, mock_lakehouse, "https://orgfarm-12345.my.salesforce.com"
    )
    result = svc.get_case(scope=internal_scope, case_id="CASE-DETAIL")

    assert result.salesforce_url == (
        "https://orgfarm-12345.my.salesforce.com/lightning/r/Case/500X000000XYZAB/view"
    )
