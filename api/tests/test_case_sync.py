"""Unit tests for CaseSyncService — the AFM→SF Case push path (Phase 05).

No DB, no network: the four Postgres helpers are stubbed on the instance
and SalesforceService is a fake whose ``create_case`` returns or raises
per scenario. Focus is the failure classification (build-doc §8) and the
Subject formatting.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from app.exceptions import BadRequest, UpstreamUnavailable
from app.models.salesforce import CaseCreateInput, SalesforceCaseRef
from app.services.case_sync import MAX_ATTEMPTS, CaseSyncService, _format_subject


class _FakeSF:
    def __init__(self, behavior: Callable[[CaseCreateInput], SalesforceCaseRef]) -> None:
        self._behavior = behavior
        self.calls: list[CaseCreateInput] = []

    async def create_case(self, payload: CaseCreateInput) -> SalesforceCaseRef:
        self.calls.append(payload)
        return self._behavior(payload)


def _row(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "case_id": "CASE-2026-000001",
        "flight_id": "abc123",
        "site_icao": "KSFO",
        "customer_region": "west",
        "case_type": "lost_signal",
        "severity": "high",
        "detection_facts": {"callsign": "SWA1"},
        "runbook_refs": ["lost-signal-cruise"],
        "sf_sync_attempts": 0,
    }
    base.update(over)
    return base


def _service(rows: list[dict[str, Any]], sf: _FakeSF) -> tuple[CaseSyncService, dict[str, list]]:
    svc = CaseSyncService(postgres=object(), salesforce=sf)  # type: ignore[arg-type]
    marks: dict[str, list] = {"synced": [], "retry": [], "failed": []}
    svc._fetch_pending = lambda _limit: rows  # type: ignore[method-assign]
    svc._mark_synced = lambda cid, sfid, att: marks["synced"].append((cid, sfid, att))  # type: ignore[method-assign]
    svc._mark_retry = lambda cid, att, err: marks["retry"].append((cid, att, err))  # type: ignore[method-assign]
    svc._mark_failed = lambda cid, att, err: marks["failed"].append((cid, att, err))  # type: ignore[method-assign]
    return svc, marks


# -- classification -------------------------------------------------------


async def test_success_marks_synced_with_salesforce_id() -> None:
    sf = _FakeSF(lambda p: SalesforceCaseRef(salesforce_id="500X", external_id=p.external_id))
    svc, marks = _service([_row()], sf)

    summary = await svc.push_pending()

    assert (summary.attempted, summary.synced, summary.retrying, summary.failed) == (1, 1, 0, 0)
    assert marks["synced"] == [("CASE-2026-000001", "500X", 1)]


async def test_transient_failure_stays_pending_not_failed() -> None:
    # Acceptance #8: a 503 from SF must leave the case pending (retryable).
    def boom(_p: CaseCreateInput) -> SalesforceCaseRef:
        raise UpstreamUnavailable("SF 503")

    sf = _FakeSF(boom)
    svc, marks = _service([_row(sf_sync_attempts=0)], sf)

    summary = await svc.push_pending()

    assert (summary.synced, summary.retrying, summary.failed) == (0, 1, 0)
    assert marks["failed"] == []
    assert marks["retry"] == [("CASE-2026-000001", 1, "SF 503")]


async def test_transient_failure_at_max_attempts_is_parked_failed() -> None:
    def boom(_p: CaseCreateInput) -> SalesforceCaseRef:
        raise UpstreamUnavailable("SF 503")

    sf = _FakeSF(boom)
    # One more attempt reaches MAX_ATTEMPTS → give up rather than loop forever.
    svc, marks = _service([_row(sf_sync_attempts=MAX_ATTEMPTS - 1)], sf)

    summary = await svc.push_pending()

    assert (summary.retrying, summary.failed) == (0, 1)
    assert marks["retry"] == []
    assert marks["failed"][0][0] == "CASE-2026-000001"
    assert marks["failed"][0][1] == MAX_ATTEMPTS


async def test_permanent_failure_marks_failed_immediately() -> None:
    def boom(_p: CaseCreateInput) -> SalesforceCaseRef:
        raise BadRequest("Unknown customer_region")

    sf = _FakeSF(boom)
    svc, marks = _service([_row(sf_sync_attempts=0)], sf)

    summary = await svc.push_pending()

    assert (summary.retrying, summary.failed) == (0, 1)
    assert marks["retry"] == []
    assert marks["failed"] == [("CASE-2026-000001", 1, "Unknown customer_region")]


async def test_empty_pending_is_a_noop() -> None:
    sf = _FakeSF(lambda p: SalesforceCaseRef(salesforce_id="x", external_id=p.external_id))
    svc, marks = _service([], sf)

    summary = await svc.push_pending()

    assert (summary.attempted, summary.synced, summary.retrying, summary.failed) == (0, 0, 0, 0)
    assert sf.calls == []


async def test_payload_maps_severity_to_priority_and_region() -> None:
    captured: list[CaseCreateInput] = []

    def capture(p: CaseCreateInput) -> SalesforceCaseRef:
        captured.append(p)
        return SalesforceCaseRef(salesforce_id="500X", external_id=p.external_id)

    sf = _FakeSF(capture)
    svc, _ = _service([_row(severity="medium", customer_region="east")], sf)

    await svc.push_pending()

    payload = captured[0]
    assert payload.priority == "Medium"
    assert payload.customer_region == "east"
    assert payload.external_id == "CASE-2026-000001"
    assert payload.status == "New"


# -- subject formatting ---------------------------------------------------


def test_subject_lost_signal() -> None:
    assert _format_subject("lost_signal", "KSFO", {"callsign": "SWA1"}) == (
        "Lost signal during cruise — SWA1 near KSFO"
    )


def test_subject_diversion_uses_route_facts() -> None:
    facts = {
        "callsign": "DAL9",
        "origin": "KJFK",
        "alternate": "KBOS",
        "expected_destination": "KLGA",
    }
    assert _format_subject("diversion", None, facts) == ("Diversion — DAL9 KJFK→KBOS (was KLGA)")


def test_subject_weather_impact_uses_category() -> None:
    assert _format_subject("weather_impact", "KSEA", {"flight_category": "LIFR"}) == (
        "Weather impact — KSEA (LIFR)"
    )


@pytest.mark.parametrize("case_type", ["excessive_hold", "go_around", "delay"])
def test_subject_falls_back_on_missing_callsign(case_type: str) -> None:
    subject = _format_subject(case_type, "KSFO", {})
    assert "unknown" in subject
