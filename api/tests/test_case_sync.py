"""Unit tests for CaseSyncService — the AFM→SF Case push path (Phase 05).

No DB, no network: the four Postgres helpers are stubbed on the instance
and SalesforceService is a fake whose ``create_case`` returns or raises
per scenario. Focus is the failure classification (build-doc §8) and the
Subject formatting.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from app.exceptions import BadRequest, UpstreamUnavailable
from app.models.salesforce import CaseCreateInput, CaseSyncRecord, SalesforceCaseRef
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


class _FakePG:
    """Minimal pool: only ``connection()`` is exercised, by the push lock."""

    @contextmanager
    def connection(self) -> Iterator[object]:
        yield object()


def _service(
    rows: list[dict[str, Any]], sf: _FakeSF, *, locked: bool = False
) -> tuple[CaseSyncService, dict[str, list]]:
    svc = CaseSyncService(postgres=_FakePG(), salesforce=sf)  # type: ignore[arg-type]
    marks: dict[str, list] = {"synced": [], "retry": [], "failed": []}
    svc._fetch_pending = lambda _limit: rows  # type: ignore[method-assign]
    svc._mark_synced = lambda cid, sfid, att: marks["synced"].append((cid, sfid, att))  # type: ignore[method-assign]
    svc._mark_retry = lambda cid, att, err: marks["retry"].append((cid, att, err))  # type: ignore[method-assign]
    svc._mark_failed = lambda cid, att, err: marks["failed"].append((cid, att, err))  # type: ignore[method-assign]
    # The advisory-lock single-flight is DB-level; stub it. ``locked=True``
    # simulates another push pass already holding the lock.
    svc._try_lock = lambda _conn: not locked  # type: ignore[method-assign]
    svc._unlock = lambda _conn: None  # type: ignore[method-assign]
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


async def test_push_skips_when_another_pass_holds_the_lock() -> None:
    # Single-flight: a concurrent push already holds the advisory lock, so
    # this pass must skip entirely — no fetch, no SF calls, no marks.
    sf = _FakeSF(lambda p: SalesforceCaseRef(salesforce_id="x", external_id=p.external_id))
    svc, marks = _service([_row()], sf, locked=True)

    summary = await svc.push_pending()

    assert (summary.attempted, summary.synced, summary.retrying, summary.failed) == (0, 0, 0, 0)
    assert sf.calls == []
    assert marks == {"synced": [], "retry": [], "failed": []}


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


# -- pull half: SF → Postgres (PIPELINES.md §3.5) -------------------------


class _FakePullSF:
    def __init__(self, records: list[CaseSyncRecord]) -> None:
        self._records = records
        self.calls: list[tuple[datetime, int]] = []

    async def query_cases_modified_since(
        self, watermark: datetime, limit: int = 200
    ) -> list[CaseSyncRecord]:
        self.calls.append((watermark, limit))
        return self._records


def _record(**over: Any) -> CaseSyncRecord:
    base: dict[str, Any] = {
        "salesforce_id": "500A",
        "external_id": "CASE-2026-000001",
        "status": "in_progress",
        "severity": "high",
        "summary": None,
        "severity_justification": None,
        "runbook_refs": [],
        "resolved_at": None,
        "system_modstamp": datetime(2026, 5, 21, 12, 0, 0, tzinfo=UTC),
    }
    base.update(over)
    return CaseSyncRecord(**base)


def _pull_service(
    records: list[CaseSyncRecord], *, matched: set[str] | None = None
) -> tuple[CaseSyncService, dict[str, list], _FakePullSF]:
    sf = _FakePullSF(records)
    svc = CaseSyncService(postgres=object(), salesforce=sf)  # type: ignore[arg-type]
    calls: dict[str, list] = {"write_watermark": []}
    # Default watermark; _apply_record matches on external_id membership.
    matched = matched if matched is not None else {r.external_id for r in records}
    svc._read_watermark = lambda: datetime(2026, 5, 21, 11, 0, 0, tzinfo=UTC)  # type: ignore[method-assign]
    svc._write_watermark = lambda ts: calls["write_watermark"].append(ts)  # type: ignore[method-assign]
    svc._apply_record = lambda rec: rec.external_id in matched  # type: ignore[method-assign]
    return svc, calls, sf


async def test_pull_applies_records_and_advances_watermark() -> None:
    newer = datetime(2026, 5, 21, 12, 30, 0, tzinfo=UTC)
    records = [
        _record(external_id="CASE-A", salesforce_id="500A"),
        _record(
            external_id="CASE-B", salesforce_id="500B", system_modstamp=newer, severity="medium"
        ),
    ]
    svc, calls, sf = _pull_service(records)

    summary = await svc.pull_from_sf(limit=200)

    assert (summary.fetched, summary.updated, summary.unmatched) == (2, 2, 0)
    assert summary.watermark == newer  # max SystemModstamp observed
    assert calls["write_watermark"] == [newer]
    assert sf.calls == [(datetime(2026, 5, 21, 11, 0, 0, tzinfo=UTC), 200)]


async def test_pull_counts_unmatched_without_failing() -> None:
    records = [
        _record(external_id="CASE-A"),
        _record(external_id="CASE-GHOST", salesforce_id="500X"),
    ]
    # Only CASE-A has a local row.
    svc, calls, _ = _pull_service(records, matched={"CASE-A"})

    summary = await svc.pull_from_sf()

    assert (summary.updated, summary.unmatched) == (1, 1)
    # Watermark still advances over the whole batch (both were observed).
    assert len(calls["write_watermark"]) == 1


async def test_pull_empty_leaves_watermark_untouched() -> None:
    svc, calls, sf = _pull_service([])

    summary = await svc.pull_from_sf()

    assert (summary.fetched, summary.updated, summary.unmatched) == (0, 0, 0)
    assert summary.watermark is None
    assert calls["write_watermark"] == []  # zero rows → preserve prior watermark


def test_material_changes_emits_only_real_diffs() -> None:
    rec = _record(status="resolved", severity="high", resolved_at=datetime(2026, 5, 21, tzinfo=UTC))
    events = CaseSyncService._material_changes(rec, "open", "low", None)
    kinds = [e[0] for e in events]
    assert kinds == ["status_changed", "severity_changed", "resolved"]


def test_material_changes_noop_when_unchanged() -> None:
    rec = _record(status="open", severity="low", resolved_at=None)
    assert CaseSyncService._material_changes(rec, "open", "low", None) == []


def test_material_changes_severity_none_is_not_a_change() -> None:
    # SF returned no/unknown Priority → severity None must not log a spurious diff.
    rec = _record(status="open", severity=None, resolved_at=None)
    assert CaseSyncService._material_changes(rec, "open", "high", None) == []
