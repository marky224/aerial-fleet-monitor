"""CaseSyncService — the AFM→Salesforce Case push path (Phase 05).

The detector (``case_detector``) writes local cases as
``sf_sync_status='pending'`` and never touches Salesforce, so an SF
outage can't block detection. This service is the decoupled push half:
it scans pending rows, creates the Case in Salesforce via
``SalesforceService``, and reconciles ``app.cases`` + ``app.case_timeline``.

It is invoked through ``POST /v1/cases/sync-pending`` (the pipelines
``sf_case_push`` asset polls that endpoint). Running it again is the
retry mechanism — each pass re-scans whatever is still ``pending``.

Failure classification is the contract that matters (build-doc §8):

* transient (``UpstreamUnavailable`` → 503: token/network/SF 5xx) leaves
  the case ``pending`` so the next pass retries — until ``MAX_ATTEMPTS``,
  past which it is parked ``failed`` to stop an unbounded retry loop;
* permanent (``BadRequest`` 400 / ``ConflictError`` 409 / any other
  ``AFMException``) parks the case ``failed`` immediately.

Salesforce field/region translation lives only in ``SalesforceService``
(SALESFORCE.md §10.1); this service builds the API-side ``CaseCreateInput``
and never names an ``AFM_*__c`` field.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, cast

from psycopg2.extras import Json

from app.exceptions import AFMException, UpstreamUnavailable
from app.logging import get_logger
from app.models.salesforce import (
    CaseCreateInput,
    CasePullSummary,
    CaseSyncRecord,
    CaseSyncSummary,
)
from app.services.postgres import PostgresPool
from app.services.salesforce import SalesforceService

log = get_logger(__name__)

# Past this many failed pushes a still-pending case is parked `failed` so a
# permanently-broken row can't retry forever. Generous enough to ride out a
# multi-minute SF outage at the ~60s push cadence.
MAX_ATTEMPTS = 5

# app.sync_watermarks key for the SF→Postgres pull (PIPELINES.md §3.5).
SF_CASE_SYNC_WATERMARK = "sf_case_sync"

# Postgres advisory-lock key for the push single-flight. The retry sensor
# fires ~every 60s; once the pending backlog makes a pass run longer than
# that, a second pass would overlap and double-submit the same rows (the
# unique AFM_External_Id__c then bounces the loser as DUPLICATE_VALUE). A
# session-level advisory lock makes overlapping passes a no-op skip. (An
# advisory lock rather than FOR UPDATE row locks: a pass holds across many
# slow SF round-trips, and a row-lock transaction open that long is an
# idle-in-transaction anti-pattern.)
_PUSH_LOCK_KEY = 0x4146_4D43  # "AFMC"

# AFM severity (cases.severity) → Salesforce standard Priority picklist.
# Priority is where AFM severity rides (models/salesforce.py CaseCreateInput).
_SEVERITY_TO_PRIORITY: dict[str, str] = {
    "low": "Low",
    "medium": "Medium",
    "high": "High",
    "critical": "High",
}

_PENDING_COLUMNS = (
    "case_id, flight_id, site_icao, customer_region, case_type, "
    "severity, detection_facts, runbook_refs, sf_sync_attempts"
)


def _format_subject(case_type: str, site_icao: str, facts: dict[str, Any]) -> str:
    """Human-readable Case subject (build-doc §"Subject formatting").

    Pulls callsign/route fields from ``detection_facts`` with safe
    fallbacks — facts are rule-authored and some keys may be absent.
    """
    callsign = facts.get("callsign") or "unknown"
    site = site_icao or "unknown site"
    if case_type == "lost_signal":
        return f"Lost signal during cruise — {callsign} near {site}"
    if case_type == "diversion":
        origin = facts.get("origin") or "?"
        alternate = facts.get("alternate") or "?"
        was = facts.get("expected_destination") or "?"
        return f"Diversion — {callsign} {origin}→{alternate} (was {was})"
    if case_type == "excessive_hold":
        return f"Excessive holding pattern — {callsign} near {site}"
    if case_type == "weather_impact":
        category = facts.get("flight_category") or "unknown"
        return f"Weather impact — {site} ({category})"
    if case_type == "go_around":
        return f"Go-around — {callsign} at {site}"
    if case_type == "delay":
        origin = facts.get("origin") or "?"
        destination = facts.get("destination") or "?"
        return f"Delayed flight — {callsign} {origin}→{destination}"
    return f"{case_type} — {callsign} near {site}"


class CaseSyncService:
    """Pushes ``pending`` ``app.cases`` rows into Salesforce."""

    def __init__(self, postgres: PostgresPool, salesforce: SalesforceService) -> None:
        self._pg = postgres
        self._sf = salesforce

    async def push_pending(self, limit: int = 50) -> CaseSyncSummary:
        # Single-flight: hold one connection's session advisory lock for the
        # whole pass so a concurrent push (overlapping sensor tick) skips
        # rather than double-submitting the same pending rows.
        with self._pg.connection() as lock_conn:
            if not await asyncio.to_thread(self._try_lock, lock_conn):
                log.info("case_sync.push_pending.skipped_locked")
                return CaseSyncSummary(attempted=0, synced=0, retrying=0, failed=0)
            try:
                return await self._push_pending_locked(limit)
            finally:
                await asyncio.to_thread(self._unlock, lock_conn)

    @staticmethod
    def _try_lock(conn: Any) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (_PUSH_LOCK_KEY,))
            acquired = bool(cur.fetchone()[0])
        conn.commit()  # release the implicit txn; session lock persists
        return acquired

    @staticmethod
    def _unlock(conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (_PUSH_LOCK_KEY,))
        conn.commit()

    async def _push_pending_locked(self, limit: int) -> CaseSyncSummary:
        rows = await asyncio.to_thread(self._fetch_pending, limit)
        synced = retrying = failed = 0
        for row in rows:
            attempts = int(row["sf_sync_attempts"]) + 1
            try:
                ref = await self._sf.create_case(self._to_payload(row))
            except UpstreamUnavailable as exc:
                if attempts >= MAX_ATTEMPTS:
                    await asyncio.to_thread(
                        self._mark_failed, row["case_id"], attempts, f"max attempts: {exc.message}"
                    )
                    failed += 1
                else:
                    await asyncio.to_thread(self._mark_retry, row["case_id"], attempts, exc.message)
                    retrying += 1
            except AFMException as exc:
                await asyncio.to_thread(self._mark_failed, row["case_id"], attempts, exc.message)
                failed += 1
            else:
                await asyncio.to_thread(
                    self._mark_synced, row["case_id"], ref.salesforce_id, attempts
                )
                synced += 1

        summary = CaseSyncSummary(
            attempted=len(rows), synced=synced, retrying=retrying, failed=failed
        )
        log.info("case_sync.push_pending", **summary.model_dump())
        return summary

    # -- payload ----------------------------------------------------------

    def _to_payload(self, row: dict[str, Any]) -> CaseCreateInput:
        facts: dict[str, Any] = row["detection_facts"] or {}
        return CaseCreateInput(
            external_id=row["case_id"],
            subject=_format_subject(row["case_type"], row["site_icao"], facts),
            status="New",
            priority=_SEVERITY_TO_PRIORITY.get(row["severity"], "Medium"),
            flight_id=row["flight_id"],
            site_icao=row["site_icao"] or None,
            customer_region=row["customer_region"],
            case_type=row["case_type"],
            detection_facts=facts,
            runbook_refs=list(row["runbook_refs"] or []),
        )

    # -- Postgres I/O -----------------------------------------------------

    def _fetch_pending(self, limit: int) -> list[dict[str, Any]]:
        return self._pg.fetchall(
            f"""
            SELECT {_PENDING_COLUMNS}
            FROM app.cases
            WHERE sf_sync_status = 'pending'
            ORDER BY created_at ASC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )

    def _mark_synced(self, case_id: str, salesforce_id: str, attempts: int) -> None:
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app.cases
                SET salesforce_id = %s, sf_sync_status = 'synced',
                    sf_sync_attempts = %s, sf_sync_last_error = NULL, updated_at = NOW()
                WHERE case_id = %s
                """,
                (salesforce_id, attempts, case_id),
            )
            cur.execute(
                """
                INSERT INTO app.case_timeline (case_id, event_type, detail, source)
                VALUES (%s, 'sf_synced', %s, 'sf_sync')
                """,
                (case_id, Json({"salesforce_id": salesforce_id})),
            )
            conn.commit()

    def _mark_retry(self, case_id: str, attempts: int, error: str) -> None:
        # Stays 'pending' — the next push pass retries it.
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app.cases
                SET sf_sync_attempts = %s, sf_sync_last_error = %s, updated_at = NOW()
                WHERE case_id = %s
                """,
                (attempts, error, case_id),
            )
            cur.execute(
                """
                INSERT INTO app.case_timeline (case_id, event_type, detail, source)
                VALUES (%s, 'sf_sync_retry', %s, 'sf_sync')
                """,
                (case_id, Json({"attempts": attempts, "error": error})),
            )
            conn.commit()

    def _mark_failed(self, case_id: str, attempts: int, error: str) -> None:
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app.cases
                SET sf_sync_status = 'failed', sf_sync_attempts = %s,
                    sf_sync_last_error = %s, updated_at = NOW()
                WHERE case_id = %s
                """,
                (attempts, error, case_id),
            )
            cur.execute(
                """
                INSERT INTO app.case_timeline (case_id, event_type, detail, source)
                VALUES (%s, 'sf_sync_failed', %s, 'sf_sync')
                """,
                (case_id, Json({"attempts": attempts, "error": error})),
            )
            conn.commit()

    # == pull half: Salesforce → Postgres (PIPELINES.md §3.5) =============

    async def pull_from_sf(self, limit: int = 200) -> CasePullSummary:
        """Mirror Cases modified in Salesforce back into app.cases.

        Reads the `sf_case_sync` watermark, fetches Cases with
        SystemModstamp > watermark, applies each to its local row (writing
        timeline events for material changes), then advances the watermark
        to the max SystemModstamp seen. Zero rows → watermark untouched so
        the next cycle re-attempts the same window (spec step 5)."""
        watermark = await asyncio.to_thread(self._read_watermark)
        records = await self._sf.query_cases_modified_since(watermark, limit)

        updated = unmatched = 0
        max_modstamp: datetime | None = None
        for rec in records:
            max_modstamp = (
                rec.system_modstamp
                if max_modstamp is None
                else max(max_modstamp, rec.system_modstamp)
            )
            if await asyncio.to_thread(self._apply_record, rec):
                updated += 1
            else:
                unmatched += 1

        new_watermark: datetime | None = None
        if max_modstamp is not None:
            await asyncio.to_thread(self._write_watermark, max_modstamp)
            new_watermark = max_modstamp

        summary = CasePullSummary(
            fetched=len(records), updated=updated, unmatched=unmatched, watermark=new_watermark
        )
        log.info(
            "case_sync.pull_from_sf",
            fetched=summary.fetched,
            updated=summary.updated,
            unmatched=summary.unmatched,
            watermark=new_watermark.isoformat() if new_watermark else None,
        )
        return summary

    def _read_watermark(self) -> datetime:
        row = self._pg.fetchone(
            "SELECT last_sync_at FROM app.sync_watermarks WHERE sync_name = %(name)s",
            {"name": SF_CASE_SYNC_WATERMARK},
        )
        if row is None:
            return SalesforceService.default_pull_watermark()
        return cast(datetime, row["last_sync_at"])

    def _write_watermark(self, modstamp: datetime) -> None:
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.sync_watermarks (sync_name, last_sync_at, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (sync_name)
                DO UPDATE SET last_sync_at = EXCLUDED.last_sync_at, updated_at = NOW()
                """,
                (SF_CASE_SYNC_WATERMARK, modstamp),
            )
            conn.commit()

    def _apply_record(self, rec: CaseSyncRecord) -> bool:
        """Apply one SF Case to its local row. Returns False if unmatched.

        SELECT-then-UPDATE in one transaction so timeline events reflect the
        true pre-update state. Matches by salesforce_id, falling back to
        external_id==case_id (which also backfills a stranded salesforce_id —
        the duplicate-recovery edge). summary/severity/justification use
        COALESCE so a null from SF never blanks a locally-set value; status
        and resolved_at are authoritative from SF (a reopen clears ClosedDate)."""
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT case_id, status, severity, resolved_at
                FROM app.cases
                WHERE salesforce_id = %s OR case_id = %s
                """,
                (rec.salesforce_id, rec.external_id),
            )
            current = cur.fetchone()
            if current is None:
                return False
            case_id, prev_status, prev_severity, prev_resolved_at = current

            cur.execute(
                """
                UPDATE app.cases
                SET status = %s,
                    severity = COALESCE(%s, severity),
                    summary = COALESCE(%s, summary),
                    severity_justification = COALESCE(%s, severity_justification),
                    runbook_refs = COALESCE(%s, runbook_refs),
                    resolved_at = %s,
                    salesforce_id = COALESCE(salesforce_id, %s),
                    updated_at = NOW()
                WHERE case_id = %s
                """,
                (
                    rec.status,
                    rec.severity,
                    rec.summary,
                    rec.severity_justification,
                    rec.runbook_refs or None,
                    rec.resolved_at,
                    rec.salesforce_id,
                    case_id,
                ),
            )

            for event_type, detail in self._material_changes(
                rec, prev_status, prev_severity, prev_resolved_at
            ):
                cur.execute(
                    """
                    INSERT INTO app.case_timeline (case_id, event_type, detail, source)
                    VALUES (%s, %s, %s, 'sf_sync')
                    """,
                    (case_id, event_type, Json(detail)),
                )
            conn.commit()
        return True

    @staticmethod
    def _material_changes(
        rec: CaseSyncRecord,
        prev_status: str,
        prev_severity: str,
        prev_resolved_at: datetime | None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """Timeline events for the changes spec step 4 calls material."""
        events: list[tuple[str, dict[str, Any]]] = []
        if rec.status != prev_status:
            events.append(("status_changed", {"from": prev_status, "to": rec.status}))
        if rec.severity is not None and rec.severity != prev_severity:
            events.append(("severity_changed", {"from": prev_severity, "to": rec.severity}))
        if rec.resolved_at is not None and prev_resolved_at is None:
            events.append(("resolved", {"resolved_at": rec.resolved_at.isoformat()}))
        return events
