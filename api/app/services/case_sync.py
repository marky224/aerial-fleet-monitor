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
from typing import Any

from psycopg2.extras import Json

from app.exceptions import AFMException, UpstreamUnavailable
from app.logging import get_logger
from app.models.salesforce import CaseCreateInput, CaseSyncSummary
from app.services.postgres import PostgresPool
from app.services.salesforce import SalesforceService

log = get_logger(__name__)

# Past this many failed pushes a still-pending case is parked `failed` so a
# permanently-broken row can't retry forever. Generous enough to ride out a
# multi-minute SF outage at the ~60s push cadence.
MAX_ATTEMPTS = 5

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
