"""BriefService — compose a daily ops brief from app.cases and post it (Phase 11).

The daily brief is the Phase-11 net-new feature: once per region per local day a
Dagster schedule calls ``POST /v1/briefs/generate``, which lands here. The
service reads ``app.cases`` for the region's PRIOR LOCAL day, composes a compact
fact set (opened / resolved / open-by-severity / by-type + a few notable cases),
asks the Salesforce Models API to synthesize a plain-text brief and post it to a
Chatter group (both via the ``AFM_BriefSynthesis`` Apex action, behind
``SalesforceService``), then upserts ``app.briefs`` idempotently on
``(region, brief_date)``.

``app.briefs`` predates this feature (migration 0001) — we reuse it rather than
add a table. ``all`` is a fleet-wide brief (no region filter), distinct from the
``customer_region='all'`` row partition. Salesforce Models / Chatter specifics
live only in ``SalesforceService`` / Apex; this service never names a model, an
SObject, or a ConnectApi type — it deals in facts and plain text.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from psycopg2.extras import Json

from app.logging import get_logger
from app.models.briefs import BriefGenerateSummary
from app.models.common import Region
from app.services.case_sync import _format_subject  # canonical Case subject formatter
from app.services.postgres import PostgresPool
from app.services.salesforce import SalesforceService

log = get_logger(__name__)

# Each region's brief covers that region's local calendar day and fires at 7am
# local (the Dagster schedule's execution_timezone matches these). `all` is
# fleet-wide and uses a central US zone so its "yesterday" is a sensible
# national boundary.
REGION_TIMEZONE: dict[str, str] = {
    "west": "America/Los_Angeles",
    "east": "America/New_York",
    "all": "America/Chicago",
}

_REGION_LABEL: dict[str, str] = {
    "west": "West region",
    "east": "East region",
    "all": "entire fleet (all regions)",
}

# Severity display order (there is no numeric severity column).
_SEVERITY_ORDER = ["critical", "high", "medium", "low"]

_NOTABLE_LIMIT = 5


class BriefService:
    """Compose + synthesize + post one region's daily brief; persist app.briefs."""

    def __init__(
        self,
        postgres: PostgresPool,
        salesforce: SalesforceService,
        chatter_group_id: str | None = None,
    ) -> None:
        self._pg = postgres
        self._sf = salesforce
        self._chatter_group_id = chatter_group_id

    async def generate_and_post(
        self, region: Region, *, as_of: datetime | None = None, force: bool = False
    ) -> BriefGenerateSummary:
        """Build, synthesize, post, and persist the brief for ``region``.

        ``as_of`` (default: now, UTC) anchors the "prior local day" window —
        tests pass a fixed instant for determinism. ``force`` regenerates even if
        a brief already exists for ``(region, brief_date)``; otherwise an existing
        row is an idempotent no-op (one brief per region per local day, so a
        manual re-trigger or a double-fired schedule doesn't double-post).
        """
        now = as_of or datetime.now(tz=UTC)
        tz_name = REGION_TIMEZONE[region]
        brief_date, window_start, window_end = self._window_for(now, tz_name)

        existing = await asyncio.to_thread(self._fetch_existing, region, brief_date)
        if existing is not None and not force:
            km = existing.get("key_metrics") or {}
            log.info(
                "brief.generate.skipped_existing",
                region=region,
                brief_date=brief_date.isoformat(),
            )
            return BriefGenerateSummary(
                region=region,
                brief_date=brief_date,
                status="already_posted",
                chatter_post_id=existing.get("chatter_post_id"),
                opened=int(km.get("opened", 0)),
                resolved=int(km.get("resolved", 0)),
                brief_id=existing.get("brief_id"),
            )

        facts = await asyncio.to_thread(self._collect_facts, region, window_start, window_end)
        prompt = _compose_prompt(region, brief_date, facts)
        subject = f"AFM Daily Fleet Brief — {region.title()} — {brief_date.isoformat()}"

        result = await self._sf.synthesize_and_post_brief(
            prompt=prompt, chatter_group_id=self._chatter_group_id, subject=subject
        )

        key_metrics: dict[str, Any] = {
            "opened": facts["opened"],
            "resolved": facts["resolved"],
            "open_by_severity": facts["open_by_severity"],
            "opened_by_type": facts["opened_by_type"],
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }
        brief_id = await asyncio.to_thread(
            self._upsert_brief,
            region,
            brief_date,
            tz_name,
            result.generated_text,
            key_metrics,
            facts["notable_case_ids"],
            result.feed_element_id,
        )
        log.info(
            "brief.generate",
            region=region,
            brief_date=brief_date.isoformat(),
            opened=facts["opened"],
            resolved=facts["resolved"],
            posted=bool(result.feed_element_id),
        )
        return BriefGenerateSummary(
            region=region,
            brief_date=brief_date,
            status="posted",
            chatter_post_id=result.feed_element_id,
            opened=facts["opened"],
            resolved=facts["resolved"],
            brief_id=brief_id,
        )

    # -- window -----------------------------------------------------------

    @staticmethod
    def _window_for(now: datetime, tz_name: str) -> tuple[date, datetime, datetime]:
        """Return ``(brief_date, window_start_utc, window_end_utc)`` for the prior local day."""
        tz = ZoneInfo(tz_name)
        local_now = now.astimezone(tz)
        brief_date = (local_now - timedelta(days=1)).date()
        start_local = datetime(brief_date.year, brief_date.month, brief_date.day, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        return brief_date, start_local.astimezone(UTC), end_local.astimezone(UTC)

    # -- region scoping ---------------------------------------------------

    @staticmethod
    def _region_filter(region: Region) -> tuple[str, dict[str, Any]]:
        """SQL fragment + params scoping a query to ``region`` (``all`` = no filter)."""
        if region == "all":
            return "", {}
        return " AND customer_region = %(region)s", {"region": region}

    # -- facts ------------------------------------------------------------

    def _collect_facts(
        self, region: Region, window_start: datetime, window_end: datetime
    ) -> dict[str, Any]:
        rfilter, rparams = self._region_filter(region)
        window = {"start": window_start, "end": window_end, **rparams}

        opened_rows = self._pg.fetchall(
            f"""
            SELECT severity, case_type, count(*) AS n
            FROM app.cases
            WHERE created_at >= %(start)s AND created_at < %(end)s{rfilter}
            GROUP BY severity, case_type
            """,
            window,
        )
        opened = sum(int(r["n"]) for r in opened_rows)
        opened_by_type: dict[str, int] = {}
        for r in opened_rows:
            opened_by_type[r["case_type"]] = opened_by_type.get(r["case_type"], 0) + int(r["n"])

        resolved_row = self._pg.fetchone(
            f"""
            SELECT count(*) AS n
            FROM app.cases
            WHERE resolved_at >= %(start)s AND resolved_at < %(end)s{rfilter}
            """,
            window,
        )
        resolved = int(resolved_row["n"]) if resolved_row else 0

        open_rows = self._pg.fetchall(
            f"""
            SELECT severity, count(*) AS n
            FROM app.cases
            WHERE status NOT IN ('resolved', 'closed'){rfilter}
            GROUP BY severity
            """,
            rparams,
        )
        open_by_severity = {r["severity"]: int(r["n"]) for r in open_rows}

        notable = self._pg.fetchall(
            f"""
            SELECT case_id, case_type, severity, site_icao, detection_facts
            FROM app.cases
            WHERE created_at >= %(start)s AND created_at < %(end)s{rfilter}
            ORDER BY
                CASE severity WHEN 'critical' THEN 4 WHEN 'high' THEN 3
                              WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC,
                created_at DESC
            LIMIT %(limit)s
            """,
            {**window, "limit": _NOTABLE_LIMIT},
        )
        notable_labels = [
            f"{_format_subject(r['case_type'], r['site_icao'], r['detection_facts'] or {})} "
            f"[{r['severity']}]"
            for r in notable
        ]
        notable_case_ids = [r["case_id"] for r in notable]

        return {
            "opened": opened,
            "resolved": resolved,
            "open_by_severity": open_by_severity,
            "opened_by_type": opened_by_type,
            "notable_labels": notable_labels,
            "notable_case_ids": notable_case_ids,
        }

    # -- persistence ------------------------------------------------------

    def _fetch_existing(self, region: Region, brief_date: date) -> dict[str, Any] | None:
        return self._pg.fetchone(
            "SELECT brief_id, chatter_post_id, key_metrics FROM app.briefs "
            "WHERE region = %(region)s AND brief_date = %(brief_date)s",
            {"region": region, "brief_date": brief_date},
        )

    def _upsert_brief(
        self,
        region: Region,
        brief_date: date,
        tz_name: str,
        summary_md: str,
        key_metrics: dict[str, Any],
        notable_case_ids: list[str],
        chatter_post_id: str | None,
    ) -> int:
        with self._pg.connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.briefs
                    (region, brief_date, timezone, summary_md, key_metrics,
                     notable_cases, chatter_post_id, generated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (region, brief_date) DO UPDATE SET
                    timezone = EXCLUDED.timezone,
                    summary_md = EXCLUDED.summary_md,
                    key_metrics = EXCLUDED.key_metrics,
                    notable_cases = EXCLUDED.notable_cases,
                    chatter_post_id = EXCLUDED.chatter_post_id,
                    generated_at = NOW()
                RETURNING brief_id
                """,
                (
                    region,
                    brief_date,
                    tz_name,
                    summary_md,
                    Json(key_metrics),
                    notable_case_ids,
                    chatter_post_id,
                ),
            )
            brief_id = int(cur.fetchone()[0])
            conn.commit()
        return brief_id


def _format_counts(counts: dict[str, int], order: list[str] | None = None) -> str:
    """Readable ``'high: 3, medium: 1'`` string; ``'none'`` when empty.

    ``order`` pins leading keys (e.g. severity); any extras follow, and with no
    ``order`` keys sort by descending count then name (stable for tests).
    """
    if not counts:
        return "none"
    if order:
        keys = [k for k in order if k in counts]
        keys += [k for k in counts if k not in order]
    else:
        keys = sorted(counts, key=lambda k: (-counts[k], k))
    return ", ".join(f"{k}: {counts[k]}" for k in keys)


def _compose_prompt(region: Region, brief_date: date, facts: dict[str, Any]) -> str:
    """Build the plain-text-bullet synthesis prompt (Chatter renders plain text)."""
    open_by_sev = _format_counts(facts["open_by_severity"], order=_SEVERITY_ORDER)
    by_type = _format_counts(facts["opened_by_type"])
    notable = facts["notable_labels"]
    notable_block = "\n".join(f"  - {label}" for label in notable) if notable else "  - none"
    return (
        "You are the Fleet Operations analyst for Aerial Fleet Monitor (AFM), a "
        "service that monitors a live fleet of aircraft for in-flight anomalies.\n"
        f"Write a concise daily operations brief for the {_REGION_LABEL[region]} "
        f"covering {brief_date.isoformat()}.\n\n"
        "Rules:\n"
        "- Plain text only. No markdown, no asterisks, no bold, no headings.\n"
        '- Use simple hyphen ("- ") bullets, one short sentence each.\n'
        "- 4 to 6 bullets total.\n"
        "- Base it ONLY on the facts below; do not invent numbers or events.\n"
        "- If the day was quiet, say so plainly in one or two bullets.\n\n"
        "Facts:\n"
        f"- Cases opened: {facts['opened']}\n"
        f"- Cases resolved: {facts['resolved']}\n"
        f"- Currently open by severity: {open_by_sev}\n"
        f"- Opened by anomaly type: {by_type}\n"
        "- Notable cases:\n"
        f"{notable_block}\n\n"
        "Write the brief now."
    )
