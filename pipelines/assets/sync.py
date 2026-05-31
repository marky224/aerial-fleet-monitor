"""sf_case_push — the decoupled AFM→Salesforce Case push asset (Phase 05).

``case_detector`` writes local cases as ``sf_sync_status='pending'`` and
never touches Salesforce. This asset drives the push half by polling the
API's ``POST /v1/cases/sync-pending``, which owns the actual SF write +
``app.cases`` reconciliation (the SF field/region translation lives in
``SalesforceService``, reachable only through the API — the pipelines
venv has neither ``app`` nor ``simple_salesforce``).

The asset is fired by ``case_sync_retry_sensor`` (~60s). Each pass
re-scans whatever is still pending, so a transient SF failure is retried
on the next tick — the asset itself carries no retry state.

An unreachable API materializes as a *skip*, not a failure: the push is a
best-effort mirror and an API/SF outage must never fail the pipeline run
(mirrors the foundry-sync skip-on-unreachable contract).
"""

import os
from typing import Any

import httpx
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)

from pipelines.resources import PostgresResource

DEFAULT_API_BASE = "http://localhost:8000"
SYNC_PENDING_PATH = "/v1/cases/sync-pending"
SYNC_FROM_SF_PATH = "/v1/cases/sync-from-sf"
PUSH_LIMIT = 50
PULL_LIMIT = 200
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

# sf_push_not_failing health check — alarm when a meaningful sample of recent
# pushes is failing. This is the early warning the operational core lacked
# when it sat SILENTLY down for 3 days (2026-05-28→31): the DE org hit its
# storage cap, every Case create 4xx'd STORAGE_LIMIT_EXCEEDED → parked
# terminal 'failed', and the push's "re-scan retries" contract meant no run
# ever failed, so nothing surfaced it.
_HEALTH_WINDOW_MIN = 30
_HEALTH_MIN_SAMPLE = 20
_HEALTH_FAIL_RATE = 0.5


def _api_base() -> str:
    return os.environ.get("AFM_API_BASE", DEFAULT_API_BASE).rstrip("/")


def push_pending_cases(base_url: str, limit: int = PUSH_LIMIT) -> dict[str, int]:
    """POST the sync-pending trigger; return the CaseSyncSummary body.

    Raises ``httpx.HTTPError`` (transport or 4xx/5xx) — the asset turns
    that into a skip.
    """
    resp = httpx.post(
        f"{base_url}{SYNC_PENDING_PATH}",
        params={"limit": limit},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return dict(resp.json())


@asset(
    group_name="detection",
    description=(
        "Pushes pending app.cases rows to Salesforce via the API's "
        "/v1/cases/sync-pending endpoint. Skips (does not fail) when the "
        "API is unreachable; retries are inherent in the re-scan."
    ),
    metadata={"target": "salesforce", "cadence": "60s"},
)
def sf_case_push(context: AssetExecutionContext) -> MaterializeResult:
    base_url = _api_base()
    try:
        summary = push_pending_cases(base_url, PUSH_LIMIT)
    except httpx.HTTPError as exc:
        context.log.warning("sf_case_push: API unreachable (%s) — skipping", exc)
        return MaterializeResult(
            metadata={
                "skipped": MetadataValue.bool(True),
                "skip_reason": MetadataValue.text(str(exc)),
            }
        )
    context.log.info("sf_case_push: %s", summary)
    metadata: dict[str, MetadataValue] = {"skipped": MetadataValue.bool(False)}
    for key, value in summary.items():
        metadata[key] = MetadataValue.int(int(value))
    return MaterializeResult(metadata=metadata)


@asset_check(
    asset=sf_case_push,
    name="sf_push_not_failing",
    description=(
        "Recent SF pushes aren't failing wholesale — surfaces a silent push "
        "outage (e.g. the DE org's STORAGE_LIMIT_EXCEEDED) in the UI."
    ),
)
def sf_push_not_failing(postgres: PostgresResource) -> AssetCheckResult:
    """Counts recent terminal SF outcomes in ``app.cases``; a high failed
    fraction over a meaningful sample fails the check (ERROR) so a broken push
    is visible instead of rotting unnoticed. Only the recent window is
    considered (the historical ``failed`` backlog from a past outage doesn't
    keep it red once the push recovers), and a minimum sample prevents a quiet
    window from flapping.
    """
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) FILTER (WHERE sf_sync_status = 'failed') AS failed,
                count(*) FILTER (WHERE sf_sync_status = 'synced') AS synced
            FROM app.cases
            WHERE updated_at >= now() - (%s * interval '1 minute')
              AND sf_sync_status IN ('failed', 'synced')
            """,
            (_HEALTH_WINDOW_MIN,),
        )
        failed, synced = cur.fetchone()
    total = failed + synced
    rate = (failed / total) if total else 0.0
    unhealthy = total >= _HEALTH_MIN_SAMPLE and rate >= _HEALTH_FAIL_RATE
    note = " — SF push is failing; check org storage / SF_PUSH_SEVERITIES" if unhealthy else ""
    return AssetCheckResult(
        passed=not unhealthy,
        severity=AssetCheckSeverity.ERROR,
        description=f"{failed}/{total} recent SF pushes failed ({rate:.0%}, last {_HEALTH_WINDOW_MIN}m){note}",
        metadata={
            "failed": MetadataValue.int(failed),
            "synced": MetadataValue.int(synced),
            "fail_rate": MetadataValue.float(round(rate, 3)),
            "window_min": MetadataValue.int(_HEALTH_WINDOW_MIN),
        },
    )


def pull_cases_from_sf(base_url: str, limit: int = PULL_LIMIT) -> dict[str, Any]:
    """POST the sync-from-sf trigger; return the CasePullSummary body.

    Raises ``httpx.HTTPError`` (transport or 4xx/5xx) — the asset turns
    that into a skip.
    """
    resp = httpx.post(
        f"{base_url}{SYNC_FROM_SF_PATH}",
        params={"limit": limit},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return dict(resp.json())


@asset(
    group_name="sync",
    description=(
        "Mirrors Salesforce-modified Cases back into app.cases via the API's "
        "/v1/cases/sync-from-sf endpoint (watermark-driven SF→PG pull). Skips "
        "(does not fail) when the API is unreachable — the watermark is "
        "untouched so the next pass re-reads the same window."
    ),
    metadata={"source": "salesforce", "cadence": "60s"},
)
def sf_case_sync(context: AssetExecutionContext) -> MaterializeResult:
    base_url = _api_base()
    try:
        summary = pull_cases_from_sf(base_url, PULL_LIMIT)
    except httpx.HTTPError as exc:
        context.log.warning("sf_case_sync: API unreachable (%s) — skipping", exc)
        return MaterializeResult(
            metadata={
                "skipped": MetadataValue.bool(True),
                "skip_reason": MetadataValue.text(str(exc)),
            }
        )
    context.log.info("sf_case_sync: %s", summary)
    metadata: dict[str, MetadataValue] = {"skipped": MetadataValue.bool(False)}
    for key in ("fetched", "updated", "unmatched"):
        metadata[key] = MetadataValue.int(int(summary.get(key, 0) or 0))
    watermark = summary.get("watermark")
    if watermark:
        metadata["watermark"] = MetadataValue.text(str(watermark))
    return MaterializeResult(metadata=metadata)
