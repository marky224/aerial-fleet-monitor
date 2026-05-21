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

import httpx
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

DEFAULT_API_BASE = "http://localhost:8000"
SYNC_PENDING_PATH = "/v1/cases/sync-pending"
PUSH_LIMIT = 50
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


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
