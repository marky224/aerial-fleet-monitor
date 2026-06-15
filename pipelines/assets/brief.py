"""daily_brief_{west,east,all} — the Phase-11 daily Chatter brief assets.

Once per region per local day a schedule fires the matching asset, which POSTs
``/v1/briefs/generate {region}`` to the AFM API. The API composes the
prior-local-day brief from ``app.cases``, synthesizes it via the Salesforce
Models API (the ``AFM_BriefSynthesis`` Apex action), posts it to the Fleet
Operations Chatter group, and persists ``app.briefs`` — the pipelines venv has
neither ``app`` nor ``simple_salesforce`` and reaches Salesforce only through the
API, exactly like ``sf_case_push``.

An unreachable / erroring API materializes as a SKIP, not a failure: the brief is
best-effort and an API/SF outage must never fail the run. The API is idempotent
per ``(region, brief_date)``, so a re-fire (manual or a double-tick) is a no-op
that reports ``already_posted``.
"""

import os
from typing import Any

import httpx
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

DEFAULT_API_BASE = "http://localhost:8000"
GENERATE_PATH = "/v1/briefs/generate"
# Generation includes a synchronous Models-API (LLM) call behind the endpoint,
# so allow well beyond the case-sync 30s; still bounded so a hung call skips.
_TIMEOUT = httpx.Timeout(120.0, connect=5.0)

# Regions that get a brief. Each has its own asset/job/schedule so the local-day
# boundary (execution_timezone) and per-region run history stay distinct.
BRIEF_REGIONS = ("west", "east", "all")


def _api_base() -> str:
    return os.environ.get("AFM_API_BASE", DEFAULT_API_BASE).rstrip("/")


def generate_brief(base_url: str, region: str) -> dict[str, Any]:
    """POST the brief-generate trigger; return the BriefGenerateSummary body.

    Raises ``httpx.HTTPError`` (transport or 4xx/5xx) — the asset turns that
    into a skip.
    """
    resp = httpx.post(
        f"{base_url}{GENERATE_PATH}",
        json={"region": region},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return dict(resp.json())


def _make_daily_brief_asset(region: str) -> AssetsDefinition:
    """Build the ``daily_brief_<region>`` asset (one per region, same body)."""

    @asset(
        name=f"daily_brief_{region}",
        group_name="briefs",
        description=(
            f"Generates the {region} daily fleet brief by POSTing "
            "/v1/briefs/generate (Models synthesis + Chatter post happen "
            "API/SF-side). Skips (does not fail) when the API is unreachable."
        ),
        metadata={
            "target": "salesforce",
            "region": region,
            "cadence": "daily 07:00 local",
        },
    )
    def _asset(context: AssetExecutionContext) -> MaterializeResult:
        base_url = _api_base()
        try:
            summary = generate_brief(base_url, region)
        except httpx.HTTPError as exc:
            context.log.warning("daily_brief_%s: API unreachable (%s) — skipping", region, exc)
            return MaterializeResult(
                metadata={
                    "skipped": MetadataValue.bool(True),
                    "skip_reason": MetadataValue.text(str(exc)),
                }
            )
        context.log.info("daily_brief_%s: %s", region, summary)
        metadata: dict[str, MetadataValue] = {
            "skipped": MetadataValue.bool(False),
            "status": MetadataValue.text(str(summary.get("status", ""))),
            "brief_date": MetadataValue.text(str(summary.get("brief_date", ""))),
            "opened": MetadataValue.int(int(summary.get("opened", 0) or 0)),
            "resolved": MetadataValue.int(int(summary.get("resolved", 0) or 0)),
        }
        post_id = summary.get("chatter_post_id")
        if post_id:
            metadata["chatter_post_id"] = MetadataValue.text(str(post_id))
        return MaterializeResult(metadata=metadata)

    return _asset


daily_brief_west = _make_daily_brief_asset("west")
daily_brief_east = _make_daily_brief_asset("east")
daily_brief_all = _make_daily_brief_asset("all")
