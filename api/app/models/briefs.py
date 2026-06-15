"""Daily-brief models (Phase 11).

The brief feature reads ``app.cases`` for a region's prior local day, asks the
Salesforce Models API — via the ``AFM_BriefSynthesis`` Apex action — to
synthesize a plain-text operations brief, posts it to a Chatter group, and
records the result in ``app.briefs``. These are the API-side shapes; the SF
field / ConnectApi details live in Apex and never leak here (SALESFORCE.md).

Region is the shared ``west|east|all`` literal — ``all`` means a fleet-wide
brief (no region filter), not the ``customer_region='all'`` partition.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

from app.models.common import Region


class BriefGenerateRequest(BaseModel):
    """Body of ``POST /v1/briefs/generate`` — which region's brief to build."""

    region: Region = Field(description="west | east | all (all = fleet-wide, no region filter).")


class BriefSynthesisResult(BaseModel):
    """Result of the ``AFM_BriefSynthesis`` Apex action (Models text + Chatter post).

    Produced by ``SalesforceService.synthesize_and_post_brief`` — the API-side
    mirror of the Apex ``BriefResult``. ``feed_element_id`` is ``None`` when no
    Chatter group was supplied (the post is skipped Apex-side).
    """

    generated_text: str = Field(description="The synthesized brief body (plain text).")
    feed_element_id: str | None = Field(
        default=None, description="Chatter FeedItem id; None when nothing was posted."
    )


class BriefGenerateSummary(BaseModel):
    """Outcome of a ``POST /v1/briefs/generate`` run."""

    region: Region
    brief_date: date = Field(description="The prior local day the brief covers.")
    status: Literal["posted", "already_posted"] = Field(
        description="posted = generated + persisted this run; already_posted = idempotent no-op."
    )
    chatter_post_id: str | None = Field(
        default=None, description="Chatter FeedItem id, when the brief was posted to a group."
    )
    opened: int = Field(default=0, description="Cases opened in the window.")
    resolved: int = Field(default=0, description="Cases resolved in the window.")
    brief_id: int | None = Field(
        default=None, description="app.briefs.brief_id of the upserted row."
    )
