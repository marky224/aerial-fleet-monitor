"""Unit tests for the briefs router (Phase 11).

Direct coroutine call (no TestClient/DB — matches the suite's style). The
router's only job is to construct a BriefService with the configured Chatter
group and forward `region` + `force`; BriefService itself is faked so this test
stays a wiring check.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.models.briefs import BriefGenerateRequest, BriefGenerateSummary
from app.routers.briefs import generate_brief


async def test_generate_brief_forwards_region_force_and_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeBriefService:
        def __init__(self, postgres: Any, sf: Any, chatter_group_id: str | None = None) -> None:
            captured["chatter_group_id"] = chatter_group_id

        async def generate_and_post(
            self, region: str, *, force: bool = False
        ) -> BriefGenerateSummary:
            captured["region"] = region
            captured["force"] = force
            return BriefGenerateSummary(
                region=region,  # type: ignore[arg-type]
                brief_date=date(2026, 6, 13),
                status="posted",
                opened=1,
                resolved=0,
            )

    monkeypatch.setattr("app.routers.briefs.BriefService", FakeBriefService)
    monkeypatch.setattr(
        "app.routers.briefs.settings.salesforce_chatter_brief_group_id", "0F9CONFIG"
    )

    res = await generate_brief(
        BriefGenerateRequest(region="east"),
        postgres=MagicMock(),
        sf=MagicMock(),
        force=True,
    )

    assert res.region == "east"
    assert res.status == "posted"
    assert captured == {"chatter_group_id": "0F9CONFIG", "region": "east", "force": True}


def test_briefs_router_mounted() -> None:
    from app.main import app

    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/v1/briefs/generate" in paths
