"""Tests for the daily_brief_* assets.

The HTTP call (``generate_brief``) is stubbed at the module seam so the assets'
metadata mapping + skip-on-unreachable behaviour are verified without a live API.
"""

from __future__ import annotations

from typing import Any

import httpx
from dagster import MaterializeResult, build_asset_context

from pipelines.assets import brief


def test_daily_brief_reports_summary(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_generate(_base: str, region: str) -> dict[str, Any]:
        captured["region"] = region
        return {
            "region": region,
            "brief_date": "2026-06-13",
            "status": "posted",
            "chatter_post_id": "0D5XYZ",
            "opened": 4,
            "resolved": 2,
        }

    monkeypatch.setattr(brief, "generate_brief", fake_generate)
    out = brief.daily_brief_west(build_asset_context())
    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["skipped"].value is False
    assert md["status"].value == "posted"
    assert md["opened"].value == 4
    assert md["resolved"].value == 2
    assert md["chatter_post_id"].value == "0D5XYZ"
    assert captured["region"] == "west"  # the factory bound the right region


def test_daily_brief_skips_when_api_unreachable(monkeypatch) -> None:
    def boom(_base: str, _region: str) -> dict[str, Any]:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(brief, "generate_brief", boom)
    out = brief.daily_brief_all(build_asset_context())
    md = out.metadata or {}
    assert md["skipped"].value is True
    assert "connection refused" in md["skip_reason"].value


def test_daily_brief_omits_post_id_when_not_posted(monkeypatch) -> None:
    # group-id unset → API returns chatter_post_id=None; metadata omits the key.
    monkeypatch.setattr(
        brief,
        "generate_brief",
        lambda _base, region: {
            "region": region,
            "brief_date": "2026-06-13",
            "status": "posted",
            "chatter_post_id": None,
            "opened": 0,
            "resolved": 0,
        },
    )
    out = brief.daily_brief_east(build_asset_context())
    md = out.metadata or {}
    assert "chatter_post_id" not in md
    assert md["opened"].value == 0
