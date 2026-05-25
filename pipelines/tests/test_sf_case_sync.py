"""Tests for the sf_case_sync asset wrapper (SF→Postgres pull).

The HTTP call (``pull_cases_from_sf``) is stubbed at the module seam so
the asset's metadata mapping + skip-on-unreachable behaviour are verified
without a live API.
"""

from __future__ import annotations

import httpx
from dagster import MaterializeResult, build_asset_context

from pipelines.assets import sync


def test_sf_case_sync_reports_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        sync,
        "pull_cases_from_sf",
        lambda _base, _limit: {
            "fetched": 5,
            "updated": 4,
            "unmatched": 1,
            "watermark": "2026-05-21T22:31:05+00:00",
        },
    )
    out = sync.sf_case_sync(build_asset_context())
    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["skipped"].value is False
    assert md["fetched"].value == 5
    assert md["updated"].value == 4
    assert md["unmatched"].value == 1
    assert md["watermark"].value == "2026-05-21T22:31:05+00:00"


def test_sf_case_sync_omits_watermark_when_null(monkeypatch) -> None:
    # Empty pull: watermark stays None — no watermark metadata key.
    monkeypatch.setattr(
        sync,
        "pull_cases_from_sf",
        lambda _base, _limit: {"fetched": 0, "updated": 0, "unmatched": 0, "watermark": None},
    )
    out = sync.sf_case_sync(build_asset_context())
    md = out.metadata or {}
    assert md["skipped"].value is False
    assert md["fetched"].value == 0
    assert "watermark" not in md


def test_sf_case_sync_skips_when_api_unreachable(monkeypatch) -> None:
    def boom(_base: str, _limit: int) -> dict[str, object]:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(sync, "pull_cases_from_sf", boom)
    out = sync.sf_case_sync(build_asset_context())
    md = out.metadata or {}
    assert md["skipped"].value is True
    assert "connection refused" in md["skip_reason"].value
