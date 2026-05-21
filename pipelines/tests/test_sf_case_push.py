"""Tests for the sf_case_push asset wrapper.

The HTTP call (``push_pending_cases``) is stubbed at the module seam so
the asset's metadata mapping + skip-on-unreachable behaviour are verified
without a live API.
"""

from __future__ import annotations

import httpx
from dagster import MaterializeResult, build_asset_context

from pipelines.assets import sync


def test_sf_case_push_reports_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        sync,
        "push_pending_cases",
        lambda _base, _limit: {"attempted": 3, "synced": 2, "retrying": 1, "failed": 0},
    )
    out = sync.sf_case_push(build_asset_context())
    assert isinstance(out, MaterializeResult)
    md = out.metadata or {}
    assert md["skipped"].value is False
    assert md["attempted"].value == 3
    assert md["synced"].value == 2
    assert md["retrying"].value == 1
    assert md["failed"].value == 0


def test_sf_case_push_skips_when_api_unreachable(monkeypatch) -> None:
    def boom(_base: str, _limit: int) -> dict[str, int]:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(sync, "push_pending_cases", boom)
    out = sync.sf_case_push(build_asset_context())
    md = out.metadata or {}
    assert md["skipped"].value is True
    assert "connection refused" in md["skip_reason"].value
