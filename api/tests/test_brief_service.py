"""Unit tests for BriefService — the Phase-11 daily-brief compose+post path.

No DB, no network: the Postgres reads/writes are stubbed (or mock_postgres
drives `_collect_facts`'s aggregation) and SalesforceService is a fake whose
`synthesize_and_post_brief` returns a canned result. Focus: the prior-local-day
window math, fact aggregation + region scoping, the plain-text prompt, and the
idempotent / force regeneration branches.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from app.models.briefs import BriefSynthesisResult
from app.services.brief_service import (
    BriefService,
    _compose_prompt,
    _format_counts,
)
from app.services.postgres import PostgresPool

# -- window math ----------------------------------------------------------


def test_window_for_prior_local_day_pdt() -> None:
    # 2026-06-14 15:00Z == 08:00 PDT (UTC-7); prior local day is 2026-06-13,
    # spanning local midnight→midnight == 07:00Z→07:00Z.
    bd, ws, we = BriefService._window_for(
        datetime(2026, 6, 14, 15, 0, tzinfo=UTC), "America/Los_Angeles"
    )
    assert bd == date(2026, 6, 13)
    assert ws == datetime(2026, 6, 13, 7, 0, tzinfo=UTC)
    assert we == datetime(2026, 6, 14, 7, 0, tzinfo=UTC)


def test_window_for_uses_region_local_date_not_utc() -> None:
    # 2026-06-14 03:00Z is still 2026-06-13 in New York (UTC-4 in June), so the
    # prior *local* day is 2026-06-12 — not 2026-06-13 (which a naive UTC
    # "yesterday" would give).
    bd, _ws, _we = BriefService._window_for(
        datetime(2026, 6, 14, 3, 0, tzinfo=UTC), "America/New_York"
    )
    assert bd == date(2026, 6, 12)


# -- count formatting + prompt -------------------------------------------


def test_format_counts_orders_and_handles_empty() -> None:
    assert _format_counts({}) == "none"
    # explicit order pins leading keys; missing ones are dropped
    assert (
        _format_counts({"high": 1, "low": 4}, order=["critical", "high", "medium", "low"])
        == "high: 1, low: 4"
    )
    # no order → descending count then name
    assert _format_counts({"a": 1, "b": 3}) == "b: 3, a: 1"


def _facts(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "opened": 2,
        "resolved": 1,
        "open_by_severity": {"high": 1, "low": 4},
        "opened_by_type": {"lost_signal": 2},
        "notable_labels": ["Lost signal during cruise — SWA1 near KSFO [high]"],
        "notable_case_ids": ["CASE-2026-000001"],
    }
    base.update(over)
    return base


def test_compose_prompt_is_plain_text_and_carries_facts() -> None:
    prompt = _compose_prompt("west", date(2026, 6, 13), _facts())
    assert "West region" in prompt
    assert "2026-06-13" in prompt
    assert "Cases opened: 2" in prompt
    assert "Cases resolved: 1" in prompt
    assert "high: 1, low: 4" in prompt
    assert "Lost signal during cruise — SWA1 near KSFO [high]" in prompt
    # Plain text only — no markdown emphasis/headers leaking in.
    assert "**" not in prompt and "#" not in prompt


def test_compose_prompt_quiet_day_says_none() -> None:
    prompt = _compose_prompt(
        "all",
        date(2026, 6, 13),
        _facts(opened=0, resolved=0, open_by_severity={}, opened_by_type={}, notable_labels=[]),
    )
    assert "entire fleet (all regions)" in prompt
    assert "Currently open by severity: none" in prompt
    assert "Opened by anomaly type: none" in prompt
    assert "  - none" in prompt


# -- fact collection + region scoping ------------------------------------


def _collect(region: str) -> tuple[dict[str, Any], MagicMock]:
    pg = MagicMock(spec=PostgresPool)
    pg.fetchall.side_effect = [
        # opened (severity, case_type, n)
        [
            {"severity": "high", "case_type": "lost_signal", "n": 2},
            {"severity": "low", "case_type": "go_around", "n": 1},
        ],
        # currently open (severity, n)
        [{"severity": "high", "n": 1}, {"severity": "low", "n": 4}],
        # notable
        [
            {
                "case_id": "CASE-2026-000001",
                "case_type": "lost_signal",
                "severity": "high",
                "site_icao": "KSFO",
                "detection_facts": {"callsign": "SWA1"},
            }
        ],
    ]
    pg.fetchone.return_value = {"n": 1}  # resolved count
    svc = BriefService(pg, MagicMock(), chatter_group_id="0F9G")
    facts = svc._collect_facts(
        region,  # type: ignore[arg-type]
        datetime(2026, 6, 13, 7, 0, tzinfo=UTC),
        datetime(2026, 6, 14, 7, 0, tzinfo=UTC),
    )
    return facts, pg


def test_collect_facts_aggregates() -> None:
    facts, _pg = _collect("west")
    assert facts["opened"] == 3
    assert facts["resolved"] == 1
    assert facts["opened_by_type"] == {"lost_signal": 2, "go_around": 1}
    assert facts["open_by_severity"] == {"high": 1, "low": 4}
    assert facts["notable_case_ids"] == ["CASE-2026-000001"]
    assert facts["notable_labels"] == ["Lost signal during cruise — SWA1 near KSFO [high]"]


def test_collect_facts_region_scoped_for_west() -> None:
    _facts_out, pg = _collect("west")
    # every windowed query carries the region filter + param
    opened_sql, opened_params = pg.fetchall.call_args_list[0].args
    assert "customer_region = %(region)s" in opened_sql
    assert opened_params["region"] == "west"


def test_collect_facts_all_region_has_no_filter() -> None:
    _facts_out, pg = _collect("all")
    opened_sql, opened_params = pg.fetchall.call_args_list[0].args
    assert "customer_region" not in opened_sql
    assert "region" not in opened_params


# -- generate_and_post flow ----------------------------------------------


def _fake_sf(feed_id: str | None = "0D5XYZ") -> MagicMock:
    sf = MagicMock()
    sf.synthesize_and_post_brief = AsyncMock(
        return_value=BriefSynthesisResult(generated_text="- all quiet", feed_element_id=feed_id)
    )
    return sf


def _flow_service(sf: MagicMock, existing: dict[str, Any] | None) -> tuple[BriefService, dict]:
    svc = BriefService(MagicMock(spec=PostgresPool), sf, chatter_group_id="0F9GROUP")
    cap: dict[str, Any] = {}
    svc._fetch_existing = lambda region, brief_date: existing  # type: ignore[method-assign]
    svc._collect_facts = lambda region, ws, we: _facts()  # type: ignore[method-assign]

    def fake_upsert(region, brief_date, tz_name, summary_md, key_metrics, notable, post_id):  # type: ignore[no-untyped-def]
        cap.update(
            region=region,
            brief_date=brief_date,
            tz_name=tz_name,
            summary_md=summary_md,
            key_metrics=key_metrics,
            notable=notable,
            post_id=post_id,
        )
        return 99

    svc._upsert_brief = fake_upsert  # type: ignore[method-assign]
    return svc, cap


async def test_generate_and_post_synthesizes_persists_and_returns_summary() -> None:
    sf = _fake_sf()
    svc, cap = _flow_service(sf, existing=None)

    summary = await svc.generate_and_post("west", as_of=datetime(2026, 6, 14, 15, 0, tzinfo=UTC))

    assert summary.status == "posted"
    assert summary.region == "west"
    assert summary.brief_date == date(2026, 6, 13)
    assert summary.chatter_post_id == "0D5XYZ"
    assert (summary.opened, summary.resolved) == (2, 1)
    assert summary.brief_id == 99
    # SF called with the group id + a plain-text prompt + a dated subject
    sf.synthesize_and_post_brief.assert_awaited_once()
    kw = sf.synthesize_and_post_brief.await_args.kwargs
    assert kw["chatter_group_id"] == "0F9GROUP"
    assert "Cases opened: 2" in kw["prompt"]
    assert kw["subject"] == "AFM Daily Fleet Brief — West — 2026-06-13"
    # persisted row carries the generated text + facts + tz + feed id
    assert cap["summary_md"] == "- all quiet"
    assert cap["tz_name"] == "America/Los_Angeles"
    assert cap["post_id"] == "0D5XYZ"
    assert cap["key_metrics"]["opened"] == 2
    assert cap["notable"] == ["CASE-2026-000001"]


async def test_generate_and_post_idempotent_when_brief_exists() -> None:
    sf = _fake_sf()
    existing = {
        "brief_id": 7,
        "chatter_post_id": "0D5OLD",
        "key_metrics": {"opened": 5, "resolved": 2},
    }
    svc, _cap = _flow_service(sf, existing=existing)

    summary = await svc.generate_and_post("west", as_of=datetime(2026, 6, 14, 15, 0, tzinfo=UTC))

    assert summary.status == "already_posted"
    assert summary.brief_id == 7
    assert summary.chatter_post_id == "0D5OLD"
    assert (summary.opened, summary.resolved) == (5, 2)
    sf.synthesize_and_post_brief.assert_not_awaited()  # no second LLM call / Chatter post


async def test_generate_and_post_force_regenerates_existing() -> None:
    sf = _fake_sf()
    existing = {"brief_id": 7, "chatter_post_id": "0D5OLD", "key_metrics": {"opened": 5}}
    svc, cap = _flow_service(sf, existing=existing)

    summary = await svc.generate_and_post(
        "west", as_of=datetime(2026, 6, 14, 15, 0, tzinfo=UTC), force=True
    )

    assert summary.status == "posted"
    sf.synthesize_and_post_brief.assert_awaited_once()
    assert cap["post_id"] == "0D5XYZ"
