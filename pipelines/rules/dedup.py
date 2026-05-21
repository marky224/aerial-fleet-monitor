"""Deduplication — suppress repeat anomalies within each rule's window.

A newly-detected anomaly is dropped if an equivalent one was already
created (in ``app.cases``) within the rule's ``dedup_window``, or if an
earlier anomaly in the *same batch* already carries the identity key.
Identity comes from ``Rule.dedup_key`` — applied identically to new
anomalies and existing-case rows so the comparison is symmetric.

The build-doc dedup table includes time "buckets" in the key; we instead
rely on the window comparison (``created_at >= now - dedup_window``) for
the time dimension and key purely on entity identity. Equivalent for
suppression, simpler to reason about.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from pipelines.rules.base import Anomaly, Rule


def deduplicate(
    anomalies: list[tuple[Anomaly, Rule]],
    existing_cases: pd.DataFrame,
    now: datetime,
) -> list[Anomaly]:
    """Return the anomalies that survive batch + existing-case dedup."""
    kept: list[Anomaly] = []
    seen: set[tuple[Any, ...]] = set()
    for anomaly, rule in anomalies:
        key = rule.dedup_key(
            icao24=anomaly.icao24,
            site_icao=anomaly.site_icao,
            detection_facts=anomaly.detection_facts,
        )
        if key in seen:
            continue
        if _matches_existing(key, rule, existing_cases, now):
            continue
        seen.add(key)
        kept.append(anomaly)
    return kept


def _matches_existing(
    key: tuple[Any, ...],
    rule: Rule,
    existing_cases: pd.DataFrame,
    now: datetime,
) -> bool:
    if existing_cases.empty:
        return False
    window_start = now - rule.dedup_window
    recent = existing_cases[
        (existing_cases["case_type"] == rule.case_type)
        & (existing_cases["created_at"] >= window_start)
    ]
    for _, row in recent.iterrows():
        facts = row.get("detection_facts")
        existing_key = rule.dedup_key(
            icao24=str(row.get("flight_id") or ""),
            site_icao=row.get("site_icao"),
            detection_facts=facts if isinstance(facts, dict) else {},
        )
        if existing_key == key:
            return True
    return False
