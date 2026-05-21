"""Tests for the dedup logic (batch + existing-case suppression)."""

from __future__ import annotations

from pipelines.rules.base import Anomaly
from pipelines.rules.dedup import deduplicate
from pipelines.rules.lost_signal import LostSignalRule
from pipelines.rules.weather_impact import WeatherImpactRule
from pipelines.tests.rule_helpers import NOW, cases_frame, empty_cases, mins

LOST = LostSignalRule()
WX = WeatherImpactRule()


def _lost(icao: str, site: str = "KDEN") -> Anomaly:
    return Anomaly(rule="lost_signal", icao24=icao, site_icao=site, customer_region="west")


def test_batch_dedup_same_key_kept_once() -> None:
    a, b = _lost("abc123"), _lost("abc123")
    kept = deduplicate([(a, LOST), (b, LOST)], empty_cases(), NOW)
    assert len(kept) == 1


def test_batch_distinct_keys_both_kept() -> None:
    kept = deduplicate([(_lost("abc123"), LOST), (_lost("def456"), LOST)], empty_cases(), NOW)
    assert len(kept) == 2


def test_suppressed_by_existing_case_within_window() -> None:
    existing = cases_frame(
        [
            {
                "case_type": "lost_signal",
                "flight_id": "abc123",
                "site_icao": "KDEN",
                "detection_facts": {},
                "created_at": NOW - mins(60),  # within the 6h window
            }
        ]
    )
    kept = deduplicate([(_lost("abc123"), LOST)], existing, NOW)
    assert kept == []


def test_allowed_when_existing_case_outside_window() -> None:
    existing = cases_frame(
        [
            {
                "case_type": "lost_signal",
                "flight_id": "abc123",
                "site_icao": "KDEN",
                "detection_facts": {},
                "created_at": NOW - mins(60 * 7),  # older than the 6h window
            }
        ]
    )
    kept = deduplicate([(_lost("abc123"), LOST)], existing, NOW)
    assert [a.icao24 for a in kept] == ["abc123"]


def test_different_icao_not_suppressed_by_existing() -> None:
    existing = cases_frame(
        [
            {
                "case_type": "lost_signal",
                "flight_id": "abc123",
                "site_icao": "KDEN",
                "detection_facts": {},
                "created_at": NOW - mins(30),
            }
        ]
    )
    kept = deduplicate([(_lost("zzz999"), LOST)], existing, NOW)
    assert [a.icao24 for a in kept] == ["zzz999"]


def test_weather_dedup_is_site_level_not_aircraft() -> None:
    # Two weather anomalies for the same site+category dedup to one even
    # though icao24 is empty for both.
    a = Anomaly(
        rule="weather_impact",
        icao24="",
        site_icao="KSFO",
        detection_facts={"flight_category": "IFR"},
    )
    b = Anomaly(
        rule="weather_impact",
        icao24="",
        site_icao="KSFO",
        detection_facts={"flight_category": "IFR"},
    )
    kept = deduplicate([(a, WX), (b, WX)], empty_cases(), NOW)
    assert len(kept) == 1


def test_weather_different_category_not_deduped() -> None:
    ifr = Anomaly(
        rule="weather_impact", site_icao="KSFO", detection_facts={"flight_category": "IFR"}
    )
    lifr = Anomaly(
        rule="weather_impact", site_icao="KSFO", detection_facts={"flight_category": "LIFR"}
    )
    kept = deduplicate([(ifr, WX), (lifr, WX)], empty_cases(), NOW)
    assert len(kept) == 2
