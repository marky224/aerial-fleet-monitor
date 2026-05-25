"""weather_impact — a watched airport is below VFR minimums.

Site-level rule: fires once per watched site reporting IFR or LIFR
flight category. Not about a specific aircraft, so ``icao24`` is empty
(the detector substitutes a site sentinel for cases.flight_id) and the
case is region 'all' (visible to every scope). Dedup is keyed on
(site, flight_category) so a site that stays IFR doesn't re-fire every
cycle within the hour window.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd

from pipelines.rules.base import AirportConditions, Anomaly, Rule
from pipelines.services.baseline_provider import BaselineProvider

IMPACT_CATEGORIES = {"IFR", "LIFR"}


class WeatherImpactRule(Rule):
    case_type = "weather_impact"
    dedup_window = timedelta(hours=1)

    def detect(
        self,
        positions: pd.DataFrame,
        weather: dict[str, AirportConditions],
        existing_cases: pd.DataFrame,
        baseline: BaselineProvider,
    ) -> list[Anomaly]:
        anomalies: list[Anomaly] = []
        for site_icao, cond in weather.items():
            if cond.flight_category not in IMPACT_CATEGORIES:
                continue
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24="",  # site-level — no specific aircraft
                    site_icao=site_icao,
                    customer_region="all",
                    detection_facts={
                        "flight_category": cond.flight_category,
                        "ceiling_ft": cond.ceiling_ft,
                        "visibility_sm": cond.visibility_sm,
                        "wind_kt": cond.wind_kt,
                    },
                    severity_hint="high" if cond.flight_category == "LIFR" else "medium",
                )
            )
        return anomalies

    def dedup_key(
        self,
        *,
        icao24: str,
        site_icao: str | None,
        detection_facts: dict[str, Any],
    ) -> tuple[Any, ...]:
        # Site-level: identity is the site + category, not the aircraft.
        return (self.case_type, site_icao, detection_facts.get("flight_category"))
