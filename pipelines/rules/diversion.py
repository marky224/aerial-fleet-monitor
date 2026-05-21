"""diversion — an aircraft landed somewhere other than its destination.

Fires when an icao24 with a known flight-plan destination is on the
ground within 5 nm of a *watched* airport that isn't that destination.
Limitation (Phase 05): nearest-site is computed against watched airports
only, so a diversion to an unwatched field isn't caught — recorded in
the Decisions log.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd

from pipelines.rules.base import (
    AirportConditions,
    Anomaly,
    Rule,
    latest_row,
    opt_float,
    opt_str,
    region_of,
)
from pipelines.services.baseline_provider import BaselineProvider

DIVERSION_RADIUS_NM = 5.0


class DiversionRule(Rule):
    case_type = "diversion"
    dedup_window = timedelta(hours=24)

    def detect(
        self,
        positions: pd.DataFrame,
        weather: dict[str, AirportConditions],
        existing_cases: pd.DataFrame,
        baseline: BaselineProvider,
    ) -> list[Anomaly]:
        if positions.empty:
            return []
        anomalies: list[Anomaly] = []
        for icao24, grp in positions.groupby("icao24"):
            last = latest_row(grp)
            destination = opt_str(last.get("destination_icao"))
            if destination is None:
                continue
            if not bool(last["on_ground"]):
                continue
            landing_site = opt_str(last.get("nearest_site_icao"))
            distance = opt_float(last.get("nearest_site_distance_nm"))
            if landing_site is None or distance is None or distance > DIVERSION_RADIUS_NM:
                continue
            if landing_site == destination:
                continue  # landed where planned — not a diversion
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=landing_site,
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "origin": opt_str(last.get("origin_icao")),
                        "expected_destination": destination,
                        "alternate": landing_site,
                    },
                    severity_hint="high",
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
        return (
            self.case_type,
            icao24,
            detection_facts.get("expected_destination"),
            detection_facts.get("alternate"),
        )
