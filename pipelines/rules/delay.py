"""delay — an in-progress flight is running well past its expected duration.

Fires when an airborne icao24 with a known origin/destination/departure
has been flying longer than ``BaselineProvider.expected_duration`` x 1.3.
If the baseline can't be computed (unknown airports), the flight is
skipped rather than guessed.
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
    opt_str,
    region_of,
)
from pipelines.services.baseline_provider import BaselineProvider

DELAY_FACTOR = 1.3


class DelayRule(Rule):
    case_type = "delay"
    dedup_window = timedelta(hours=4)

    def detect(
        self,
        positions: pd.DataFrame,
        weather: dict[str, AirportConditions],
        existing_cases: pd.DataFrame,
        baseline: BaselineProvider,
    ) -> list[Anomaly]:
        if positions.empty:
            return []
        now = positions["ts_polled"].max()
        anomalies: list[Anomaly] = []
        for icao24, grp in positions.groupby("icao24"):
            last = latest_row(grp)
            if bool(last["on_ground"]):
                continue  # only in-progress flights
            origin = opt_str(last.get("origin_icao"))
            destination = opt_str(last.get("destination_icao"))
            departure = last.get("departure_time")
            if origin is None or destination is None or departure is None or pd.isna(departure):
                continue
            expected = baseline.expected_duration(origin, destination)
            if expected is None:
                continue
            elapsed = now - departure
            if elapsed <= expected * DELAY_FACTOR:
                continue
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=destination,
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "origin": origin,
                        "destination": destination,
                        "elapsed_minutes": round(elapsed.total_seconds() / 60, 1),
                        "expected_minutes": round(expected.total_seconds() / 60, 1),
                    },
                    severity_hint="medium",
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
        return (self.case_type, icao24, detection_facts.get("destination"))
