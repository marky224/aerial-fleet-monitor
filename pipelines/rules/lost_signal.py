"""lost_signal — an aircraft at cruise stops reporting.

Fires when an icao24's most recent snapshot is at cruise altitude,
airborne, and is older than the latest poll by 2-30 minutes — i.e. the
feed went quiet on a flight that should still be transmitting. Beyond
30 minutes we assume it landed or genuinely left coverage, not a fresh
signal loss (the dedup window then keeps it from re-firing for 6h).
"""

from __future__ import annotations

from datetime import timedelta

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

CRUISE_FLOOR_FT = 25_000
LOST_MIN_GAP = timedelta(minutes=2)
LOST_MAX_GAP = timedelta(minutes=30)


class LostSignalRule(Rule):
    case_type = "lost_signal"
    dedup_window = timedelta(hours=6)

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
                continue
            alt = last["altitude_ft"]
            if pd.isna(alt) or alt < CRUISE_FLOOR_FT:
                continue
            gap = now - last["ts_polled"]
            if not (LOST_MIN_GAP <= gap <= LOST_MAX_GAP):
                continue
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=opt_str(last.get("nearest_site_icao")),
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "last_lat": opt_float(last.get("lat")),
                        "last_lon": opt_float(last.get("lon")),
                        "last_altitude_ft": int(alt),
                        "last_seen": last["ts_polled"].isoformat(),
                        "gap_minutes": round(gap.total_seconds() / 60, 1),
                    },
                    severity_hint="high",
                )
            )
        return anomalies
