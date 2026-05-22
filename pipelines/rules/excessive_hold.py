"""excessive_hold — an aircraft is holding near a watched airport.

Heuristic: across the last hour, an icao24 spends >= 30 minutes airborne
within 15 nm of a watched airport at < 15,000 ft while its heading
sweeps through many compass sectors (a circling/holding pattern rather
than transiting through). Wrap-safe circling test = number of distinct
45-degree heading sectors visited (>= 6 of 8).

The 15 nm radius is deliberately tight: a diagnosis of one live cycle
showed a 40 nm radius fired ~723x/cycle, of which >50% kept the aircraft
20+ nm from the field the whole time — arrival sequencing / vectoring /
transit near busy airports, not a hold. A published holding pattern sits
within ~5-15 nm of the fix; tightening radius (40->15), sectors (5->6),
and duration (20->30 min) cut fires ~86% to the aircraft that actually
loiter close to the field.
"""

from __future__ import annotations

from datetime import timedelta

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

HOLD_RADIUS_NM = 15.0
HOLD_CEILING_FT = 15_000
HOLD_MIN_DURATION = timedelta(minutes=30)
MIN_SNAPSHOTS = 10
MIN_DISTINCT_SECTORS = 6  # of 8 45-degree sectors


class ExcessiveHoldRule(Rule):
    case_type = "excessive_hold"
    dedup_window = timedelta(minutes=30)

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
            near = grp[
                (grp["nearest_site_distance_nm"] <= HOLD_RADIUS_NM)
                & (grp["altitude_ft"] <= HOLD_CEILING_FT)
                & (~grp["on_ground"].astype(bool))
            ]
            if len(near) < MIN_SNAPSHOTS:
                continue
            duration = near["ts_polled"].max() - near["ts_polled"].min()
            if duration < HOLD_MIN_DURATION:
                continue
            sectors = _distinct_heading_sectors(near["heading_deg"])
            if sectors < MIN_DISTINCT_SECTORS:
                continue
            site = near["nearest_site_icao"].mode()
            site_icao = opt_str(site.iloc[0]) if not site.empty else None
            last = latest_row(near)
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=site_icao,
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "duration_minutes": round(duration.total_seconds() / 60, 1),
                        "distinct_heading_sectors": sectors,
                        "snapshots": int(len(near)),
                    },
                    severity_hint="medium",
                )
            )
        return anomalies


def _distinct_heading_sectors(headings: pd.Series) -> int:
    """Count distinct 45-degree compass sectors among non-null headings."""
    valid = headings.dropna()
    if valid.empty:
        return 0
    sectors = (valid % 360 // 45).astype(int)
    return int(sectors.nunique())
