"""go_around — an aborted landing at a watched airport.

Heuristic: within 10 nm of a watched airport, an icao24 descends to a
low altitude (< 3,000 ft, i.e. on final/short final) and then climbs
back by >= 1,000 ft while still near the field — the signature of a
rejected landing / go-around.
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

GO_AROUND_RADIUS_NM = 10.0
GO_AROUND_FLOOR_FT = 3_000
CLIMB_THRESHOLD_FT = 1_000
MIN_SNAPSHOTS = 3


class GoAroundRule(Rule):
    case_type = "go_around"
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
                (grp["nearest_site_distance_nm"] <= GO_AROUND_RADIUS_NM)
                & (~grp["on_ground"].astype(bool))
            ].sort_values("ts_polled")
            if len(near) < MIN_SNAPSHOTS:
                continue
            alts = near["altitude_ft"].dropna()
            if alts.empty:
                continue
            min_label = alts.idxmin()
            min_alt = float(alts.loc[min_label])
            if min_alt > GO_AROUND_FLOOR_FT:
                continue
            min_ts = near.loc[min_label, "ts_polled"]
            after = near[near["ts_polled"] > min_ts]["altitude_ft"].dropna()
            if after.empty:
                continue
            climb = float(after.max()) - min_alt
            if climb < CLIMB_THRESHOLD_FT:
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
                        "min_altitude_ft": int(min_alt),
                        "climb_ft": int(climb),
                    },
                    severity_hint="medium",
                )
            )
        return anomalies
