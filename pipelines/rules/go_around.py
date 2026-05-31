"""go_around — an aborted landing at a watched airport.

Heuristic: within 10 nm of a watched airport, an icao24 traces a
*valley* — it descends by >= 1,000 ft to a low point (< 3,000 ft, i.e.
on final/short final) and then climbs back by >= 1,000 ft while still
near the field — the signature of a rejected landing / go-around.

The descent-into-the-low-point check is what separates a go-around from
a normal departure: a departing aircraft's lowest near-field snapshot is
its *first* one (it only climbs after), so it has no descent leg and is
correctly ignored. Without this check the rule fired on essentially
every departure from a watched field (~87% of its raw matches).

CADENCE SENSITIVITY (important): this rule needs ``MIN_SNAPSHOTS`` (3)
position fixes while the aircraft is within ``GO_AROUND_RADIUS_NM`` (10 nm)
of the field to reconstruct the descend->low-point->climb valley. A
go-around keeps the aircraft near the field for only ~6-10 min, so the poll
cadence has to sample it at least ~3x in that window. At the 120s cadence
that held through 2026-05-31 that was ~4-5 fixes (fine); at the 300s cadence
adopted 2026-05-31 (for OpenSky headroom — see definitions.py) it is ~1-2
fixes, so the valley can no longer be traced and this rule is effectively
DORMANT. This is structural (you cannot define a valley from <3 points), not
a tunable threshold — lowering MIN_SNAPSHOTS would just re-admit the
departure false positives the valley check exists to kill. The rule is left
registered so it resumes automatically if the poll cadence is ever tightened
again; revisit OPENSKY_POLL_INTERVAL_SECONDS if go-around coverage is needed.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from pipelines.rules.base import (
    AirportConditions,
    Anomaly,
    Rule,
    is_general_aviation_callsign,
    latest_row,
    opt_str,
    region_of,
)
from pipelines.services.baseline_provider import BaselineProvider

GO_AROUND_RADIUS_NM = 10.0
GO_AROUND_FLOOR_FT = 3_000
DESCENT_THRESHOLD_FT = 1_000
CLIMB_THRESHOLD_FT = 1_000
# Geometric minimum to trace a descend->low-point->climb valley (before /
# at / after the low point). NOT lowerable — see the cadence-sensitivity
# note in the module docstring: at the 300s poll cadence the aircraft is
# sampled ~1-2x near the field, so this floor leaves the rule dormant.
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
            before = near[near["ts_polled"] < min_ts]["altitude_ft"].dropna()
            after = near[near["ts_polled"] > min_ts]["altitude_ft"].dropna()
            if before.empty or after.empty:
                continue
            # Valley shape: descended into the low point AND climbed out of
            # it. The descent leg is what excludes normal departures (whose
            # lowest near-field snapshot is their first — no descent before).
            descent = float(before.max()) - min_alt
            climb = float(after.max()) - min_alt
            if descent < DESCENT_THRESHOLD_FT or climb < CLIMB_THRESHOLD_FT:
                continue
            site = near["nearest_site_icao"].mode()
            site_icao = opt_str(site.iloc[0]) if not site.empty else None
            last = latest_row(near)
            # Skip GA training patterns (N-numbered callsigns). A
            # touch-and-go produces the same descent→low-point→climb
            # signature as a real commercial go-around but isn't
            # operationally meaningful for a commercial-fleet console.
            # 57% of pre-filter go_around fires were GA (2026-05-28).
            if is_general_aviation_callsign(opt_str(last.get("callsign"))):
                continue
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=site_icao,
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "min_altitude_ft": int(min_alt),
                        "descent_ft": int(descent),
                        "climb_ft": int(climb),
                    },
                    severity_hint="medium",
                )
            )
        return anomalies
