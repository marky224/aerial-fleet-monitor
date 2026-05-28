"""lost_signal — an aircraft at cruise stops reporting.

Fires when an icao24's most recent snapshot is airborne, at cruise
altitude, in roughly *level* flight, and is older than the latest poll
by 14-30 minutes — i.e. the feed went quiet on a flight that should
still be transmitting. Beyond 30 minutes we assume it landed or
genuinely left coverage, not a fresh signal loss (the dedup window
then keeps it from re-firing for 6h).

Two precision guards keep this off normal OpenSky coverage churn (the
free `/states/all` feed routinely drops a cruising aircraft for a few
polls or as it crosses the polled region's edge):

* a 14-minute floor — at the post-PR-#25 polling cadence of 120s,
  14 min = 7 missed polls; below that is feed jitter or transient
  coverage, not an operationally meaningful silence. The floor was
  8 min when polling was 30s (8 min = ~16 missed polls). The cadence
  change required recalibration; 14 min was picked as the most
  aggressive setting that still preserves a useful detection signal
  (~42 fires/day at this floor vs ~24/day at 15 min, which essentially
  kills the rule). Coupled to cadence; revisit if OPENSKY_POLL_INTERVAL
  changes again. Note the [14, 15) range is narrow: most surviving
  fires immediately cross LONG_GAP_THRESHOLD (15 min) and get promoted
  one tier — by design, gaps that long are concerning.
* level flight (|vertical_rate| <= 500 fpm) — "lost *at cruise*" means
  steady cruise, so an aircraft still climbing out or already descending
  to land is excluded (it's transitioning, expected to leave the band).
  A missing vertical rate is treated as level (we don't drop a candidate
  for absent data).

Severity is gradated per `_classify_severity` (B+C hybrid; user-approved
2026-05-26): altitude bands as the base tier, sparse-coverage cells
demote, long gaps promote. Replaces the pre-2026-05-26 unconditional
"high" emission that drove the 99.9% high-severity ratio across all
lost_signal fires. The skip-on-low guard (PR #28) further suppresses
fires below the operational noise floor.
"""

from __future__ import annotations

import json
import math
from datetime import timedelta
from pathlib import Path

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
LOST_MIN_GAP = timedelta(minutes=14)
LOST_MAX_GAP = timedelta(minutes=30)
LEVEL_VRATE_FPM = 500

# Severity gradation (B+C hybrid; user-approved 2026-05-26):
# - Altitude bands give the base tier — lower altitude is closer to terminal
#   ops where signal loss is genuinely operationally relevant.
# - Sparse-coverage cells demote one tier — recurring lost_signal fires in the
#   same 1deg x 1deg cell from many distinct callsigns reflect receiver
#   geography (Gulf of Maine corner, Sierra Nevada, Appalachians, etc.), not
#   incidents. Regenerate the cell list via `pipelines/rules/_build_sparse_cells.py`.
# - >=15-min gaps promote one tier — at that duration the silence is past
#   normal feed jitter regardless of altitude.
SEVERITY_TIERS: tuple[str, ...] = ("low", "medium", "high")
ALT_HIGH_CEILING_FT = 30_000  # alt < 30k  -> base "high"
ALT_MED_CEILING_FT = 35_000  # 30k-35k     -> base "medium"; >=35k -> base "low"
LONG_GAP_THRESHOLD = timedelta(minutes=15)


def _load_sparse_cells() -> frozenset[tuple[int, int]]:
    """Load hot-cell list from the regenerator's JSON output.

    Fail-soft: a missing or malformed file yields an empty set, which
    means the demote step is a no-op (every fire keeps its base tier).
    Importing this module never fails — that protects detector startup
    if the JSON hasn't been generated yet (e.g. a fresh checkout).
    """
    data_path = Path(__file__).parent / "data" / "lost_signal_sparse_cells.json"
    if not data_path.exists():
        return frozenset()
    try:
        payload = json.loads(data_path.read_text())
        return frozenset((int(lat), int(lon)) for lat, lon in payload.get("cells", []))
    except (ValueError, TypeError):
        return frozenset()


_SPARSE_CELLS: frozenset[tuple[int, int]] = _load_sparse_cells()


def _shift_tier(severity: str, delta: int) -> str:
    """Move severity up (delta=+1) or down (delta=-1), clamped to [low, high]."""
    idx = SEVERITY_TIERS.index(severity)
    new_idx = max(0, min(len(SEVERITY_TIERS) - 1, idx + delta))
    return SEVERITY_TIERS[new_idx]


def _classify_severity(
    alt_ft: int,
    gap: timedelta,
    lat: float | None,
    lon: float | None,
) -> str:
    """Compute severity for one lost_signal fire (B+C hybrid).

    Base tier from altitude → demote if cell is a known sparse-coverage
    geography → promote if the gap is genuinely long. Missing lat/lon
    skips the demote (a cell lookup needs both).
    """
    if alt_ft < ALT_HIGH_CEILING_FT:
        severity = "high"
    elif alt_ft < ALT_MED_CEILING_FT:
        severity = "medium"
    else:
        severity = "low"

    if lat is not None and lon is not None:
        cell = (math.floor(lat), math.floor(lon))
        if cell in _SPARSE_CELLS:
            severity = _shift_tier(severity, -1)

    if gap >= LONG_GAP_THRESHOLD:
        severity = _shift_tier(severity, +1)

    return severity


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
            # Level flight only: a climbing/descending aircraft is
            # transitioning out of cruise, not a steady-cruise signal loss.
            # Missing vertical rate counts as level (don't drop on absent data).
            vrate = last.get("vertical_rate_fpm")
            if vrate is not None and not pd.isna(vrate) and abs(float(vrate)) > LEVEL_VRATE_FPM:
                continue
            last_lat = opt_float(last.get("lat"))
            last_lon = opt_float(last.get("lon"))
            severity = _classify_severity(int(alt), gap, last_lat, last_lon)
            # Skip fires the gradation classifies "low": these are below the
            # operational noise floor (cruise altitude in a sparse-coverage
            # cell with a short-ish gap). Persisting them is just IO churn —
            # they wouldn't drive a Task downstream, just clutter dashboards
            # and sync layers. Backed by the historical-projection split that
            # showed ~83% of fires fall into this band.
            if severity == "low":
                continue
            anomalies.append(
                Anomaly(
                    rule=self.case_type,
                    icao24=str(icao24),
                    site_icao=opt_str(last.get("nearest_site_icao")),
                    customer_region=region_of(last),
                    detection_facts={
                        "callsign": opt_str(last.get("callsign")),
                        "last_lat": last_lat,
                        "last_lon": last_lon,
                        "last_altitude_ft": int(alt),
                        "last_seen": last["ts_polled"].isoformat(),
                        "gap_minutes": round(gap.total_seconds() / 60, 1),
                    },
                    severity_hint=severity,
                )
            )
        return anomalies
