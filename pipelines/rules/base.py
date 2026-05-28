"""Rule abstraction + shared types for the case detector.

## The enriched positions frame

Rules receive ``positions``: a pandas DataFrame of the last hour of
lakehouse position snapshots (multiple rows per icao24, one per ~30s
poll), enriched by the detector with flight-plan + nearest-site columns
so rules stay pure (no I/O, no airport-reference lookups of their own).

Column contract (the detector guarantees these columns exist):

| Column | Source | Notes |
|---|---|---|
| ``icao24`` | lakehouse | lowercase hex |
| ``callsign`` | lakehouse | may be None |
| ``lat`` / ``lon`` | lakehouse | degrees |
| ``altitude_ft`` | lakehouse | barometric feet; may be None |
| ``speed_kt`` | lakehouse | ground speed; may be None |
| ``heading_deg`` | lakehouse | may be None |
| ``vertical_rate_fpm`` | lakehouse | + = climb; may be None |
| ``on_ground`` | lakehouse | bool |
| ``squawk`` | lakehouse | may be None |
| ``ts_polled`` | lakehouse | tz-aware UTC; the snapshot time |
| ``customer_region`` | lakehouse | 'west' / 'east' / 'all' / None |
| ``origin_icao`` | app.flight_plans | broadcast per icao24; may be None |
| ``destination_icao`` | app.flight_plans | broadcast per icao24; may be None |
| ``departure_time`` | app.flight_plans | tz-aware UTC; may be None |
| ``nearest_site_icao`` | detector (haversine vs watched) | nearest *watched* airport; may be None |
| ``nearest_site_distance_nm`` | detector | distance to ``nearest_site_icao``; may be None |

The detector also passes ``now`` implicitly as the max ``ts_polled`` in
the frame — rules that need "current time" use ``positions['ts_polled'].max()``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar, cast

import pandas as pd
from pydantic import BaseModel, Field

from pipelines.services.baseline_provider import BaselineProvider


@dataclass(frozen=True)
class AirportConditions:
    """Current weather at a watched site (subset of app.airport_conditions)."""

    site_icao: str
    flight_category: str | None  # VFR / MVFR / IFR / LIFR
    wind_kt: int | None = None
    visibility_sm: float | None = None
    ceiling_ft: int | None = None


class Anomaly(BaseModel):
    """One detected anomaly. The detector turns this into a Case.

    ``icao24`` is empty for site-level anomalies (e.g. ``weather_impact``)
    that aren't about a specific aircraft; the detector substitutes a
    site sentinel for the NOT-NULL ``cases.flight_id`` column in that
    case.
    """

    rule: str  # == the firing rule's case_type
    icao24: str = ""
    site_icao: str | None = None
    customer_region: str = "all"
    detection_facts: dict[str, Any] = Field(default_factory=dict)
    severity_hint: str | None = None  # rule may suggest; agent decides later


class Rule(ABC):
    """A single anomaly-detection rule.

    Subclasses set ``case_type`` + ``dedup_window`` and implement
    ``detect``. ``dedup_key`` returns the identity tuple used to suppress
    repeat firings within the window (the detector compares it against
    both the new batch and existing open cases).
    """

    case_type: ClassVar[str]
    dedup_window: ClassVar[timedelta]

    @abstractmethod
    def detect(
        self,
        positions: pd.DataFrame,
        weather: dict[str, AirportConditions],
        existing_cases: pd.DataFrame,
        baseline: BaselineProvider,
    ) -> list[Anomaly]:
        """Return zero or more anomalies found in this cycle's data."""
        ...

    def dedup_key(
        self,
        *,
        icao24: str,
        site_icao: str | None,
        detection_facts: dict[str, Any],
    ) -> tuple[Any, ...]:
        """Identity tuple for suppression. Default = (case_type, icao24, site_icao).

        Rules whose identity needs extra facts (diversion's alternate,
        delay's destination) override this. The same function is applied
        to new anomalies and to existing-case rows, so it must read only
        fields both carry (icao24/flight_id, site_icao, detection_facts).
        """
        return (self.case_type, icao24, site_icao)


# -- shared row-extraction helpers (used by every rule) -------------------


def region_of(row: pd.Series) -> str:
    """Customer region for a position row; 'all' (visible to everyone) if unset."""
    region = row.get("customer_region")
    return str(region) if isinstance(region, str) and region else "all"


def opt_str(value: Any) -> str | None:
    """Non-empty string or None — collapses NaN/empty to None."""
    return str(value) if isinstance(value, str) and value else None


def opt_float(value: Any) -> float | None:
    """Float or None — collapses NaN/None to None."""
    return None if value is None or pd.isna(value) else float(value)


def is_general_aviation_callsign(callsign: str | None) -> bool:
    """True for US general-aviation N-numbered callsigns (e.g. 'N816M', 'N5169E').

    AFM's portfolio scope is commercial fleet operations; GA traffic
    (flight training, scenic, private piston/turbo) produces patterns
    that look like real holds + go-arounds in the data but aren't
    operationally relevant for commercial fleet support. Rules opt in
    to this filter by checking the callsign before emitting an Anomaly.

    Match pattern: starts with 'N' followed by a digit. Covers the
    dominant US-registered GA population (~80%+ of excessive_hold
    fires, ~57% of go_around fires per the 2026-05-28 snapshot).
    Non-US GA prefixes (G-, F-, D-, JA, VH, etc.) and 3-letter
    operator codes (DAL, UAL, UPS, SKW, RCH) are NOT matched — those
    stay in scope. Lost-signal at cruise (>=25k ft) is naturally
    light on GA (6.8% in the same snapshot) so its rule does not
    apply this filter.
    """
    if not callsign or len(callsign) < 2:
        return False
    return callsign[0] == "N" and callsign[1].isdigit()


def latest_row(frame: pd.DataFrame, ts_col: str = "ts_polled") -> pd.Series:
    """The single row with the max ``ts_col``, typed as a Series.

    ``frame.loc[label]`` is typed ``Series | DataFrame`` by pandas-stubs
    (a label could be non-unique). Within a per-icao24 group the max-ts
    label is unique in practice, so the cast is safe and keeps callers
    free of the union.
    """
    return cast("pd.Series[Any]", frame.loc[frame[ts_col].idxmax()])
