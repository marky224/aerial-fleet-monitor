"""BaselineProvider — expected city-pair flight durations for delay detection.

The ``delay`` rule needs a baseline "how long should this flight take?"
to decide whether an in-progress flight is running long. SPEC §3.5 names
two implementations (``OpenSkyHistoricalProvider`` over the OpenSky
``/flights`` endpoint, ``LocalParquetProvider`` over our own ≥30-day
history). Both are deferred for Phase 05:

  * OpenSky ``/flights/*`` charges 1 credit per 1-hour query window;
    a useful multi-day baseline per origin airport would consume most
    of the daily free-tier headroom (see ``flight_plan_enrichment`` for
    the budget math).
  * The lakehouse positions carry no origin/destination, and
    ``app.flight_plans`` keeps only the latest row per icao24 — so no
    city-pair history accumulates to aggregate.

Phase 05 therefore ships ``HeuristicBaselineProvider``: a deterministic
great-circle estimate from airport coordinates (already loaded in
``ref.airports``). It needs no upstream calls, always returns an
estimate for a known city pair, and is more than adequate to flag
*gross* delays in a demo. The ``BaselineProvider`` ABC + the
``BASELINE_PROVIDER`` env switch are preserved so the historical/parquet
providers can slot in later without touching the rule.

Recorded as a SPEC §3.5 deviation in the Phase-05 Decisions log.
"""

from __future__ import annotations

import math
import os
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.resources import PostgresResource

# Earth radius in nautical miles — keeps distance in nm so the cruise
# speed below can stay in knots without unit juggling.
_EARTH_RADIUS_NM = 3440.065

# Defaults for the heuristic. 450 kt ground speed is a reasonable
# jet-cruise average; 35 min covers taxi-out, climb, descent, and
# approach overhead that doesn't scale with cruise distance.
DEFAULT_CRUISE_SPEED_KT = 450.0
DEFAULT_FIXED_OVERHEAD_MIN = 35.0


class BaselineProvider(ABC):
    """Expected flight duration for a city pair (delay-rule baseline)."""

    @abstractmethod
    def expected_duration(
        self,
        origin_icao: str,
        destination_icao: str,
        aircraft_type: str | None = None,
    ) -> timedelta | None:
        """Estimated nominal duration, or ``None`` if it can't be computed.

        ``None`` means "no baseline" — the ``delay`` rule treats that as
        "can't assess this flight" and skips it rather than guessing.
        """
        ...


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points, in nautical miles."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return _EARTH_RADIUS_NM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class HeuristicBaselineProvider(BaselineProvider):
    """Great-circle estimate: ``distance / cruise_speed + fixed_overhead``.

    ``airport_coords`` maps ICAO code → ``(lat, lon)``. Unknown airports
    (or an origin == destination pair) yield ``None``.
    """

    def __init__(
        self,
        airport_coords: dict[str, tuple[float, float]],
        cruise_speed_kt: float = DEFAULT_CRUISE_SPEED_KT,
        fixed_overhead_min: float = DEFAULT_FIXED_OVERHEAD_MIN,
    ) -> None:
        self._coords = airport_coords
        self._cruise_speed_kt = cruise_speed_kt
        self._fixed_overhead_min = fixed_overhead_min

    def expected_duration(
        self,
        origin_icao: str,
        destination_icao: str,
        aircraft_type: str | None = None,  # interface compat; unused by heuristic
    ) -> timedelta | None:
        if not origin_icao or not destination_icao:
            return None
        if origin_icao == destination_icao:
            # A same-airport "flight" is nonsensical for delay baselining.
            return None
        origin = self._coords.get(origin_icao)
        destination = self._coords.get(destination_icao)
        if origin is None or destination is None:
            return None

        distance_nm = _haversine_nm(origin[0], origin[1], destination[0], destination[1])
        cruise_minutes = (distance_nm / self._cruise_speed_kt) * 60.0
        return timedelta(minutes=cruise_minutes + self._fixed_overhead_min)


def load_airport_coords(postgres: PostgresResource) -> dict[str, tuple[float, float]]:
    """Load ICAO → (lat, lon) for every airport with coordinates in ref.airports."""
    sql = "SELECT icao, lat, lon FROM ref.airports WHERE lat IS NOT NULL AND lon IS NOT NULL"
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return {row[0]: (float(row[1]), float(row[2])) for row in cur.fetchall()}


def build_baseline_provider(
    airport_coords: dict[str, tuple[float, float]],
    provider_name: str | None = None,
) -> BaselineProvider:
    """Construct the configured provider.

    ``provider_name`` defaults to the ``BASELINE_PROVIDER`` env var, then
    to ``"heuristic"``. Only ``"heuristic"`` is implemented in Phase 05;
    ``"opensky"``/``"parquet"`` raise a clear error so a misconfigured
    env fails loudly rather than silently degrading.
    """
    name = (provider_name or os.getenv("BASELINE_PROVIDER") or "heuristic").lower()
    if name == "heuristic":
        return HeuristicBaselineProvider(airport_coords)
    raise NotImplementedError(
        f"BASELINE_PROVIDER='{name}' is not implemented in Phase 05 "
        "(only 'heuristic'). See the Phase-05 Decisions log for the "
        "SPEC §3.5 deviation; 'opensky'/'parquet' are future work."
    )
