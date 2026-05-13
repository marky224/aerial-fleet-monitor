"""Watchlist resource.

Reads ``ref.airports WHERE is_watched = TRUE`` (the 35 SPEC §4 airports
plus ``customer_regions`` tags) and exposes:

* ``get_airports()`` — full list, ordered by ICAO.
* ``infer_regions(lats, lons)`` — vectorized: for each (lat, lon),
  return the nearest watched airport's ``customer_regions`` tuple if
  within 50 nm, else ``None``.

The asset uses ``infer_regions`` to denormalize ``customer_region`` onto
every position row at write time (matches the column in both
``app.current_positions`` §2.5 and the Parquet per-row schema §4.2).
NULL semantics per ``01_ingestion.md``: NULL = "internal-ops only"
downstream; do not default to ``'all'``.

The DB read is lazy and cached for the lifetime of the resource
instance — Dagster's modern Pythonic resources are freshly constructed
per asset run, so caching across runs isn't needed (and 35 rows is
~5 ms to fetch anyway). Caching within a run avoids re-querying if the
asset (or a future co-resident asset) calls ``infer_regions`` more than
once.

Region inference uses brute-force vectorized haversine over the 35
airports. For a typical 5,000-position cycle that's 175k float ops —
sub-50ms in numpy. No KD-tree, no PostGIS, no per-row Python loops.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from dagster import ConfigurableResource
from pydantic import PrivateAttr

from pipelines.resources.postgres import PostgresResource

# 50 nautical miles, per docs/build/01_ingestion.md "Customer region inference".
NM_TO_METERS = 1852.0
REGION_INFERENCE_RADIUS_METERS = 50.0 * NM_TO_METERS

# Mean Earth radius (meters). Adequate for inter-airport distances at this scale.
EARTH_RADIUS_METERS = 6_371_000.0


@dataclass(frozen=True)
class WatchedAirport:
    """One row from ``ref.airports WHERE is_watched = TRUE``."""

    icao: str
    lat: float
    lon: float
    customer_regions: tuple[str, ...]


class WatchlistResource(ConfigurableResource):  # type: ignore[type-arg]
    """Watchlist lookup + region inference.

    Attributes:
        postgres: Dagster resource dependency on the AFM Postgres
            connection factory. Injected by ``pipelines/definitions.py``.
    """

    postgres: PostgresResource

    _airports_cache: list[WatchedAirport] | None = PrivateAttr(default=None)
    _coords_cache: np.ndarray | None = PrivateAttr(default=None)

    def get_airports(self) -> list[WatchedAirport]:
        """Return the watched-airport list, loading on first call."""
        if self._airports_cache is None:
            self._load()
        assert self._airports_cache is not None
        return self._airports_cache

    def infer_regions(
        self,
        lats: np.ndarray,
        lons: np.ndarray,
    ) -> list[tuple[str, ...] | None]:
        """For each (lat, lon), return nearest watched airport's regions or None.

        Positions with NaN coordinates yield ``None``. Positions farther
        than 50 nm from every watched airport yield ``None``.

        Args:
            lats: 1-D array of latitudes in degrees, length N. NaN allowed.
            lons: 1-D array of longitudes in degrees, length N. NaN allowed.

        Returns:
            List of length N. Each element is the ``customer_regions``
            tuple for the nearest watched airport within 50 nm, or
            ``None`` if no airport qualifies.
        """
        if lats.shape != lons.shape:
            raise ValueError(f"lats shape {lats.shape} != lons shape {lons.shape}")
        if lats.ndim != 1:
            raise ValueError(f"expected 1-D arrays, got {lats.ndim}-D")

        airports = self.get_airports()
        if not airports:
            return [None] * len(lats)
        assert self._coords_cache is not None

        # Mask NaN positions; compute haversine for the rest.
        valid = np.isfinite(lats) & np.isfinite(lons)
        results: list[tuple[str, ...] | None] = [None] * len(lats)

        if not np.any(valid):
            return results

        valid_lats = lats[valid]
        valid_lons = lons[valid]

        distances = _haversine_meters_to_airports(valid_lats, valid_lons, self._coords_cache)
        # distances shape: (n_valid, n_airports)
        nearest_idx = np.argmin(distances, axis=1)
        nearest_dist = distances[np.arange(len(valid_lats)), nearest_idx]
        within_radius = nearest_dist <= REGION_INFERENCE_RADIUS_METERS

        valid_indices = np.flatnonzero(valid)
        for i, position_index in enumerate(valid_indices):
            if within_radius[i]:
                results[position_index] = airports[nearest_idx[i]].customer_regions

        return results

    def _load(self) -> None:
        with self.postgres.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT icao, lat, lon, customer_regions
                FROM ref.airports
                WHERE is_watched = TRUE
                ORDER BY icao
                """
            )
            rows = cur.fetchall()

        airports: list[WatchedAirport] = []
        coords_list: list[tuple[float, float]] = []
        for icao, lat, lon, regions in rows:
            airports.append(
                WatchedAirport(
                    icao=icao,
                    lat=float(lat),
                    lon=float(lon),
                    customer_regions=tuple(regions or ()),
                )
            )
            coords_list.append((float(lat), float(lon)))

        self._airports_cache = airports
        # Empty watchlist still produces a well-shaped (0, 2) array so
        # downstream haversine never sees an undefined shape.
        self._coords_cache = (
            np.asarray(coords_list, dtype=np.float64)
            if coords_list
            else np.zeros((0, 2), dtype=np.float64)
        )


def _haversine_meters_to_airports(
    lats: np.ndarray,
    lons: np.ndarray,
    airport_coords: np.ndarray,
) -> np.ndarray:
    """Vectorized haversine distance from each (lat, lon) to each airport.

    Args:
        lats: 1-D array of position latitudes in degrees, length N.
        lons: 1-D array of position longitudes in degrees, length N.
        airport_coords: 2-D array of shape ``(M, 2)`` with ``[lat, lon]``
            per row in degrees.

    Returns:
        2-D array of shape ``(N, M)`` with distances in meters.
    """
    lat1 = np.radians(lats)[:, None]  # (N, 1)
    lon1 = np.radians(lons)[:, None]  # (N, 1)
    lat2 = np.radians(airport_coords[:, 0])[None, :]  # (1, M)
    lon2 = np.radians(airport_coords[:, 1])[None, :]  # (1, M)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
    return np.asarray(EARTH_RADIUS_METERS * c)
