"""`opensky_positions` — every-30s poll of OpenSky's ``/states/all``.

For each polling cycle:

1. Fetch the bbox-scoped state vector list from OpenSky.
2. Drop rows whose ``icao24`` matches a denylist prefix
   (``pipelines/data/icao24_denylist.txt``).
3. Drop rows without usable lat/lon (can't be written to either store).
4. Convert OpenSky's SI units to AFM's storage units
   (m → ft, m/s → kt, m/s vertical → ft/min).
5. Denormalize ``customer_region`` per row using the nearest watched
   airport within 50 nm (via ``WatchlistResource``).
6. Write one atomic Parquet snapshot per cycle (skipped if zero rows
   after filtering).
7. UPSERT every row into ``app.current_positions``. Conflict update
   only when the new ``last_seen_at`` is greater than the existing one;
   ``updated_at`` always bumps.

Step 3 scope leaves ``aircraft_type``, ``origin_icao``, and
``destination_icao`` NULL in ``current_positions``. They become an
enrichment pass (registry join + callsign parsing) in a follow-up —
acceptance criteria for Step 3 don't require them populated.
"""

import logging
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from typing import Any

import numpy as np
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from psycopg2.extras import execute_values

from pipelines.resources.lakehouse import LakehouseResource
from pipelines.resources.opensky import (
    OpenSkyAuthError,
    OpenSkyError,
    OpenSkyResource,
    OpenSkyState,
)
from pipelines.resources.postgres import PostgresResource
from pipelines.resources.watchlist import WatchlistResource

logger = logging.getLogger(__name__)

# Unit conversions from OpenSky (SI) to AFM storage units.
METERS_TO_FEET = 3.280839895
MPS_TO_KNOTS = 1.943844492
MPS_TO_FPM = 196.850393701  # m/s vertical → ft/min

# Hex chars used to validate denylist entries.
HEX_CHARS = frozenset("0123456789abcdef")

CURRENT_POSITIONS_UPSERT_SQL = """
INSERT INTO app.current_positions (
    icao24, callsign, lat, lon,
    altitude_ft, speed_kt, heading_deg, vertical_rate_fpm,
    on_ground, squawk, customer_region, last_seen_at
) VALUES %s
ON CONFLICT (icao24) DO UPDATE SET
    callsign          = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.callsign
                             ELSE app.current_positions.callsign END,
    lat               = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.lat
                             ELSE app.current_positions.lat END,
    lon               = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.lon
                             ELSE app.current_positions.lon END,
    altitude_ft       = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.altitude_ft
                             ELSE app.current_positions.altitude_ft END,
    speed_kt          = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.speed_kt
                             ELSE app.current_positions.speed_kt END,
    heading_deg       = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.heading_deg
                             ELSE app.current_positions.heading_deg END,
    vertical_rate_fpm = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.vertical_rate_fpm
                             ELSE app.current_positions.vertical_rate_fpm END,
    on_ground         = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.on_ground
                             ELSE app.current_positions.on_ground END,
    squawk            = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.squawk
                             ELSE app.current_positions.squawk END,
    customer_region   = CASE WHEN EXCLUDED.last_seen_at > app.current_positions.last_seen_at
                             THEN EXCLUDED.customer_region
                             ELSE app.current_positions.customer_region END,
    last_seen_at      = GREATEST(EXCLUDED.last_seen_at, app.current_positions.last_seen_at),
    updated_at        = NOW()
"""


@lru_cache(maxsize=1)
def _load_denylist_prefixes() -> tuple[str, ...]:
    """Load and validate icao24 hex prefixes. Cached per-process."""
    raw = files("pipelines.data").joinpath("icao24_denylist.txt").read_text()
    prefixes: list[str] = []
    for lineno, raw_line in enumerate(raw.splitlines(), start=1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if not all(c in HEX_CHARS for c in line):
            raise ValueError(
                f"icao24_denylist.txt line {lineno}: '{raw_line}' contains "
                "non-hex characters (expected lowercase hex)."
            )
        if line != line.lower():
            raise ValueError(f"icao24_denylist.txt line {lineno}: '{raw_line}' must be lowercase.")
        prefixes.append(line)
    return tuple(prefixes)


def _is_denied(icao24: str, prefixes: tuple[str, ...]) -> bool:
    return any(icao24.startswith(prefix) for prefix in prefixes)


def _denormalize_region(customer_regions: tuple[str, ...] | None) -> str | None:
    """Collapse a watched airport's region tags into one scope value.

    - ``None`` (no airport within 50 nm) → ``None`` (internal-ops only).
    - Empty tuple (matched airport, no customer affiliation) → ``None``
      (internal-ops only).
    - Single tag → that tag.
    - Multiple tags (an airport serving both customer pools) → ``'all'``
      (visible to every scope per SALESFORCE.md §5.4 scope filter).
    """
    if not customer_regions:
        return None
    if len(customer_regions) == 1:
        return customer_regions[0]
    return "all"


def _convert_states_to_rows(
    states: tuple[OpenSkyState, ...],
    polled_at: datetime,
    watchlist: WatchlistResource,
) -> list[dict[str, Any]]:
    """Filter, region-infer, and unit-convert a cycle's states.

    Drops rows without lat/lon. Returns a list keyed by
    ``POSITIONS_COLUMNS`` (extra keys are ignored by the writer; all
    required keys are present).
    """
    # Vectorized region inference: build arrays first.
    lats = np.array([s.lat if s.lat is not None else np.nan for s in states], dtype=np.float64)
    lons = np.array([s.lon if s.lon is not None else np.nan for s in states], dtype=np.float64)
    regions = watchlist.infer_regions(lats, lons)

    rows: list[dict[str, Any]] = []
    for state, region_tuple in zip(states, regions, strict=True):
        if state.lat is None or state.lon is None:
            continue

        rows.append(
            {
                "icao24": state.icao24,
                "callsign": state.callsign,
                "origin_country": state.origin_country,
                "lat": state.lat,
                "lon": state.lon,
                "altitude_ft": (
                    int(round(state.baro_altitude_m * METERS_TO_FEET))
                    if state.baro_altitude_m is not None
                    else None
                ),
                "speed_kt": (
                    int(round(state.velocity_ms * MPS_TO_KNOTS))
                    if state.velocity_ms is not None
                    else None
                ),
                "heading_deg": (
                    int(round(state.true_track_deg)) if state.true_track_deg is not None else None
                ),
                "vertical_rate_fpm": (
                    int(round(state.vertical_rate_ms * MPS_TO_FPM))
                    if state.vertical_rate_ms is not None
                    else None
                ),
                "on_ground": state.on_ground,
                "squawk": state.squawk,
                "ts_polled": polled_at,
                "ts_position": (
                    datetime.fromtimestamp(state.last_contact, tz=UTC)
                    if state.last_contact
                    else None
                ),
                "customer_region": _denormalize_region(region_tuple),
            }
        )
    return rows


def _upsert_current_positions(
    postgres: PostgresResource,
    rows: list[dict[str, Any]],
) -> int:
    """Batch UPSERT into ``app.current_positions``. Returns rows affected."""
    if not rows:
        return 0

    tuples = [
        (
            row["icao24"],
            row["callsign"],
            row["lat"],
            row["lon"],
            row["altitude_ft"],
            row["speed_kt"],
            row["heading_deg"],
            row["vertical_rate_fpm"],
            row["on_ground"],
            row["squawk"],
            row["customer_region"],
            row["ts_position"] or row["ts_polled"],
        )
        for row in rows
    ]

    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, CURRENT_POSITIONS_UPSERT_SQL, tuples, page_size=1000)
            rowcount = int(cur.rowcount)
        conn.commit()
    return rowcount


@asset(
    group_name="ingestion",
    description="Polls OpenSky for current positions in the US bounding box.",
    metadata={
        "source": "OpenSky Network /states/all",
        "cadence": "30s",
        "credit_cost_per_call": 1,
    },
)
def opensky_positions(
    context: AssetExecutionContext,
    opensky: OpenSkyResource,
    postgres: PostgresResource,
    lakehouse: LakehouseResource,
    watchlist: WatchlistResource,
) -> MaterializeResult:
    polled_at = datetime.now(UTC)
    denylist = _load_denylist_prefixes()

    try:
        response = opensky.fetch_states()
    except OpenSkyAuthError:
        raise
    except OpenSkyError as exc:
        context.log.warning("OpenSky fetch skipped: %s", exc)
        return MaterializeResult(
            metadata={
                "aircraft_count": MetadataValue.int(0),
                "parquet_bytes_written": MetadataValue.int(0),
                "postgres_rows_upserted": MetadataValue.int(0),
                "opensky_credits_used": MetadataValue.int(0),
                "skip_reason": MetadataValue.text(str(exc)),
                "polled_at": MetadataValue.text(polled_at.isoformat()),
            }
        )

    raw_count = len(response.states)
    denied_states = [s for s in response.states if _is_denied(s.icao24, denylist)]
    kept_states = tuple(s for s in response.states if not _is_denied(s.icao24, denylist))

    rows = _convert_states_to_rows(kept_states, polled_at, watchlist)
    rows_without_coords = len(kept_states) - len(rows)

    parquet_path: str | None = None
    parquet_bytes = 0
    if rows:
        final_path, parquet_bytes = lakehouse.write_positions_snapshot(rows, polled_at)
        parquet_path = str(final_path)
    else:
        context.log.info(
            "Zero rows after filtering (raw=%d, denied=%d, no_coords=%d) — "
            "skipping Parquet write.",
            raw_count,
            len(denied_states),
            rows_without_coords,
        )

    postgres_rows = _upsert_current_positions(postgres, rows)

    lag_seconds = max(0, int(polled_at.timestamp()) - response.api_time)

    metadata: dict[str, MetadataValue] = {
        "aircraft_count": MetadataValue.int(len(rows)),
        "raw_state_count": MetadataValue.int(raw_count),
        "filtered_denylist": MetadataValue.int(len(denied_states)),
        "filtered_no_coords": MetadataValue.int(rows_without_coords),
        "parquet_bytes_written": MetadataValue.int(parquet_bytes),
        "postgres_rows_upserted": MetadataValue.int(postgres_rows),
        "opensky_credits_used": MetadataValue.int(response.credits_used),
        "pipeline_lag_seconds": MetadataValue.int(lag_seconds),
        "polled_at": MetadataValue.text(polled_at.isoformat()),
        "opensky_api_time": MetadataValue.int(response.api_time),
    }
    if parquet_path is not None:
        metadata["parquet_path"] = MetadataValue.path(parquet_path)
    if response.rate_limit_remaining is not None:
        metadata["opensky_rate_limit_remaining"] = MetadataValue.int(response.rate_limit_remaining)

    return MaterializeResult(metadata=metadata)
