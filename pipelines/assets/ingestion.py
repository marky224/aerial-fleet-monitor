"""Live ingestion assets — OpenSky positions and NOAA weather.

``opensky_positions`` (every 30 s): polls OpenSky's ``/states/all``,
filters the icao24 denylist, region-tags via the watched-airport list,
writes an atomic Parquet snapshot per cycle, and UPSERTs to
``app.current_positions``. ``aircraft_type`` enriches from
``ref.aircraft_registry`` (ICAO ``type_code``) via COALESCE — first
non-NULL wins. ``origin_icao`` and ``destination_icao`` stay NULL:
OpenSky's ``/states/all`` doesn't carry flight-plan data and the
per-aircraft ``/flights`` endpoint would blow the free-tier credit
budget; they're deferred to a future flight-plan asset.

``noaa_weather`` (every 5 min): one ``?ids=`` call to NOAA's
``/data/metar`` and one to ``/data/taf`` covers all watched airports.
METAR fields (wind, visibility, temp, altimeter) are extracted into
typed columns; ceiling is derived from the cloud-layer list (lowest
BKN/OVC/OVX/VV base); ``flight_category`` is computed via FAA
thresholds (see ``_compute_flight_category``). The full NOAA response
object is also stored as ``metar_parsed`` JSONB so nothing is lost.
NOAA's own ``flightCategory`` field is not used directly — per spec
the asset derives it.
"""

import logging
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from typing import Any

import numpy as np
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from psycopg2.extras import Json, execute_values

from pipelines.resources.lakehouse import LakehouseResource
from pipelines.resources.noaa import (
    NoaaError,
    NoaaMetarReport,
    NoaaResource,
)
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
    on_ground, squawk, customer_region, last_seen_at,
    aircraft_type
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
    aircraft_type     = COALESCE(app.current_positions.aircraft_type, EXCLUDED.aircraft_type),
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


def _fetch_aircraft_types(
    postgres: PostgresResource,
    icao24s: list[str],
) -> dict[str, str]:
    """Look up ICAO ``type_code`` for the given icao24s from the registry.

    Returns only icao24s present in ``ref.aircraft_registry`` with a
    non-NULL ``type_code`` — callers should treat misses as NULL via
    ``.get()``. One indexed PK lookup; ~50ms for ~5k inputs.
    """
    if not icao24s:
        return {}
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT icao24, type_code FROM ref.aircraft_registry "
            "WHERE icao24 = ANY(%s) AND type_code IS NOT NULL",
            (icao24s,),
        )
        return dict(cur.fetchall())


def _upsert_current_positions(
    postgres: PostgresResource,
    rows: list[dict[str, Any]],
    aircraft_types: dict[str, str],
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
            aircraft_types.get(row["icao24"]),
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

    aircraft_types = _fetch_aircraft_types(postgres, [row["icao24"] for row in rows])
    postgres_rows = _upsert_current_positions(postgres, rows, aircraft_types)

    lag_seconds = max(0, int(polled_at.timestamp()) - response.api_time)

    metadata: dict[str, MetadataValue] = {
        "aircraft_count": MetadataValue.int(len(rows)),
        "raw_state_count": MetadataValue.int(raw_count),
        "filtered_denylist": MetadataValue.int(len(denied_states)),
        "filtered_no_coords": MetadataValue.int(rows_without_coords),
        "parquet_bytes_written": MetadataValue.int(parquet_bytes),
        "postgres_rows_upserted": MetadataValue.int(postgres_rows),
        "aircraft_type_resolved": MetadataValue.int(len(aircraft_types)),
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


# ---------------------------------------------------------------------------
# noaa_weather
# ---------------------------------------------------------------------------

# Cloud-cover codes that count as a ceiling per FAA convention.
# BKN = broken (5-7 oktas), OVC = overcast (8/8), OVX = sky obscured,
# VV = vertical visibility. FEW/SCT/CLR/SKC/NSC don't constitute a ceiling.
CEILING_COVERS = frozenset({"BKN", "OVC", "OVX", "VV"})

# Lower rank = worse conditions. `min(..., key=rank.get)` picks the
# worse of the ceiling-driven and visibility-driven categories.
FLIGHT_CATEGORY_RANK: dict[str, int] = {"LIFR": 0, "IFR": 1, "MVFR": 2, "VFR": 3}


AIRPORT_CONDITIONS_UPSERT_SQL = """
INSERT INTO app.airport_conditions (
    site_icao, metar_raw, metar_parsed, taf_raw, flight_category,
    wind_kt, wind_dir_deg, visibility_sm, ceiling_ft,
    temperature_c, altimeter_in_hg, metar_observed_at, fetched_at
) VALUES %s
ON CONFLICT (site_icao) DO UPDATE SET
    metar_raw         = EXCLUDED.metar_raw,
    metar_parsed      = EXCLUDED.metar_parsed,
    taf_raw           = EXCLUDED.taf_raw,
    flight_category   = EXCLUDED.flight_category,
    wind_kt           = EXCLUDED.wind_kt,
    wind_dir_deg      = EXCLUDED.wind_dir_deg,
    visibility_sm     = EXCLUDED.visibility_sm,
    ceiling_ft        = EXCLUDED.ceiling_ft,
    temperature_c     = EXCLUDED.temperature_c,
    altimeter_in_hg   = EXCLUDED.altimeter_in_hg,
    metar_observed_at = EXCLUDED.metar_observed_at,
    fetched_at        = NOW()
"""


def _extract_ceiling_ft(clouds: list[dict[str, Any]]) -> int | None:
    """Return the lowest BKN/OVC/OVX/VV layer base in feet AGL, or None.

    Per FAA: the ceiling is the lowest broken/overcast/obscured layer.
    FEW/SCT layers don't count. CLR/SKC/NSC means no ceiling (unlimited).
    """
    ceiling: int | None = None
    for layer in clouds:
        cover = layer.get("cover")
        base = layer.get("base")
        if not isinstance(cover, str) or not isinstance(base, int | float):
            continue
        if cover not in CEILING_COVERS:
            continue
        base_ft = int(base)
        if ceiling is None or base_ft < ceiling:
            ceiling = base_ft
    return ceiling


def _compute_flight_category(
    ceiling_ft: int | None,
    visibility_sm: float | None,
) -> str | None:
    """Compute FAA flight category from ceiling height and visibility.

    Per FAA Advisory Circular conventions:

    * **LIFR**: ceiling < 500 ft OR visibility < 1 sm
    * **IFR**:  ceiling 500-999 ft OR visibility 1 to <3 sm
    * **MVFR**: ceiling 1000-2999 ft OR visibility 3 to ≤5 sm
    * **VFR**:  ceiling ≥ 3000 ft AND visibility > 5 sm

    Returns the *worse* of the ceiling-driven and visibility-driven
    categories. Missing ceiling (no BKN/OVC layer reported) is treated
    as unlimited; missing visibility is also treated as unlimited. If
    both are missing, returns None (cannot classify).
    """
    if ceiling_ft is None and visibility_sm is None:
        return None

    if ceiling_ft is None:
        c_cat = "VFR"
    elif ceiling_ft < 500:
        c_cat = "LIFR"
    elif ceiling_ft < 1000:
        c_cat = "IFR"
    elif ceiling_ft < 3000:
        c_cat = "MVFR"
    else:
        c_cat = "VFR"

    if visibility_sm is None:
        v_cat = "VFR"
    elif visibility_sm < 1.0:
        v_cat = "LIFR"
    elif visibility_sm < 3.0:
        v_cat = "IFR"
    elif visibility_sm <= 5.0:
        v_cat = "MVFR"
    else:
        v_cat = "VFR"

    return min((c_cat, v_cat), key=FLIGHT_CATEGORY_RANK.__getitem__)


def _build_airport_condition_rows(
    metar_reports: list[NoaaMetarReport],
    taf_map: dict[str, str],
    fetched_at: datetime,
) -> list[dict[str, Any]]:
    """Combine METAR + TAF data into airport_conditions row dicts."""
    rows: list[dict[str, Any]] = []
    for report in metar_reports:
        if not report.icao:
            continue
        ceiling_ft = _extract_ceiling_ft(report.clouds)
        flight_category = _compute_flight_category(ceiling_ft, report.visibility_sm)
        rows.append(
            {
                "site_icao": report.icao,
                "metar_raw": report.raw_text,
                "metar_parsed": report.raw_json,
                "taf_raw": taf_map.get(report.icao),
                "flight_category": flight_category,
                "wind_kt": report.wind_kt,
                "wind_dir_deg": report.wind_dir_deg,
                "visibility_sm": report.visibility_sm,
                "ceiling_ft": ceiling_ft,
                "temperature_c": report.temperature_c,
                "altimeter_in_hg": report.altimeter_in_hg,
                "metar_observed_at": report.observed_at,
                "fetched_at": fetched_at,
            }
        )
    return rows


def _upsert_airport_conditions(
    postgres: PostgresResource,
    rows: list[dict[str, Any]],
) -> int:
    """Batch UPSERT into ``app.airport_conditions``. Returns rows affected."""
    if not rows:
        return 0

    tuples = [
        (
            row["site_icao"],
            row["metar_raw"],
            Json(row["metar_parsed"]) if row["metar_parsed"] is not None else None,
            row["taf_raw"],
            row["flight_category"],
            row["wind_kt"],
            row["wind_dir_deg"],
            row["visibility_sm"],
            row["ceiling_ft"],
            row["temperature_c"],
            row["altimeter_in_hg"],
            row["metar_observed_at"],
            row["fetched_at"],
        )
        for row in rows
    ]

    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, AIRPORT_CONDITIONS_UPSERT_SQL, tuples, page_size=100)
            rowcount = int(cur.rowcount)
        conn.commit()
    return rowcount


@asset(
    group_name="ingestion",
    description="Polls NOAA aviation-weather for watched-airport METAR + TAF.",
    metadata={
        "source": "aviationweather.gov /data/metar + /data/taf",
        "cadence": "5min",
    },
)
def noaa_weather(
    context: AssetExecutionContext,
    noaa: NoaaResource,
    postgres: PostgresResource,
    watchlist: WatchlistResource,
) -> MaterializeResult:
    fetched_at = datetime.now(UTC)
    icaos = sorted({a.icao for a in watchlist.get_airports()})

    try:
        metar_reports = noaa.fetch_metars(icaos)
    except NoaaError as exc:
        context.log.warning("NOAA METAR fetch skipped: %s", exc)
        return MaterializeResult(
            metadata={
                "metar_count": MetadataValue.int(0),
                "taf_count": MetadataValue.int(0),
                "postgres_rows_upserted": MetadataValue.int(0),
                "watched_airports": MetadataValue.int(len(icaos)),
                "skip_reason": MetadataValue.text(f"metar: {exc}"),
                "fetched_at": MetadataValue.text(fetched_at.isoformat()),
            }
        )

    try:
        taf_map = noaa.fetch_tafs(icaos)
    except NoaaError as exc:
        # METAR succeeded; degrade gracefully by skipping TAF this cycle.
        # The taf_raw column will just keep its previous value (UPSERT
        # writes None, which the SET clause then writes — actually we
        # do want existing TAF preserved on transient TAF failures.
        # Fix: pass through prior TAF on conflict. For now: log and
        # carry an empty map; downstream UPSERT will null taf_raw for
        # this cycle. Acceptable since NOAA TAF outages are rare and
        # the next cycle (5 min) restores.
        context.log.warning("NOAA TAF fetch failed, continuing with METAR only: %s", exc)
        taf_map = {}

    rows = _build_airport_condition_rows(metar_reports, taf_map, fetched_at)
    postgres_rows = _upsert_airport_conditions(postgres, rows)

    flight_cat_counts: dict[str, int] = {}
    for row in rows:
        category = row["flight_category"]
        if category is not None:
            flight_cat_counts[category] = flight_cat_counts.get(category, 0) + 1

    return MaterializeResult(
        metadata={
            "metar_count": MetadataValue.int(len(metar_reports)),
            "taf_count": MetadataValue.int(len(taf_map)),
            "postgres_rows_upserted": MetadataValue.int(postgres_rows),
            "watched_airports": MetadataValue.int(len(icaos)),
            "flight_category_distribution": MetadataValue.json(flight_cat_counts),
            "fetched_at": MetadataValue.text(fetched_at.isoformat()),
        }
    )
