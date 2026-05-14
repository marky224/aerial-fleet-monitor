"""`static_reference` — loads `ref.airports` and `ref.aircraft_registry`.

Two reference tables, two CSVs, one asset:

* ``ref.airports`` is loaded from OurAirports ``data/airports.csv`` plus the
  bundled ``pipelines/data/watchlist.json`` (US + scheduled service + large/
  medium airports; watchlist members get ``is_watched = TRUE`` and a
  ``customer_regions`` tag). ~500 rows. Idempotent UPSERT via ``execute_values``.

* ``ref.aircraft_registry`` is loaded from OpenSky ``data/aircraft.csv``
  (~600k rows). Atomic TRUNCATE + ``COPY FROM STDIN`` inside one transaction
  — much faster than per-row UPSERT at this scale and matches the table's
  "may be partial" semantics (OpenSky's DB is the only authority; we don't
  accumulate local state).

The two halves run in **separate transactions**. If the aircraft half fails,
the airports half has already committed; the next materialization re-runs
both (each is idempotent) and converges.

Invoked both for first-time setup (``make db-seed``) and on the weekly
schedule (wired in Phase 01 Step 5).
"""

import csv
import io
import json
import os
from importlib.resources import files
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from psycopg2.extras import execute_values

from pipelines.resources.postgres import PostgresResource

AIRPORTS_CSV_PATH = Path("/srv/data/airports.csv")
AIRCRAFT_CSV_PATH = Path("/srv/data/aircraft.csv")
"""Container-side defaults. The Makefile overrides via ``AFM_AIRPORTS_CSV`` /
``AFM_AIRCRAFT_CSV`` for host execution."""

ALLOWED_AIRPORT_TYPES = frozenset({"large_airport", "medium_airport"})

AIRPORTS_UPSERT_SQL = """
INSERT INTO ref.airports (
    icao, iata, name, city, state, country,
    lat, lon, elevation_ft, timezone,
    is_watched, customer_regions
) VALUES %s
ON CONFLICT (icao) DO UPDATE SET
    iata             = EXCLUDED.iata,
    name             = EXCLUDED.name,
    city             = EXCLUDED.city,
    state            = EXCLUDED.state,
    country          = EXCLUDED.country,
    lat              = EXCLUDED.lat,
    lon              = EXCLUDED.lon,
    elevation_ft     = EXCLUDED.elevation_ft,
    timezone         = EXCLUDED.timezone,
    is_watched       = EXCLUDED.is_watched,
    customer_regions = EXCLUDED.customer_regions
"""

AIRCRAFT_COPY_SQL = (
    "COPY ref.aircraft_registry "
    "(icao24, registration, type_code, type_name, operator, operator_icao, country) "
    "FROM STDIN WITH (FORMAT csv, NULL '')"
)


# ----------------------------------------------------------------------------
# Airports half
# ----------------------------------------------------------------------------


def _load_watchlist() -> dict[str, list[str]]:
    """Return a mapping of ``ICAO -> customer_regions`` for the 35 watched airports."""
    raw = files("pipelines.data").joinpath("watchlist.json").read_text()
    parsed = json.loads(raw)
    return {icao: entry["customer_regions"] for icao, entry in parsed["airports"].items()}


def _airports_csv_path() -> Path:
    override = os.environ.get("AFM_AIRPORTS_CSV")
    return Path(override) if override else AIRPORTS_CSV_PATH


def _parse_state(iso_region: str) -> str | None:
    """``US-CA`` -> ``CA``. OurAirports' format is always ``ISO-SUBDIV``."""
    if not iso_region or "-" not in iso_region:
        return None
    return iso_region.split("-", 1)[1] or None


def _parse_int(raw: str) -> int | None:
    return int(raw) if raw not in ("", None) else None


def _build_airport_rows(
    csv_path: Path, watchlist: dict[str, list[str]]
) -> tuple[list[tuple[Any, ...]], set[str]]:
    rows: list[tuple[Any, ...]] = []
    seen_watched: set[str] = set()

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            if record["iso_country"] != "US":
                continue
            if record["type"] not in ALLOWED_AIRPORT_TYPES:
                continue
            if record["scheduled_service"] != "yes":
                continue

            icao = record["ident"].strip()
            if not icao:
                continue

            regions = watchlist.get(icao, [])
            is_watched = icao in watchlist
            if is_watched:
                seen_watched.add(icao)

            rows.append(
                (
                    icao,
                    record["iata_code"].strip() or None,
                    record["name"].strip(),
                    record["municipality"].strip() or None,
                    _parse_state(record["iso_region"]),
                    "US",
                    float(record["latitude_deg"]),
                    float(record["longitude_deg"]),
                    _parse_int(record["elevation_ft"]),
                    None,  # timezone: not in OurAirports CSV
                    is_watched,
                    regions,
                )
            )

    return rows, seen_watched


def _load_airports(
    context: AssetExecutionContext, postgres: PostgresResource
) -> dict[str, MetadataValue]:
    csv_path = _airports_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Airports CSV not found at {csv_path}. "
            "Run `python scripts/download_airports.py` first, "
            "or set AFM_AIRPORTS_CSV to an explicit path."
        )

    watchlist = _load_watchlist()
    context.log.info("Loaded watchlist with %d ICAOs", len(watchlist))

    rows, seen_watched = _build_airport_rows(csv_path, watchlist)
    context.log.info("Parsed %d US airports with scheduled service from %s", len(rows), csv_path)

    missing = set(watchlist) - seen_watched
    if missing:
        raise ValueError(
            f"Watchlist contains {len(missing)} ICAO(s) not present in airports.csv "
            f"after filtering (US + scheduled_service + large/medium): {sorted(missing)}. "
            "Either the CSV is stale or the watchlist names a wrong ICAO."
        )

    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, AIRPORTS_UPSERT_SQL, rows, page_size=500)
            cur.execute("SELECT COUNT(*) FROM ref.airports")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ref.airports WHERE is_watched")
            watched = cur.fetchone()[0]
        conn.commit()

    if watched != len(watchlist):
        raise RuntimeError(
            f"Post-upsert invariant failed: ref.airports has {watched} watched rows, "
            f"expected {len(watchlist)}."
        )

    return {
        "airports_total": MetadataValue.int(total),
        "airports_watched": MetadataValue.int(watched),
        "airports_csv": MetadataValue.path(str(csv_path)),
        "airports_watchlist_size": MetadataValue.int(len(watchlist)),
    }


# ----------------------------------------------------------------------------
# Aircraft half
# ----------------------------------------------------------------------------


def _aircraft_csv_path() -> Path:
    override = os.environ.get("AFM_AIRCRAFT_CSV")
    return Path(override) if override else AIRCRAFT_CSV_PATH


def _stream_aircraft_csv(csv_path: Path) -> tuple[io.StringIO, int, int, int]:
    """Read OpenSky's aircraft DB CSV and project it into the schema's column set.

    Returns ``(buffer, rows_read, rows_kept, duplicates_skipped)``.

    OpenSky's CSV contains a leading placeholder row with an empty icao24
    plus a small number of true duplicate icao24 entries (≈1k of ~520k).
    The first occurrence wins — deterministic and matches the order of the
    upstream file.

    ``country`` is left NULL for every row: OpenSky's CSV has no country
    field; the icao24 hex prefix encodes country of registration but
    mapping it requires a separate range lookup table that's out of scope
    for this sub-step.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    seen: set[str] = set()
    rows_read = 0
    rows_kept = 0
    duplicates_skipped = 0

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for record in reader:
            rows_read += 1
            icao24 = record["icao24"].strip().lower()
            if not icao24:
                continue
            if icao24 in seen:
                duplicates_skipped += 1
                continue
            seen.add(icao24)
            writer.writerow(
                [
                    icao24,
                    record["registration"].strip() or None,
                    record["typecode"].strip() or None,
                    record["model"].strip() or None,
                    record["operator"].strip() or None,
                    record["operatoricao"].strip() or None,
                    None,  # country
                ]
            )
            rows_kept += 1

    buffer.seek(0)
    return buffer, rows_read, rows_kept, duplicates_skipped


def _load_aircraft(
    context: AssetExecutionContext, postgres: PostgresResource
) -> dict[str, MetadataValue]:
    csv_path = _aircraft_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Aircraft CSV not found at {csv_path}. "
            "Run `python scripts/download_aircraft.py` first, "
            "or set AFM_AIRCRAFT_CSV to an explicit path."
        )

    buffer, rows_read, rows_kept, duplicates_skipped = _stream_aircraft_csv(csv_path)
    context.log.info(
        "Parsed %d aircraft rows from %s (kept %d after deduping; %d duplicate icao24s skipped)",
        rows_read,
        csv_path,
        rows_kept,
        duplicates_skipped,
    )

    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ref.aircraft_registry")
            cur.copy_expert(AIRCRAFT_COPY_SQL, buffer)
            cur.execute("SELECT COUNT(*) FROM ref.aircraft_registry")
            total = cur.fetchone()[0]
        conn.commit()

    if total != rows_kept:
        raise RuntimeError(
            f"Post-load invariant failed: ref.aircraft_registry has {total} rows, "
            f"expected {rows_kept} (rows fed to COPY)."
        )

    return {
        "aircraft_csv_rows_read": MetadataValue.int(rows_read),
        "aircraft_duplicates_skipped": MetadataValue.int(duplicates_skipped),
        "aircraft_rows_loaded": MetadataValue.int(total),
        "aircraft_csv": MetadataValue.path(str(csv_path)),
    }


# ----------------------------------------------------------------------------
# Asset
# ----------------------------------------------------------------------------


@asset(group_name="reference")
def static_reference(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult:
    airport_meta = _load_airports(context, postgres)
    aircraft_meta = _load_aircraft(context, postgres)
    return MaterializeResult(metadata={**airport_meta, **aircraft_meta})
