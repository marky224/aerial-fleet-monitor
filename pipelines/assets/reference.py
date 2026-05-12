"""`static_reference` — loads `ref.airports` from OurAirports CSV + watchlist.json.

Reads ``data/airports.csv`` (downloaded via ``scripts/download_airports.py``)
and the bundled ``pipelines/data/watchlist.json``. Upserts every active US
airport with scheduled service into ``ref.airports`` and flips ``is_watched``
+ ``customer_regions`` for the 35 ICAOs named in the watchlist.

Single transaction, idempotent: re-running yields the same end state. The
asset is invoked both for first-time setup (``make db-seed``) and on the
weekly schedule (wired in Phase 01 Step 5).
"""

import csv
import json
import os
from importlib.resources import files
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from psycopg2.extras import execute_values

from pipelines.resources.postgres import PostgresResource

AIRPORTS_CSV_PATH = Path("/srv/data/airports.csv")
"""Container-side path. The Makefile overrides via ``AFM_AIRPORTS_CSV`` for host runs."""

ALLOWED_AIRPORT_TYPES = frozenset({"large_airport", "medium_airport"})

UPSERT_SQL = """
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


def _load_watchlist() -> dict[str, list[str]]:
    """Return a mapping of ``ICAO -> customer_regions`` for the 35 watched airports."""
    raw = files("pipelines.data").joinpath("watchlist.json").read_text()
    parsed = json.loads(raw)
    return {icao: entry["customer_regions"] for icao, entry in parsed["airports"].items()}


def _airports_csv_path() -> Path:
    """Resolve the on-disk CSV path. The ``AFM_AIRPORTS_CSV`` env var wins when set."""
    override = os.environ.get("AFM_AIRPORTS_CSV")
    return Path(override) if override else AIRPORTS_CSV_PATH


def _parse_state(iso_region: str) -> str | None:
    """``US-CA`` -> ``CA``. OurAirports' format is always ``ISO-SUBDIV``."""
    if not iso_region or "-" not in iso_region:
        return None
    return iso_region.split("-", 1)[1] or None


def _parse_int(raw: str) -> int | None:
    return int(raw) if raw not in ("", None) else None


def _build_rows(
    csv_path: Path, watchlist: dict[str, list[str]]
) -> tuple[list[tuple[Any, ...]], set[str]]:
    """Read the OurAirports CSV and return (rows-to-upsert, ICAOs-seen-in-watchlist)."""
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
                    None,  # timezone: not in OurAirports CSV; populated in a later step
                    is_watched,
                    regions,
                )
            )

    return rows, seen_watched


@asset(group_name="reference")
def static_reference(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult:
    csv_path = _airports_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Airports CSV not found at {csv_path}. "
            "Run `python scripts/download_airports.py` first, "
            "or set AFM_AIRPORTS_CSV to an explicit path."
        )

    watchlist = _load_watchlist()
    context.log.info("Loaded watchlist with %d ICAOs", len(watchlist))

    rows, seen_watched = _build_rows(csv_path, watchlist)
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
            execute_values(cur, UPSERT_SQL, rows, page_size=500)
            cur.execute("SELECT COUNT(*) FROM ref.airports")
            total_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ref.airports WHERE is_watched")
            watched_count = cur.fetchone()[0]
        conn.commit()

    if watched_count != len(watchlist):
        raise RuntimeError(
            f"Post-upsert invariant failed: ref.airports has {watched_count} watched rows, "
            f"expected {len(watchlist)}."
        )

    return MaterializeResult(
        metadata={
            "total_airports": MetadataValue.int(total_count),
            "watched_airports": MetadataValue.int(watched_count),
            "csv_source": MetadataValue.path(str(csv_path)),
            "watchlist_size": MetadataValue.int(len(watchlist)),
        }
    )
