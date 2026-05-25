"""Shared builders for rule unit tests.

``make_positions`` assembles a DataFrame matching the enriched-positions
column contract (see ``rules/base.py``), filling sensible defaults so a
test only specifies the fields its rule cares about. ``cases_frame``
builds an existing-cases DataFrame for dedup tests.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

# A fixed "now" for deterministic gap math. The latest ts_polled in a
# frame is what rules treat as "now", so tests anchor to this.
NOW = dt.datetime(2026, 5, 21, 18, 0, 0, tzinfo=dt.UTC)

_ENRICHED_COLUMNS = [
    "icao24",
    "callsign",
    "lat",
    "lon",
    "altitude_ft",
    "speed_kt",
    "heading_deg",
    "vertical_rate_fpm",
    "on_ground",
    "squawk",
    "ts_polled",
    "customer_region",
    "origin_icao",
    "destination_icao",
    "departure_time",
    "nearest_site_icao",
    "nearest_site_distance_nm",
]

_DEFAULTS: dict[str, Any] = {
    "icao24": "abc123",
    "callsign": "TEST123",
    "lat": 37.0,
    "lon": -122.0,
    "altitude_ft": 35_000,
    "speed_kt": 450,
    "heading_deg": 90,
    "vertical_rate_fpm": 0,
    "on_ground": False,
    "squawk": "1200",
    "ts_polled": NOW,
    "customer_region": "west",
    "origin_icao": None,
    "destination_icao": None,
    "departure_time": None,
    "nearest_site_icao": None,
    "nearest_site_distance_nm": 999.0,
}


def mins(n: float) -> dt.timedelta:
    return dt.timedelta(minutes=n)


def make_positions(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Build an enriched positions frame; each row overrides the defaults."""
    full = [{**_DEFAULTS, **row} for row in rows]
    df = pd.DataFrame(full, columns=_ENRICHED_COLUMNS)
    df["ts_polled"] = pd.to_datetime(df["ts_polled"], utc=True)
    return df


def empty_cases() -> pd.DataFrame:
    """An empty existing-cases frame with the columns dedup reads."""
    return pd.DataFrame(
        columns=["case_type", "flight_id", "site_icao", "detection_facts", "created_at"]
    )


def cases_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Build an existing-cases frame for dedup tests."""
    df = pd.DataFrame(
        rows,
        columns=["case_type", "flight_id", "site_icao", "detection_facts", "created_at"],
    )
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df
