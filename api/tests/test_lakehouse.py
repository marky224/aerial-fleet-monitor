"""Real-DuckDB integration tests for LakehouseQuery.

The seeded_lakehouse fixture writes a 2-row Parquet at a hive-partitioned
path (year=2026/month=05/day=14/hour=12). These tests exercise the actual
DuckDB read path including the TIMESTAMPTZ -> Python datetime conversion
that requires pytz at runtime.
"""

from __future__ import annotations

import duckdb
import pytest

from app.exceptions import UpstreamUnavailable
from app.services.lakehouse import LakehouseQuery


def test_query_reads_seeded_parquet(seeded_lakehouse: LakehouseQuery) -> None:
    """SELECT against the seeded data returns both rows; TIMESTAMPTZ is tz-aware."""
    rows = seeded_lakehouse.query(
        "SELECT icao24, lat, ts_polled "
        "FROM read_parquet($lake_glob, hive_partitioning = true) "
        "ORDER BY ts_polled",
        lake_glob=seeded_lakehouse.positions_glob,
    )
    assert len(rows) == 2
    assert all(r["icao24"] == "a2024b" for r in rows)
    # TIMESTAMPTZ materializes as tz-aware datetime (requires pytz).
    assert rows[0]["ts_polled"].tzinfo is not None
    assert rows[0]["lat"] == 37.6


def test_query_io_error_maps_to_upstream_unavailable() -> None:
    """Missing lake path -> DuckDB IOException -> UpstreamUnavailable (503)."""
    bad_lakehouse = LakehouseQuery(lake_path="/nonexistent/afm-lake-path")
    with pytest.raises(UpstreamUnavailable, match="DuckDB lakehouse read failed"):
        bad_lakehouse.query(
            "SELECT * FROM read_parquet($lake_glob, hive_partitioning = true)",
            lake_glob=bad_lakehouse.positions_glob,
        )


def test_query_sql_error_propagates_unmasked(seeded_lakehouse: LakehouseQuery) -> None:
    """SQL parser/binder errors propagate as duckdb.Error — NOT wrapped as 503.

    The narrow IOException-only mapping in lakehouse.py is intentional:
    masking SQL bugs as 'upstream unavailable' would hide development
    errors behind a misleading status. Bugs should surface loud as 500s.
    """
    with pytest.raises(duckdb.Error):
        seeded_lakehouse.query(
            "SELECT BANANA FROM read_parquet($g, hive_partitioning = true)",
            g=seeded_lakehouse.positions_glob,
        )


def test_query_stream_yields_same_rows_across_fetchmany_batches(
    seeded_lakehouse: LakehouseQuery,
) -> None:
    """query_stream yields the same rows as query, lazily. batch_size=1
    forces multiple fetchmany() pulls so the cross-batch path is exercised
    and the connection stays open for the iterator's lifetime."""
    gen = seeded_lakehouse.query_stream(
        "SELECT icao24, lat FROM read_parquet($lake_glob, hive_partitioning = true) "
        "ORDER BY ts_polled",
        batch_size=1,
        lake_glob=seeded_lakehouse.positions_glob,
    )
    rows = list(gen)
    assert [r["icao24"] for r in rows] == ["a2024b", "a2024b"]
    assert rows[0]["lat"] == 37.6


def test_query_stream_io_error_maps_to_upstream_unavailable() -> None:
    """Missing lake path -> IOException -> UpstreamUnavailable, raised when
    the generator is first consumed (it is lazy)."""
    bad = LakehouseQuery(lake_path="/nonexistent/afm-lake-path")
    with pytest.raises(UpstreamUnavailable, match="DuckDB lakehouse read failed"):
        list(
            bad.query_stream(
                "SELECT * FROM read_parquet($lake_glob, hive_partitioning = true)",
                lake_glob=bad.positions_glob,
            )
        )
