"""Tests for LakehouseResource.read_recent_positions.

Regression coverage for the multi-hour-partition read path: the live lake
stores positions under year=/month=/day=/hour= directories, so a 60-minute
lookback that straddles the top of the hour spans *two* partition dirs.
`pyarrow.dataset()` treats a list of paths as *files*, not directories, so
passing a list of partition dirs raised `IsADirectoryError` — a defect that
only surfaced against the real hour-partitioned lake (the detection asset's
unit tests mock the lakehouse, so they never exercised this). These tests
write real parquet partitions to a tmp lake and read them back.
"""

from datetime import UTC, datetime, timedelta

from pipelines.resources.lakehouse import LakehouseResource


def _row(icao24: str, region: str | None = "east") -> dict[str, object]:
    return {
        "icao24": icao24,
        "lat": 38.95,
        "lon": -77.45,
        "on_ground": False,
        "customer_region": region,
    }


def test_reads_across_two_hour_partitions_and_applies_cutoff(tmp_path):
    """A 60-min window straddling the hour reads BOTH partition dirs, and
    the ts_polled >= cutoff predicate excludes rows before the window."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 21, 18, 10, tzinfo=UTC)  # cutoff = 17:10

    lake.write_positions_snapshot([_row("aaa111")], datetime(2026, 5, 21, 17, 5, tzinfo=UTC))
    lake.write_positions_snapshot([_row("bbb222")], datetime(2026, 5, 21, 17, 30, tzinfo=UTC))
    lake.write_positions_snapshot([_row("ccc333")], datetime(2026, 5, 21, 18, 5, tzinfo=UTC))

    df = lake.read_recent_positions(60, now=now)

    # bbb222 (hour=17, in window) + ccc333 (hour=18) — across two dirs;
    # aaa111 (17:05, before the 17:10 cutoff) excluded by the predicate.
    assert set(df["icao24"]) == {"bbb222", "ccc333"}
    cutoff = now - timedelta(minutes=60)
    assert (df["ts_polled"] >= cutoff).all()


def test_empty_lake_returns_correctly_columned_frame(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    df = lake.read_recent_positions(60, now=datetime(2026, 5, 21, 18, 10, tzinfo=UTC))
    assert df.empty
    assert "icao24" in df.columns and "customer_region" in df.columns
