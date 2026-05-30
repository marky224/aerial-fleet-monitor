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

from datetime import UTC, date, datetime, timedelta

import pytest

from pipelines.resources.lakehouse import FLIGHTS_ARCHIVE_COLUMNS, LakehouseResource


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


# ---------------------------------------------------------------------------
# Flights archive (Phase B cold store)
# ---------------------------------------------------------------------------


def _flight_row(
    flight_id: str,
    icao24: str,
    landed_at: datetime,
    *,
    archived_at: datetime | None = None,
    **over: object,
) -> dict[str, object]:
    """A completed-flight archive row with the wire's JSON-string list fields."""
    row: dict[str, object] = {
        "flight_id": flight_id,
        "icao24": icao24,
        "takeoff_ts": landed_at - timedelta(hours=2),
        "landed_at": landed_at,
        "callsign": "UAL123",
        "registration": "N123UA",
        "aircraft_type": "B738",
        "operator_icao": "UAL",
        "customer_region": "east",
        "origin_icao": "KORD",
        "destination_icao": "KSFO",
        "eta_minutes": None,
        "status": "landed",
        "current_stage": "landed",
        "lat": 37.6,
        "lon": -122.4,
        "open_case_count": 0,
        "open_case_ids": "[]",
        "status_timeline": '[{"stage": "departed", "occurred_at": "2026-05-21T15:00:00Z"}]',
        "trail_2h": '[{"ts": "2026-05-21T16:00:00Z", "lat": 37.6, "lon": -122.4}]',
        "archived_at": archived_at or (landed_at + timedelta(hours=2)),
    }
    row.update(over)
    return row


def test_write_read_roundtrip_preserves_json_list_fields(tmp_path):
    """The three list fields survive the Parquet round-trip as opaque JSON
    strings — exactly the form ontology_writers.flight_params puts on the wire."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    landed = datetime(2026, 5, 21, 17, 30, tzinfo=UTC)
    row = _flight_row("abc123-1700000000", "abc123", landed)

    final_path, written = lake.write_flights_archive([row], landed.date())

    assert written == 1
    # Partition path is keyed on the landed date (no hour component).
    assert "flights_archive/year=2026/month=05/day=21" in str(final_path)

    df = lake.read_flights_archive(30, now=landed + timedelta(hours=3))
    assert len(df) == 1
    got = df.iloc[0]
    assert got["flight_id"] == "abc123-1700000000"
    assert got["icao24"] == "abc123"
    assert got["open_case_ids"] == "[]"
    assert got["status_timeline"] == row["status_timeline"]
    assert got["trail_2h"] == row["trail_2h"]
    # position/trail_path are NOT archived — only the source lat/lon + trail.
    assert "position" not in df.columns
    assert "trail_path" not in df.columns


def test_reads_across_two_day_partitions_and_applies_cutoff(tmp_path):
    """A multi-day lookback unions day-partition dirs, and the landed_at >=
    cutoff predicate excludes rows that landed earlier the same day."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)  # cutoff (lookback 2) = 05-20 12:00

    early_20 = datetime(2026, 5, 20, 8, 0, tzinfo=UTC)  # same dir as f2, before cutoff
    late_20 = datetime(2026, 5, 20, 18, 0, tzinfo=UTC)
    on_21 = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_flight_row("f0", "aaa", early_20)], early_20.date())
    lake.write_flights_archive([_flight_row("f2", "bbb", late_20)], late_20.date())
    lake.write_flights_archive([_flight_row("f3", "ccc", on_21)], on_21.date())

    df = lake.read_flights_archive(2, now=now)

    # f2 (05-20 18:00 ≥ cutoff) + f3 (05-21) — across two day-dirs;
    # f0 (05-20 08:00, before the 05-20 12:00 cutoff) excluded by the predicate.
    assert set(df["flight_id"]) == {"f2", "f3"}


def test_read_lookback_none_returns_entire_archive(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    old = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)
    recent = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_flight_row("old", "aaa", old)], old.date())
    lake.write_flights_archive([_flight_row("new", "bbb", recent)], recent.date())

    df = lake.read_flights_archive(lookback_days=None)
    assert set(df["flight_id"]) == {"old", "new"}


def test_read_columns_projection_omits_heavy_fields(tmp_path):
    """columns= projects to a subset so the exactly-once-move check can pull
    just flight_id over the whole archive without loading the trail JSON."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    landed = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_flight_row("p1", "aaa", landed)], landed.date())

    df = lake.read_flights_archive(lookback_days=None, columns=["flight_id"])
    assert list(df.columns) == ["flight_id"]
    assert set(df["flight_id"]) == {"p1"}
    assert "trail_2h" not in df.columns

    # Projection composes with the landed_at window predicate too.
    windowed = lake.read_flights_archive(30, now=landed + timedelta(hours=1), columns=["flight_id"])
    assert list(windowed.columns) == ["flight_id"]
    assert set(windowed["flight_id"]) == {"p1"}

    # Empty archive still yields exactly the projected columns.
    empty = LakehouseResource(lake_path=str(tmp_path / "void"))
    empty_df = empty.read_flights_archive(lookback_days=None, columns=["flight_id"])
    assert empty_df.empty
    assert list(empty_df.columns) == ["flight_id"]


def test_count_flights_archive_totals_all_partitions(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    assert lake.count_flights_archive() == 0
    d20 = datetime(2026, 5, 20, 18, 0, tzinfo=UTC)
    d21 = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    # Two files in the 05-20 dir + one in 05-21 → 3 rows total.
    lake.write_flights_archive([_flight_row("a", "aaa", d20)], d20.date())
    lake.write_flights_archive([_flight_row("b", "bbb", d20)], d20.date())
    lake.write_flights_archive([_flight_row("c", "ccc", d21)], d21.date())
    assert lake.count_flights_archive() == 3


def test_count_flights_archived_at_filters_on_run_stamp(tmp_path):
    """Counts only rows carrying exactly one run's archived_at — the basis for
    the row-count check (robust to a concurrent purge of old partitions)."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    landed = datetime(2026, 5, 21, 17, 0, tzinfo=UTC)
    run_a = datetime(2026, 5, 21, 19, 0, tzinfo=UTC)
    run_b = datetime(2026, 5, 21, 20, 0, tzinfo=UTC)
    lake.write_flights_archive(
        [
            _flight_row("a1", "aaa", landed, archived_at=run_a),
            _flight_row("a2", "bbb", landed, archived_at=run_a),
        ],
        landed.date(),
    )
    lake.write_flights_archive([_flight_row("b1", "ccc", landed, archived_at=run_b)], landed.date())

    assert lake.count_flights_archived_at(run_a) == 2
    assert lake.count_flights_archived_at(run_b) == 1
    assert lake.count_flights_archived_at(datetime(2026, 5, 21, 21, 0, tzinfo=UTC)) == 0


def test_oldest_flights_archive_partition(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    assert lake.oldest_flights_archive_partition() is None
    early = datetime(2026, 5, 20, 10, 0, tzinfo=UTC)
    late = datetime(2026, 5, 22, 10, 0, tzinfo=UTC)
    lake.write_flights_archive([_flight_row("a", "aaa", late)], late.date())
    lake.write_flights_archive([_flight_row("b", "bbb", early)], early.date())
    assert lake.oldest_flights_archive_partition() == date(2026, 5, 20)


def test_purge_drops_old_day_dirs_and_keeps_recent(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    d20 = datetime(2026, 5, 20, 18, 0, tzinfo=UTC)
    d21 = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_flight_row("a", "aaa", d20)], d20.date())
    lake.write_flights_archive([_flight_row("c", "ccc", d21)], d21.date())

    dropped = lake.purge_flights_archive_before(date(2026, 5, 21))

    assert dropped == [date(2026, 5, 20)]
    # Only the 05-21 partition survives.
    assert lake.count_flights_archive() == 1
    assert set(lake.read_flights_archive(lookback_days=None)["flight_id"]) == {"c"}
    assert not (tmp_path / "flights_archive/year=2026/month=05/day=20").exists()
    assert (tmp_path / "flights_archive/year=2026/month=05/day=21").exists()


def test_write_empty_rows_raises(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    with pytest.raises(ValueError, match="zero rows"):
        lake.write_flights_archive([], date(2026, 5, 21))


def test_empty_archive_returns_correctly_columned_frame(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    df = lake.read_flights_archive(30, now=datetime(2026, 5, 21, 18, 10, tzinfo=UTC))
    assert df.empty
    assert list(df.columns) == list(FLIGHTS_ARCHIVE_COLUMNS)
