"""Parquet lakehouse resource.

Step 3 scope: a single ``write_positions_snapshot`` helper that takes a
list of position rows and writes one Parquet file per polling cycle
under the Hive-style layout from ``DATA_MODEL.md`` §4.1.

The write is atomic: pyarrow serializes to a sibling temp path, then
``os.rename`` moves it to the final path in one inode operation. This
keeps DuckDB scans (used by later assets and by the acceptance-criteria
verification query) from ever seeing a half-written file.

DuckDB session helpers will be added when ``case_detector`` and
``site_metrics_refresh`` land in later phases; they're not needed for
ingestion writes.

Column order and types follow ``DATA_MODEL.md`` §4.2 exactly. The schema
constant is module-level so every cycle writes the same file layout —
adding a column later is backward-compatible (DuckDB returns NULL for
missing); renaming or removing one needs a backfill, not just a code
change.

Phase B adds a second partition root, ``flights_archive/year=/month=/day=``
(partitioned by LANDED date, no hour) — the durable cold store for completed
Flight records evicted from the Foundry Ontology before deletion.
``write_flights_archive`` / ``read_flights_archive`` / ``count_flights_archive``
/ ``purge_flights_archive_before`` mirror the positions helpers; 30-day
retention is a day-dir unlink, never a row-level delete. The list fields
(trail / timeline / open_case_ids) ride as JSON strings exactly as they
serialize on the Foundry wire (``ontology_writers.flight_params``). See
``_private/docs/build/flight_lifecycle_archive.md`` § Phase B.
"""

from __future__ import annotations

import logging
import os
import shutil
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


# Per DATA_MODEL.md §4.2. Column order is significant for downstream DuckDB
# scans that select by position; we always write in this exact order.
POSITIONS_SCHEMA = pa.schema(
    [
        pa.field("icao24", pa.string(), nullable=False),
        pa.field("callsign", pa.string()),
        pa.field("origin_country", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("altitude_ft", pa.int32()),
        pa.field("speed_kt", pa.int32()),
        pa.field("heading_deg", pa.int32()),
        pa.field("vertical_rate_fpm", pa.int32()),
        pa.field("on_ground", pa.bool_(), nullable=False),
        pa.field("squawk", pa.string()),
        pa.field("ts_polled", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ts_position", pa.timestamp("us", tz="UTC")),
        pa.field("customer_region", pa.string()),
    ]
)

POSITIONS_COLUMNS: tuple[str, ...] = tuple(field.name for field in POSITIONS_SCHEMA)

PARQUET_COMPRESSION = "zstd"


# Phase B cold store. snake_case columns mirroring the Foundry Flight object
# (foundry/sync/.../models.py::Flight) one-for-one, EXCEPT the two write-time
# geo derivations (``position``/``trail_path``) — those are rebuilt from
# (lat, lon) and ``trail_2h`` on read, so the archive stores the source data,
# not the projection. The three list fields ride as JSON strings exactly as
# ``ontology_writers.flight_params`` serializes them onto the wire (and exactly
# as the tenant stores them, so a reconcile scan hands them back unchanged).
# ``archived_at`` is the only non-Flight column: the cold-store entry instant,
# stamped by the archive asset for observability + dedup-on-read after a
# crash-retry. Non-nullable fields are the archive's invariants: every record
# is a *completed* flight (``landed_at`` set) with a synthesized identity.
FLIGHTS_ARCHIVE_SCHEMA = pa.schema(
    [
        # Identity / synthesis (minted at takeoff; always present)
        pa.field("flight_id", pa.string(), nullable=False),
        pa.field("icao24", pa.string(), nullable=False),
        pa.field("takeoff_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        # Completion (the archive scope: completed flights only)
        pa.field("landed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        # Identity (enriched)
        pa.field("callsign", pa.string()),
        pa.field("registration", pa.string()),
        pa.field("aircraft_type", pa.string()),
        pa.field("operator_icao", pa.string()),
        pa.field("customer_region", pa.string()),
        # Routing (enriched)
        pa.field("origin_icao", pa.string()),
        pa.field("destination_icao", pa.string()),
        pa.field("eta_minutes", pa.int32()),
        # Current status (denormalized from the timeline tail)
        pa.field("status", pa.string()),
        pa.field("current_stage", pa.string()),
        # Last-known position (geopoint rebuilt from these on read)
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        # Open cases (Phase 05)
        pa.field("open_case_count", pa.int32()),
        pa.field("open_case_ids", pa.string()),  # JSON string, like the wire
        # History (JSON strings, like the wire)
        pa.field("status_timeline", pa.string()),
        pa.field("trail_2h", pa.string()),
        # Archive metadata (the only non-Flight column)
        pa.field("archived_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

FLIGHTS_ARCHIVE_COLUMNS: tuple[str, ...] = tuple(field.name for field in FLIGHTS_ARCHIVE_SCHEMA)


def _table_from_rows(
    rows: Sequence[dict[str, Any]],
    columns: tuple[str, ...],
    schema: pa.Schema,
) -> pa.Table:
    """Build a schema-conformant Arrow table from row dicts, column-wise.

    Going column-wise (rather than ``Table.from_pylist``) lets the explicit
    ``schema`` enforce types and surface a clear error on drift in a single
    pass. Missing keys become None; extra keys are ignored.
    """
    cols: dict[str, list[Any]] = {name: [] for name in columns}
    for row in rows:
        for name in columns:
            cols[name].append(row.get(name))
    return pa.table(cols, schema=schema)


class LakehouseResource(ConfigurableResource):  # type: ignore[type-arg]
    """Filesystem-backed Parquet store rooted at ``lake_path``.

    Attributes:
        lake_path: Root directory for the Parquet tree. ``/lake`` in
            container; overridable via ``AFM_LAKE_PATH`` env var (injected
            in ``pipelines/definitions.py``) for host-side runs.
    """

    lake_path: str

    def write_positions_snapshot(
        self,
        rows: Sequence[dict[str, Any]],
        polled_at: datetime,
    ) -> tuple[Path, int]:
        """Atomically write one polling cycle's rows to the positions partition.

        Args:
            rows: Position records keyed by ``POSITIONS_COLUMNS``. Missing
                keys are treated as None. Extra keys are ignored.
            polled_at: The poll timestamp (UTC). Drives both the partition
                path and the ``ts_polled`` column for every row.

        Returns:
            ``(final_path, bytes_written)``. ``final_path`` is the
            absolute path of the committed Parquet file.

        Raises:
            ValueError: ``rows`` is empty (caller should skip the write
                entirely on empty cycles — see ``ingestion.py``).
            OSError: Filesystem error during write or rename. The temp
                file is best-effort cleaned up before re-raising.
        """
        if not rows:
            raise ValueError(
                "write_positions_snapshot called with zero rows; "
                "the asset should skip the write on empty cycles."
            )
        if polled_at.tzinfo is None or polled_at.utcoffset() is None:
            raise ValueError(
                "polled_at must be timezone-aware; a naive datetime would be "
                "interpreted in the host's local tz and corrupt partition paths."
            )

        polled_at_utc = polled_at.astimezone(UTC)

        partition_dir = (
            Path(self.lake_path)
            / "positions"
            / (f"year={polled_at_utc.year:04d}")
            / (f"month={polled_at_utc.month:02d}")
            / (f"day={polled_at_utc.day:02d}")
            / (f"hour={polled_at_utc.hour:02d}")
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Microsecond suffix + short UUID prevents collisions on retries or
        # concurrent writes within the same second (a Dagster retry policy
        # lands in Step 5; manual materializes during a scheduled cycle are
        # already a possibility today).
        filename = f"snapshot_{polled_at_utc.strftime('%H%M%S_%f')}_{uuid4().hex[:8]}.parquet"
        final_path = partition_dir / filename
        temp_path = partition_dir / f".{filename}.tmp"

        # Enforce ts_polled = polled_at_utc on every row so the column always
        # matches the partition path, regardless of what the caller passed.
        normalized_rows = [{**row, "ts_polled": polled_at_utc} for row in rows]
        table = self._build_table(normalized_rows)

        try:
            pq.write_table(table, temp_path, compression=PARQUET_COMPRESSION)
            os.rename(temp_path, final_path)
        except Exception:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError as cleanup_exc:
                    logger.warning("Failed to clean up temp file %s: %s", temp_path, cleanup_exc)
            raise

        bytes_written = final_path.stat().st_size
        return final_path.resolve(), bytes_written

    def read_recent_positions(
        self,
        lookback_minutes: int = 60,
        *,
        now: datetime | None = None,
    ) -> pd.DataFrame:
        """Read the last ``lookback_minutes`` of position snapshots.

        Reads only the hour-partition directories spanning the window
        (1-2 dirs) and predicate-pushes ``ts_polled >= cutoff`` so the
        scan stays bounded regardless of how much history the lake holds.
        Returns an empty (correctly-columned) frame when no partitions
        in range exist yet.

        DuckDB is the project's analytical engine for complex SQL
        (site_metrics); this last-hour filter+load needs none of that, so
        it uses pyarrow.dataset (already a dependency) instead.
        """
        now = now or datetime.now(UTC)
        cutoff = now - timedelta(minutes=lookback_minutes)
        dirs = self._recent_partition_dirs(cutoff, now)
        if not dirs:
            return pd.DataFrame(columns=list(POSITIONS_COLUMNS))
        # pyarrow.dataset() treats a list as *file* paths; a list of hour
        # partition *directories* must be unioned as dataset objects (each
        # discovers its own parquet files), per pyarrow's own guidance.
        dataset = pds.dataset([pds.dataset(str(d), format="parquet") for d in dirs])
        cutoff_scalar = pa.scalar(cutoff, type=pa.timestamp("us", tz="UTC"))
        table = dataset.to_table(filter=pds.field("ts_polled") >= cutoff_scalar)
        return cast("pd.DataFrame", table.to_pandas())

    def _recent_partition_dirs(self, cutoff: datetime, now: datetime) -> list[Path]:
        """Existing hour-partition dirs from ``cutoff``'s hour through ``now``'s."""
        root = Path(self.lake_path) / "positions"
        dirs: list[Path] = []
        cur = cutoff.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        end = now.astimezone(UTC)
        while cur <= end:
            part = (
                root
                / f"year={cur.year:04d}"
                / f"month={cur.month:02d}"
                / f"day={cur.day:02d}"
                / f"hour={cur.hour:02d}"
            )
            if part.exists():
                dirs.append(part)
            cur += timedelta(hours=1)
        return dirs

    @staticmethod
    def _build_table(rows: Sequence[dict[str, Any]]) -> pa.Table:
        return _table_from_rows(rows, POSITIONS_COLUMNS, POSITIONS_SCHEMA)

    # ------------------------------------------------------------------
    # Flights archive (Phase B cold store)
    # ------------------------------------------------------------------

    def write_flights_archive(
        self,
        rows: Sequence[dict[str, Any]],
        landed_date: date,
    ) -> tuple[Path, int]:
        """Atomically write completed-flight records to a landed-date partition.

        Args:
            rows: Flight archive records keyed by ``FLIGHTS_ARCHIVE_COLUMNS``.
                Missing keys are treated as None; extra keys are ignored.
                Every row MUST carry the schema's non-nullable fields
                (``flight_id``/``icao24``/``takeoff_ts``/``landed_at``/
                ``archived_at``) or the pyarrow build raises — a loud signal
                of an asset bug. ``landed_at`` is the flight's own landing
                instant; the caller groups rows so they all share
                ``landed_date``.
            landed_date: The UTC calendar date all ``rows`` landed on. Drives
                the partition path only (year/month/day). The caller computes
                it as ``landed_at.astimezone(UTC).date()``; partitioning by
                landed date makes 30-day retention a whole-directory unlink.

        Returns:
            ``(final_path, rows_written)`` — the committed Parquet file's
            absolute path and the number of rows in it.

        Raises:
            ValueError: ``rows`` is empty (the asset should skip empty runs).
            OSError: Filesystem error during write or rename; the temp file
                is best-effort cleaned up before re-raising.
        """
        if not rows:
            raise ValueError(
                "write_flights_archive called with zero rows; "
                "the asset should skip the write when nothing is archived."
            )

        partition_dir = (
            Path(self.lake_path)
            / "flights_archive"
            / f"year={landed_date.year:04d}"
            / f"month={landed_date.month:02d}"
            / f"day={landed_date.day:02d}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        # A day-level partition has no finer time component to key the
        # filename on, and the archive asset runs hourly — so several files
        # land in the same day-dir. A short UUID alone guarantees uniqueness
        # across those runs (and across crash-retries that re-archive a id).
        filename = f"flights_{uuid4().hex[:12]}.parquet"
        final_path = partition_dir / filename
        temp_path = partition_dir / f".{filename}.tmp"

        table = _table_from_rows(rows, FLIGHTS_ARCHIVE_COLUMNS, FLIGHTS_ARCHIVE_SCHEMA)

        try:
            pq.write_table(table, temp_path, compression=PARQUET_COMPRESSION)
            os.rename(temp_path, final_path)
        except Exception:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError as cleanup_exc:
                    logger.warning("Failed to clean up temp file %s: %s", temp_path, cleanup_exc)
            raise

        return final_path.resolve(), len(rows)

    def read_flights_archive(
        self,
        lookback_days: int | None = 30,
        *,
        now: datetime | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read archived flights, optionally bounded to a recent landed window.

        With ``lookback_days`` set (default 30, the retention window), reads
        only the day-partition dirs spanning ``[now - lookback_days, now]``
        and predicate-pushes ``landed_at >= cutoff`` so the scan stays
        bounded. Pass ``lookback_days=None`` to read the entire retained
        archive (e.g. the exactly-once-move asset check, which compares every
        archived ``flight_id`` against the live tenant).

        ``columns`` projects to a subset of ``FLIGHTS_ARCHIVE_COLUMNS``;
        pyarrow never reads the unselected columns off disk, so the
        exactly-once-move check can pull just ``["flight_id"]`` over the whole
        archive without materializing the heavy ``trail_2h`` / ``status_timeline``
        JSON (the Phase 03 trail-OOM footgun). ``None`` returns all columns.

        Returns an empty (correctly-columned) frame when no partitions in
        range exist. Mirrors :meth:`read_recent_positions`: a pyarrow.dataset
        union over the leaf day-dirs (not a Hive-partition-inferring scan of
        the root), so no synthetic partition columns appear in the frame.
        """
        empty_cols = columns if columns is not None else list(FLIGHTS_ARCHIVE_COLUMNS)

        if lookback_days is None:
            dirs = [path for _, path in self._archive_day_dirs()]
            if not dirs:
                return pd.DataFrame(columns=empty_cols)
            dataset = pds.dataset([pds.dataset(str(d), format="parquet") for d in dirs])
            return cast("pd.DataFrame", dataset.to_table(columns=columns).to_pandas())

        now = now or datetime.now(UTC)
        cutoff = now - timedelta(days=lookback_days)
        dirs = [path for _, path in self._archive_day_dirs(start=cutoff.date(), end=now.date())]
        if not dirs:
            return pd.DataFrame(columns=empty_cols)
        dataset = pds.dataset([pds.dataset(str(d), format="parquet") for d in dirs])
        cutoff_scalar = pa.scalar(cutoff, type=pa.timestamp("us", tz="UTC"))
        table = dataset.to_table(columns=columns, filter=pds.field("landed_at") >= cutoff_scalar)
        return cast("pd.DataFrame", table.to_pandas())

    def read_flights_archive_files(
        self,
        paths: Sequence[Path | str],
        *,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read specific archive Parquet files (not a partition window).

        The archive asset calls this to VERIFY a just-written chunk is durable
        before it deletes those flights from Foundry — the cross-store move is
        not one transaction, so a write that didn't land must never trigger a
        delete. A list of FILE paths is pyarrow.dataset's native input (unlike
        the directory union the windowed reads use). Missing paths are skipped;
        an all-missing list yields an empty (correctly-columned) frame.
        """
        existing = [str(p) for p in paths if Path(p).exists()]
        empty_cols = columns if columns is not None else list(FLIGHTS_ARCHIVE_COLUMNS)
        if not existing:
            return pd.DataFrame(columns=empty_cols)
        table = pds.dataset(existing, format="parquet").to_table(columns=columns)
        return cast("pd.DataFrame", table.to_pandas())

    def count_flights_archive(self) -> int:
        """Total archived-flight rows across every retained day-partition.

        Reads Parquet metadata only (no row materialization), so it stays
        cheap to call before and after each archive run for the row-count-
        delta asset check. Returns 0 when the archive is empty.
        """
        dirs = [path for _, path in self._archive_day_dirs()]
        if not dirs:
            return 0
        dataset = pds.dataset([pds.dataset(str(d), format="parquet") for d in dirs])
        return int(dataset.count_rows())

    def count_flights_archived_at(self, archived_at: datetime) -> int:
        """Count archive rows stamped with exactly this ``archived_at`` instant.

        Every row a single archive run writes carries that run's one
        ``archived_at`` value, so this re-reads the lake to confirm
        *independently* that the run persisted exactly the rows it claims (the
        row-count-matches check). Robust to a concurrent purge: the purge only
        ever drops OLD day-partitions, never this run's freshly-stamped rows.
        Reads Parquet metadata + the filter only (no materialization).
        """
        dirs = [path for _, path in self._archive_day_dirs()]
        if not dirs:
            return 0
        dataset = pds.dataset([pds.dataset(str(d), format="parquet") for d in dirs])
        scalar = pa.scalar(archived_at, type=pa.timestamp("us", tz="UTC"))
        return int(dataset.count_rows(filter=pds.field("archived_at") == scalar))

    def oldest_flights_archive_partition(self) -> date | None:
        """The earliest landed-date partition present, or None if empty.

        The retention check asserts this is within the retention window — a
        partition older than ``now - retention`` is proof the daily purge did
        NOT fire. ``_archive_day_dirs`` returns dirs sorted ascending, so the
        first is the oldest.
        """
        dirs = self._archive_day_dirs()
        return dirs[0][0] if dirs else None

    def purge_flights_archive_before(self, cutoff_date: date) -> list[date]:
        """Drop whole day-partition dirs whose landed date is < ``cutoff_date``.

        Directory unlink only — never a row-level DELETE+VACUUM (the locked
        retention model). Now-empty ``month=`` / ``year=`` parents are
        best-effort removed to keep the tree tidy. Returns the landed dates
        whose partitions were dropped (sorted ascending) so the caller can
        log exactly what retention evicted — no silent truncation.
        """
        dropped: list[date] = []
        for part_date, path in self._archive_day_dirs(end=cutoff_date - timedelta(days=1)):
            shutil.rmtree(path)
            dropped.append(part_date)
            # Best-effort cleanup of now-empty month/ then year/ parents.
            # rmdir only succeeds when empty, so a parent still holding other
            # partitions is left intact (and the loop stops at the first one).
            for parent in (path.parent, path.parent.parent):
                try:
                    parent.rmdir()
                except OSError:
                    break
        return dropped

    def _archive_day_dirs(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[tuple[date, Path]]:
        """Existing ``flights_archive`` day-partition dirs, sorted by date.

        Globs ``year=*/month=*/day=*`` and parses each into a date; dirs that
        don't parse (stray files, partial writes) are skipped. ``start`` /
        ``end`` are inclusive date bounds when given.
        """
        root = Path(self.lake_path) / "flights_archive"
        if not root.exists():
            return []
        out: list[tuple[date, Path]] = []
        for day_dir in root.glob("year=*/month=*/day=*"):
            if not day_dir.is_dir():
                continue
            try:
                year = int(day_dir.parent.parent.name.split("=", 1)[1])
                month = int(day_dir.parent.name.split("=", 1)[1])
                day = int(day_dir.name.split("=", 1)[1])
                part_date = date(year, month, day)
            except (ValueError, IndexError):
                continue
            if start is not None and part_date < start:
                continue
            if end is not None and part_date > end:
                continue
            out.append((part_date, day_dir))
        out.sort(key=lambda t: t[0])
        return out
