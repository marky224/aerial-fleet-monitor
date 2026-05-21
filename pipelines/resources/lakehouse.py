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
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
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
        dataset = pds.dataset([str(d) for d in dirs], format="parquet")
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
        # pyarrow.Table.from_pylist would also work, but going column-wise
        # lets us enforce the schema (and surface a clear error on
        # type drift) without a second pass.
        columns: dict[str, list[Any]] = {name: [] for name in POSITIONS_COLUMNS}
        for row in rows:
            for name in POSITIONS_COLUMNS:
                columns[name].append(row.get(name))
        return pa.table(columns, schema=POSITIONS_SCHEMA)
