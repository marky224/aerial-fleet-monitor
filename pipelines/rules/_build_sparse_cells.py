"""Regenerate ``data/lost_signal_sparse_cells.json`` from the lakehouse.

A "sparse cell" is a 1deg x 1deg lat/lon square where ADS-B coverage is
normally gappy enough that the ``lost_signal`` rule's 8-minute gap floor
fires on routine feed jitter rather than on a real signal loss. The
rule demotes one severity tier in these cells (see
``pipelines/rules/lost_signal.py`` for the gradation logic).

Method (memory-safe streaming):
1. Walk ``$AFM_LAKE_PATH/positions`` one hour-partition at a time
   (~400k rows each) -- the full lake is tens of millions of rows and
   won't sort in a 2 GB container.
2. Per partition: for each (icao24, lat-cell, lon-cell) seen there,
   record one observation = the aircraft's median inter-poll gap in
   that cell during that hour, plus a "gappy" flag if median > 6 min.
3. Accumulate per-cell totals across all partitions in a small dict
   (one entry per cell, ~150 cells in CONUS).
4. A cell is sparse when at least ``SPARSE_AIRCRAFT_FRACTION`` of its
   observations are gappy, provided the cell has at least
   ``MIN_OBSERVATIONS_PER_CELL`` total (one unlucky flight shouldn't
   classify a cell).

Cross-hour continuity is intentionally not preserved: an aircraft sits
in a 1deg cell for at most ~10 min at cruise speed, so per-hour
sampling already captures the in-cell experience. Each cell ends up
with tens-to-thousands of (aircraft, hour) observations across the lake
window, which is plenty for the fraction-based classifier.

Refresh cadence: monthly. Sparse geography is a property of where
ADS-B ground receivers physically sit, which doesn't move on human
timescales. Re-run when receiver coverage materially changes (rare) or
when a new round of false-positive Cases shows up in a region not in
the current list.

Run:
    docker cp pipelines/rules/_build_sparse_cells.py \\
        afm-dagster-user-code-1:/opt/venv/lib/python3.12/site-packages/pipelines/rules/
    docker exec afm-dagster-user-code-1 \\
        python -m pipelines.rules._build_sparse_cells

The script is read-only against the lakehouse and writes exactly one
file (``data/lost_signal_sparse_cells.json``) on the host. Output path
is computed relative to this file so the JSON lands in the repo even
when the script runs from the container's site-packages copy.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as pds

logger = logging.getLogger(__name__)

CELL_DEGREES = 1
SPARSE_GAP_THRESHOLD_MINUTES = 6.0
SPARSE_AIRCRAFT_FRACTION = 0.30
MIN_OBSERVATIONS_PER_CELL = 20

DEFAULT_LAKE_PATH = "/lake"


def _output_path() -> Path:
    """Where to write the JSON.

    Inside the container the script lives at
    ``/opt/venv/lib/python3.12/site-packages/pipelines/rules/_build_sparse_cells.py``
    where writing would only update the site-packages copy. So when run
    in-container we write to the bind-mounted host data dir if we can
    find it via ``AFM_REPO_ROOT``; otherwise we fall back to the
    sibling ``data/`` next to this file (works when run from the host
    checkout directly).
    """
    repo_root = os.environ.get("AFM_REPO_ROOT")
    if repo_root:
        return Path(repo_root) / "pipelines" / "rules" / "data" / "lost_signal_sparse_cells.json"
    return Path(__file__).parent / "data" / "lost_signal_sparse_cells.json"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    lake_path = os.environ.get("AFM_LAKE_PATH", DEFAULT_LAKE_PATH)
    positions_root = Path(lake_path) / "positions"
    if not positions_root.exists():
        raise SystemExit(f"positions root not found: {positions_root}")

    partitions = _walk_hour_partitions(positions_root)
    logger.info("found %d hour-partitions under %s", len(partitions), positions_root)

    # Per-cell accumulators. observations = (aircraft, hour) pairs seen in
    # the cell; gappy = subset of those with median in-cell gap above the
    # threshold.
    cell_observations: dict[tuple[int, int], int] = {}
    cell_gappy: dict[tuple[int, int], int] = {}

    threshold = pd.Timedelta(minutes=SPARSE_GAP_THRESHOLD_MINUTES)
    for i, partition_dir in enumerate(partitions, start=1):
        per_partition = _classify_partition(partition_dir, threshold)
        for cell, (obs, gappy) in per_partition.items():
            cell_observations[cell] = cell_observations.get(cell, 0) + obs
            cell_gappy[cell] = cell_gappy.get(cell, 0) + gappy
        if i % 24 == 0 or i == len(partitions):
            logger.info("processed %d / %d partitions", i, len(partitions))

    sparse_cells = sorted(
        cell
        for cell, obs in cell_observations.items()
        if obs >= MIN_OBSERVATIONS_PER_CELL
        and cell_gappy.get(cell, 0) / obs >= SPARSE_AIRCRAFT_FRACTION
    )
    logger.info(
        "identified %d sparse cells out of %d cells with >= %d observations",
        len(sparse_cells),
        sum(1 for obs in cell_observations.values() if obs >= MIN_OBSERVATIONS_PER_CELL),
        MIN_OBSERVATIONS_PER_CELL,
    )

    output_path = _output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "cell_degrees": CELL_DEGREES,
        "sparse_gap_threshold_minutes": SPARSE_GAP_THRESHOLD_MINUTES,
        "sparse_aircraft_fraction": SPARSE_AIRCRAFT_FRACTION,
        "min_observations_per_cell": MIN_OBSERVATIONS_PER_CELL,
        "cells": [list(cell) for cell in sparse_cells],
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    logger.info("wrote %s", output_path)


def _walk_hour_partitions(root: Path) -> list[Path]:
    """Return every existing hour-partition dir under ``root``, sorted chronologically."""
    parts: list[Path] = []
    # Hive layout: positions/year=YYYY/month=MM/day=DD/hour=HH/
    for year in sorted(root.glob("year=*")):
        for month in sorted(year.glob("month=*")):
            for day in sorted(month.glob("day=*")):
                for hour in sorted(day.glob("hour=*")):
                    if hour.is_dir():
                        parts.append(hour)
    return parts


def _classify_partition(
    partition_dir: Path,
    threshold: pd.Timedelta,
) -> dict[tuple[int, int], tuple[int, int]]:
    """For one hour-partition, return ``{cell: (observations, gappy_count)}``.

    Each (icao24, cell) seen in this partition contributes one
    observation; it's counted as gappy when the p90 inter-poll gap for
    that aircraft in that cell exceeds the threshold. We use p90 (not
    median, not p95) because the rule fires on tail events: at a 30-60s
    polling cadence median gaps are always ~poll-interval even in
    sparse cells, so only the tail reflects coverage holes. p90 stays
    well-defined at the typical 5-30 polls/hour we see per aircraft
    in a cell.
    """
    table = pds.dataset(str(partition_dir), format="parquet").to_table(
        columns=["icao24", "lat", "lon", "ts_polled"]
    )
    if table.num_rows == 0:
        return {}
    df = table.to_pandas()
    df = df.dropna(subset=["lat", "lon"])
    if df.empty:
        return {}
    df["lat_cell"] = (np.floor(df["lat"] / CELL_DEGREES) * CELL_DEGREES).astype(np.int32)
    df["lon_cell"] = (np.floor(df["lon"] / CELL_DEGREES) * CELL_DEGREES).astype(np.int32)

    # Sort once by (icao24, cell, ts), diff ts within each group.
    df = df.sort_values(["icao24", "lat_cell", "lon_cell", "ts_polled"], kind="mergesort")
    group_cols = ["icao24", "lat_cell", "lon_cell"]
    df["gap"] = df.groupby(group_cols, observed=True, sort=False)["ts_polled"].diff()

    gaps = df.dropna(subset=["gap"])
    if gaps.empty:
        return {}
    tail_gap = gaps.groupby(group_cols, observed=True, sort=False)["gap"].quantile(0.90)
    tail_gap.name = "tail_gap"
    grouped = tail_gap.reset_index()
    grouped["gappy"] = grouped["tail_gap"] > threshold

    cell_obs = grouped.groupby(["lat_cell", "lon_cell"], observed=True, sort=False).size()
    cell_gappy = (
        grouped[grouped["gappy"]]
        .groupby(["lat_cell", "lon_cell"], observed=True, sort=False)
        .size()
    )
    out: dict[tuple[int, int], tuple[int, int]] = {}
    for (lat, lon), obs in cell_obs.items():
        gappy = int(cell_gappy.get((lat, lon), 0))
        out[(int(lat), int(lon))] = (int(obs), gappy)
    return out


if __name__ == "__main__":
    main()
