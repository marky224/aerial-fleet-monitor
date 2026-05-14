"""DuckDB wrapper for the Parquet lakehouse (/lake).

Phase 02 introduces DuckDB as a real api runtime dep. This module is the
only place duckdb is imported, so callers can stub LakehouseQuery for
tests without depending on the duckdb package.

Per-request DuckDB connection per the Phase 02 design lean (Q2).
Connection cost is ~5-50 ms; if endpoint latency demands it, swapping
to a process-level connection with cursor-per-request is a one-class
change with no impact on call sites.

Glob pattern: positions live at `{lake_path}/positions/year=YYYY/
month=MM/day=DD/hour=HH/*.parquet`. DuckDB's `read_parquet(...,
hive_partitioning = true)` prunes partitions based on the WHERE clause,
so filtering on ts_polled (-derived columns) is cheap even with months
of data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from app.exceptions import UpstreamUnavailable


class LakehouseQuery:
    """Read-only DuckDB-backed Parquet query interface.

    Use ``query(sql, **params)`` for one-shot reads. Each call opens a
    fresh in-process DuckDB connection — there's no shared state to
    isolate between requests.
    """

    def __init__(self, lake_path: str) -> None:
        self._lake_path = lake_path
        self._positions_glob = str(Path(lake_path) / "positions" / "**" / "*.parquet")

    @property
    def positions_glob(self) -> str:
        """Glob for Hive-partitioned position Parquet files."""
        return self._positions_glob

    def query(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        """Execute ``sql`` with named ``$param`` bindings; return rows as dicts.

        Each call opens a fresh in-memory DuckDB connection — appropriate
        for Phase 02's per-request pattern. The connection is closed on
        scope exit even if the query raises.

        Only IO-class errors (disk, permissions, missing partitions) are
        translated to UpstreamUnavailable (503); SQL-level errors
        (parser, binder, type) propagate as 500s so bugs surface loud.
        """
        try:
            with duckdb.connect(":memory:") as conn:
                result = conn.execute(sql, params)
                columns = [desc[0] for desc in result.description]
                return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
        except duckdb.IOException as e:
            raise UpstreamUnavailable(
                "DuckDB lakehouse read failed",
                details={"reason": str(e)},
            ) from e
