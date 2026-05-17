"""Dagster wiring test for the maintenance prune asset.

Verifies ``prune_stale_positions`` issues a bounded DELETE against
``app.current_positions``, commits, and surfaces the deleted-row count +
retention as materialization metadata. The DB is faked — this tests the
asset boundary (SQL shape, commit, metadata), not Postgres itself.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from dagster import MaterializeResult, build_asset_context

from pipelines.assets import maintenance


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.rowcount = 1234

    def execute(self, sql: str, *_a: object) -> None:
        self.executed.append(sql)


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur
        self.committed = False

    @contextmanager
    def cursor(self) -> Any:
        yield self._cur

    def commit(self) -> None:
        self.committed = True


class _FakePostgres:
    def __init__(self) -> None:
        self.cur = _FakeCursor()
        self.conn = _FakeConn(self.cur)

    @contextmanager
    def get_conn(self) -> Any:
        yield self.conn


def test_prune_deletes_by_retention_commits_and_reports() -> None:
    pg = _FakePostgres()

    result = maintenance.prune_stale_positions(
        build_asset_context(),
        postgres=pg,  # type: ignore[arg-type]
    )

    assert isinstance(result, MaterializeResult)
    md = result.metadata or {}
    assert md["rows_deleted"].value == 1234
    assert md["retention"].value == maintenance.POSITION_RETENTION

    sql = pg.cur.executed[0]
    assert "DELETE FROM app.current_positions" in sql
    assert f"NOW() - INTERVAL '{maintenance.POSITION_RETENTION}'" in sql
    assert pg.conn.committed is True


def test_retention_window_exceeds_widest_reader() -> None:
    """Guard the safety invariant: retention must stay >= the widest
    current_positions reader (site in/outbound = 60 min) so a prune can
    never starve a consumer. Encoded as 3 h."""
    assert maintenance.POSITION_RETENTION == "3 hours"
