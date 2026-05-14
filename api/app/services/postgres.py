"""Postgres connection pool + helpers for the API.

Wraps psycopg2's ThreadedConnectionPool so each request can check out a
connection, run its SELECTs, and return it. Thread-safe because FastAPI
runs sync endpoint handlers in a worker threadpool — connections must
not be shared across threads.

The pool is constructed in `app.main`'s lifespan startup and disposed
on shutdown; QueryService receives it via `app.dependencies`.

Also exposes `WatchedAirportsProvider` — the API-side reader for
`ref.airports.is_watched`. The Provider abstraction means swapping the
current per-request SELECT for a startup-cache is a one-class change,
no callers affected.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from app.exceptions import UpstreamUnavailable

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection


PostgresParams = Sequence[Any] | dict[str, Any] | None


class PostgresPool:
    """Thread-safe Postgres connection pool.

    Use ``with pool.connection() as conn`` to check out a raw connection,
    or call ``fetchall`` / ``fetchone`` for the common single-query case.
    Connection errors are translated to UpstreamUnavailable (503);
    programming errors propagate unchanged (-> 500 via the global handler).
    """

    def __init__(self, dsn: str, min_conns: int = 1, max_conns: int = 10) -> None:
        try:
            self._pool = ThreadedConnectionPool(min_conns, max_conns, dsn)
        except psycopg2.OperationalError as e:
            raise UpstreamUnavailable(
                "Postgres pool initialization failed",
                details={"reason": str(e)},
            ) from e

    def close(self) -> None:
        """Close all connections in the pool. Call on app shutdown."""
        self._pool.closeall()

    @contextmanager
    def connection(self) -> Iterator[PgConnection]:
        """Check out a connection; auto-return on context exit."""
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def fetchall(self, query: str, params: PostgresParams = None) -> list[dict[str, Any]]:
        """Run a SELECT and return all rows as a list of dicts.

        RealDictCursor keys rows by column name so Pydantic models can
        construct directly from each row.
        """
        try:
            with (
                self.connection() as conn,
                conn.cursor(cursor_factory=RealDictCursor) as cur,
            ):
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
        except psycopg2.OperationalError as e:
            raise UpstreamUnavailable(
                "Postgres query failed",
                details={"reason": str(e)},
            ) from e

    def fetchone(self, query: str, params: PostgresParams = None) -> dict[str, Any] | None:
        """Run a SELECT and return the first row or None."""
        try:
            with (
                self.connection() as conn,
                conn.cursor(cursor_factory=RealDictCursor) as cur,
            ):
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None
        except psycopg2.OperationalError as e:
            raise UpstreamUnavailable(
                "Postgres query failed",
                details={"reason": str(e)},
            ) from e


class WatchedAirportsProvider:
    """Provider for the list of watched-airport ICAOs.

    Phase 02: per-request SELECT against ``ref.airports.is_watched``. The
    partial index ``idx_airports_watched`` makes this <1 ms for the
    35-row watched set.

    The Provider wraps the call so swapping to a startup-cache or
    background-refreshed cache is a single-class change with no impact
    on call sites.
    """

    def __init__(self, postgres: PostgresPool) -> None:
        self._postgres = postgres

    def get(self) -> list[str]:
        """Return the list of watched ICAO codes, ordered for determinism."""
        rows = self._postgres.fetchall(
            "SELECT icao FROM ref.airports WHERE is_watched = TRUE ORDER BY icao"
        )
        return [str(row["icao"]) for row in rows]
