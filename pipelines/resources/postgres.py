"""Postgres connection resource for Dagster assets.

A thin wrapper around `psycopg2.connect`. Each `get_conn()` call opens a
fresh connection; callers use it as a context manager so the connection
closes deterministically when the block exits.

The DSN is injected from the `DATABASE_URL` environment variable via
`dagster.EnvVar` in `pipelines/definitions.py`. For host-side execution
(`make db-seed`), the Makefile rewrites the docker-network hostname
`postgres` to `127.0.0.1` so the asset reaches the exposed port.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import psycopg2
from dagster import ConfigurableResource

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection


class PostgresResource(ConfigurableResource):  # type: ignore[type-arg]
    """Connection factory for the AFM operational database.

    Attributes:
        dsn: Standard libpq URL (e.g. ``postgresql://afm:***@host:5432/afm``).
    """

    dsn: str

    @contextmanager
    def get_conn(self) -> Iterator[PgConnection]:
        conn = psycopg2.connect(self.dsn)
        try:
            yield conn
        finally:
            conn.close()
