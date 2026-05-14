"""Shared pytest fixtures for the AFM API test suite.

`pytest_configure` runs before test modules import the app, so env-var
defaults set here are visible to `app.settings.Settings()`. This lets
the suite run in CI without a `.env` file and without DATABASE_URL on
the runner — required values are stubbed out because no test actually
opens a Postgres connection (PostgresPool is mocked at the dep level).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Stub required env vars before app modules import.

    Real `.env` values, if present, win (`setdefault` is a no-op when the
    var is already set).
    """
    os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    os.environ.setdefault("AFM_LAKE_PATH", "/tmp/afm-test-lake")


@pytest.fixture
def internal_scope():  # type: ignore[no-untyped-def]
    """Full-access scope matching the Phase 02 auth stub."""
    from app.models.common import Scope

    return Scope(
        user_handle="internal-ops",
        region="all",
        custom_perms=[],
        sites_in_scope=["KSFO", "KJFK", "KLAX", "PHNL"],
    )


@pytest.fixture
def west_scope():  # type: ignore[no-untyped-def]
    """Narrower scope for testing scope-violation paths."""
    from app.models.common import Scope

    return Scope(
        user_handle="west-coast-ops",
        region="west",
        custom_perms=["AFM_Region_West"],
        sites_in_scope=["KSFO", "KLAX"],
    )


@pytest.fixture
def mock_postgres() -> MagicMock:
    """MagicMock spec'd to PostgresPool — attr typos surface as AttributeError."""
    from app.services.postgres import PostgresPool

    return MagicMock(spec=PostgresPool)


@pytest.fixture
def mock_lakehouse() -> MagicMock:
    """MagicMock spec'd to LakehouseQuery, with positions_glob pre-set."""
    from app.services.lakehouse import LakehouseQuery

    mock = MagicMock(spec=LakehouseQuery)
    mock.positions_glob = "/lake/positions/**/*.parquet"
    return mock


@pytest.fixture
def query_service(mock_postgres: MagicMock, mock_lakehouse: MagicMock):  # type: ignore[no-untyped-def]
    """QueryService composed of mocked dependencies."""
    from app.services.query_service import QueryService

    return QueryService(postgres=mock_postgres, lakehouse=mock_lakehouse)


@pytest.fixture
def seeded_lakehouse(tmp_path: Path) -> Iterator:  # type: ignore[type-arg]
    """Real LakehouseQuery with one hive-partitioned Parquet seeded with two rows.

    Used by test_lakehouse.py to exercise the actual DuckDB read path,
    including the TIMESTAMPTZ -> Python datetime conversion that needs pytz.
    """
    import duckdb

    from app.services.lakehouse import LakehouseQuery

    partition_dir = tmp_path / "positions" / "year=2026" / "month=05" / "day=14" / "hour=12"
    partition_dir.mkdir(parents=True)
    parquet_path = partition_dir / "data.parquet"

    with duckdb.connect(":memory:") as conn:
        # Explicit casts so DuckDB stores lat/lon as DOUBLE, not DECIMAL —
        # matches the production Parquet schema written by Dagster.
        conn.execute(
            "CREATE TABLE seed AS SELECT * FROM (VALUES "
            "('a2024b', TIMESTAMPTZ '2026-05-14 12:00:00+00', "
            "37.6::DOUBLE, -122.4::DOUBLE, 17025, 259), "
            "('a2024b', TIMESTAMPTZ '2026-05-14 12:01:00+00', "
            "37.7::DOUBLE, -122.3::DOUBLE, 17050, 260)"
            ") AS t(icao24, ts_polled, lat, lon, altitude_ft, speed_kt)"
        )
        conn.execute(f"COPY seed TO '{parquet_path}' (FORMAT PARQUET)")

    yield LakehouseQuery(lake_path=str(tmp_path))
