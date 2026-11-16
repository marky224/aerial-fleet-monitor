"""Unit tests for the Alembic environment (``alembic/env.py``).

SQLAlchemy 2.1 resolves a plain ``postgresql://`` URL to its psycopg (v3)
driver, while the api installs psycopg2 only, so env.py has to name the
driver for Alembic on both the offline (``--sql``) and the online path.

Each test runs the real env.py through Alembic with a migration function
that returns no steps. Nothing connects to a database: the offline path
never does, and the online path is stopped at ``Engine.connect``.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.runtime.environment import EnvironmentContext
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Engine, make_url

from app.settings import settings

SCRIPT_LOCATION = Path(__file__).resolve().parents[1] / "alembic"

# Punctuation in the password is deliberate: the rest of the DSN must reach
# SQLAlchemy as it was written.
PLAIN_DSN = "postgresql://afm:afm!pw@localhost:5432/afm"


class _StopBeforeConnect(Exception):
    """Raised in place of opening a connection on the online path."""


def _run_env(*, as_sql: bool) -> tuple[Config, list[MigrationContext]]:
    """Run env.py offline (``as_sql=True``) or online; return what it configured."""
    # No ini file: env.py then skips fileConfig(), which would otherwise
    # reconfigure logging for the rest of the test session.
    cfg = Config(output_buffer=io.StringIO())
    cfg.set_main_option("script_location", str(SCRIPT_LOCATION))
    script = ScriptDirectory.from_config(cfg)
    contexts: list[MigrationContext] = []

    def no_steps(rev: object, migration_context: MigrationContext) -> list[object]:
        contexts.append(migration_context)
        return []

    with EnvironmentContext(cfg, script, fn=no_steps, as_sql=as_sql):
        script.run_env()
    return cfg, contexts


def test_offline_path_names_the_psycopg2_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    """A plain postgresql:// DSN is rendered offline with the psycopg2 dialect."""
    monkeypatch.setattr(settings, "database_url", PLAIN_DSN)

    _, contexts = _run_env(as_sql=True)

    assert contexts[0].dialect.driver == "psycopg2"


def test_online_path_builds_a_psycopg2_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """The engine env.py builds from a plain DSN uses psycopg2, same DSN otherwise."""
    monkeypatch.setattr(settings, "database_url", PLAIN_DSN)
    engines: list[Engine] = []

    def stop_before_connect(self: Engine) -> None:
        engines.append(self)
        raise _StopBeforeConnect

    monkeypatch.setattr(Engine, "connect", stop_before_connect)

    with pytest.raises(_StopBeforeConnect):
        _run_env(as_sql=False)

    assert engines[0].dialect.driver == "psycopg2"
    assert engines[0].url == make_url(PLAIN_DSN).set(drivername="postgresql+psycopg2")


def test_url_that_names_a_driver_is_left_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """A DSN that already names a driver reaches Alembic exactly as written."""
    dsn = "postgresql+psycopg://afm:afm!pw@localhost:5432/afm"
    monkeypatch.setattr(settings, "database_url", dsn)

    cfg, contexts = _run_env(as_sql=True)

    assert cfg.get_main_option("sqlalchemy.url") == dsn
    assert contexts[0].dialect.driver == "psycopg"
