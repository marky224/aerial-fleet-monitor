"""Alembic migration environment for Aerial Fleet Monitor.

Raw-SQL migrations: target_metadata is intentionally None so autogenerate
is disabled. All schema changes are hand-written via op.execute() in
versions/*.py to preserve fidelity to docs/DATA_MODEL.md.

DATABASE_URL is read via app.settings.settings, which owns the env-var-or-
.env lookup chain (Pydantic Settings). This module stays decoupled from
dotenv loading — Settings is the single source of truth.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.settings import settings


def _sqlalchemy_url(dsn: str) -> str:
    """Name the psycopg2 driver in a plain ``postgresql://`` DSN.

    SQLAlchemy 2.1 resolves a bare ``postgresql://`` URL to its psycopg (v3)
    driver, and the api installs psycopg2 only. A URL that already names a
    driver is returned unchanged.

    Only the scheme is rewritten; the rest of the DSN passes through as
    written. (Re-rendering it through ``sqlalchemy.engine.URL`` would
    percent-encode password punctuation, and ``set_main_option`` rejects a
    bare ``%``.) ``settings.database_url`` itself stays the plain libpq DSN:
    the app's connection pool hands it straight to psycopg2, which does not
    accept a ``+psycopg2`` scheme.
    """
    plain = "postgresql://"
    if dsn.startswith(plain):
        return "postgresql+psycopg2://" + dsn[len(plain) :]
    return dsn


# Alembic Config object, providing access to values in alembic.ini.
config = context.config

# Hand the resolved DATABASE_URL to Alembic, with the driver named. Both the
# offline and the online path below read it back from this option. Settings
# raises at import time if DATABASE_URL is missing, so reaching this line
# means we have one.
config.set_main_option("sqlalchemy.url", _sqlalchemy_url(settings.database_url))

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Raw-SQL migrations only — no SQLAlchemy ORM metadata. autogenerate is off.
target_metadata = None


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting to a database.

    Useful for review: `alembic upgrade head --sql` produces the SQL
    that would run, so the migration can be eyeballed in a PR diff.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
