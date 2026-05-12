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

# Alembic Config object, providing access to values in alembic.ini.
config = context.config

# Hand the resolved DATABASE_URL to Alembic. Settings raises at import
# time if DATABASE_URL is missing, so reaching this line means we have one.
config.set_main_option("sqlalchemy.url", settings.database_url)

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
