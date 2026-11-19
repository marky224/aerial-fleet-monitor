"""Dagster instance storage that names the psycopg2 driver.

``dagster.yaml`` builds Dagster's run, event-log and schedule storage from
``DAGSTER_DATABASE_URL``, a plain ``postgresql://`` URL. SQLAlchemy 2.1
resolves a URL with no driver to psycopg (v3), and this package installs
psycopg2 only, so the stock ``dagster_postgres`` classes fail when they
create their engine. The classes here are those stock classes with one
change: the URL they hand to SQLAlchemy names the driver.

``DAGSTER_DATABASE_URL`` itself stays a plain URL. The API reads the same
variable for its run-store gauges and passes it straight to psycopg2, which
does not accept a ``+psycopg2`` scheme.

``dagster.yaml`` names these classes by module path, so the module has to
be importable wherever a Dagster instance is built: the webserver, the
daemon and the user-code container, where run workers rebuild the instance.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self

from dagster_postgres import (
    PostgresEventLogStorage,
    PostgresRunStorage,
    PostgresScheduleStorage,
)
from dagster_postgres.utils import pg_url_from_config

if TYPE_CHECKING:
    from dagster._serdes import ConfigurableClassData


def sqlalchemy_url(dsn: str) -> str:
    """Name the psycopg2 driver in a plain ``postgresql://`` DSN.

    Only the scheme is rewritten; the rest of the DSN passes through as
    written. A URL that already names a driver is returned unchanged.
    """
    plain = "postgresql://"
    if dsn.startswith(plain):
        return "postgresql+psycopg2://" + dsn[len(plain) :]
    return dsn


# Each class overrides only the factory Dagster calls when it loads
# ``dagster.yaml``. The stock factories build the stock class by name, so
# these build ``cls`` with the same three arguments instead.


class Psycopg2RunStorage(PostgresRunStorage):
    """``PostgresRunStorage`` on a URL that names psycopg2."""

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(
            inst_data=inst_data,
            postgres_url=sqlalchemy_url(pg_url_from_config(config_value)),
            should_autocreate_tables=config_value.get("should_autocreate_tables", True),
        )


class Psycopg2EventLogStorage(PostgresEventLogStorage):
    """``PostgresEventLogStorage`` on a URL that names psycopg2."""

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(
            inst_data=inst_data,
            postgres_url=sqlalchemy_url(pg_url_from_config(config_value)),
            should_autocreate_tables=config_value.get("should_autocreate_tables", True),
        )


class Psycopg2ScheduleStorage(PostgresScheduleStorage):
    """``PostgresScheduleStorage`` on a URL that names psycopg2."""

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData | None, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(
            inst_data=inst_data,
            postgres_url=sqlalchemy_url(pg_url_from_config(config_value)),
            should_autocreate_tables=config_value.get("should_autocreate_tables", True),
        )
