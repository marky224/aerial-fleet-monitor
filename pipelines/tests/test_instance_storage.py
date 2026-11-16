"""Unit tests for the Dagster instance storage wiring (``dagster.yaml``).

SQLAlchemy 2.1 resolves a plain ``postgresql://`` URL to its psycopg (v3)
driver, while the pipelines package installs psycopg2 only, so the storage
classes ``dagster.yaml`` names have to hand SQLAlchemy a URL that names the
driver. ``DAGSTER_DATABASE_URL`` itself stays a plain URL: the API passes
the same variable straight to psycopg2.

Each test loads the repo's own ``dagster.yaml`` through Dagster's instance
loader and builds a storage the way the webserver, the daemon and a run
worker do. Nothing connects to a database: the first group replaces the
storage constructor, the second is stopped at ``Engine.connect``.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest
from dagster._core.instance.ref import InstanceRef
from dagster._serdes import ConfigurableClassData
from sqlalchemy.engine import Engine, make_url

PIPELINES_DIR = Path(__file__).resolve().parents[1]

# Punctuation in the password is deliberate: the rest of the DSN must reach
# SQLAlchemy as it was written.
PLAIN_DSN = "postgresql://afm:afm!pw@localhost:5432/dagster"
DRIVER_DSN = "postgresql+psycopg2://afm:afm!pw@localhost:5432/dagster"

STORAGES = ["run_storage_data", "event_storage_data", "schedule_storage_data"]


class _StopBeforeConnect(Exception):
    """Raised in place of opening a connection."""


def _storage_data(tmp_path: Path, name: str) -> ConfigurableClassData:
    """One storage's module, class and config, as ``dagster.yaml`` declares it."""
    ref = InstanceRef.from_dir(str(tmp_path), config_dir=str(PIPELINES_DIR))
    data = getattr(ref, name)
    assert isinstance(data, ConfigurableClassData)
    return data


def _url_storage_is_built_with(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str, dsn: str
) -> str:
    """Rehydrate one storage and return the URL its constructor receives."""
    monkeypatch.setenv("DAGSTER_DATABASE_URL", dsn)
    data = _storage_data(tmp_path, name)
    storage_class = getattr(importlib.import_module(data.module_name), data.class_name)
    urls: list[str] = []

    def record_url(self: object, postgres_url: str, **kwargs: Any) -> None:
        urls.append(postgres_url)

    monkeypatch.setattr(storage_class, "__init__", record_url)
    data.rehydrate()
    return urls[0]


@pytest.mark.parametrize("name", STORAGES)
def test_storage_url_resolves_to_the_psycopg2_driver(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    """A plain postgresql:// URL reaches the storage with the psycopg2 driver named."""
    url = _url_storage_is_built_with(monkeypatch, tmp_path, name, PLAIN_DSN)

    assert make_url(url).get_dialect().driver == "psycopg2"


@pytest.mark.parametrize("name", STORAGES)
def test_storage_builds_a_psycopg2_engine(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str
) -> None:
    """The engine a storage builds from a plain URL uses psycopg2, same DSN otherwise."""
    monkeypatch.setenv("DAGSTER_DATABASE_URL", PLAIN_DSN)
    engines: list[Engine] = []

    def stop_before_connect(self: Engine) -> None:
        engines.append(self)
        raise _StopBeforeConnect

    monkeypatch.setattr(Engine, "connect", stop_before_connect)

    with pytest.raises(_StopBeforeConnect):
        _storage_data(tmp_path, name).rehydrate()

    assert engines[0].dialect.driver == "psycopg2"
    assert engines[0].url == make_url(PLAIN_DSN).set(drivername="postgresql+psycopg2")


def test_url_that_names_a_driver_is_left_unchanged(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A URL that already names its driver reaches every storage as written."""
    urls = [
        _url_storage_is_built_with(monkeypatch, tmp_path, name, DRIVER_DSN) for name in STORAGES
    ]

    assert urls == [DRIVER_DSN] * 3
