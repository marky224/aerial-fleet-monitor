"""Tests for the sync service's structlog configuration.

Logging config mutates process-global state (the root logger's handlers and
structlog's defaults), so each test runs under an autouse fixture that
snapshots and restores both — keeping the suite order-independent.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from afm_foundry_sync.logging import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _isolate_logging_state() -> Iterator[None]:
    """Restore root-logger handlers/level and structlog defaults after each test."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_httpx_level = logging.getLogger("httpx").level
    try:
        yield
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        logging.getLogger("httpx").setLevel(saved_httpx_level)
        structlog.reset_defaults()


def test_configure_logging_is_idempotent() -> None:
    """Repeated calls leave exactly one handler — handlers are reassigned, not appended."""
    configure_logging()
    configure_logging()
    configure_logging()

    assert len(logging.getLogger().handlers) == 1


def test_log_level_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """AFM_SYNC_LOG_LEVEL drives the root logger level, case-insensitively."""
    monkeypatch.setenv("AFM_SYNC_LOG_LEVEL", "debug")

    configure_logging()

    assert logging.getLogger().level == logging.DEBUG


def test_default_log_level_is_info(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no env override, the level defaults to INFO."""
    monkeypatch.delenv("AFM_SYNC_LOG_LEVEL", raising=False)

    configure_logging()

    assert logging.getLogger().level == logging.INFO


def test_httpx_logger_tamed_to_warning() -> None:
    """httpx's per-request INFO line is suppressed; warnings still pass."""
    configure_logging()

    assert logging.getLogger("httpx").level == logging.WARNING


def test_json_format_emits_parseable_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """AFM_SYNC_LOG_FORMAT=json yields one JSON object per log line."""
    monkeypatch.setenv("AFM_SYNC_LOG_FORMAT", "json")
    configure_logging()

    get_logger("test.json").info("hello_event", customer_region="west")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "hello_event"
    assert record["level"] == "info"
    assert record["customer_region"] == "west"


def test_console_format_is_not_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Default (console) format is human-readable, not JSON."""
    monkeypatch.delenv("AFM_SYNC_LOG_FORMAT", raising=False)
    configure_logging()

    get_logger("test.console").info("plain_event", icao24="abc123")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    with pytest.raises(json.JSONDecodeError):
        json.loads(line)
    assert "plain_event" in line
    assert "abc123" in line


def test_get_logger_returns_bound_logger() -> None:
    """get_logger yields a logger that materializes to the configured stdlib BoundLogger.

    structlog returns a lazy proxy until first use; ``.bind()`` forces it to
    resolve to the configured ``wrapper_class``, which is what the ``cast`` in
    ``get_logger`` promises callers.
    """
    configure_logging()

    bound = get_logger("test.bound").bind()

    assert isinstance(bound, structlog.stdlib.BoundLogger)
