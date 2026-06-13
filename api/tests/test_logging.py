"""Unit tests for structured-logging setup (``app.logging``).

Focus: uvicorn's server/access/error loggers must be detached from uvicorn's
own plain-text handlers and propagate to the root JSON handler, so their lines
render in the same JSON shape as app logs (and ``| json``-parse in Loki).
Previously ``configure_logging`` only set those loggers' *level*, leaving
uvicorn's ``propagate=False`` handlers in place, so access logs stayed plain
text (``INFO:     <ip> - "GET /x" 200``).
"""

from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from app import logging as app_logging

UVICORN_LOGGERS = ("uvicorn", "uvicorn.access", "uvicorn.error")


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Snapshot + restore the global logging state configure_logging mutates."""
    root = logging.getLogger()
    saved_root_handlers = root.handlers[:]
    saved_root_level = root.level
    saved = {
        name: (lg.handlers[:], lg.propagate, lg.level)
        for name, lg in ((n, logging.getLogger(n)) for n in UVICORN_LOGGERS)
    }
    try:
        yield
    finally:
        root.handlers[:] = saved_root_handlers
        root.setLevel(saved_root_level)
        for name, (handlers, propagate, level) in saved.items():
            lg = logging.getLogger(name)
            lg.handlers[:] = handlers
            lg.propagate = propagate
            lg.setLevel(level)
        structlog.reset_defaults()


def _seed_uvicorn_state() -> None:
    """Mimic uvicorn's own logging config: each logger owns a handler, no propagate."""
    for name in UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.StreamHandler()]
        lg.propagate = False


def test_uvicorn_loggers_detached_and_propagating(restore_logging: None) -> None:
    """configure_logging strips uvicorn's own handlers and re-enables propagation."""
    _seed_uvicorn_state()
    app_logging.configure_logging()

    for name in UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [], f"{name} should have no handler of its own"
        assert lg.propagate is True, f"{name} should propagate to root"

    # uvicorn enables access logging iff the access logger hasHandlers(); that
    # must stay True by resolving root's handler through propagation.
    assert logging.getLogger("uvicorn.access").hasHandlers() is True


def test_uvicorn_access_record_renders_as_json(
    restore_logging: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real uvicorn access record routes to root and renders as one JSON line."""
    monkeypatch.setattr(app_logging.settings, "log_format", "json")
    _seed_uvicorn_state()
    app_logging.configure_logging()

    # Redirect the single root JSON handler from stdout to a buffer.
    root_handler = logging.getLogger().handlers[0]
    buf = io.StringIO()
    monkeypatch.setattr(root_handler, "stream", buf)

    # Exactly uvicorn 0.32's access call shape (h11_impl.py): format + 5 args.
    logging.getLogger("uvicorn.access").info(
        '%s - "%s %s HTTP/%s" %d', "127.0.0.1:54321", "GET", "/v1/metrics", "1.1", 200
    )

    line = buf.getvalue().strip()
    assert line, "expected exactly one rendered access log line"
    record = json.loads(line)  # the whole point: it must be valid JSON
    assert record["logger"] == "uvicorn.access"
    assert record["level"] == "info"
    assert record["event"] == '127.0.0.1:54321 - "GET /v1/metrics HTTP/1.1" 200'
