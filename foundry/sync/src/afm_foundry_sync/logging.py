"""Structured logging setup for the AFM Foundry sync service.

Mirrors ``api/app/logging.py`` so sync output is shaped like the rest of the
stack, but is a standalone module: the sync service has its own venv and does
not cross-import from ``api/`` (see the local-mirror decision in the Phase 03
handoff). Three deliberate divergences from the api reference:

  - **Config source is the environment, not settings.** ``FoundrySettings`` is
    the object that raises ``ValidationError`` -> ``FoundrySyncSkipped`` when
    Foundry credentials are absent. Logging must be usable *before and
    independent of* that failure path — logs matter most when settings are
    broken. So format/level come from ``AFM_SYNC_LOG_FORMAT`` /
    ``AFM_SYNC_LOG_LEVEL`` in the environment, with safe defaults, and
    ``settings.py`` is left untouched.
  - **Foreign-logger taming targets httpx, not uvicorn.** The sync has no
    uvicorn. httpx logs every request at INFO, which would double up with the
    explicit ``afm_api_request`` events in ``api_readers.py``; it is pinned to
    WARNING. The generic stdlib -> structlog bridge is kept so httpx/tenacity
    errors still render in our format.
  - **Dagster-embedding caveat (documented, not solved here).** Like the api
    module, ``configure_logging()`` replaces the root logger's handlers. That
    is clean for standalone runs but can collide with Dagster's own root
    configuration when the sync runs in-process under an asset. The decision
    of *whether* to call ``configure_logging()`` in the embedded path belongs
    to ``sync_jobs.py``; this module just makes the call available and
    idempotent.

Call ``configure_logging()`` once at process start, before anything else logs.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import cast

import structlog
from structlog.types import Processor

_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_LOG_FORMAT = "console"


def _log_level() -> str:
    return os.environ.get("AFM_SYNC_LOG_LEVEL", _DEFAULT_LOG_LEVEL).upper()


def _log_format() -> str:
    """`json` for machine-ingestible output, anything else for dev console."""
    return os.environ.get("AFM_SYNC_LOG_FORMAT", _DEFAULT_LOG_FORMAT).lower()


def configure_logging() -> None:
    """Wire structlog + stdlib logging for the sync service.

    Idempotent: ``root.handlers`` is reassigned (not appended) on every call,
    so repeated invocation — e.g. across tests or asset ticks — leaves exactly
    one handler installed.
    """
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if _log_format() == "json":
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    # Route structlog calls through stdlib so `add_logger_name` has a real
    # stdlib logger to read from. Native and foreign records converge at the
    # StreamHandler below, where the final rendering happens.
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared_processors,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
        )
    )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(_log_level())

    # httpx logs one INFO line per request, which duplicates the explicit
    # `afm_api_request` events emitted in api_readers.py. Keep its warnings
    # and errors, drop the per-request noise.
    logging.getLogger("httpx").setLevel("WARNING")


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger. Use module ``__name__`` as the conventional name."""
    # structlog.get_logger is typed Any in the stubs; the runtime type matches
    # the configured wrapper_class (stdlib.BoundLogger), so the cast is safe.
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


# Re-export so callers can `from afm_foundry_sync.logging import get_logger`
# without importing structlog directly.
__all__ = ["configure_logging", "get_logger"]
