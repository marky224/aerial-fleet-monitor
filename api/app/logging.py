"""Structured logging setup.

Configures structlog so that:
  - Application code calls `structlog.get_logger(...)` and gets a logger
    that emits JSON (prod) or human-readable lines (dev).
  - Third-party libraries that use the stdlib `logging` module (FastAPI,
    uvicorn, simple_salesforce, etc.) are routed through the same pipeline
    so their output appears in the same format.

Call `configure_logging()` once at app startup, before anything else logs.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor

from app.settings import settings


def _drop_color_message_key(
    _logger: Any, _method_name: str, event_dict: EventDict
) -> EventDict:
    """uvicorn duplicates the message under `color_message`; drop it."""
    event_dict.pop("color_message", None)
    return event_dict


def configure_logging() -> None:
    """Wire structlog + stdlib logging.

    Idempotent: safe to call more than once (e.g. in tests).
    """
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.add_logger_name,
        _drop_color_message_key,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_format == "json":
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logging through the same renderer so uvicorn / FastAPI /
    # third-party libs produce structured output in the same format.
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
    root.setLevel(settings.log_level.upper())

    # Tame uvicorn's access logger volume but keep it informational.
    logging.getLogger("uvicorn.access").setLevel("INFO")
    logging.getLogger("uvicorn.error").setLevel("INFO")


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger. Use module `__name__` as the conventional name."""
    return structlog.get_logger(name)
