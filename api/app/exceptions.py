"""AFM application exceptions and error envelope.

Phase 02: every endpoint surfaces failures through this hierarchy. The
global handler in `app.main` maps each subclass to its documented HTTP
status (API.md §1.2) and emits the standard envelope (API.md §1.3).

Subclasses set `status_code` and `code` at the class level. Callers pass
a human-readable message and an optional `details` dict for context.
"""

from __future__ import annotations

from typing import Any


class AFMException(Exception):
    """Base class for AFM-raised HTTP errors."""

    status_code: int = 500
    code: str = "internal_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(AFMException):
    """Resource doesn't exist or hasn't been observed recently."""

    status_code = 404
    code = "not_found"


class ScopeViolation(AFMException):
    """Caller's Scope does not include the requested resource."""

    status_code = 403
    code = "scope_insufficient"


class UpstreamUnavailable(AFMException):
    """An external dependency (Postgres, lakehouse, Salesforce, ...) is degraded."""

    status_code = 503
    code = "upstream_unavailable"


class ConflictError(AFMException):
    """A write would collide with existing state."""

    status_code = 409
    code = "conflict"
