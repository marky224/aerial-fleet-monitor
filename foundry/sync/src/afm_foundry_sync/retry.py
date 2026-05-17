"""Shared transient-retry policy for the sync service's HTTP clients.

Single source of truth so ``api_readers`` (reads from the local AFM API) and
``ontology_writers`` (writes to the Foundry Action API) cannot drift apart.

Policy: retry ``httpx.TransportError`` (connect/read/write/pool failures) and
HTTP responses whose status is in ``RETRIABLE_STATUSES``, up to 3 attempts
with exponential backoff (0.5s → 2.0s cap). ``reraise=True`` so the caller
sees the original ``httpx`` exception after attempts are exhausted, not a
``RetryError`` wrapper. 4xx is never retried — a 404 on a missing icao24, or
a 400 from a malformed Action payload, is a real signal, not a transient.
"""

from __future__ import annotations

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

RETRIABLE_STATUSES = frozenset({502, 503, 504})


def should_retry(exc: BaseException) -> bool:
    """Tenacity predicate: transient transport errors and 5xx-retriable statuses."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRIABLE_STATUSES
    return False


# A configured tenacity decorator, reusable across functions. Each decorated
# call gets its own retry state; the controller itself is stateless.
transient_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=2.0),
    retry=retry_if_exception(should_retry),
    reraise=True,
)

__all__ = ["RETRIABLE_STATUSES", "should_retry", "transient_retry"]
