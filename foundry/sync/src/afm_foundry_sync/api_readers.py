"""Async HTTP clients for the AFM /v1 endpoints the sync reads from.

Wraps a long-lived ``httpx.AsyncClient`` so the Dagster asset firing every
30s reuses connections. Use as::

    async with AfmApiClient(settings) as client:
        positions = await client.fetch_positions_live()

Retry policy: ``httpx.TransportError`` and HTTP responses with status in
{502, 503, 504} are retried up to 3 times with exponential backoff
(tenacity). 4xx responses raise immediately — a 404 on a missing icao24 is
a real signal, not a transient.

Errors propagate as ``httpx.HTTPError`` subclasses (``HTTPStatusError`` for
non-2xx after retry, ``TransportError`` for network failures, ``TimeoutException``
for timeouts). Translation to ``FoundrySyncSkipped`` (the local-standalone
guarantee from the build doc) lives in ``sync_jobs.py`` and only covers the
Foundry side. If the local AFM API is unreachable, every pipeline is
broken — that's a different failure domain than a missing Foundry tenant.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, Self

import httpx
import structlog

from afm_foundry_sync.models import (
    FlightDetail,
    PositionsLiveResponse,
    SiteDetail,
    SiteFlightDirection,
    SiteFlightListResponse,
    SiteListResponse,
    SiteSla,
    SlaPeriod,
    TrailLookback,
    TrailResponse,
)
from afm_foundry_sync.retry import transient_retry
from afm_foundry_sync.settings import FoundrySettings

logger = structlog.get_logger(__name__)

_REQUEST_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


class AfmApiClient:
    """Async HTTP client for the local AFM /v1 API.

    Connection-pooled via a single ``httpx.AsyncClient`` for the lifetime of
    the context manager — re-entered every Dagster asset tick (~30s).
    """

    def __init__(self, settings: FoundrySettings) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.AFM_API_BASE,
            timeout=_REQUEST_TIMEOUT,
            # TODO(phase-04): inject Authorization header when real JWT auth lands.
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._client.aclose()

    @transient_retry
    async def _get_json(self, path: str, *, params: dict[str, str] | None = None) -> Any:
        logger.info("afm_api_request", path=path, params=params or {})
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def fetch_positions_live(self) -> PositionsLiveResponse:
        data = await self._get_json("/v1/positions/live")
        result = PositionsLiveResponse.model_validate(data)
        if result.truncated:
            # The API clipped the in-scope live set at its safety ceiling, so
            # this snapshot — and therefore the tenant sync derived from it —
            # is incomplete. Not retryable; surface it loudly for the operator.
            logger.warning(
                "positions_live_truncated_upstream",
                count=result.count,
                hint="AFM API clipped the live set; tenant Aircraft will be incomplete this cycle",
            )
        return result

    async def fetch_flight(self, icao24: str) -> FlightDetail:
        data = await self._get_json(f"/v1/flights/{icao24}")
        return FlightDetail.model_validate(data)

    async def fetch_flight_trail(
        self, icao24: str, lookback: TrailLookback = "2h"
    ) -> TrailResponse:
        data = await self._get_json(f"/v1/flights/{icao24}/trail", params={"lookback": lookback})
        return TrailResponse.model_validate(data)

    async def fetch_sites(self) -> SiteListResponse:
        data = await self._get_json("/v1/sites")
        return SiteListResponse.model_validate(data)

    async def fetch_site(self, icao: str) -> SiteDetail:
        data = await self._get_json(f"/v1/sites/{icao}")
        return SiteDetail.model_validate(data)

    async def fetch_site_sla(self, icao: str, period: SlaPeriod = "last_24h") -> SiteSla:
        data = await self._get_json(f"/v1/sites/{icao}/sla", params={"period": period})
        return SiteSla.model_validate(data)

    async def fetch_site_flights(
        self, icao: str, direction: SiteFlightDirection
    ) -> SiteFlightListResponse:
        data = await self._get_json(f"/v1/sites/{icao}/{direction}")
        return SiteFlightListResponse.model_validate(data)
