"""OpenSky Network REST client resource.

Thin HTTP wrapper over OpenSky's ``/states/all`` endpoint with a fixed
contiguous-US bounding box, and ``/flights/aircraft`` for per-aircraft
flight history (origin/destination + first/last seen).

The resource is a faithful representation of the API — unit conversions
(m/s → kt, meters → feet), the icao24 denylist, and region inference all
happen in the consuming asset.

Bbox ``/states/all`` cost is 1 credit per call (``PIPELINES.md`` §3.1).
At a 30-second poll that totals 2,880 credits/day, leaving ~1,120 of the
4,000/day free-tier budget for ``/flights/aircraft`` queries
(``flight_plan_enrichment``, Phase 05, ~600 cr/day at steady state).

``/flights/aircraft`` charges 1 credit per 1-hour window of the
``begin``→``end`` range (rounded up) and returns 404 — *not* an error —
when no flights match. The fetch method surfaces 404 as an empty tuple
so callers can cache a ``not_found`` row and avoid re-spamming.

**Auth:** OpenSky migrated free-tier auth from HTTP Basic (username +
password) to OAuth2 client-credentials in 2024-2025. Registration now
issues a ``client_id`` (typically ``<email>-api-client``) and a
``client_secret``. The resource exchanges those at OpenSky's Keycloak
token endpoint for a short-lived bearer token, caches it until ~60s
before expiry, and sends ``Authorization: Bearer <token>`` on every
call. A 401 mid-flight invalidates the cache and forces one
refresh-and-retry.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from dagster import ConfigurableResource
from pydantic import PrivateAttr

logger = logging.getLogger(__name__)


# Contiguous US bounding box (PIPELINES.md §3.1).
US_BBOX_LAMIN = 24.0
US_BBOX_LOMIN = -125.0
US_BBOX_LAMAX = 49.0
US_BBOX_LOMAX = -66.0

# (connect, read) seconds. Read is generous; OpenSky usually returns in 2-5s.
DEFAULT_TIMEOUT = (5.0, 30.0)
RETRY_DELAY_SECONDS = 2.0
USER_AGENT = "aerial-fleet-monitor/0.1 (+https://github.com/marky224/aerial-fleet-monitor)"

# OpenSky's Keycloak token endpoint. Uses the legacy `/auth/` path prefix
# (Keycloak <17 layout); a probe against the newer `/realms/...` form
# returned 404 as of 2026-05.
DEFAULT_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/" "protocol/openid-connect/token"
)

# Refresh the cached token this many seconds before its stated expiry, to
# avoid mid-request expirations under clock skew.
TOKEN_REFRESH_LEEWAY_SECONDS = 60.0


class OpenSkyError(Exception):
    """Base for all OpenSky resource errors."""


class OpenSkyAuthError(OpenSkyError):
    """401/403 from the API — bad creds; loud failure."""


class OpenSkyRateLimited(OpenSkyError):
    """429 from the API — skip this cycle, succeed on the next."""


class OpenSkyServerError(OpenSkyError):
    """5xx or network error after one retry."""


class OpenSkyParseError(OpenSkyError):
    """Response was 2xx but JSON shape was unexpected."""


@dataclass(frozen=True)
class OpenSkyState:
    """One aircraft row from ``/states/all``.

    Field names and units mirror the OpenSky API exactly. The asset
    converts to AFM's storage units (feet, knots) at write time.

    Reference: https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors
    """

    icao24: str  # lowercase hex
    callsign: str | None  # trimmed; may be None
    origin_country: str
    time_position: int | None  # epoch seconds; None if no recent fix
    last_contact: int  # epoch seconds
    lon: float | None
    lat: float | None
    baro_altitude_m: float | None
    on_ground: bool
    velocity_ms: float | None
    true_track_deg: float | None
    vertical_rate_ms: float | None  # positive = climb
    geo_altitude_m: float | None
    squawk: str | None  # 4-digit transponder code
    spi: bool  # special purpose indicator
    position_source: int  # 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM


@dataclass(frozen=True)
class OpenSkyResponse:
    """Parsed ``/states/all`` payload plus diagnostics."""

    api_time: int  # OpenSky's reported time field (epoch s)
    states: tuple[OpenSkyState, ...]
    credits_used: int  # 1 per bbox call (PIPELINES.md §3.1)
    rate_limit_remaining: int | None  # from X-Rate-Limit-Remaining if present
    http_status: int


@dataclass(frozen=True)
class OpenSkyFlight:
    """One flight row from ``/flights/aircraft``.

    Times are epoch seconds (OpenSky's native representation). Airport
    codes are ICAO 4-letter codes; either may be ``None`` when OpenSky
    couldn't identify the airport from the flight track. ``callsign`` is
    space-padded in the API; the resource trims it.

    Reference:
    https://openskynetwork.github.io/opensky-api/rest.html#flights-by-aircraft
    """

    icao24: str  # lowercase hex
    first_seen: int  # epoch seconds — flight-track start (≈ takeoff)
    last_seen: int  # epoch seconds — flight-track end (≈ landing)
    callsign: str | None
    est_departure_airport: str | None  # ICAO; None if unidentified
    est_arrival_airport: str | None  # ICAO; None if unidentified
    departure_airport_candidates_count: int
    arrival_airport_candidates_count: int


class OpenSkyResource(ConfigurableResource):  # type: ignore[type-arg]
    """Dagster resource wrapping the OpenSky REST API.

    Attributes:
        client_id: OAuth2 client ID issued at OpenSky registration
            (e.g. ``<email>-api-client``).
        client_secret: OAuth2 client secret.
        base_url: REST API base; override only for tests.
        token_url: OpenSky Keycloak token endpoint; override only if
            OpenSky moves the auth server.
    """

    client_id: str
    client_secret: str
    base_url: str = "https://opensky-network.org/api"
    token_url: str = DEFAULT_TOKEN_URL

    _cached_token: str | None = PrivateAttr(default=None)
    _cached_token_expires_at: float = PrivateAttr(default=0.0)

    def fetch_states(self) -> OpenSkyResponse:
        """GET ``/states/all`` with the US bbox. Raises ``OpenSkyError`` on failure."""
        url = f"{self.base_url}/states/all"
        params: dict[str, Any] = {
            "lamin": US_BBOX_LAMIN,
            "lomin": US_BBOX_LOMIN,
            "lamax": US_BBOX_LAMAX,
            "lomax": US_BBOX_LOMAX,
        }
        try:
            response = self._get_with_retry(url, params=params)
        except OpenSkyAuthError:
            # Mid-flight 401 likely means our cached token expired faster
            # than its stated lifetime. Drop the cache, refresh, and retry
            # exactly once before declaring auth dead.
            logger.info("OpenSky returned 401; clearing cached token and retrying once.")
            self._cached_token = None
            self._cached_token_expires_at = 0.0
            response = self._get_with_retry(url, params=params)
        return self._parse_response(response)

    def fetch_flight_history(
        self,
        icao24: str,
        begin: int,
        end: int,
    ) -> tuple[OpenSkyFlight, ...]:
        """GET ``/flights/aircraft`` for one icao24 over an epoch-second window.

        Returns the flights touching the window, ordered as OpenSky returns
        them (typically chronological). 404 (no flights for this icao24 in
        the range) is surfaced as an empty tuple — the caller decides
        whether to cache that as ``not_found``.

        Auth/rate-limit/server failures bubble up as the usual
        ``OpenSkyError`` subclasses.
        """
        url = f"{self.base_url}/flights/aircraft"
        params: dict[str, Any] = {"icao24": icao24.lower(), "begin": begin, "end": end}
        try:
            response = self._get_with_retry(url, params=params, not_found_ok=True)
        except OpenSkyAuthError:
            logger.info("OpenSky returned 401; clearing cached token and retrying once.")
            self._cached_token = None
            self._cached_token_expires_at = 0.0
            response = self._get_with_retry(url, params=params, not_found_ok=True)
        if response.status_code == 404:
            return ()
        return self._parse_flights_response(response)

    def _get_with_retry(
        self,
        url: str,
        *,
        params: dict[str, Any],
        not_found_ok: bool = False,
    ) -> requests.Response:
        token = self._get_token()
        # Annotated explicitly so the dict satisfies the (invariant)
        # `MutableMapping[str, str | bytes]` param in the `types-requests`
        # stub used by some CI environments. `dict[str, str]` is fine for
        # newer stubs that loosened to `Mapping[str, str | bytes | None]`,
        # but the wider annotation here is a subtype of both.
        headers: dict[str, str | bytes] = {
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {token}",
        }
        last_network_exc: Exception | None = None

        for attempt in (1, 2):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=DEFAULT_TIMEOUT,
                )
            except requests.RequestException as exc:
                last_network_exc = exc
                logger.warning("OpenSky network error (attempt %d/2): %s", attempt, exc)
                if attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise OpenSkyServerError(f"Network error after retry: {exc}") from exc

            status = response.status_code
            if status in (401, 403):
                raise OpenSkyAuthError(
                    f"OpenSky rejected token (HTTP {status}). "
                    "Check OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET."
                )
            if status == 429:
                raise OpenSkyRateLimited("OpenSky rate limit hit (HTTP 429); skipping cycle.")
            if 500 <= status < 600:
                logger.warning("OpenSky HTTP %d (attempt %d/2)", status, attempt)
                if attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise OpenSkyServerError(f"OpenSky HTTP {status} after retry.")
            if status == 200:
                return response
            if status == 404 and not_found_ok:
                return response

            raise OpenSkyError(f"Unexpected HTTP {status}: {response.text[:200]}")

        # Loop always returns or raises; this is defensive.
        raise OpenSkyServerError(f"Exhausted retries: {last_network_exc}")

    def _get_token(self) -> str:
        """Return a valid bearer token, fetching/caching as needed."""
        now = time.time()
        if self._cached_token is not None and now < self._cached_token_expires_at:
            return self._cached_token

        token, expires_in = self._fetch_token()
        self._cached_token = token
        self._cached_token_expires_at = now + max(0.0, expires_in - TOKEN_REFRESH_LEEWAY_SECONDS)
        return token

    def _fetch_token(self) -> tuple[str, float]:
        """POST to the token endpoint. Returns ``(access_token, expires_in)``."""
        try:
            response = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                headers={"User-Agent": USER_AGENT},
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException as exc:
            raise OpenSkyServerError(f"Token endpoint network error: {exc}") from exc

        status = response.status_code
        if status in (400, 401, 403):
            raise OpenSkyAuthError(
                f"OpenSky token endpoint rejected credentials (HTTP {status}). "
                "Check OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET. "
                f"Body: {response.text[:300]}"
            )
        if status != 200:
            raise OpenSkyServerError(
                f"Token endpoint returned HTTP {status}: {response.text[:300]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyParseError(f"Token endpoint returned non-JSON: {exc}") from exc

        access_token = payload.get("access_token")
        expires_in = payload.get("expires_in")
        if not isinstance(access_token, str) or not access_token:
            raise OpenSkyParseError(f"Token endpoint missing access_token: {payload!r}")
        if not isinstance(expires_in, int | float) or expires_in <= 0:
            raise OpenSkyParseError(f"Token endpoint missing/invalid expires_in: {payload!r}")

        return access_token, float(expires_in)

    @staticmethod
    def _parse_response(response: requests.Response) -> OpenSkyResponse:
        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyParseError(f"Non-JSON response: {exc}") from exc

        if not isinstance(payload, dict) or "time" not in payload:
            raise OpenSkyParseError(f"Unexpected payload shape: {type(payload).__name__}")

        try:
            api_time = int(payload["time"])
        except (TypeError, ValueError) as exc:
            raise OpenSkyParseError(f"Invalid 'time' field: {payload.get('time')!r}") from exc

        raw_states = payload.get("states") or []
        if not isinstance(raw_states, list):
            raise OpenSkyParseError(f"'states' is not a list: {type(raw_states).__name__}")

        states = tuple(
            OpenSkyResource._row_to_state(row)
            for row in raw_states
            if isinstance(row, list) and len(row) >= 17 and isinstance(row[0], str) and row[0]
        )

        rate_limit_header = response.headers.get("X-Rate-Limit-Remaining")
        rate_limit_remaining = (
            int(rate_limit_header)
            if rate_limit_header is not None and rate_limit_header.lstrip("-").isdigit()
            else None
        )

        return OpenSkyResponse(
            api_time=api_time,
            states=states,
            credits_used=1,
            rate_limit_remaining=rate_limit_remaining,
            http_status=response.status_code,
        )

    @staticmethod
    def _parse_flights_response(response: requests.Response) -> tuple[OpenSkyFlight, ...]:
        """Parse a 200 from ``/flights/aircraft`` into a tuple of OpenSkyFlight.

        Caller has already filtered 404 to ``()``. Anything else with a
        2xx body that doesn't look like a flight list raises
        ``OpenSkyParseError``.
        """
        try:
            payload = response.json()
        except ValueError as exc:
            raise OpenSkyParseError(f"Non-JSON flights response: {exc}") from exc
        if not isinstance(payload, list):
            raise OpenSkyParseError(f"Flights payload not a list: {type(payload).__name__}")

        flights: list[OpenSkyFlight] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            icao24_raw = row.get("icao24")
            first_seen_raw = row.get("firstSeen")
            last_seen_raw = row.get("lastSeen")
            if not isinstance(icao24_raw, str) or not icao24_raw:
                continue
            if not isinstance(first_seen_raw, int | float):
                continue
            if not isinstance(last_seen_raw, int | float):
                continue

            callsign_raw = row.get("callsign")
            callsign = (
                callsign_raw.strip()
                if isinstance(callsign_raw, str) and callsign_raw.strip()
                else None
            )

            dep = row.get("estDepartureAirport")
            arr = row.get("estArrivalAirport")
            dep_count = row.get("departureAirportCandidatesCount", 0)
            arr_count = row.get("arrivalAirportCandidatesCount", 0)

            flights.append(
                OpenSkyFlight(
                    icao24=icao24_raw.lower(),
                    first_seen=int(first_seen_raw),
                    last_seen=int(last_seen_raw),
                    callsign=callsign,
                    est_departure_airport=dep if isinstance(dep, str) and dep else None,
                    est_arrival_airport=arr if isinstance(arr, str) and arr else None,
                    departure_airport_candidates_count=(
                        int(dep_count) if isinstance(dep_count, int | float) else 0
                    ),
                    arrival_airport_candidates_count=(
                        int(arr_count) if isinstance(arr_count, int | float) else 0
                    ),
                )
            )
        return tuple(flights)

    @staticmethod
    def _row_to_state(row: list[Any]) -> OpenSkyState:
        callsign_raw = row[1]
        callsign = callsign_raw.strip() if isinstance(callsign_raw, str) else None
        if callsign == "":
            callsign = None

        squawk_raw = row[14]
        squawk = squawk_raw if isinstance(squawk_raw, str) and squawk_raw else None

        position_source_raw = row[16]
        position_source = int(position_source_raw) if position_source_raw is not None else 0

        return OpenSkyState(
            icao24=row[0].lower(),
            callsign=callsign,
            origin_country=str(row[2]) if row[2] is not None else "",
            time_position=int(row[3]) if row[3] is not None else None,
            last_contact=int(row[4]),
            lon=float(row[5]) if row[5] is not None else None,
            lat=float(row[6]) if row[6] is not None else None,
            baro_altitude_m=float(row[7]) if row[7] is not None else None,
            on_ground=bool(row[8]),
            velocity_ms=float(row[9]) if row[9] is not None else None,
            true_track_deg=float(row[10]) if row[10] is not None else None,
            vertical_rate_ms=float(row[11]) if row[11] is not None else None,
            geo_altitude_m=float(row[13]) if row[13] is not None else None,
            squawk=squawk,
            spi=bool(row[15]),
            position_source=position_source,
        )
