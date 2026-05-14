"""NOAA Aviation Weather Center REST client resource.

Thin HTTP wrapper over ``aviationweather.gov/api`` for two endpoints:
``/data/metar`` and ``/data/taf``. Both are unauthenticated and accept a
comma-separated ``?ids=`` parameter so the entire watched-airport list
is satisfied in one call per endpoint per cycle (per ``PIPELINES.md``
§3.2 — "single ?ids= call").

The resource is intentionally thin: it parses the JSON and converts a
few unit-laden fields (altimeter hPa → inHg; observation epoch →
``datetime``) but leaves business logic (ceiling derivation from cloud
layers, flight-category computation) in the consuming asset.

No credentials required; no daily call budget published for this volume
(35 airports x 1 call per endpoint every 5 min). A 30-second read
timeout is used per call.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import requests
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


# (connect, read) seconds. NOAA's API is usually <1s.
DEFAULT_TIMEOUT = (5.0, 30.0)
RETRY_DELAY_SECONDS = 2.0
USER_AGENT = "aerial-fleet-monitor/0.1 (+https://github.com/marky224/aerial-fleet-monitor)"

# hPa → inHg. 1013.25 hPa = 29.92 inHg (standard atmosphere).
HPA_TO_IN_HG = 0.02953


class NoaaError(Exception):
    """Base for all NOAA resource errors."""


class NoaaRateLimited(NoaaError):
    """429 from the API — skip this cycle, succeed on the next."""


class NoaaServerError(NoaaError):
    """5xx or network error after one retry."""


class NoaaParseError(NoaaError):
    """Response was 2xx but JSON shape was unexpected."""


@dataclass(frozen=True)
class NoaaMetarReport:
    """One station's METAR observation, lightly parsed.

    ``clouds`` is the raw NOAA list (each element a dict with ``cover``
    and ``base``); ceiling derivation happens in the asset for
    testability. ``raw_json`` is the whole NOAA response object,
    persisted into the ``metar_parsed`` JSONB column.
    """

    icao: str
    raw_text: str | None
    observed_at: datetime | None
    wind_kt: int | None
    wind_dir_deg: int | None  # None for VRB / variable
    visibility_sm: float | None
    temperature_c: float | None
    altimeter_in_hg: float | None
    clouds: list[dict[str, Any]]
    raw_json: dict[str, Any]


class NoaaResource(ConfigurableResource):  # type: ignore[type-arg]
    """Dagster resource wrapping the NOAA aviation-weather REST API.

    Attributes:
        base_url: REST API base; override only for tests.
        timeout_seconds: read timeout per call. Connect timeout is fixed
            at 5s (NOAA is usually <1s in practice).
    """

    base_url: str = "https://aviationweather.gov/api"

    def fetch_metars(self, icaos: list[str]) -> list[NoaaMetarReport]:
        """GET ``/data/metar?ids=...&format=json``. Returns one report per station present.

        Stations absent from the response (NOAA has no recent METAR for
        them) are silently dropped — the asset's UPSERT then skips them.
        """
        if not icaos:
            return []
        url = f"{self.base_url}/data/metar"
        params = {"ids": ",".join(icaos), "format": "json"}
        payload = self._get_json(url, params=params)
        if not isinstance(payload, list):
            raise NoaaParseError(f"METAR payload is not a list: {type(payload).__name__}")
        return [self._parse_metar(item) for item in payload if isinstance(item, dict)]

    def fetch_tafs(self, icaos: list[str]) -> dict[str, str]:
        """GET ``/data/taf?ids=...&format=json``. Returns {icao: rawTAF text}.

        Only stations with a non-empty ``rawTAF`` are included. Many
        non-TAF stations exist in our watchlist (smaller GA-only fields);
        absence from the returned map is normal.
        """
        if not icaos:
            return {}
        url = f"{self.base_url}/data/taf"
        params = {"ids": ",".join(icaos), "format": "json"}
        payload = self._get_json(url, params=params)
        if not isinstance(payload, list):
            raise NoaaParseError(f"TAF payload is not a list: {type(payload).__name__}")

        result: dict[str, str] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            icao = item.get("icaoId")
            raw_taf = item.get("rawTAF")
            if isinstance(icao, str) and isinstance(raw_taf, str) and raw_taf:
                result[icao.upper()] = raw_taf
        return result

    def _get_json(self, url: str, *, params: dict[str, Any]) -> Any:
        """Issue a GET with one retry on 5xx/network errors. Returns parsed JSON."""
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
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
                logger.warning("NOAA network error (attempt %d/2): %s", attempt, exc)
                if attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise NoaaServerError(f"Network error after retry: {exc}") from exc

            status = response.status_code
            if status == 429:
                raise NoaaRateLimited("NOAA rate limit hit (HTTP 429); skipping cycle.")
            if 500 <= status < 600:
                logger.warning("NOAA HTTP %d (attempt %d/2)", status, attempt)
                if attempt == 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                raise NoaaServerError(f"NOAA HTTP {status} after retry.")
            if status == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    raise NoaaParseError(f"Non-JSON response: {exc}") from exc

            raise NoaaError(f"Unexpected HTTP {status}: {response.text[:200]}")

        raise NoaaServerError(f"Exhausted retries: {last_network_exc}")

    @staticmethod
    def _parse_metar(item: dict[str, Any]) -> NoaaMetarReport:
        """Convert one NOAA METAR JSON object into a NoaaMetarReport.

        Tolerant of missing/None fields — the resulting report has
        ``None`` for anything NOAA didn't return or returned unparseable.
        The full NOAA object is preserved in ``raw_json`` so nothing is
        lost on the way to the JSONB column.
        """
        icao = _str_or_none(item.get("icaoId")) or ""

        obs_time_raw = item.get("obsTime")
        observed_at: datetime | None
        if isinstance(obs_time_raw, int | float) and obs_time_raw > 0:
            observed_at = datetime.fromtimestamp(int(obs_time_raw), tz=UTC)
        else:
            observed_at = None

        # Wind: NOAA gives an int knots or None; calm reported as 0.
        wspd_raw = item.get("wspd")
        wind_kt = int(wspd_raw) if isinstance(wspd_raw, int | float) else None

        # Wind direction: 0-360 int, or "VRB" (variable) → None.
        wdir_raw = item.get("wdir")
        if isinstance(wdir_raw, int | float):
            wind_dir_deg: int | None = int(wdir_raw)
        else:
            wind_dir_deg = None

        # Visibility: float, int, or "10+" string ("greater than 10 sm").
        # Fractional strings like "1 1/2" are dropped (None) — we'd need
        # a parser; not worth it for the rare GA report.
        visibility_sm = _parse_visibility(item.get("visib"))

        temp_raw = item.get("temp")
        temperature_c = float(temp_raw) if isinstance(temp_raw, int | float) else None

        # NOAA returns altimeter in hPa (hectopascals); convert to inHg
        # to match the column unit and pilot convention.
        altim_raw = item.get("altim")
        altimeter_in_hg = (
            round(float(altim_raw) * HPA_TO_IN_HG, 2)
            if isinstance(altim_raw, int | float)
            else None
        )

        clouds_raw = item.get("clouds")
        clouds = (
            [layer for layer in clouds_raw if isinstance(layer, dict)]
            if isinstance(clouds_raw, list)
            else []
        )

        return NoaaMetarReport(
            icao=icao.upper(),
            raw_text=_str_or_none(item.get("rawOb")),
            observed_at=observed_at,
            wind_kt=wind_kt,
            wind_dir_deg=wind_dir_deg,
            visibility_sm=visibility_sm,
            temperature_c=temperature_c,
            altimeter_in_hg=altimeter_in_hg,
            clouds=clouds,
            raw_json=item,
        )


def _str_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _parse_visibility(value: Any) -> float | None:
    """Parse NOAA's `visib` field into statute miles.

    Handles ints, floats, and the ``"N+"`` capped string (e.g. ``"10+"``
    → ``10.0``, meaning visibility ≥ N sm). Fractional METAR notation
    (``"1 1/2"``) and other strings yield ``None``.
    """
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s.endswith("+") and s[:-1].replace(".", "", 1).isdigit():
            return float(s[:-1])
        try:
            return float(s)
        except ValueError:
            return None
    return None
