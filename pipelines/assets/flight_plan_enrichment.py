"""flight_plan_enrichment — per-aircraft origin/destination cache.

Writes ``origin_icao``, ``destination_icao``, and flight first/last seen
times into ``app.flight_plans`` so the Phase 05 case detector's
``diversion`` and ``delay`` rules (and the ``BaselineProvider`` for
``delay``) can read flight-plan data without per-detection upstream
calls.

OpenSky's ``/states/all`` (used by the ingestion asset) does not carry
flight-plan data; ``/flights/aircraft`` does, but costs 1 credit per
1-hour window per call. This asset uses a Postgres cache (12h TTL) so
each watched icao24 incurs at most 2 fetches/day. At ~50 watched
aircraft x 6h fetch window x 2 refreshes/day, steady-state cost is
~600 cr/day — comfortably under the ~1,120 cr/day headroom past
``/states/all``.

Cycle (hourly):
  1. SELECT distinct icao24 from ``app.current_positions`` last hour
     where ``customer_region`` is non-null (in-scope traffic).
  2. LEFT JOIN ``app.flight_plans``; pick rows where
     ``refreshed_at IS NULL`` OR ``refreshed_at < NOW() - INTERVAL '12h'``.
  3. Cap the per-cycle fetch list at ``MAX_FETCHES_PER_CYCLE`` so a
     transient spike never lets a single run consume the day's budget.
  4. For each picked icao24, call
     ``OpenSkyResource.fetch_flight_history(icao24, now-6h, now)``:
       - Multiple flights returned → keep the one with the largest
         ``last_seen`` (most recent).
       - Empty tuple (404) → write ``fetch_status='not_found'``.
       - ``OpenSkyRateLimited`` → break out; remaining icao24s wait for
         the next cycle.
       - Other ``OpenSkyError`` → write ``fetch_status='error'``,
         continue. The next 12h cycle will retry.
  5. UPSERT one row per icao24 in ``app.flight_plans``.

Materialization metadata exposes the per-status counts so verification
can be done at metadata level rather than RUN_SUCCESS (the project's
established discipline — RUN_SUCCESS != verified).
"""

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipelines.assets.ingestion import opensky_positions
from pipelines.resources import OpenSkyResource, PostgresResource
from pipelines.resources.opensky import (
    OpenSkyAuthError,
    OpenSkyError,
    OpenSkyFlight,
    OpenSkyRateLimited,
)

# 6-hour fetch window — covers nearly all in-air commercial flights at
# 6 credits/call. Smaller windows miss long-haul; larger windows burn
# the budget faster.
FETCH_WINDOW_SECONDS = 6 * 3600

# 12-hour cache TTL. Below this, the SELECT in step 2 sees the row as
# fresh and skips it.
CACHE_TTL_SECONDS = 12 * 3600

# Per-cycle safety cap. At 6 credits/call, 30 fetches = 180 credits per
# cycle — a single bad cycle can't blow the daily 4,000 cr cap.
MAX_FETCHES_PER_CYCLE = 30

# Rows in ``app.current_positions`` older than this aren't considered
# "active" — there's no point enriching flight plans for stale data.
ACTIVE_WINDOW_SECONDS = 3600


@dataclass(frozen=True)
class EnrichmentResult:
    """Per-cycle outcome surfaced as Dagster metadata."""

    candidates: int  # icao24s in scope this cycle (active + in-scope)
    fetch_attempts: int  # icao24s actually queried (capped by MAX_FETCHES_PER_CYCLE)
    fetched_success: int  # OpenSky returned flights → flight_plans row updated
    fetched_not_found: int  # 404 → cached as not_found for the next 12h
    fetched_error: int  # transient OpenSkyError → cached as error, retry next cycle
    rate_limited: bool  # True if a 429 cut the cycle short
    deferred: int = field(default=0)  # icao24s still stale but past the per-cycle cap


def _result_metadata(result: EnrichmentResult) -> dict[str, MetadataValue]:
    return {
        "candidates": MetadataValue.int(result.candidates),
        "fetch_attempts": MetadataValue.int(result.fetch_attempts),
        "fetched_success": MetadataValue.int(result.fetched_success),
        "fetched_not_found": MetadataValue.int(result.fetched_not_found),
        "fetched_error": MetadataValue.int(result.fetched_error),
        "rate_limited": MetadataValue.bool(result.rate_limited),
        "deferred": MetadataValue.int(result.deferred),
    }


def _select_stale_icao24s(postgres: PostgresResource) -> list[str]:
    """Return icao24s in ``current_positions`` lacking a fresh flight_plans row."""
    sql = """
        SELECT cp.icao24
          FROM app.current_positions cp
          LEFT JOIN app.flight_plans fp ON fp.icao24 = cp.icao24
         WHERE cp.last_seen_at >= NOW() - make_interval(secs => %s)
           AND cp.customer_region IS NOT NULL
           AND (
                 fp.refreshed_at IS NULL
              OR fp.refreshed_at < NOW() - make_interval(secs => %s)
               )
         ORDER BY fp.refreshed_at NULLS FIRST, cp.icao24
    """
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (ACTIVE_WINDOW_SECONDS, CACHE_TTL_SECONDS))
        return [row[0] for row in cur.fetchall()]


def _pick_most_recent(flights: tuple[OpenSkyFlight, ...]) -> OpenSkyFlight:
    """Largest ``last_seen`` wins. OpenSky may return multiple legs in window."""
    return max(flights, key=lambda f: f.last_seen)


def _upsert_flight_plan(
    postgres: PostgresResource,
    icao24: str,
    flight: OpenSkyFlight | None,
    fetch_status: str,
) -> None:
    if flight is not None:
        origin = flight.est_departure_airport
        destination = flight.est_arrival_airport
        callsign = flight.callsign
        departure = datetime.fromtimestamp(flight.first_seen, tz=UTC)
        arrival = datetime.fromtimestamp(flight.last_seen, tz=UTC)
    else:
        origin = None
        destination = None
        callsign = None
        departure = None
        arrival = None

    sql = """
        INSERT INTO app.flight_plans (
            icao24, origin_icao, destination_icao, callsign,
            departure_time, arrival_time, refreshed_at, fetch_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (icao24) DO UPDATE
        SET origin_icao = EXCLUDED.origin_icao,
            destination_icao = EXCLUDED.destination_icao,
            callsign = EXCLUDED.callsign,
            departure_time = EXCLUDED.departure_time,
            arrival_time = EXCLUDED.arrival_time,
            refreshed_at = EXCLUDED.refreshed_at,
            fetch_status = EXCLUDED.fetch_status
    """
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (icao24, origin, destination, callsign, departure, arrival, fetch_status),
            )
        conn.commit()


def run_flight_plan_enrichment(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    opensky: OpenSkyResource,
) -> EnrichmentResult:
    """Body extracted for unit-testing without Dagster's @asset wrapper."""
    stale = _select_stale_icao24s(postgres)
    candidates = len(stale)
    to_fetch = stale[:MAX_FETCHES_PER_CYCLE]
    deferred = max(0, candidates - len(to_fetch))

    now = int(time.time())
    begin = now - FETCH_WINDOW_SECONDS

    success = 0
    not_found = 0
    errors = 0
    rate_limited = False
    attempts = 0

    for icao24 in to_fetch:
        attempts += 1
        try:
            flights = opensky.fetch_flight_history(icao24, begin, now)
        except OpenSkyRateLimited:
            # Stop the cycle — remaining icao24s will be picked up next
            # cycle when their TTL still has them stale.
            rate_limited = True
            attempts -= 1  # we didn't successfully attempt this one
            context.log.warning(
                "flight_plan_enrichment: rate-limited after %d attempts; " "deferring %d remaining",
                attempts,
                len(to_fetch) - attempts,
            )
            break
        except OpenSkyAuthError:
            # Bad creds — raise loudly so the asset shows a real failure
            # rather than silently caching every icao24 as 'error'.
            raise
        except OpenSkyError as exc:
            errors += 1
            context.log.warning("flight_plan_enrichment: fetch error for %s: %s", icao24, exc)
            _upsert_flight_plan(postgres, icao24, None, "error")
            continue

        if not flights:
            not_found += 1
            _upsert_flight_plan(postgres, icao24, None, "not_found")
            continue

        chosen = _pick_most_recent(flights)
        _upsert_flight_plan(postgres, icao24, chosen, "success")
        success += 1

    return EnrichmentResult(
        candidates=candidates,
        fetch_attempts=attempts,
        fetched_success=success,
        fetched_not_found=not_found,
        fetched_error=errors,
        rate_limited=rate_limited,
        deferred=deferred,
    )


@asset(
    group_name="enrichment",
    deps=[opensky_positions],
    description=(
        "Enriches active icao24s in app.current_positions with origin/"
        "destination airports from OpenSky /flights/aircraft. Writes to "
        "app.flight_plans with a 12h cache TTL; the case detector's "
        "diversion + delay rules read this cache."
    ),
    metadata={"target": "app.flight_plans", "cadence": "hourly"},
)
def flight_plan_enrichment(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    opensky: OpenSkyResource,
) -> MaterializeResult:
    result = run_flight_plan_enrichment(context, postgres, opensky)
    context.log.info(
        "flight_plan_enrichment: candidates=%d attempts=%d success=%d "
        "not_found=%d error=%d rate_limited=%s deferred=%d",
        result.candidates,
        result.fetch_attempts,
        result.fetched_success,
        result.fetched_not_found,
        result.fetched_error,
        result.rate_limited,
        result.deferred,
    )
    return MaterializeResult(metadata=_result_metadata(result))
