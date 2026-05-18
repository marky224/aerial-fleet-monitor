"""Orchestration layer: composes readers → transforms → Foundry upserts.

This is the only module that wires ``api_readers`` (local /v1 reads),
``transforms`` (pure mappers), and ``ontology_writers`` (Foundry Action
writes) into runnable jobs. It owns:

  - ``FoundrySyncSkipped`` — the typed *local-standalone* signal. AFM's
    local stack must run with Foundry creds absent or Foundry unreachable;
    the Dagster asset layer (``pipelines/``) catches this and records a
    *skipped* materialization rather than a failure. Defined here because
    ``api_readers`` / ``settings`` / ``logging`` / ``ontology_writers``
    only reference it in prose — this is its single import home.
  - ``guarded_sync`` — maps the two skip-worthy conditions
    (``pydantic.ValidationError`` from absent/bad ``_private/foundry/.env``;
    ``httpx.HTTPError`` surviving the readers'/writer's retry) into
    ``FoundrySyncSkipped``. A 4xx from a malformed Action payload, or any
    other exception, is a *defect* and propagates unchanged.
  - ``incremental_sync_positions`` / ``full_sync_sites`` — the two jobs.
  - ``TakeoffDetector`` — stateful on-ground→airborne edge detection that
    synthesizes ``Flight`` primary keys. **Now wired** (Flight schema +
    ``upsert-flight`` proven 2026-05-16): a detected takeoff triggers a
    *create-only* ``upsert_flight_batch`` of the takeoff-shaped Flight
    (``transforms.takeoff_to_flight``).
  - ``enriched_sync_flights`` — the deferred FlightDetail/trail backfill,
    a per-icao24 ``/v1/flights`` (+ 2h trail) fanout off a slower cadence
    (hourly), out of scope for the 30s positions tick. ``/v1/flights`` is
    icao24-keyed and returns the aircraft's *current* flight, so only the
    **latest flight_id per icao24** is a safe enrichment target.

Cursor & detector state are *returned* (``SyncResult.cursor`` /
``SyncResult.detector_state``), never persisted here: this module stays
I/O-pure for unit testing. The Dagster asset owns persistence — it seeds
a fresh ``TakeoffDetector(prior_state)`` from its prior materialization
metadata each tick and writes the post-run state back, so detector state
survives process restarts (an in-process-only detector would reset and
miss every cross-restart edge). ``state_for`` bounds the persisted map to
aircraft seen *this run* (a >1-tick absence is treated as a fresh
sighting — acceptable per the detector's "first sighting only seeds"
rule, and it keeps the metadata blob proportional to live traffic, not
to every icao24 ever observed).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import structlog
from pydantic import ValidationError

from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import Flight, FlightDetail, Position
from afm_foundry_sync.ontology_writers import BatchResult, FoundryWriter
from afm_foundry_sync.settings import FoundrySettings
from afm_foundry_sync.transforms import (
    flight_detail_to_flight,
    position_to_aircraft,
    site_to_site,
    takeoff_to_flight,
)

logger = structlog.get_logger(__name__)


class FoundrySyncSkipped(Exception):
    """Sync did not run because Foundry is unconfigured or unreachable.

    Not an error: the local stack is designed to run standalone. The
    Dagster asset layer translates this into a skipped materialization
    (``MetadataValue.text("skipped: ...")``), not a failed run.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Outcome of one sync job run.

    ``cursor`` is the value the caller should persist and pass back as
    ``since`` next run (positions only; ``None`` for the full site sync).
    ``takeoffs_detected`` counts state-machine edges this run;
    ``flights_written`` is the create-only Flight upsert count from those
    edges (≤ takeoffs_detected; equal on full success). ``detector_state``
    is the post-run on-ground map the caller persists and seeds next
    tick's detector with (positions w/ detector only; ``None`` otherwise).
    """

    attempted: int
    succeeded: int
    failed: int = 0
    cursor: datetime | None = None
    takeoffs_detected: int = 0
    flights_written: int = 0
    detector_state: dict[str, bool] | None = None


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Outcome of one tenant-Aircraft reconcile (Fix C).

    The positions sync is upsert-only, so aircraft that have left the live
    feed persist in the Ontology forever. This job diffs the tenant's
    Aircraft set against the live feed and deletes the difference.

    ``live`` / ``tenant`` are the two set sizes; ``orphans`` is
    ``tenant - live``; ``deleted`` is how many of those the delete batch
    confirmed (== ``orphans`` on full success). ``skipped_empty_live`` is
    True when the live feed was empty and the reconcile bailed *without*
    enumerating the tenant or deleting anything — an empty feed means
    "fleet unknown right now" (e.g. an upstream OpenSky 429), never "fleet
    is empty", so evicting on no-knowledge would wipe the whole tenant.
    """

    live: int
    tenant: int
    orphans: int
    deleted: int
    skipped_empty_live: bool = False


@dataclass(frozen=True, slots=True)
class FlightEnrichmentResult:
    """Outcome of one deferred Flight-enrichment pass.

    The takeoff path writes a Flight with only the 3 synthesized identity
    fields; this backfills route/operator/registration/status + 2h trail
    from ``/v1/flights/{icao24}``. ``/v1/flights`` keys on icao24 and
    returns the aircraft's *current* flight (404 outside the API recency
    window), so only the **latest flight_id per icao24** is a safe target —
    an older PK would be re-written with a newer flight's data.

    ``tenant_flights`` is every Flight PK in the tenant; ``candidates`` is
    the latest-per-icao24 subset actually enriched; ``enriched`` is the
    confirmed upsert count (== candidates - skipped - failed on full
    success); ``skipped_inactive`` 404'd (aircraft not currently flying —
    nothing to enrich, not an error); ``fetch_failed`` hit a non-404 HTTP
    *status* error **or** a transport/timeout error for one icao24 (counted,
    the pass continues so one bad/slow flight can't sink the batch; a total
    API outage just makes every flight ``fetch_failed`` — a safe, observable
    no-op. Foundry-side I/O failure still surfaces as ``FoundrySyncSkipped``).
    """

    tenant_flights: int
    candidates: int
    enriched: int
    skipped_inactive: int = 0
    fetch_failed: int = 0


@contextlib.asynccontextmanager
async def guarded_sync(job: str) -> AsyncIterator[None]:
    """Translate config-absent / Foundry-unreachable into ``FoundrySyncSkipped``.

    ``ValidationError`` → creds missing/malformed (``FoundrySettings()``).
    ``httpx.HTTPError`` → local API or Foundry unreachable after the
    readers'/writer's own retry budget is spent (includes ``HTTPStatusError``;
    a malformed-payload 4xx is a defect, but Foundry-side 4xx during a
    sync run is operationally a skip-worthy "Foundry won't take this"
    signal — kept conservative: only transport/HTTP errors map, not
    arbitrary exceptions). Anything else propagates as a real defect.
    """
    try:
        yield
    except ValidationError as exc:
        logger.warning("foundry_sync_skipped", job=job, reason="config_absent")
        raise FoundrySyncSkipped(f"{job}: foundry config absent") from exc
    except httpx.HTTPError as exc:
        logger.warning("foundry_sync_skipped", job=job, reason="unreachable", error=str(exc))
        raise FoundrySyncSkipped(f"{job}: foundry/api unreachable: {exc}") from exc


def _dedupe_latest(positions: Iterable[Position]) -> list[Position]:
    """Collapse duplicate icao24 to the row with the newest ``last_seen_at``.

    A single /v1/positions/live batch can carry stale + fresh rows for the
    same aircraft; the Ontology object is keyed on icao24, so the newest
    wins (build-doc dedup contract). Insertion order of survivors is
    preserved for stable logging/testing.
    """
    latest: dict[str, Position] = {}
    for p in positions:
        prev = latest.get(p.icao24)
        if prev is None or p.last_seen_at > prev.last_seen_at:
            latest[p.icao24] = p
    return list(latest.values())


def synthesize_flight_id(icao24: str, takeoff_ts: datetime) -> str:
    """Flight PK = ``{icao24}-{unix_takeoff_ts}`` (ONTOLOGY.md Flight key)."""
    return f"{icao24}-{int(takeoff_ts.timestamp())}"


def parse_flight_id(flight_id: str) -> tuple[str, datetime]:
    """Inverse of ``synthesize_flight_id``: ``{icao24}-{unix_ts}`` →
    ``(icao24, takeoff_ts)``.

    icao24 is lowercase hex (never contains ``-``), so the unix timestamp
    is exactly the segment after the last ``-``. Raises ``ValueError`` on a
    malformed PK (empty icao24, missing or non-numeric timestamp) so the
    enrichment caller can drop that PK with a warning rather than crash.
    """
    icao24, _, ts = flight_id.rpartition("-")
    if not icao24 or not ts:
        raise ValueError(f"malformed flight_id: {flight_id!r}")
    return icao24, datetime.fromtimestamp(int(ts), tz=UTC)


@dataclass(frozen=True, slots=True)
class Takeoff:
    """A detected on-ground→airborne transition and its synthesized Flight PK."""

    icao24: str
    takeoff_ts: datetime
    flight_id: str


class TakeoffDetector:
    """Per-icao24 on-ground edge detector.

    A transition from a previously-observed ``on_ground=True`` to
    ``on_ground=False`` is a takeoff; ``takeoff_ts`` is that row's
    ``last_seen_at``. First sighting of an aircraft only seeds state (no
    edge — we cannot infer a transition without a prior sample). State is
    caller-owned (see module docstring): the Dagster asset seeds a fresh
    instance from persisted state each tick and writes the post-run state
    back, so edges survive process restarts.
    """

    def __init__(self, prior_on_ground: dict[str, bool] | None = None) -> None:
        # Copy: callers pass deserialized metadata we must not alias/mutate.
        self._on_ground: dict[str, bool] = dict(prior_on_ground or {})

    def state_for(self, icao24s: Iterable[str]) -> dict[str, bool]:
        """On-ground state restricted to the given icao24s (the run's batch).

        Bounds the persisted blob to live traffic: aircraft absent this run
        are dropped, so a >1-tick gap re-seeds as a first sighting (no
        edge) rather than growing the map unboundedly.
        """
        return {k: self._on_ground[k] for k in set(icao24s) if k in self._on_ground}

    def observe(self, positions: Iterable[Position]) -> list[Takeoff]:
        takeoffs: list[Takeoff] = []
        for p in positions:
            prev = self._on_ground.get(p.icao24)
            if prev is True and p.on_ground is False:
                takeoffs.append(
                    Takeoff(
                        icao24=p.icao24,
                        takeoff_ts=p.last_seen_at,
                        flight_id=synthesize_flight_id(p.icao24, p.last_seen_at),
                    )
                )
            self._on_ground[p.icao24] = p.on_ground
        return takeoffs


async def incremental_sync_positions(
    client: AfmApiClient,
    writer: FoundryWriter,
    *,
    since: datetime | None = None,
    detector: TakeoffDetector | None = None,
) -> SyncResult:
    """Sync /v1/positions/live → Aircraft Ontology objects.

    ``since`` is the previous run's cursor; the v1 API returns the full
    live set (no server-side delta), so it is advisory/logged for now and
    the cursor returned is the response ``server_time``. If a ``detector``
    is supplied, its on-ground→airborne edges trigger a *create-only*
    ``upsert_flight_batch`` (takeoff-shaped Flight per
    ``takeoff_to_flight``; FlightDetail enrichment is deferred — see module
    docstring) and the post-run detector state is returned for the caller
    to persist and re-seed next tick.
    """
    response = await client.fetch_positions_live()
    deduped = _dedupe_latest(response.items)
    logger.info(
        "foundry_positions_sync",
        since=since.isoformat() if since else None,
        received=len(response.items),
        deduped=len(deduped),
        server_time=response.server_time.isoformat(),
    )

    takeoffs = detector.observe(deduped) if detector is not None else []
    if takeoffs:
        logger.info("foundry_takeoffs_detected", count=len(takeoffs))

    batch: BatchResult = await writer.upsert_aircraft_batch(
        [position_to_aircraft(p) for p in deduped]
    )
    # Create-only Flight write off detected takeoffs. Empty list → no-op
    # (upsert_flight_batch short-circuits), so this is safe with no detector.
    flight_batch: BatchResult = await writer.upsert_flight_batch(
        [takeoff_to_flight(t.flight_id, t.icao24, t.takeoff_ts) for t in takeoffs]
    )
    return SyncResult(
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
        cursor=response.server_time,
        takeoffs_detected=len(takeoffs),
        flights_written=flight_batch.succeeded,
        detector_state=(
            detector.state_for(p.icao24 for p in deduped) if detector is not None else None
        ),
    )


async def full_sync_sites(
    client: AfmApiClient,
    writer: FoundryWriter,
) -> SyncResult:
    """Full refresh of Site Ontology objects from /v1/sites + per-site SLA.

    Low cardinality (watched-airport count) so a full upsert each run is
    fine. A per-site SLA fetch failure is non-fatal: the Site is still
    written from its detail with SLA fields null (``site_to_site`` already
    treats ``sla=None`` as "no SLA"), so one bad SLA endpoint can't sink
    the whole batch.
    """
    listing = await client.fetch_sites()
    sites = []
    for item in listing.items:
        detail = await client.fetch_site(item.icao)
        try:
            sla = await client.fetch_site_sla(item.icao)
        except httpx.HTTPError as exc:
            logger.warning("foundry_site_sla_skipped", icao=item.icao, error=str(exc))
            sla = None
        sites.append(site_to_site(detail, sla))

    logger.info("foundry_sites_sync", count=len(sites))
    batch = await writer.upsert_site_batch(sites)
    return SyncResult(
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
    )


async def reconcile_aircraft(
    client: AfmApiClient,
    writer: FoundryWriter,
) -> ReconcileResult:
    """Evict tenant Aircraft objects no longer in the live feed (Fix C).

    The positions sync is upsert-only and never deletes, so aircraft that
    have departed (icao24 absent from ``/v1/positions/live`` after the
    API's recency window + the Postgres prune) accumulate in the Ontology
    indefinitely. This diffs ``tenant - live`` and deletes the orphans via
    the ``delete-aircraft`` Action.

    **Empty-live safety guard:** if the live feed is empty, bail *before*
    enumerating the tenant — ``tenant - {}`` is the entire tenant, and an
    empty feed means the fleet is momentarily unknown (e.g. an upstream
    OpenSky 429), not that every aircraft has gone. Reconciling on
    no-knowledge would delete every Aircraft object. The interim manual
    purge tool runs with a human watching; an automated hourly job must
    not. Returns a no-op result flagged ``skipped_empty_live``.
    """
    response = await client.fetch_positions_live()
    live = {p.icao24 for p in response.items}
    if not live:
        logger.warning("foundry_reconcile_skipped_empty_live")
        return ReconcileResult(live=0, tenant=0, orphans=0, deleted=0, skipped_empty_live=True)

    tenant = await writer.list_aircraft_pks()
    orphans = sorted(tenant - live)
    logger.info(
        "foundry_reconcile_aircraft",
        live=len(live),
        tenant=len(tenant),
        orphans=len(orphans),
    )
    batch = await writer.delete_aircraft_batch(orphans)
    return ReconcileResult(
        live=len(live),
        tenant=len(tenant),
        orphans=len(orphans),
        deleted=batch.succeeded,
    )


# The upsert-flush unit. Enriched Flights (each carrying a full 2h trail,
# ~hundreds of points) accumulate to this many, are upserted, then
# discarded, so peak memory is bounded to one chunk regardless of tenant
# size — an accumulate-all list OOM-killed the asset container at ~2k
# candidates (#12). With the trail now fetched in ONE batched lakehouse
# scan (``stream_flight_trails``) rather than per-flight, chunk size no
# longer drives scan count; it is purely the memory bound and the unit of
# partial progress (an upsert that raises → FoundrySyncSkipped leaves
# earlier chunks persisted; the next hourly run carries forward).
_ENRICHMENT_CHUNK = 50

# Concurrency of the per-icao24 *detail* fanout (``fetch_flight``). The 2h
# trail — formerly the bottleneck, fetched per-flight as a full lakehouse
# scan whose icao24 predicate pruned nothing (~0.5 s each x ~thousands ≈
# the bulk of the ~56 min run) — is now a single batched scan, so the only
# per-flight call left is ``fetch_flight``: a recency-bounded
# ``current_positions`` point lookup (cheap, indexed). #18 lowered this
# 8→4 specifically because the heavy trail endpoint ReadTimeout'd at 8
# concurrent; that reason is gone with the batched trail, so it returns to
# 8. A per-detail timeout still never aborts the pass (counted
# ``fetch_failed`` — see ``_fetch_detail``); ``transient_retry`` still
# absorbs the occasional 429/5xx.
_ENRICHMENT_CONCURRENCY = 8


async def enriched_sync_flights(
    client: AfmApiClient,
    writer: FoundryWriter,
) -> FlightEnrichmentResult:
    """Backfill create-only takeoff Flights from /v1/flights (+ 2h trail).

    ``incremental_sync_positions`` writes a Flight at takeoff with only the
    synthesized identity (flight_id / icao24 / takeoff_ts); every routing /
    status / trail field stays null until this runs.
    ``/v1/flights/{icao24}`` is keyed on icao24 and returns that aircraft's
    *current* flight (404 outside the API's recency window), so enriching a
    stale flight_id would bleed a newer flight's data onto it. We therefore
    enrich only the **latest flight_id per icao24** (max ``takeoff_ts``
    parsed from the PK).

    **Two phases, one trail scan.** Phase 1 fans the per-icao24 *detail*
    fetch (``fetch_flight``) out concurrently (``_ENRICHMENT_CONCURRENCY``):
    a 404 means the aircraft isn't currently flying (``skipped_inactive``,
    not failed); a non-404 status error or a transport/timeout for one
    icao24 is counted (``fetch_failed``) and skipped so it can't sink the
    pass — under a concurrent fanout a single transient is near-certain, so
    letting one abort meant enrichment never completed (see
    ``_fetch_detail``). Phase 2 fetches the 2h trail for **every active
    icao24 in a single batched lakehouse scan** (``stream_flight_trails``)
    instead of one heavy per-flight scan each; trails stream back grouped by
    icao24, are paired with the held detail, built, and flushed in
    ``_ENRICHMENT_CHUNK`` upserts so peak memory stays one chunk.

    **Trail resilience.** If the batched trail call fails (transport, or a
    server-side mid-scan IO error truncating the NDJSON after a 200), the
    unseen icao24 are enriched **detail-only** (route / registration /
    status — just no trail) rather than aborting; the next hourly run
    carries the trail forward (idempotent, latest-per-icao24 ⇒ convergent).
    Foundry-side I/O (``list_flight_pks`` / ``upsert_flight_batch``) still
    bubbles to ``guarded_sync`` → ``FoundrySyncSkipped`` — the tenant being
    unreachable *is* a skip. Upsert-only: no deletes, an empty tenant
    no-ops, so no empty-feed guard is needed (unlike the reconcile).
    """
    flight_pks = await writer.list_flight_pks()

    # Collapse to the most recent flight_id per icao24 — see docstring.
    latest: dict[str, tuple[datetime, str]] = {}
    for pk in flight_pks:
        try:
            icao24, takeoff_ts = parse_flight_id(pk)
        except ValueError:
            logger.warning("foundry_enrichment_bad_flight_id", flight_id=pk)
            continue
        current = latest.get(icao24)
        if current is None or takeoff_ts > current[0]:
            latest[icao24] = (takeoff_ts, pk)

    candidates = list(latest.items())
    logger.info(
        "foundry_flight_enrichment",
        tenant_flights=len(flight_pks),
        candidates=len(candidates),
    )

    semaphore = asyncio.Semaphore(_ENRICHMENT_CONCURRENCY)

    async def _fetch_detail(icao24: str) -> tuple[str, FlightDetail | None]:
        """Fetch one flight's detail. Never raises a per-flight HTTP error.

        404 → ``inactive``; other 4xx/5xx → ``failed``; a transport/timeout
        error → also ``failed``, counted, the pass continues (a single
        transient is near-certain across a concurrent fanout over thousands
        of candidates; letting one abort meant enrichment never completed).
        A genuine total outage makes *every* detail fail →
        ``enriched=0``/``fetch_failed=candidates``, observable in the
        metadata and a safe no-op. Foundry-side I/O is outside this helper
        and still bubbles to ``guarded_sync``.
        """
        async with semaphore:
            try:
                return "ok", await client.fetch_flight(icao24)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return "inactive", None
                logger.warning(
                    "foundry_enrichment_fetch_failed",
                    icao24=icao24,
                    status=exc.response.status_code,
                )
                return "failed", None
            except httpx.HTTPError as exc:
                logger.warning(
                    "foundry_enrichment_fetch_failed",
                    icao24=icao24,
                    error=type(exc).__name__,
                )
                return "failed", None

    # Phase 1 — concurrent detail fanout (semaphore-bounded; candidates are
    # already resolved to latest-per-icao24, so fetch order is irrelevant).
    detail_outcomes = await asyncio.gather(*(_fetch_detail(icao24) for icao24, _ in candidates))
    skipped_inactive = sum(1 for kind, _ in detail_outcomes if kind == "inactive")
    fetch_failed = sum(1 for kind, _ in detail_outcomes if kind == "failed")
    active: dict[str, tuple[str, datetime, FlightDetail]] = {}
    for (icao24, (takeoff_ts, flight_id)), (kind, detail) in zip(
        candidates, detail_outcomes, strict=True
    ):
        if kind == "ok" and detail is not None:
            active[icao24] = (flight_id, takeoff_ts, detail)

    # Phase 2 — one batched trail scan; build + flush per chunk.
    enriched = 0
    buffer: list[Flight] = []
    seen: set[str] = set()

    async def _flush() -> None:
        nonlocal enriched
        if not buffer:
            return
        batch = await writer.upsert_flight_batch(buffer)
        enriched += batch.succeeded
        buffer.clear()

    if active:
        try:
            async for trail in client.stream_flight_trails(list(active), "2h"):
                entry = active.get(trail.icao24)
                if entry is None:
                    continue  # defensive: only active icao24 were requested
                flight_id, takeoff_ts, detail = entry
                buffer.append(flight_detail_to_flight(flight_id, takeoff_ts, detail, trail))
                seen.add(trail.icao24)
                if len(buffer) >= _ENRICHMENT_CHUNK:
                    await _flush()
        except httpx.HTTPError as exc:
            # Batched trail call failed — enrich the rest detail-only; the
            # next hourly run carries the trail forward (convergent).
            logger.warning(
                "foundry_enrichment_trail_batch_failed",
                error=type(exc).__name__,
            )

    # Active icao24 with no trail line (no rows in the window, OR the trail
    # stream ended early) → detail-only enrichment, same chunked flush.
    for icao24, (flight_id, takeoff_ts, detail) in active.items():
        if icao24 in seen:
            continue
        buffer.append(flight_detail_to_flight(flight_id, takeoff_ts, detail, None))
        if len(buffer) >= _ENRICHMENT_CHUNK:
            await _flush()
    await _flush()

    return FlightEnrichmentResult(
        tenant_flights=len(flight_pks),
        candidates=len(candidates),
        enriched=enriched,
        skipped_inactive=skipped_inactive,
        fetch_failed=fetch_failed,
    )


async def run_positions_sync(
    *,
    since: datetime | None = None,
    detector: TakeoffDetector | None = None,
) -> SyncResult:
    """Entrypoint the Dagster positions asset calls. Skip-guarded.

    Builds settings + clients inside ``guarded_sync`` so an absent
    ``_private/foundry/.env`` (``ValidationError``) or an unreachable
    endpoint surfaces as ``FoundrySyncSkipped``, not a crash.
    """
    async with guarded_sync("positions"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await incremental_sync_positions(client, writer, since=since, detector=detector)


async def run_sites_sync() -> SyncResult:
    """Entrypoint the Dagster sites asset calls. Skip-guarded."""
    async with guarded_sync("sites"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await full_sync_sites(client, writer)


async def run_aircraft_reconcile() -> ReconcileResult:
    """Entrypoint the Dagster reconcile asset calls (Fix C). Skip-guarded.

    Same standalone discipline as the sync entrypoints: an absent
    ``_private/foundry/.env`` or an unreachable endpoint surfaces as
    ``FoundrySyncSkipped``, not a crash.
    """
    async with guarded_sync("reconcile"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await reconcile_aircraft(client, writer)


async def run_flight_enrichment() -> FlightEnrichmentResult:
    """Entrypoint the Dagster flight-enrichment asset calls. Skip-guarded.

    Same standalone discipline as the other entrypoints: an absent
    ``_private/foundry/.env`` or an unreachable endpoint (including a
    transport failure to the local /v1 API) surfaces as
    ``FoundrySyncSkipped``, not a crash.
    """
    async with guarded_sync("flight_enrichment"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await enriched_sync_flights(client, writer)


__all__ = [
    "FlightEnrichmentResult",
    "FoundrySyncSkipped",
    "ReconcileResult",
    "SyncResult",
    "Takeoff",
    "TakeoffDetector",
    "enriched_sync_flights",
    "full_sync_sites",
    "guarded_sync",
    "incremental_sync_positions",
    "parse_flight_id",
    "reconcile_aircraft",
    "run_aircraft_reconcile",
    "run_flight_enrichment",
    "run_positions_sync",
    "run_sites_sync",
    "synthesize_flight_id",
]
