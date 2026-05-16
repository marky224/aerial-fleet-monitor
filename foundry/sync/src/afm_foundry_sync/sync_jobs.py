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
    (``transforms.takeoff_to_flight``). FlightDetail/trail **enrichment is
    deferred** — it needs a per-icao24 ``/v1/flights`` fanout off a slower
    cadence, out of scope for the 30s positions tick.

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

import contextlib
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime

import httpx
import structlog
from pydantic import ValidationError

from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import Position
from afm_foundry_sync.ontology_writers import BatchResult, FoundryWriter
from afm_foundry_sync.settings import FoundrySettings
from afm_foundry_sync.transforms import (
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
            detector.state_for(p.icao24 for p in deduped)
            if detector is not None
            else None
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
            return await incremental_sync_positions(
                client, writer, since=since, detector=detector
            )


async def run_sites_sync() -> SyncResult:
    """Entrypoint the Dagster sites asset calls. Skip-guarded."""
    async with guarded_sync("sites"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await full_sync_sites(client, writer)


__all__ = [
    "FoundrySyncSkipped",
    "SyncResult",
    "Takeoff",
    "TakeoffDetector",
    "full_sync_sites",
    "guarded_sync",
    "incremental_sync_positions",
    "run_positions_sync",
    "run_sites_sync",
    "synthesize_flight_id",
]
