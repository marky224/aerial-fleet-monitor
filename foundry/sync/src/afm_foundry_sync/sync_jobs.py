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
  - ``FlightLifecycleDetector`` — stateful on-ground edge detection over the
    full takeoff→landing lifecycle that synthesizes ``Flight`` primary keys.
    A detected *takeoff* (on-ground→airborne) triggers a *create-only*
    ``upsert_flight_batch`` of the takeoff-shaped Flight
    (``transforms.takeoff_to_flight``); a detected *landing* (airborne→
    on-ground) triggers a *partial* ``stamp_flight_landed_batch`` that sets
    ``landed_at`` / ``status='landed'`` on the tracked open leg without
    clobbering its enrichment (the modify path preserves omitted params —
    verified live 2026-05-29).
  - ``enriched_sync_flights`` — the deferred FlightDetail/trail backfill,
    a per-icao24 ``/v1/flights`` (+ 2h trail) fanout off a slower cadence
    (hourly), out of scope for the 30s positions tick. ``/v1/flights`` is
    icao24-keyed and returns the aircraft's *current* flight, so only the
    **latest flight_id per icao24** is a safe enrichment target.

Cursor & detector state are *returned* (``SyncResult.cursor`` /
``SyncResult.detector_state``), never persisted here: this module stays
I/O-pure for unit testing. The Dagster asset owns persistence — it seeds
a fresh ``FlightLifecycleDetector(prior_on_ground, prior_open_flight)`` from
its prior materialization metadata each tick and writes the post-run state
back, so detector state survives process restarts (an in-process-only
detector would reset and miss every cross-restart edge). ``state_for`` /
``open_flight_state_for`` bound the persisted maps to aircraft seen *this
run* (a >1-tick absence is treated as a fresh sighting — acceptable per the
detector's "first sighting only seeds" rule, and it keeps the metadata blob
proportional to live traffic, not to every icao24 ever observed).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import structlog
from pydantic import ValidationError

from afm_foundry_sync.api_readers import AfmApiClient
from afm_foundry_sync.models import Flight, FlightDetail, Position
from afm_foundry_sync.ontology_writers import (
    BatchResult,
    FlightLandedStamp,
    FlightLiveStamp,
    FoundryWriter,
)
from afm_foundry_sync.settings import FoundrySettings
from afm_foundry_sync.transforms import (
    case_for_sync_to_case,
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
    ``takeoffs_detected`` / ``landings_detected`` count the lifecycle edges
    this run; ``flights_written`` is the create-only Flight upsert count from
    takeoffs and ``flights_landed`` the landing-stamp count (each ≤ its edge
    count; equal on full success). ``detector_state`` is the post-run
    on-ground map and ``open_flight_state`` the post-run icao24→open-flight
    map; the caller persists both and seeds next tick's detector with them
    (positions w/ detector only; ``None`` otherwise).
    """

    attempted: int
    succeeded: int
    failed: int = 0
    cursor: datetime | None = None
    takeoffs_detected: int = 0
    flights_written: int = 0
    landings_detected: int = 0
    flights_landed: int = 0
    detector_state: dict[str, bool] | None = None
    open_flight_state: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Outcome of one tenant-Aircraft reconcile (Fix C).

    The positions sync is upsert-only, so aircraft that have left the live
    feed persist in the Ontology forever. This job diffs the tenant's
    Aircraft set against the live feed and deletes the difference.

    ``live`` is the in-scope (East/West) live-feed size; ``tenant`` is every
    Aircraft PK in the Ontology; ``orphans`` is ``tenant - live``; ``deleted``
    is how many of those the delete batch confirmed; ``remaining`` is the
    orphans still queued after the per-run cap (logged, never silently dropped —
    on full delete success ``orphans == deleted + remaining``).
    ``skipped_empty_live`` is
    True when the live feed was empty and the reconcile bailed *without*
    enumerating the tenant or deleting anything — an empty feed means
    "fleet unknown right now" (e.g. an upstream OpenSky 429), never "fleet
    is empty", so evicting on no-knowledge would wipe the whole tenant.
    """

    live: int
    tenant: int
    orphans: int
    deleted: int
    remaining: int = 0
    skipped_empty_live: bool = False


@dataclass(frozen=True, slots=True)
class FlightReconcileResult:
    """Outcome of one tenant-Flight reconcile (Phase A — the Flight-side mirror
    of ``ReconcileResult``/Fix C).

    The takeoff + enrichment path is upsert-only, so Flight objects accumulate
    in the Ontology forever (no delete path existed). This job keeps only the
    *live working set* — the latest flight_id of each currently-airborne
    aircraft, plus any flight whose takeoff is still inside the TTL backstop —
    and evicts the rest via ``delete-flight``.

    ``live_airborne`` is the count of currently-airborne icao24 in the feed;
    ``tenant`` is every Flight PK in the Ontology; ``keep`` is the union
    keep-set size; ``orphans`` is ``tenant - keep`` (everything outside the
    live working set).

    **Phase-B split (deletes STUBS ONLY):** the orphan set is two kinds —
    *completed* flights (``landed_at`` set) and *stubs* (no ``landed_at``:
    never-completed, lost-at-cruise, or legacy backlog). This job deletes
    stubs only; completed flights are evicted exclusively by the archive
    asset (archive-to-cold-store then delete), so no completed flight is ever
    deleted unarchived. ``completed_skipped`` is the count of completed
    orphans deliberately left in the tenant for the archive asset.
    ``deleted`` is how many stub orphans this run actually removed;
    ``remaining`` is the stub orphans still queued after the per-run cap
    (logged, never silently dropped). On full delete success the invariant is
    ``orphans == completed_skipped + deleted + remaining``.

    ``skipped_empty_live`` is True when the live feed was empty and the
    reconcile bailed *before* enumerating the tenant or deleting anything — an
    empty feed means "fleet unknown right now" (e.g. an upstream OpenSky 429),
    never "fleet is empty", so the airborne keep-set would be empty and we'd
    evict the entire within-TTL-excepted tenant on no knowledge. Mirrors the
    aircraft reconcile's empty-live guard exactly.
    """

    live_airborne: int
    tenant: int
    keep: int
    orphans: int
    deleted: int
    completed_skipped: int = 0
    remaining: int = 0
    skipped_empty_live: bool = False
    # Liveness sweep (Tier 2-lite). Delta writes to Flight.isLive this run:
    # `marked_true` = newly-live legs flipped on, `marked_false` = stale-live
    # legs flipped off. Both 0 when FOUNDRY_FLIGHT_ISLIVE_ENABLED is unset
    # (the sweep is skipped entirely) or there was no delta this run.
    live_marked_true: int = 0
    live_marked_false: int = 0


@dataclass(frozen=True, slots=True)
class CaseSyncResult:
    """Outcome of one cases sync run (Phase 05 task #5).

    ``cursor`` is the value the caller should persist and pass back as
    ``since`` next run (= max ``updated_at`` observed across this run's
    pages, or the unchanged prior ``since`` when the API returned zero
    rows; ``None`` only on the very first run with an empty backlog).
    The cases sync is upsert-only — no reconcile/eviction. Resolved
    cases just stop advancing ``updated_at`` and fall out of the moving
    window naturally (and App 1's panel applies its own status filter).
    """

    attempted: int
    succeeded: int
    failed: int = 0
    cursor: datetime | None = None


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


def load_foundry_settings() -> FoundrySettings:
    """Construct ``FoundrySettings`` from the environment / ``.env``.

    A typed factory so callers OUTSIDE this package (the pipelines
    ``foundry_flight_archive`` asset, which owns the cross-store ordering and
    so opens the writer itself rather than via a ``run_*`` entrypoint) get a
    ``FoundrySettings`` without constructing the pydantic-settings class
    directly — only this package's mypy config carries the pydantic plugin
    that knows its fields come from env. Call it INSIDE ``guarded_sync`` so an
    absent ``.env`` (``ValidationError``) surfaces as ``FoundrySyncSkipped``.
    """
    return FoundrySettings()


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


# Live-set scope. ``None`` = keep ALL tracked aircraft (the full live 15-min
# feed) — the DEFAULT. A set restricts the live set to those customer regions,
# e.g. ``frozenset({"west", "east", "all"})`` for East/West only. This ONE knob
# bounds the Aircraft upsert + the takeoff/landing detector in
# ``incremental_sync_positions`` AND the ``reconcile_aircraft`` keep-set, so the
# tenant — and the Flights minted off detector edges — follow it together.
_LIVE_SCOPE_REGIONS: frozenset[str] | None = None


def _in_live_scope(p: Position) -> bool:
    """Whether a position is inside the live set (see ``_LIVE_SCOPE_REGIONS``)."""
    return _LIVE_SCOPE_REGIONS is None or p.customer_region in _LIVE_SCOPE_REGIONS


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


@dataclass(frozen=True, slots=True)
class Landing:
    """A detected airborne→on-ground transition for a tracked open flight.

    ``flight_id`` is the PK minted at this aircraft's most recent takeoff
    (carried in the detector's open-flight map), so the landing stamps the
    *correct* leg rather than re-synthesizing from the landing timestamp.
    ``landed_at`` is the on-ground row's ``last_seen_at``.
    """

    icao24: str
    flight_id: str
    landed_at: datetime


@dataclass(frozen=True, slots=True)
class DetectedEdges:
    """The takeoff + landing edges from one ``observe`` pass over a batch."""

    takeoffs: list[Takeoff]
    landings: list[Landing]


class FlightLifecycleDetector:
    """Per-icao24 on-ground edge detector for the full takeoff→landing lifecycle.

    A previously-observed ``on_ground=True`` → ``on_ground=False`` is a
    *takeoff* (``takeoff_ts`` = that row's ``last_seen_at``, minting a Flight
    PK); the reverse ``on_ground=False`` → ``on_ground=True`` is a *landing*.
    First sighting of an aircraft only seeds state (no edge — we cannot infer
    a transition without a prior sample).

    Two state maps, both caller-owned (see module docstring) and persisted to
    the asset's materialization metadata each tick so edges survive process
    restarts:

      - ``_on_ground`` — the last observed on-ground bool per icao24 (drives
        edge detection).
      - ``_open_flight`` — icao24 → the flight_id minted at its most recent
        takeoff. Set on a takeoff edge; consumed + cleared on the matching
        landing edge so the landing stamps that exact leg. A landing for an
        icao24 with no open flight (we never saw its takeoff — e.g. it was
        already airborne when tracking began) emits no Landing: we don't own
        a flight_id to stamp, and the hourly enrichment + reconcile handle it.
    """

    def __init__(
        self,
        prior_on_ground: dict[str, bool] | None = None,
        prior_open_flight: dict[str, str] | None = None,
    ) -> None:
        # Copy: callers pass deserialized metadata we must not alias/mutate.
        self._on_ground: dict[str, bool] = dict(prior_on_ground or {})
        self._open_flight: dict[str, str] = dict(prior_open_flight or {})

    def state_for(self, icao24s: Iterable[str]) -> dict[str, bool]:
        """On-ground state restricted to the given icao24s (the run's batch).

        Bounds the persisted blob to live traffic: aircraft absent this run
        are dropped, so a >1-tick gap re-seeds as a first sighting (no
        edge) rather than growing the map unboundedly.
        """
        seen = set(icao24s)
        return {k: self._on_ground[k] for k in seen if k in self._on_ground}

    def open_flight_state_for(self, icao24s: Iterable[str]) -> dict[str, str]:
        """Open-flight map restricted to the given icao24s (bounds the blob).

        Same discipline as ``state_for``: an aircraft absent this run drops
        its open-flight entry (a flight whose aircraft vanishes mid-air loses
        landing tracking — acceptable; the TTL reconcile and enrichment 404
        handle it), keeping the persisted map proportional to live traffic.
        """
        seen = set(icao24s)
        return {k: self._open_flight[k] for k in seen if k in self._open_flight}

    def observe(self, positions: Iterable[Position]) -> DetectedEdges:
        takeoffs: list[Takeoff] = []
        landings: list[Landing] = []
        for p in positions:
            prev = self._on_ground.get(p.icao24)
            if prev is True and p.on_ground is False:
                flight_id = synthesize_flight_id(p.icao24, p.last_seen_at)
                takeoffs.append(
                    Takeoff(icao24=p.icao24, takeoff_ts=p.last_seen_at, flight_id=flight_id)
                )
                # Track the open flight so the matching landing stamps this leg.
                self._open_flight[p.icao24] = flight_id
            elif prev is False and p.on_ground is True:
                open_flight_id = self._open_flight.pop(p.icao24, None)
                if open_flight_id is not None:
                    landings.append(
                        Landing(
                            icao24=p.icao24,
                            flight_id=open_flight_id,
                            landed_at=p.last_seen_at,
                        )
                    )
            self._on_ground[p.icao24] = p.on_ground
        return DetectedEdges(takeoffs=takeoffs, landings=landings)


async def incremental_sync_positions(
    client: AfmApiClient,
    writer: FoundryWriter,
    *,
    since: datetime | None = None,
    detector: FlightLifecycleDetector | None = None,
) -> SyncResult:
    """Sync /v1/positions/live → Aircraft Ontology objects.

    ``since`` is the previous run's cursor; the v1 API returns the full
    live set (no server-side delta), so it is advisory/logged for now and
    the cursor returned is the response ``server_time``. If a ``detector``
    is supplied, its lifecycle edges drive two Flight writes:

      - **takeoffs** → a *create-only* ``upsert_flight_batch`` (takeoff-shaped
        Flight per ``takeoff_to_flight``; FlightDetail enrichment is deferred,
        see module docstring).
      - **landings** → a *partial* ``stamp_flight_landed_batch`` that sets
        ``landed_at`` / ``status='landed'`` on the open leg WITHOUT clobbering
        the route / timeline / trail enrichment already on the object
        (the modify path preserves omitted params — verified live 2026-05-29).

    The post-run on-ground + open-flight maps are returned for the caller to
    persist and re-seed next tick.
    """
    response = await client.fetch_positions_live()
    deduped = _dedupe_latest(response.items)
    # Bound to the live set (see _in_live_scope / _LIVE_SCOPE_REGIONS; default =
    # all aircraft). Filtering here bounds BOTH the Aircraft upsert and the
    # detector's takeoff/landing edges, so the tenant — and the Flights minted
    # off these edges — follow the same scope.
    scoped = [p for p in deduped if _in_live_scope(p)]
    logger.info(
        "foundry_positions_sync",
        since=since.isoformat() if since else None,
        received=len(response.items),
        deduped=len(deduped),
        scoped=len(scoped),
        server_time=response.server_time.isoformat(),
    )

    edges = detector.observe(scoped) if detector is not None else DetectedEdges([], [])
    if edges.takeoffs:
        logger.info("foundry_takeoffs_detected", count=len(edges.takeoffs))
    if edges.landings:
        logger.info("foundry_landings_detected", count=len(edges.landings))

    batch: BatchResult = await writer.upsert_aircraft_batch(
        [position_to_aircraft(p) for p in scoped]
    )
    # Create-only Flight write off detected takeoffs. Empty list → no-op
    # (upsert_flight_batch short-circuits), so this is safe with no detector.
    flight_batch: BatchResult = await writer.upsert_flight_batch(
        [takeoff_to_flight(t.flight_id, t.icao24, t.takeoff_ts) for t in edges.takeoffs]
    )
    # Partial landing stamp off detected landings (preserves enrichment).
    # Empty list → no-op (stamp_flight_landed_batch short-circuits).
    landed_batch: BatchResult = await writer.stamp_flight_landed_batch(
        [
            FlightLandedStamp(
                flight_id=lz.flight_id,
                icao24=lz.icao24,
                takeoff_ts=parse_flight_id(lz.flight_id)[1],
                landed_at=lz.landed_at,
            )
            for lz in edges.landings
        ]
    )
    return SyncResult(
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
        cursor=response.server_time,
        takeoffs_detected=len(edges.takeoffs),
        flights_written=flight_batch.succeeded,
        landings_detected=len(edges.landings),
        flights_landed=landed_batch.succeeded,
        detector_state=(
            detector.state_for(p.icao24 for p in scoped) if detector is not None else None
        ),
        open_flight_state=(
            detector.open_flight_state_for(p.icao24 for p in scoped)
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


# Per-run delete cap, mirroring _FLIGHT_RECONCILE_DELETE_CAP. The first scoped
# reconcile faces a one-time transition backlog (the whole pre-scope tenant,
# ~9k, minus the ~700 in-scope live set); at _MAX_BATCH=100 that is ~90
# sequential applyBatch calls in one run against a slow Foundry API on a
# co-resident host. Cap deletes per run and let the backlog drain over a few
# 2-min ticks (the overlap guard prevents run stacking). Steady-state runs
# delete far below this, so the cap is invisible after the initial drain.
_AIRCRAFT_RECONCILE_DELETE_CAP = 5000


async def reconcile_aircraft(
    client: AfmApiClient,
    writer: FoundryWriter,
) -> ReconcileResult:
    """Evict tenant Aircraft objects outside the live East/West set (Fix C).

    The positions sync is upsert-only and never deletes, so aircraft that
    have departed (icao24 absent from ``/v1/positions/live`` after the
    API's recency window + the Postgres prune) accumulate in the Ontology
    indefinitely. This diffs ``tenant - keep`` and deletes the orphans via
    the ``delete-aircraft`` Action, where ``keep`` is the in-scope subset of the
    live feed (``_in_live_scope`` / ``_LIVE_SCOPE_REGIONS``; default = all
    aircraft). Run frequently (every ~2 min) so the tenant stays continuously
    equal to the live set; deletes are capped per run
    (``_AIRCRAFT_RECONCILE_DELETE_CAP``) so a one-time large drain (e.g. a scope
    change) spreads over a few ticks, with ``remaining`` logged (never silently
    dropped).

    **Empty-live safety guard:** if the *full* live feed is empty, bail *before*
    enumerating the tenant — ``tenant - {}`` is the entire tenant, and an empty
    feed means the fleet is momentarily unknown (e.g. an upstream OpenSky 429),
    not that every aircraft has gone. The guard checks the **full** feed, not the
    in-scope subset, so a healthy feed that happens to carry no in-scope traffic
    right now still reconciles (correctly evicting stale in-scope objects).
    Returns a no-op result flagged ``skipped_empty_live``.
    """
    response = await client.fetch_positions_live()
    if not response.items:
        logger.warning("foundry_reconcile_skipped_empty_live")
        return ReconcileResult(live=0, tenant=0, orphans=0, deleted=0, skipped_empty_live=True)

    # Keep-set = in-scope aircraft currently in the live feed (East/West). The
    # scoped positions sync writes only in-scope aircraft, so in steady state the
    # tenant is already in-scope and orphans are just the aircraft that left the
    # feed since the last tick; the same diff drains any pre-scope out-of-region
    # objects during the one-time transition. Sorted before capping so the drain
    # is deterministic across runs.
    live = {p.icao24 for p in response.items if _in_live_scope(p)}
    tenant = await writer.list_aircraft_pks()
    orphans = sorted(tenant - live)
    to_delete = orphans[:_AIRCRAFT_RECONCILE_DELETE_CAP]
    remaining = len(orphans) - len(to_delete)
    logger.info(
        "foundry_reconcile_aircraft",
        live=len(live),
        tenant=len(tenant),
        orphans=len(orphans),
        deleting=len(to_delete),
        remaining=remaining,
    )
    batch = await writer.delete_aircraft_batch(to_delete)
    return ReconcileResult(
        live=len(live),
        tenant=len(tenant),
        orphans=len(orphans),
        deleted=batch.succeeded,
        remaining=remaining,
    )


# Flight-reconcile TTL backstop. A Flight whose takeoff is within this window
# is kept regardless of whether its aircraft is currently airborne — it
# catches flights that never produced a landing edge (signal lost at cruise)
# and the legacy backlog. The real eviction work is done by the airborne
# keep-set; this only bounds how much recent non-airborne history stays hot.
# 24 h while there is no archive safety-net (Phase A); tighten toward 12 h /
# keep-set-only in Phase B once landed flights are archived to cold storage.
_FLIGHT_RECONCILE_TTL = timedelta(hours=24)

# Per-run delete cap. The first reconcile faces a one-time ~85k backlog; at
# _MAX_BATCH=100 that is ~850 sequential applyBatch calls in one run against a
# slow Foundry API on a co-resident host. Cap the deletes per run and let the
# backlog drain over several hourly ticks (the overlap guard prevents run
# stacking). Steady-state runs delete far below this, so the cap is invisible
# after the initial drain. Tunable: raise/lower by watching the first drain.
_FLIGHT_RECONCILE_DELETE_CAP = 5000


async def reconcile_flights(
    client: AfmApiClient,
    writer: FoundryWriter,
    *,
    now: datetime | None = None,
) -> FlightReconcileResult:
    """Evict tenant Flight objects outside the live working set (Phase A).

    The takeoff (``incremental_sync_positions``) + enrichment
    (``enriched_sync_flights``) path is upsert-only and never deletes, so
    every flight ever synthesized persists in the Ontology indefinitely
    (~85k and climbing ~20k/day at design time). This keeps only the **union
    keep-set** and deletes the complement via the ``delete-flight`` Action:

      keep = (latest flight_id per CURRENTLY-AIRBORNE icao24)
             UNION (any flight whose takeoff_ts is within ``_FLIGHT_RECONCILE_TTL``)

    The airborne half MUST be *latest-per-icao24*, not "any flight whose
    icao24 is airborne" — each airborne aircraft carries many historical
    flights (measured ~7.5x), so the naive form would keep tens of thousands
    and barely dent the backlog. It is the same latest-per-icao24 collapse
    ``enriched_sync_flights`` uses, parsed from the PK
    (``{icao24}-{unix_takeoff_ts}``). A genuinely-airborne long-haul is its
    aircraft's latest flight (no landing ⇒ no newer takeoff edge), so it is
    always in the airborne half and protected regardless of the TTL value —
    the TTL is purely a tenant-size knob, never a clip-a-live-flight risk.

    **Deletes STUBS ONLY (Phase B):** of everything outside the keep-set,
    *completed* flights (``landed_at`` set) are left in the tenant for the
    archive asset, which is the sole path that removes them (archive-to-cold-
    store, verify, then delete) — so a completed flight is never deleted
    unarchived. This job deletes only *stubs*: never-completed / lost-at-
    cruise / legacy-backlog legs that carry no ``landed_at`` and have no
    reference value to archive (their raw positions remain in the lake). The
    completion flag rides on the same tenant scan for free
    (``list_flight_pks_with_completion``), so the exclusion adds no round-trip.

    **Empty-live safety guard** (mirrors ``reconcile_aircraft``): if the live
    feed is empty, bail *before* enumerating the tenant. An empty feed means
    the fleet is momentarily unknown (e.g. an OpenSky 429), so the airborne
    keep-set would be empty and we'd evict everything outside the TTL on no
    knowledge. Returns a no-op flagged ``skipped_empty_live``.

    **Per-run delete cap**: at most ``_FLIGHT_RECONCILE_DELETE_CAP`` orphans
    are deleted per run; any excess is reported as ``remaining`` (logged, not
    silently dropped) and drained by subsequent hourly runs. Orphans are
    sorted before capping so the drain is deterministic across runs.

    **Liveness sweep (Tier 2-lite)**: when ``writer.islive_enabled`` is set,
    this run also reconciles ``Flight.isLive`` to the live set — the latest
    NON-COMPLETED flight per airborne icao24 (``latest_airborne`` minus
    ``completed``) — writing only the delta vs the tenant's current
    ``isLive=true`` set (``live_marked_true`` / ``live_marked_false``). This is the authoritative
    backbone for the map's "show only in-progress flights" filter: tick-level
    edges set ``isLive`` at takeoff/landing, but landings are detected only
    ~1.1% of the time, so this hourly sweep is what flips the stale-airborne
    legs (undetected landing / supersession / dropout / restart) off. Skipped
    entirely (no extra writes) when the flag is unset.
    """
    now = now or datetime.now(UTC)
    response = await client.fetch_positions_live()
    airborne = {p.icao24 for p in response.items if p.on_ground is False}
    if not airborne:
        logger.warning("foundry_flight_reconcile_skipped_empty_live")
        return FlightReconcileResult(
            live_airborne=0, tenant=0, keep=0, orphans=0, deleted=0, skipped_empty_live=True
        )

    tenant, completed, current_live = await writer.list_flight_pks_with_completion()

    # Latest flight_id per currently-airborne icao24 (the live-working-set half
    # of the keep-set). Malformed PKs are skipped with a warning, never crash.
    latest_airborne: dict[str, tuple[datetime, str]] = {}
    cutoff = now - _FLIGHT_RECONCILE_TTL
    keep: set[str] = set()
    for pk in tenant:
        try:
            icao24, takeoff_ts = parse_flight_id(pk)
        except ValueError:
            # Unparseable PK "can't happen" (synthesize_flight_id always
            # yields a parseable id), but if one ever appears we KEEP it —
            # deleting on "can't classify its age" is destructive on
            # uncertainty (same conservative stance as the empty-live guard,
            # and as enriched_sync_flights, which skips/leaves such PKs).
            logger.warning("foundry_flight_reconcile_bad_flight_id", flight_id=pk)
            keep.add(pk)
            continue
        if takeoff_ts >= cutoff:
            keep.add(pk)  # TTL-backstop half
        if icao24 in airborne:
            current = latest_airborne.get(icao24)
            if current is None or takeoff_ts > current[0]:
                latest_airborne[icao24] = (takeoff_ts, pk)
    keep.update(pk for _, pk in latest_airborne.values())

    # Phase-B split: of everything outside the live working set, COMPLETED
    # flights (landed_at set) are left for the archive asset (archive-then-
    # delete) and are NEVER deleted here, so no completed flight is removed
    # unarchived. This job deletes STUBS ONLY — never-completed / lost-at-
    # cruise / legacy-backlog legs with no landed_at and no reference value.
    evictable = tenant - keep
    completed_skipped = len(evictable & completed)
    stub_orphans = sorted(evictable - completed)
    to_delete = stub_orphans[:_FLIGHT_RECONCILE_DELETE_CAP]
    remaining = len(stub_orphans) - len(to_delete)
    logger.info(
        "foundry_flight_reconcile",
        live_airborne=len(airborne),
        tenant=len(tenant),
        keep=len(keep),
        orphans=len(evictable),
        completed_skipped=completed_skipped,
        deleting=len(to_delete),
        remaining=remaining,
    )
    batch = await writer.delete_flight_batch(to_delete)

    # ---- Liveness sweep (Tier 2-lite): reconcile Flight.isLive to the live set.
    # The live set = latest NON-COMPLETED flight per currently-airborne icao24
    # (``latest_airborne`` minus ``completed``, both already computed for the
    # keep-set). Landings are detected only ~1.1% of the time, so this hourly
    # delta-write — not the tick-level edges — is what keeps isLive honest: it
    # flips stale ``true``→``false`` (undetected landing / supersession / feed
    # dropout / process-restart orphan) and ``null/false``→``true`` for newly-live
    # legs the takeoff-create may have missed. Gated on the provisioning flag;
    # writes only the delta vs the tenant's current isLive=true set, and skips
    # legs deleted above (their isLive is moot).
    #
    # COMPLETED-leg exclusion: a leg with ``landed_at`` set is never "live", even
    # when it is the *latest* leg of a now-airborne aircraft — that happens when
    # the aircraft landed (leg stamped landed + isLive=false), then took off again
    # on an edge AFM didn't observe, so no newer leg was minted. Without this
    # exclusion the sweep would re-mark that completed leg true every run (fighting
    # the landing stamp) and the map would show its stale trail. Excluding
    # ``completed`` here both stops that and flips any such currently-live leg
    # false (it falls into the false set below, and completed legs are never in
    # ``to_delete`` — they are left for the archive asset).
    live_marked_true = 0
    live_marked_false = 0
    if writer.islive_enabled:
        target_live = {
            pk: (icao24, ts)
            for icao24, (ts, pk) in latest_airborne.items()
            if pk not in completed
        }
        true_stamps = [
            FlightLiveStamp(flight_id=pk, icao24=icao24, takeoff_ts=ts, is_live=True)
            for pk, (icao24, ts) in target_live.items()
            if pk not in current_live
        ]
        target_pks = set(target_live)
        false_stamps: list[FlightLiveStamp] = []
        for pk in sorted((current_live - target_pks) - set(to_delete)):
            try:
                icao24, ts = parse_flight_id(pk)
            except ValueError:
                logger.warning("foundry_flight_liveness_bad_flight_id", flight_id=pk)
                continue
            false_stamps.append(
                FlightLiveStamp(flight_id=pk, icao24=icao24, takeoff_ts=ts, is_live=False)
            )
        live_marked_true = (await writer.set_flight_live_batch(true_stamps)).succeeded
        live_marked_false = (await writer.set_flight_live_batch(false_stamps)).succeeded
        logger.info(
            "foundry_flight_liveness_sweep",
            target_live=len(target_pks),
            current_live=len(current_live),
            marked_true=live_marked_true,
            marked_false=live_marked_false,
        )

    return FlightReconcileResult(
        live_airborne=len(airborne),
        tenant=len(tenant),
        keep=len(keep),
        orphans=len(evictable),
        completed_skipped=completed_skipped,
        deleted=batch.succeeded,
        remaining=remaining,
        live_marked_true=live_marked_true,
        live_marked_false=live_marked_false,
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
    detector: FlightLifecycleDetector | None = None,
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


async def run_flight_reconcile() -> FlightReconcileResult:
    """Entrypoint the Dagster Flight-reconcile asset calls (Phase A). Skip-guarded.

    Same standalone discipline as the other entrypoints: an absent
    ``_private/foundry/.env`` or an unreachable endpoint (local API or
    Foundry) surfaces as ``FoundrySyncSkipped``, not a crash.
    """
    async with guarded_sync("flight_reconcile"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await reconcile_flights(client, writer)


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


async def incremental_sync_cases(
    client: AfmApiClient,
    writer: FoundryWriter,
    *,
    since: datetime | None = None,
) -> CaseSyncResult:
    """Sync ``GET /v1/cases/all-for-sync`` → Case Ontology objects.

    Drains every page from the API (paginated incrementally on
    ``updated_at``), maps each row to a Foundry ``Case`` via
    :func:`case_for_sync_to_case`, and upserts the batch via
    ``upsert-case``. Returns the new cursor (max ``updated_at``
    observed, or the unchanged ``since`` when zero rows) for the
    Dagster asset to persist as the watermark for the next tick.

    Upsert-only — no Foundry-side delete. Resolved cases stop
    advancing ``updated_at`` and fall out of the API's moving window
    naturally; the App 1 Cases panel applies its own ``status``
    filter at display time, so leaving resolved Cases in the tenant
    is harmless.

    Cursor is persisted *post-write* by the caller (the Dagster
    asset's ``MaterializeResult`` metadata) so a transient Foundry
    failure raised before the return leaves the watermark untouched
    and the next tick re-reads the same window — same skip-as-success
    convention as the other foundry-sync assets.
    """
    items, cursor = await client.fetch_cases_for_sync(since=since)
    logger.info(
        "foundry_cases_sync",
        since=since.isoformat() if since else None,
        received=len(items),
        cursor=cursor.isoformat() if cursor else None,
    )
    cases = [case_for_sync_to_case(item) for item in items]
    batch: BatchResult = await writer.upsert_case_batch(cases)
    return CaseSyncResult(
        attempted=batch.attempted,
        succeeded=batch.succeeded,
        failed=batch.failed,
        cursor=cursor,
    )


async def run_cases_sync(*, since: datetime | None = None) -> CaseSyncResult:
    """Entrypoint the Dagster cases asset calls. Skip-guarded.

    Same standalone discipline as the other entrypoints: an absent
    ``_private/foundry/.env`` or an unreachable endpoint (Foundry-side
    or local AFM API) surfaces as ``FoundrySyncSkipped``, not a crash.
    """
    async with guarded_sync("cases"):
        settings = FoundrySettings()
        async with AfmApiClient(settings) as client, FoundryWriter(settings) as writer:
            return await incremental_sync_cases(client, writer, since=since)


__all__ = [
    "CaseSyncResult",
    "FlightEnrichmentResult",
    "FlightReconcileResult",
    "FoundrySyncSkipped",
    "ReconcileResult",
    "SyncResult",
    "Takeoff",
    "enriched_sync_flights",
    "full_sync_sites",
    "guarded_sync",
    "incremental_sync_cases",
    "incremental_sync_positions",
    "load_foundry_settings",
    "parse_flight_id",
    "reconcile_aircraft",
    "reconcile_flights",
    "run_aircraft_reconcile",
    "run_cases_sync",
    "run_flight_enrichment",
    "run_flight_reconcile",
    "run_positions_sync",
    "run_sites_sync",
    "synthesize_flight_id",
]
