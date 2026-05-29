"""Flight archival assets — the durable cold store for completed flights (Phase B).

``foundry_flight_archive`` (hourly at :50) is the SOLE path that removes
*completed* flights from the Foundry Ontology. The :45 reconcile deletes stubs
only and deliberately leaves completed flights (``landed_at`` set) in the
tenant; this asset captures each one to the Parquet ``flights_archive/`` cold
store and only THEN deletes it from Foundry — so no completed flight is ever
deleted unarchived.

The cross-store move is not one transaction, so the ordering is **archive →
verify → delete**, owned explicitly here (not in ``afm_foundry_sync``, which
only talks to Foundry and the local API — it cannot touch the lakehouse). A
crash between archive and delete leaves the flight in BOTH stores; the next run
re-archives it (idempotent by ``flight_id`` — a harmless duplicate row) and
retries the delete, so the move is convergent.

Memory is bounded by streaming the tenant scan page by page
(``iter_completed_flights``) and flushing each page's archive write + delete
before the next loads — the 2 h trail is heavy and an accumulate-all OOM-killed
the Phase-03 enrichment asset. A per-run delete cap is a safety bound (steady-
state completed volume sits far below it); hitting it is surfaced as ``capped``,
never silently truncated.

``foundry_flight_archive_purge`` (daily, off-peak) enforces the 30-day
retention by dropping whole ``flights_archive/`` day-partition directories —
a directory unlink, never a row-level DELETE+VACUUM (the locked retention
model).
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from afm_foundry_sync.ontology_writers import FoundryWriter
from afm_foundry_sync.sync_jobs import (
    FoundrySyncSkipped,
    guarded_sync,
    load_foundry_settings,
)
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DagsterRunStatus,
    MaterializeResult,
    MetadataValue,
    RunsFilter,
    asset,
    asset_check,
)

from pipelines.resources.lakehouse import LakehouseResource

# Inline check names (declared as check_specs on foundry_flight_archive and
# emitted from the same run that has the data). Kept as constants so the spec
# and the emitted result can't drift apart.
_CHECK_ROWCOUNT = "archive_rowcount_matches"
_CHECK_PAST_RETENTION = "no_completed_flight_past_retention"

# Archive completed flights whose landing is older than this grace window. This
# is a CORRECTNESS floor, not arbitrary: the A2 landing stamp can momentarily
# mark a go-around `landed`, and the next hourly enrichment self-heals it — but
# only while the flight is still in Foundry. 2 h (>= 2 enrichment cycles)
# guarantees the self-heal can fire before archive+delete removes the flight.
# Tunable module constant; flag for review before changing.
_ARCHIVE_GRACE = timedelta(hours=2)

# Per-run delete cap — a safety bound mirroring the reconcile's drain cap.
# Completed-flight volume is modest (landing detection only shipped 2026-05-29,
# so there is no large completed backlog), so steady-state runs sit far below
# this and the cap is invisible; it only bounds a pathological burst. Hitting
# it sets ``capped`` and the remainder drains on the next hourly run.
_ARCHIVE_DELETE_CAP = 5000

# Retention window. Day-partition dirs whose landed date is older than this are
# dropped wholesale by the purge asset.
_ARCHIVE_RETENTION = timedelta(days=30)


@dataclass(frozen=True, slots=True)
class FlightArchiveResult:
    """Outcome of one ``foundry_flight_archive`` run.

    ``completed_seen`` — completed flights streamed from the tenant (landed_at
    set). ``settled`` — of those, the ones older than the grace window
    (eligible to archive this run). ``archived`` — rows durably written to the
    cold store AND verified readable. ``deleted`` — flights removed from
    Foundry after the archive verified (``archived == deleted`` on full
    success; ``archived > deleted`` flags a partial Foundry delete whose
    remainder retries next run, still in both stores meanwhile). ``capped`` —
    the per-run cap was hit and the remainder is left for the next run.

    Check inputs: ``past_retention`` — completed flights seen whose landed date
    is already older than the retention window (normally 0; non-zero means the
    archive fell ≥ a retention-window behind — the post-outage edge surfaced by
    the no-completed-flight-past-retention check). ``archived_persisted`` — an
    INDEPENDENT re-read of the lake (rows stamped with this run's
    ``archived_at``); equals ``archived`` unless a write silently lost rows
    (the row-count-matches check).
    """

    completed_seen: int
    settled: int
    archived: int
    deleted: int
    capped: bool = False
    past_retention: int = 0
    archived_persisted: int = 0


def _parse_iso(value: Any) -> datetime | None:
    """Parse a Foundry ISO-8601 timestamp (trailing ``Z``) to a UTC datetime.

    Returns None for a missing/blank value. Mirrors the inverse of
    ``ontology_writers._iso_utc`` (which emits the ``Z`` form); the ``Z`` →
    ``+00:00`` swap keeps it robust across Python versions.
    """
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _archive_row(
    obj: dict[str, Any], *, landed_at: datetime, archived_at: datetime
) -> dict[str, Any]:
    """Map a raw Foundry Flight object (camelCase) to a ``flights_archive`` row.

    One-for-one with ``FLIGHTS_ARCHIVE_COLUMNS``. The three list fields ride
    through as the tenant's JSON strings unchanged (the same form
    ``ontology_writers.flight_params`` wrote them in). The geo derivations
    (``position`` / ``trailPath``) are intentionally dropped — the archive
    keeps the source (lat/lon + ``trail2h``), not the projection. Timestamps
    are parsed to datetimes for the Arrow timestamp columns; ``archived_at`` is
    the per-run cold-store entry instant (one value for every row this run, so
    the row-count-delta check is robust to a concurrent purge).
    """
    return {
        "flight_id": str(obj.get("flightId") or obj.get("__primaryKey")),
        "icao24": obj.get("icao24"),
        "takeoff_ts": _parse_iso(obj.get("takeoffTs")),
        "landed_at": landed_at,
        "callsign": obj.get("callsign"),
        "registration": obj.get("registration"),
        "aircraft_type": obj.get("aircraftType"),
        "operator_icao": obj.get("operatorIcao"),
        "customer_region": obj.get("customerRegion"),
        "origin_icao": obj.get("originIcao"),
        "destination_icao": obj.get("destinationIcao"),
        "eta_minutes": obj.get("etaMinutes"),
        "status": obj.get("status"),
        "current_stage": obj.get("currentStage"),
        "lat": obj.get("lat"),
        "lon": obj.get("lon"),
        "open_case_count": obj.get("openCaseCount"),
        "open_case_ids": obj.get("openCaseIds"),
        "status_timeline": obj.get("statusTimeline"),
        "trail_2h": obj.get("trail2h"),
        "archived_at": archived_at,
    }


async def _run_flight_archive(
    lakehouse: LakehouseResource,
    *,
    now: datetime,
    grace: timedelta = _ARCHIVE_GRACE,
    cap: int = _ARCHIVE_DELETE_CAP,
    retention: timedelta = _ARCHIVE_RETENTION,
) -> FlightArchiveResult:
    """Stream completed flights → archive → verify → delete, page by page.

    Skip-guarded: an absent ``_private/foundry/.env`` or an unreachable
    Foundry surfaces as ``FoundrySyncSkipped`` (same standalone discipline as
    the ``run_*`` entrypoints), never a crash. Each page is processed fully
    (archive + verify + delete) before the next loads, so a mid-stream failure
    leaves earlier pages durably archived-and-deleted (partial progress; the
    next run carries forward). A verify failure raises BEFORE any delete, so an
    archive that didn't land never costs a Foundry object.

    Also tallies ``past_retention`` (completed flights already older than the
    retention window — the post-outage edge the asset check watches) and, after
    the stream, re-reads the lake for ``archived_persisted`` (an independent
    confirmation of the rows written this run, feeding the row-count check).
    """
    settled_before = now - grace
    retention_before = now - retention
    completed_seen = settled = archived = deleted = past_retention = 0
    capped = False

    async with guarded_sync("flight_archive"):
        settings = load_foundry_settings()
        async with FoundryWriter(settings) as writer:
            async for page in writer.iter_completed_flights():
                completed_seen += len(page)

                # Settled = landed_at older than the grace window (self-heal
                # window has passed). A go-around briefly stamped `landed`
                # within the window is intentionally left for now.
                eligible: list[tuple[dict[str, Any], datetime]] = []
                for obj in page:
                    landed = _parse_iso(obj.get("landedAt"))
                    if landed is None:
                        continue
                    if landed < retention_before:
                        past_retention += 1
                    if landed < settled_before:
                        eligible.append((obj, landed))
                settled += len(eligible)
                if not eligible:
                    continue

                room = cap - archived
                if room <= 0:
                    capped = True
                    break
                if len(eligible) > room:
                    eligible = eligible[:room]
                    capped = True

                # Group by landed date so each write lands in its own day
                # partition (retention drops whole day-dirs).
                rows_by_date: dict[date, list[dict[str, Any]]] = {}
                for obj, landed in eligible:
                    row = _archive_row(obj, landed_at=landed, archived_at=now)
                    rows_by_date.setdefault(landed.astimezone(UTC).date(), []).append(row)

                written_paths: list[Path] = []
                written_ids: set[str] = set()
                for landed_date, rows in rows_by_date.items():
                    path, _ = lakehouse.write_flights_archive(rows, landed_date)
                    written_paths.append(path)
                    written_ids.update(r["flight_id"] for r in rows)

                # VERIFY durability before the cross-store delete.
                readback = set(
                    lakehouse.read_flights_archive_files(written_paths, columns=["flight_id"])[
                        "flight_id"
                    ]
                )
                missing = written_ids - readback
                if missing:
                    # Atomic temp+rename means a returned path is durable, so a
                    # miss here is a genuine fault (file vanished / schema drift),
                    # never a flake — fail loudly rather than swallow it into a
                    # green run, and crucially BEFORE any delete so a flight that
                    # isn't durably archived never leaves Foundry. Name a sample
                    # of the missing ids — the traceback is the only diagnostic.
                    raise RuntimeError(
                        f"flight archive verify failed: {len(missing)} of {len(written_ids)} "
                        f"id(s) not readable from the cold store after write "
                        f"(sample: {sorted(missing)[:10]}); refusing to delete from Foundry"
                    )
                archived += len(written_ids)

                batch = await writer.delete_flight_batch(sorted(written_ids))
                deleted += batch.succeeded

                if capped:
                    break

    # Independent confirmation (re-read the lake, don't trust the in-loop
    # tally): how many rows carry this run's archived_at stamp. Equals
    # ``archived`` unless a write silently lost rows — the row-count check.
    archived_persisted = lakehouse.count_flights_archived_at(now) if archived else 0

    return FlightArchiveResult(
        completed_seen=completed_seen,
        settled=settled,
        archived=archived,
        deleted=deleted,
        capped=capped,
        past_retention=past_retention,
        archived_persisted=archived_persisted,
    )


def _archive_metadata(result: FlightArchiveResult) -> dict[str, MetadataValue]:
    return {
        "completed_seen": MetadataValue.int(result.completed_seen),
        "settled": MetadataValue.int(result.settled),
        "archived": MetadataValue.int(result.archived),
        "deleted": MetadataValue.int(result.deleted),
        "capped": MetadataValue.bool(result.capped),
        "past_retention": MetadataValue.int(result.past_retention),
        "archived_persisted": MetadataValue.int(result.archived_persisted),
    }


def _archive_check_results(result: FlightArchiveResult) -> list[AssetCheckResult]:
    """The two inline checks, derived from the run's own result.

    ``archive_rowcount_matches`` (ERROR): the independent lake re-read of this
    run's stamped rows equals the count the run reported writing — a silent
    write loss fails it. ``no_completed_flight_past_retention`` (WARN): no
    completed flight was seen with a landed date older than retention — non-zero
    means the archive fell ≥ a retention window behind (post-outage edge).
    """
    return [
        AssetCheckResult(
            check_name=_CHECK_ROWCOUNT,
            passed=result.archived == result.archived_persisted,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "archived": MetadataValue.int(result.archived),
                "persisted_in_cold_store": MetadataValue.int(result.archived_persisted),
            },
        ),
        AssetCheckResult(
            check_name=_CHECK_PAST_RETENTION,
            passed=result.past_retention == 0,
            severity=AssetCheckSeverity.WARN,
            metadata={"past_retention": MetadataValue.int(result.past_retention)},
        ),
    ]


def _archive_skipped(context: AssetExecutionContext, reason: str) -> MaterializeResult:
    context.log.warning("flight archive skipped: %s", reason)
    # A skipped run did nothing (archived 0 == persisted 0, past_retention 0),
    # so both checks pass on a zeroed result — emitted so the declared checks
    # never read as "never evaluated".
    skipped = FlightArchiveResult(completed_seen=0, settled=0, archived=0, deleted=0)
    return MaterializeResult(
        metadata={**_archive_metadata(skipped), "skip_reason": MetadataValue.text(reason)},
        check_results=_archive_check_results(skipped),
    )


@asset(
    group_name="foundry_sync",
    description=(
        "Hourly at :50 (Phase B): the SOLE path that removes COMPLETED flights "
        "from the Foundry Ontology. Captures each completed flight (landed_at "
        "older than the grace window) to the Parquet flights_archive/ cold "
        "store, verifies it durable, THEN deletes it from Foundry — so no "
        "completed flight is ever deleted unarchived. Streamed + capped."
    ),
    metadata={
        "target": "lakehouse: flights_archive/ ← Foundry Ontology: Flight",
        "cadence": "hourly",
    },
    check_specs=[
        AssetCheckSpec(
            name=_CHECK_ROWCOUNT,
            asset="foundry_flight_archive",
            description=(
                "Rows persisted to the cold store this run (independent lake "
                "re-read by run stamp) == flights the run reported archiving."
            ),
        ),
        AssetCheckSpec(
            name=_CHECK_PAST_RETENTION,
            asset="foundry_flight_archive",
            description=(
                "No completed flight in the tenant has a landed date older than "
                "retention (non-zero ⇒ archive fell ≥ a retention window behind)."
            ),
        ),
    ],
)
def foundry_flight_archive(
    context: AssetExecutionContext,
    lakehouse: LakehouseResource,
) -> MaterializeResult:
    # Overlap guard: never stack a new archive on a still-running one (mirrors
    # the reconcile + enrichment guard). The cross-store sequence is not
    # idempotent under concurrency — two runs could both archive then both
    # delete the same flight.
    in_progress = context.instance.get_run_records(
        RunsFilter(
            job_name="foundry_flight_archive_job",
            statuses=[
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
                DagsterRunStatus.CANCELING,
            ],
        )
    )
    if any(r.dagster_run.run_id != context.run_id for r in in_progress):
        return _archive_skipped(
            context,
            "another foundry_flight_archive run is already in progress (coalesced)",
        )
    try:
        result = asyncio.run(_run_flight_archive(lakehouse, now=datetime.now(UTC)))
    except FoundrySyncSkipped as exc:
        return _archive_skipped(context, exc.reason)
    context.log.info(
        "flight archive: completed_seen=%d settled=%d archived=%d deleted=%d "
        "capped=%s past_retention=%d persisted=%d",
        result.completed_seen,
        result.settled,
        result.archived,
        result.deleted,
        result.capped,
        result.past_retention,
        result.archived_persisted,
    )
    return MaterializeResult(
        metadata=_archive_metadata(result),
        check_results=_archive_check_results(result),
    )


@asset(
    group_name="foundry_sync",
    description=(
        "Daily (off-peak): enforce 30-day retention on the flights_archive/ "
        "cold store by dropping whole landed-date partition directories older "
        "than the cutoff — a directory unlink, never a row-level DELETE+VACUUM."
    ),
    metadata={"target": "lakehouse: flights_archive/", "cadence": "daily"},
)
def foundry_flight_archive_purge(
    context: AssetExecutionContext,
    lakehouse: LakehouseResource,
) -> MaterializeResult:
    cutoff = (datetime.now(UTC) - _ARCHIVE_RETENTION).date()
    dropped = lakehouse.purge_flights_archive_before(cutoff)
    context.log.info(
        "flight archive purge: cutoff=%s dropped %d partition(s): %s",
        cutoff.isoformat(),
        len(dropped),
        ", ".join(d.isoformat() for d in dropped) or "(none)",
    )
    return MaterializeResult(
        metadata={
            "cutoff_date": MetadataValue.text(cutoff.isoformat()),
            "partitions_dropped": MetadataValue.int(len(dropped)),
            "dropped_dates": MetadataValue.text(
                ", ".join(d.isoformat() for d in dropped) or "(none)"
            ),
        }
    )


# ---------------------------------------------------------------------------
# Standalone asset checks — cross-cutting invariants the run can't self-check.
# (The run-data checks are emitted inline above; these inspect both stores /
# the whole archive, so they live as independently-runnable @asset_check.)
# ---------------------------------------------------------------------------


async def _live_flight_pks() -> set[str]:
    """Every Flight PK currently in the live Foundry tenant.

    Skip-guarded: an absent ``.env`` / unreachable Foundry surfaces as
    ``FoundrySyncSkipped`` so the check can report "not verified" rather than
    fail on infra absence (the standalone-stack discipline).
    """
    async with guarded_sync("flight_archive_check"):
        settings = load_foundry_settings()
        async with FoundryWriter(settings) as writer:
            return await writer.list_flight_pks()


@asset_check(
    asset=foundry_flight_archive,
    name="archive_and_tenant_disjoint",
    description="Exactly-once move: no flight_id is in BOTH the cold store and the live tenant.",
)
def archive_and_tenant_disjoint(lakehouse: LakehouseResource) -> AssetCheckResult:
    """The integrity invariant of the archive-then-delete move: a flight lives
    in the cold store XOR the live tenant, never both. A crash mid-move can
    momentarily leave a duplicate; convergence should clear it, and this check
    catches a stuck overlap. Reads every archived flight_id (projected, so the
    heavy trail is never loaded) and intersects with the live tenant PKs.
    Foundry unreachable ⇒ cannot verify ⇒ passes with a WARN note.
    """
    archived_ids = set(
        lakehouse.read_flights_archive(lookback_days=None, columns=["flight_id"])["flight_id"]
    )
    try:
        live = asyncio.run(_live_flight_pks())
    except FoundrySyncSkipped as exc:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description=f"not verified — Foundry unreachable: {exc.reason}",
            metadata={
                "archived": MetadataValue.int(len(archived_ids)),
                "verified": MetadataValue.bool(False),
            },
        )
    overlap = sorted(archived_ids & live)
    return AssetCheckResult(
        passed=not overlap,
        severity=AssetCheckSeverity.ERROR,
        description=("disjoint" if not overlap else f"{len(overlap)} flight_id(s) in BOTH stores"),
        metadata={
            "archived": MetadataValue.int(len(archived_ids)),
            "live_tenant": MetadataValue.int(len(live)),
            "overlap": MetadataValue.int(len(overlap)),
            "overlap_sample": MetadataValue.text(str(overlap[:10])),
        },
    )


@asset_check(
    asset=foundry_flight_archive_purge,
    name="archive_retention_enforced",
    description="Oldest cold-store partition is within the retention window (proves purge fired).",
)
def archive_retention_enforced(lakehouse: LakehouseResource) -> AssetCheckResult:
    """A partition older than ``now - retention`` is proof the daily purge did
    NOT fire. An empty archive passes trivially.
    """
    oldest = lakehouse.oldest_flights_archive_partition()
    cutoff = (datetime.now(UTC) - _ARCHIVE_RETENTION).date()
    passed = oldest is None or oldest >= cutoff
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description=(
            "archive empty"
            if oldest is None
            else f"oldest partition {oldest.isoformat()} (cutoff {cutoff.isoformat()})"
        ),
        metadata={
            "oldest_partition": MetadataValue.text(oldest.isoformat() if oldest else "(none)"),
            "retention_cutoff": MetadataValue.text(cutoff.isoformat()),
        },
    )
