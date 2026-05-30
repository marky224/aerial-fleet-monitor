"""Tests for the Phase-B flight archival assets + cross-store driver.

``_run_flight_archive`` interleaves a Foundry scan with lakehouse writes, a
verify, and the cross-store delete. The Foundry side is faked here (its HTTP
layer is unit-tested in foundry/sync via respx) while the lakehouse is a REAL
``LakehouseResource`` on tmp_path — so these exercise the archive-before-delete
ordering, the settled-grace filter, the per-run cap, and the durable round-trip
(including that the geo projections are NOT archived). The asset boundary
(skip / success / defect / purge) is tested separately, stubbing the driver.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from afm_foundry_sync.ontology_writers import BatchResult
from afm_foundry_sync.sync_jobs import FoundrySyncSkipped
from dagster import AssetCheckSeverity, build_asset_context

from pipelines.assets import flight_archival
from pipelines.assets.flight_archival import FlightArchiveResult
from pipelines.resources.lakehouse import LakehouseResource


def _foundry_flight(flight_id: str, *, landed: str, **over: object) -> dict[str, object]:
    """A raw Foundry Flight object (camelCase), including the geo projections
    the tenant carries but the archive must drop."""
    obj: dict[str, object] = {
        "flightId": flight_id,
        "icao24": flight_id.split("-")[0],
        "takeoffTs": "2026-05-29T13:00:00Z",
        "landedAt": landed,
        "callsign": "UAL1",
        "registration": "N1",
        "aircraftType": "B738",
        "operatorIcao": "UAL",
        "customerRegion": "east",
        "originIcao": "KORD",
        "destinationIcao": "KSFO",
        "etaMinutes": None,
        "status": "landed",
        "currentStage": "landed",
        "lat": 37.6,
        "lon": -122.4,
        "openCaseCount": 0,
        "openCaseIds": "[]",
        "statusTimeline": '[{"stage":"departed","occurred_at":"2026-05-29T13:00:00Z"}]',
        "trail2h": '[{"ts":"2026-05-29T14:00:00Z","lat":37.6,"lon":-122.4}]',
        "position": {"type": "Point", "coordinates": [-122.4, 37.6]},
        "trailPath": {"type": "LineString", "coordinates": [[-122.4, 37.6], [-122.5, 37.7]]},
    }
    obj.update(over)
    return obj


def _seed_archive_row(flight_id: str, landed_at: datetime) -> dict[str, object]:
    """A minimal complete archive row (non-null fields set) for seeding the
    purge test directly through the lakehouse write path."""
    return {
        "flight_id": flight_id,
        "icao24": "aaa",
        "takeoff_ts": landed_at - timedelta(hours=2),
        "landed_at": landed_at,
        "open_case_ids": "[]",
        "status_timeline": "[]",
        "trail_2h": "[]",
        "archived_at": landed_at,
    }


class _FakeWriter:
    """Stands in for FoundryWriter: yields canned pages, records deletes, and
    snapshots the cold store at delete time to prove archive-before-delete."""

    def __init__(self, pages, lake, delete_probe):  # type: ignore[no-untyped-def]
        self._pages = pages
        self._lake = lake
        self._probe = delete_probe

    async def __aenter__(self):  # type: ignore[no-untyped-def]
        return self

    async def __aexit__(self, *exc):  # type: ignore[no-untyped-def]
        return False

    async def iter_completed_flights(self):  # type: ignore[no-untyped-def]
        for page in self._pages:
            yield page

    async def delete_flight_batch(self, ids):  # type: ignore[no-untyped-def]
        present = set(
            self._lake.read_flights_archive(lookback_days=None, columns=["flight_id"])["flight_id"]
        )
        self._probe.append((sorted(ids), set(ids) <= present))
        return BatchResult(attempted=len(ids), succeeded=len(ids))


def _patch_writer(monkeypatch, pages, lake, delete_probe):  # type: ignore[no-untyped-def]
    monkeypatch.setattr(flight_archival, "load_foundry_settings", lambda: object())
    monkeypatch.setattr(
        flight_archival, "FoundryWriter", lambda settings: _FakeWriter(pages, lake, delete_probe)
    )


# ---------------------------------------------------------------------------
# _run_flight_archive (the cross-store driver)
# ---------------------------------------------------------------------------


def test_archive_writes_and_verifies_before_deleting(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 29, 18, 0, tzinfo=UTC)
    # landed 3h ago > 2h grace -> settled, eligible.
    pages = [[_foundry_flight("abc123-100", landed="2026-05-29T15:00:00Z")]]
    probe: list[tuple[list[str], bool]] = []
    _patch_writer(monkeypatch, pages, lake, probe)

    result = asyncio.run(flight_archival._run_flight_archive(lake, now=now))

    assert (
        result.completed_seen,
        result.settled,
        result.archived,
        result.deleted,
        result.capped,
    ) == (
        1,
        1,
        1,
        1,
        False,
    )
    # Archive-before-delete: at the moment of the delete, the id was already in
    # the cold store (the verify gates the delete).
    assert probe == [(["abc123-100"], True)]

    df = lake.read_flights_archive(lookback_days=None)
    assert set(df["flight_id"]) == {"abc123-100"}
    row = df.iloc[0]
    # JSON list fields ride through unchanged; geo projections are NOT archived.
    assert row["trail_2h"] == pages[0][0]["trail2h"]
    assert row["status_timeline"] == pages[0][0]["statusTimeline"]
    assert "position" not in df.columns and "trail_path" not in df.columns
    # Per-run archived_at stamp == the run instant.
    assert row["archived_at"].to_pydatetime() == now
    # Partition keyed on landed date.
    assert (tmp_path / "flights_archive/year=2026/month=05/day=29").exists()


def test_archive_skips_unsettled_within_grace(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 29, 18, 0, tzinfo=UTC)
    # landed 1h ago < 2h grace -> NOT settled (go-around self-heal window open).
    pages = [[_foundry_flight("abc123-100", landed="2026-05-29T17:00:00Z")]]
    probe: list[tuple[list[str], bool]] = []
    _patch_writer(monkeypatch, pages, lake, probe)

    result = asyncio.run(flight_archival._run_flight_archive(lake, now=now))

    assert (result.completed_seen, result.settled, result.archived, result.deleted) == (1, 0, 0, 0)
    assert probe == []  # nothing archived, nothing deleted
    assert lake.count_flights_archive() == 0


def test_archive_caps_per_run(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 29, 18, 0, tzinfo=UTC)
    settled = "2026-05-29T15:00:00Z"
    pages = [[_foundry_flight(f"aaa-{i}", landed=settled) for i in range(3)]]
    probe: list[tuple[list[str], bool]] = []
    _patch_writer(monkeypatch, pages, lake, probe)

    result = asyncio.run(flight_archival._run_flight_archive(lake, now=now, cap=2))

    assert result.capped is True
    assert (result.archived, result.deleted) == (2, 2)
    assert lake.count_flights_archive() == 2


def test_archive_groups_rows_by_landed_date(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 5, 29, 18, 0, tzinfo=UTC)
    pages = [
        [
            _foundry_flight("a-1", landed="2026-05-28T10:00:00Z"),
            _foundry_flight("b-1", landed="2026-05-29T10:00:00Z"),
        ]
    ]
    probe: list[tuple[list[str], bool]] = []
    _patch_writer(monkeypatch, pages, lake, probe)

    result = asyncio.run(flight_archival._run_flight_archive(lake, now=now))

    assert result.archived == 2
    assert (tmp_path / "flights_archive/year=2026/month=05/day=28").exists()
    assert (tmp_path / "flights_archive/year=2026/month=05/day=29").exists()


# ---------------------------------------------------------------------------
# Asset boundary
# ---------------------------------------------------------------------------


def test_flight_archive_skip_is_a_materialization_not_a_failure(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))

    async def _raise(*_a: object, **_k: object) -> FlightArchiveResult:
        raise FoundrySyncSkipped("flight_archive: foundry config absent")

    monkeypatch.setattr(flight_archival, "_run_flight_archive", _raise)

    res = flight_archival.foundry_flight_archive(build_asset_context(), lakehouse=lake)
    md = res.metadata or {}
    assert md["skip_reason"].value == "flight_archive: foundry config absent"
    assert md["archived"].value == 0
    assert md["deleted"].value == 0
    # A skipped run did nothing, so its declared checks are emitted as passing
    # (0 == 0, past_retention 0) rather than reading as never-evaluated.
    assert all(c.passed for c in (res.check_results or []))
    assert len(res.check_results or []) == 2


def test_flight_archive_success_surfaces_counts(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))

    async def _ok(*_a: object, **_k: object) -> FlightArchiveResult:
        return FlightArchiveResult(
            completed_seen=120,
            settled=40,
            archived=40,
            deleted=40,
            capped=False,
            past_retention=0,
            archived_persisted=40,
        )

    monkeypatch.setattr(flight_archival, "_run_flight_archive", _ok)

    md = (
        flight_archival.foundry_flight_archive(build_asset_context(), lakehouse=lake).metadata or {}
    )
    assert md["completed_seen"].value == 120
    assert md["settled"].value == 40
    assert md["archived"].value == 40
    assert md["deleted"].value == 40
    assert md["capped"].value is False
    assert md["past_retention"].value == 0
    assert md["archived_persisted"].value == 40


def _archive_checks(monkeypatch, lake, result: FlightArchiveResult) -> dict:  # type: ignore[type-arg]
    async def _ok(*_a: object, **_k: object) -> FlightArchiveResult:
        return result

    monkeypatch.setattr(flight_archival, "_run_flight_archive", _ok)
    res = flight_archival.foundry_flight_archive(build_asset_context(), lakehouse=lake)
    return {c.check_name: c for c in (res.check_results or [])}


def test_inline_checks_pass_on_consistent_run(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    checks = _archive_checks(
        monkeypatch,
        lake,
        FlightArchiveResult(
            completed_seen=10,
            settled=4,
            archived=4,
            deleted=4,
            past_retention=0,
            archived_persisted=4,
        ),
    )
    assert checks["archive_rowcount_matches"].passed is True
    assert checks["no_completed_flight_past_retention"].passed is True


def test_rowcount_check_fails_when_persisted_differs(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    checks = _archive_checks(
        monkeypatch,
        lake,
        FlightArchiveResult(
            completed_seen=10,
            settled=4,
            archived=4,
            deleted=4,
            past_retention=0,
            archived_persisted=3,
        ),
    )
    assert checks["archive_rowcount_matches"].passed is False
    assert checks["archive_rowcount_matches"].severity == AssetCheckSeverity.ERROR


def test_past_retention_check_warns_when_nonzero(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    checks = _archive_checks(
        monkeypatch,
        lake,
        FlightArchiveResult(
            completed_seen=10,
            settled=4,
            archived=4,
            deleted=4,
            past_retention=2,
            archived_persisted=4,
        ),
    )
    assert checks["no_completed_flight_past_retention"].passed is False
    assert checks["no_completed_flight_past_retention"].severity == AssetCheckSeverity.WARN


def test_flight_archive_real_defect_is_not_swallowed(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))

    async def _bug(*_a: object, **_k: object) -> FlightArchiveResult:
        raise RuntimeError("archive verify failed: 1 id(s) not readable")

    monkeypatch.setattr(flight_archival, "_run_flight_archive", _bug)

    with pytest.raises(RuntimeError, match="archive verify failed"):
        flight_archival.foundry_flight_archive(build_asset_context(), lakehouse=lake)


def test_flight_archive_purge_drops_old_and_surfaces_metadata(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    recent = datetime.now(UTC) - timedelta(days=1)
    old = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)
    lake.write_flights_archive([_seed_archive_row("old-1", old)], old.date())
    lake.write_flights_archive([_seed_archive_row("new-1", recent)], recent.date())

    md = (
        flight_archival.foundry_flight_archive_purge(build_asset_context(), lakehouse=lake).metadata
        or {}
    )

    assert md["partitions_dropped"].value == 1
    assert "2020-01-01" in md["dropped_dates"].value
    assert lake.count_flights_archive() == 1  # only the recent partition remains


def test_driver_tallies_past_retention_and_persisted(tmp_path, monkeypatch):
    """The driver feeds the inline checks: past_retention counts completed
    flights already older than retention, and archived_persisted is an
    independent lake re-count by this run's stamp."""
    lake = LakehouseResource(lake_path=str(tmp_path))
    now = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)  # retention cutoff = 2026-06-01
    pages = [
        [
            _foundry_flight("r-1", landed="2026-07-01T09:00:00Z"),  # settled, recent
            _foundry_flight("o-1", landed="2026-05-20T09:00:00Z"),  # settled, > 30d old
        ]
    ]
    _patch_writer(monkeypatch, pages, lake, [])

    result = asyncio.run(flight_archival._run_flight_archive(lake, now=now))

    assert result.completed_seen == 2
    assert result.settled == 2
    assert result.past_retention == 1  # only o-1 is older than the 30-day window
    assert result.archived == 2
    assert result.archived_persisted == 2  # independent lake re-count by stamp matches


# ---------------------------------------------------------------------------
# Standalone asset checks
# ---------------------------------------------------------------------------


def test_archive_tenant_disjoint_passes_when_no_overlap(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    landed = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_seed_archive_row("arch-1", landed)], landed.date())

    async def _live() -> set[str]:
        return {"live-1", "live-2"}

    monkeypatch.setattr(flight_archival, "_live_flight_pks", _live)
    res = flight_archival.archive_and_tenant_disjoint(lakehouse=lake)
    assert res.passed is True
    assert res.metadata["overlap"].value == 0


def test_archive_tenant_disjoint_fails_on_overlap(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))
    landed = datetime(2026, 5, 21, 9, 0, tzinfo=UTC)
    lake.write_flights_archive([_seed_archive_row("dup-1", landed)], landed.date())

    async def _live() -> set[str]:
        return {"dup-1", "live-2"}  # dup-1 is in BOTH stores -> violation

    monkeypatch.setattr(flight_archival, "_live_flight_pks", _live)
    res = flight_archival.archive_and_tenant_disjoint(lakehouse=lake)
    assert res.passed is False
    assert res.severity == AssetCheckSeverity.ERROR
    assert res.metadata["overlap"].value == 1


def test_archive_tenant_disjoint_unverified_when_foundry_unreachable(tmp_path, monkeypatch):
    lake = LakehouseResource(lake_path=str(tmp_path))

    async def _live() -> set[str]:
        raise FoundrySyncSkipped("flight_archive_check: foundry/api unreachable")

    monkeypatch.setattr(flight_archival, "_live_flight_pks", _live)
    res = flight_archival.archive_and_tenant_disjoint(lakehouse=lake)
    assert res.passed is True  # infra absence is not a violation
    assert res.metadata["verified"].value is False


def test_retention_enforced_passes_within_window(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    recent = datetime.now(UTC) - timedelta(days=2)
    lake.write_flights_archive([_seed_archive_row("r-1", recent)], recent.date())
    res = flight_archival.archive_retention_enforced(lakehouse=lake)
    assert res.passed is True


def test_retention_enforced_fails_when_old_partition_present(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    old = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)
    lake.write_flights_archive([_seed_archive_row("o-1", old)], old.date())
    res = flight_archival.archive_retention_enforced(lakehouse=lake)
    assert res.passed is False
    assert res.severity == AssetCheckSeverity.ERROR


def test_retention_enforced_passes_when_archive_empty(tmp_path):
    lake = LakehouseResource(lake_path=str(tmp_path))
    res = flight_archival.archive_retention_enforced(lakehouse=lake)
    assert res.passed is True
