"""Unit tests for the AFM Prometheus collector (``app.observability``).

No TestClient, no DB — matching the suite's style. The collector reads
``app.state.postgres`` / ``app.state.dagster_postgres`` directly, so we feed it
MagicMock pools and assert on the emitted gauges. Each ``fetchall.side_effect``
list mirrors the *source order* of queries inside the ``_collect_*`` method.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.observability import AFMMetricsCollector
from app.services.postgres import PostgresPool


def _samples(metrics: Any) -> dict[str, dict[tuple, float]]:
    """Flatten a collected GaugeMetricFamily iterable -> {name: {labels: value}}."""
    out: dict[str, dict[tuple, float]] = {}
    for m in metrics:
        bucket = out.setdefault(m.name, {})
        for s in m.samples:
            bucket[tuple(sorted(s.labels.items()))] = s.value
    return out


def _make_app(postgres: Any = None, dagster_postgres: Any = None) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(postgres=postgres, dagster_postgres=dagster_postgres)
    )


def _fleet_row() -> dict[str, int]:
    return {"active": 5, "tracked": 9, "lag_seconds": 42}


def _case_fetchall() -> list[list[dict[str, Any]]]:
    """The 7 ``app.cases`` fetchall result sets, in the order _collect_cases runs."""
    return [
        [{"severity": "high", "status": "open", "n": 3}],  # cases(severity,status)
        [{"region": "west", "n": 4}],  # by_region
        [{"case_type": "go_around", "n": 4}],  # by_type
        [{"case_type": "go_around", "severity": "medium", "n": 4}],  # by_type_severity
        [{"case_type": "lost_signal", "n": 2}],  # created_24h
        [{"site_icao": "KLAX", "n": 4}],  # by_site
        [{"sync_status": "synced", "n": 4}],  # sf_sync_backlog
    ]


def _business_pool() -> MagicMock:
    pool = MagicMock(spec=PostgresPool)
    pool.fetchone.return_value = _fleet_row()
    pool.fetchall.side_effect = _case_fetchall()
    return pool


def test_collect_dagster_emits_expected_gauges() -> None:
    dpool = MagicMock(spec=PostgresPool)
    dpool.fetchall.side_effect = [
        [  # runs_24h
            {"pipeline": "opensky_positions_job", "status": "SUCCESS", "n": 280},
            {"pipeline": "opensky_positions_job", "status": "FAILURE", "n": 2},
        ],
        [  # per-pipeline freshness (DISTINCT ON)
            {
                "pipeline": "opensky_positions_job",
                "start_time": 100.0,
                "end_time": 112.5,
                "age": 42.0,
            },
            {"pipeline": "stuck_job", "start_time": None, "end_time": 5.0, "age": 9.0},
        ],
    ]
    collector = AFMMetricsCollector(_make_app(dagster_postgres=dpool))
    s = _samples(collector._collect_dagster(dpool))

    runs = s["afm_dagster_runs_24h"]
    assert runs[(("pipeline", "opensky_positions_job"), ("status", "SUCCESS"))] == 280
    assert runs[(("pipeline", "opensky_positions_job"), ("status", "FAILURE"))] == 2

    dur = s["afm_dagster_job_last_duration_seconds"]
    age = s["afm_dagster_job_last_age_seconds"]
    assert dur[(("pipeline", "opensky_positions_job"),)] == pytest.approx(12.5)
    assert age[(("pipeline", "opensky_positions_job"),)] == 42.0
    # start_time is None -> duration is omitted, but age is still emitted.
    assert (("pipeline", "stuck_job"),) not in dur
    assert age[(("pipeline", "stuck_job"),)] == 9.0


def test_dagster_failure_keeps_business_metrics_and_up_signal() -> None:
    """A dagster-store failure must not zero the business gauges or the up-signal."""
    dpool = MagicMock(spec=PostgresPool)
    dpool.fetchall.side_effect = RuntimeError("dagster store down")
    collector = AFMMetricsCollector(_make_app(postgres=_business_pool(), dagster_postgres=dpool))
    s = _samples(collector.collect())

    assert s["afm_active_aircraft"][()] == 5
    assert "afm_cases" in s
    assert s["afm_metrics_collector_up"][()] == 1.0  # NOT zeroed by dagster failure
    assert "afm_dagster_runs_24h" not in s  # no dagster gauges emitted


def test_dagster_pool_none_emits_business_only() -> None:
    collector = AFMMetricsCollector(_make_app(postgres=_business_pool(), dagster_postgres=None))
    s = _samples(collector.collect())

    assert s["afm_metrics_collector_up"][()] == 1.0
    assert s["afm_tracked_aircraft"][()] == 9
    assert "afm_dagster_runs_24h" not in s


def test_main_pool_none_reports_collector_down() -> None:
    collector = AFMMetricsCollector(_make_app(postgres=None))
    s = _samples(collector.collect())

    assert s["afm_metrics_collector_up"][()] == 0.0
    assert "afm_active_aircraft" not in s


def test_cases_by_site_caps_at_top_20_plus_other() -> None:
    # 25 sites, counts 25..1 already descending (the SQL ORDER BY n DESC).
    site_rows = [{"site_icao": f"K{i:03d}", "n": 25 - i} for i in range(25)]
    fetchalls = _case_fetchall()
    fetchalls[5] = site_rows
    pool = MagicMock(spec=PostgresPool)
    pool.fetchone.return_value = _fleet_row()
    pool.fetchall.side_effect = fetchalls

    collector = AFMMetricsCollector(_make_app(postgres=pool))
    by_site = _samples(collector.collect())["afm_cases_by_site"]

    assert len(by_site) == 21  # 20 named + 1 "other"
    # remainder = the 5 smallest counts: 5+4+3+2+1
    assert by_site[(("site_icao", "other"),)] == 15
