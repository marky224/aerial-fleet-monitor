"""Prometheus instrumentation for the API (Phase 09).

Two exposition surfaces:

  * ``/v1/metrics`` — RED metrics (request rate / errors / duration) from
    prometheus-fastapi-instrumentator. Multiprocess-aware: when
    ``PROMETHEUS_MULTIPROC_DIR`` is set (it is, in-stack — the API runs 3
    uvicorn workers), each worker writes to the shared dir and ``expose()``
    aggregates them, so a scrape reflects every worker rather than whichever
    one happened to answer.

  * ``/v1/metrics/extras`` — AFM business gauges derived **live from Postgres**
    on each scrape. Reading the operational store (the source of truth)
    instead of in-process counters keeps the numbers correct across all
    workers, surviving restarts, and never drifting from the database. Served
    from its own registry via a custom collector so it doesn't interleave with
    the multiprocess RED registry.

Dagster pipeline-run metrics (from the separate ``dagster`` database) are a
follow-up: the one Phase-09 dashboard uses pipeline *lag* (derived here from
``current_positions``) as its freshness signal.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response

from app.logging import get_logger

if TYPE_CHECKING:
    from fastapi import FastAPI

    from app.services.postgres import PostgresPool

log = get_logger(__name__)

_COLLECTOR_UP = "afm_metrics_collector_up"
_COLLECTOR_UP_HELP = "1 if the AFM Postgres-derived collector scraped cleanly, else 0."


class AFMMetricsCollector:
    """Custom Prometheus collector: AFM business gauges from Postgres.

    ``collect()`` runs a handful of aggregate queries against ``app.cases``
    and ``app.current_positions`` on each scrape and yields point-in-time
    gauges. Every query is a cheap COUNT/GROUP BY on indexed columns.

    Resilient by design: if the pool isn't up yet (a scrape during startup) or
    a query fails, it emits ``afm_metrics_collector_up 0`` and no business
    gauges, rather than failing the whole scrape with a 500.
    """

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    def _pool(self) -> PostgresPool | None:
        return getattr(self._app.state, "postgres", None)

    def collect(self) -> Iterator[GaugeMetricFamily]:
        pool = self._pool()
        if pool is None:
            yield GaugeMetricFamily(_COLLECTOR_UP, _COLLECTOR_UP_HELP, value=0.0)
            return
        try:
            yield from self._collect_fleet(pool)
            yield from self._collect_cases(pool)
        except Exception as exc:
            log.warning("metrics.extras.collect_failed", error=str(exc))
            yield GaugeMetricFamily(_COLLECTOR_UP, _COLLECTOR_UP_HELP, value=0.0)
            return
        yield GaugeMetricFamily(_COLLECTOR_UP, _COLLECTOR_UP_HELP, value=1.0)

    def _collect_fleet(self, pool: PostgresPool) -> Iterator[GaugeMetricFamily]:
        row = pool.fetchone(
            """
            SELECT
              count(*) FILTER (WHERE last_seen_at > now() - interval '15 minutes') AS active,
              count(*) AS tracked,
              coalesce(extract(epoch FROM now() - max(last_seen_at)), -1)::int AS lag_seconds
            FROM app.current_positions
            """
        ) or {"active": 0, "tracked": 0, "lag_seconds": -1}
        yield GaugeMetricFamily(
            "afm_active_aircraft",
            "Aircraft with a position fix in the last 15 minutes.",
            value=float(row["active"]),
        )
        yield GaugeMetricFamily(
            "afm_tracked_aircraft",
            "Total aircraft rows in current_positions.",
            value=float(row["tracked"]),
        )
        yield GaugeMetricFamily(
            "afm_pipeline_lag_seconds",
            "Seconds since the most recent fleet-wide position fix (-1 if none).",
            value=float(row["lag_seconds"]),
        )

    def _collect_cases(self, pool: PostgresPool) -> Iterator[GaugeMetricFamily]:
        cases = GaugeMetricFamily(
            "afm_cases",
            "Current case count by severity and status.",
            labels=["severity", "status"],
        )
        for r in pool.fetchall(
            """
            SELECT coalesce(severity, 'unknown') AS severity,
                   coalesce(status, 'unknown')   AS status,
                   count(*) AS n
            FROM app.cases GROUP BY 1, 2
            """
        ):
            cases.add_metric([r["severity"], r["status"]], float(r["n"]))
        yield cases

        by_region = GaugeMetricFamily(
            "afm_cases_by_region",
            "Current case count by customer region.",
            labels=["region"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(customer_region, 'unknown') AS region, count(*) AS n "
            "FROM app.cases GROUP BY 1"
        ):
            by_region.add_metric([r["region"]], float(r["n"]))
        yield by_region

        by_type = GaugeMetricFamily(
            "afm_cases_by_type",
            "Current case count by detector rule (case_type).",
            labels=["case_type"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(case_type, 'unknown') AS case_type, count(*) AS n "
            "FROM app.cases GROUP BY 1"
        ):
            by_type.add_metric([r["case_type"]], float(r["n"]))
        yield by_type

        sync = GaugeMetricFamily(
            "afm_sf_sync_backlog",
            "Case count by Salesforce sync status (pending/failed/skipped/synced).",
            labels=["sync_status"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(sf_sync_status, 'unknown') AS sync_status, count(*) AS n "
            "FROM app.cases GROUP BY 1"
        ):
            sync.add_metric([r["sync_status"]], float(r["n"]))
        yield sync


def setup_observability(app: FastAPI) -> None:
    """Wire RED metrics (/v1/metrics) + PG-derived gauges (/v1/metrics/extras)."""
    Instrumentator(
        should_group_status_codes=False,
        excluded_handlers=["/v1/metrics", "/v1/metrics/extras", "/v1/health"],
    ).instrument(app).expose(app, endpoint="/v1/metrics", include_in_schema=False)

    registry = CollectorRegistry()
    registry.register(AFMMetricsCollector(app))

    @app.get("/v1/metrics/extras", include_in_schema=False)
    async def metrics_extras() -> Response:
        payload = await run_in_threadpool(generate_latest, registry)
        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)
