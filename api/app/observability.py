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

Dagster pipeline-run metrics (job run counts, last-run duration/age) come
from the separate ``dagster`` run-store database via a second, independent
pool (``app.state.dagster_postgres``). That collection is isolated in its own
try/except so a dagster-store hiccup never zeroes the business gauges.
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
    gauges. Every query is a cheap COUNT/GROUP BY on indexed columns. It also
    reads Dagster's run store (a second pool) for pipeline-run metrics.

    Resilient by design: if the main pool isn't up yet (a scrape during
    startup) or a business query fails, it emits ``afm_metrics_collector_up 0``
    and no business gauges, rather than failing the whole scrape with a 500.
    Dagster collection is guarded separately, so its failure leaves the
    business gauges (and the up-signal) intact.
    """

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    def _pool(self) -> PostgresPool | None:
        return getattr(self._app.state, "postgres", None)

    def _dagster_pool(self) -> PostgresPool | None:
        return getattr(self._app.state, "dagster_postgres", None)

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
        # Dagster pipeline-run metrics live in a separate database/pool and are
        # collected under their own guard: a dagster-store failure must never
        # zero the business gauges above (nor the up-signal).
        dagster_pool = self._dagster_pool()
        if dagster_pool is not None:
            try:
                yield from self._collect_dagster(dagster_pool)
            except Exception as exc:
                log.warning("metrics.extras.dagster_collect_failed", error=str(exc))
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

        by_type_severity = GaugeMetricFamily(
            "afm_cases_by_type_severity",
            "Current case count by detector rule (case_type) and severity.",
            labels=["case_type", "severity"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(case_type, 'unknown') AS case_type, "
            "coalesce(severity, 'unknown') AS severity, count(*) AS n "
            "FROM app.cases GROUP BY 1, 2"
        ):
            by_type_severity.add_metric([r["case_type"], r["severity"]], float(r["n"]))
        yield by_type_severity

        created_24h = GaugeMetricFamily(
            "afm_cases_created_24h",
            "Cases created in the last 24 hours by detector rule (case_type).",
            labels=["case_type"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(case_type, 'unknown') AS case_type, count(*) AS n "
            "FROM app.cases WHERE created_at > now() - interval '24 hours' GROUP BY 1"
        ):
            created_24h.add_metric([r["case_type"]], float(r["n"]))
        yield created_24h

        # By site: cap label cardinality at the top 20 busiest airports, with
        # the long tail folded into a single "other" bucket.
        by_site = GaugeMetricFamily(
            "afm_cases_by_site",
            "Current case count by site ICAO (top 20 busiest; remainder as 'other').",
            labels=["site_icao"],
        )
        site_rows = pool.fetchall(
            "SELECT coalesce(site_icao, 'unknown') AS site_icao, count(*) AS n "
            "FROM app.cases GROUP BY 1 ORDER BY n DESC"
        )
        for r in site_rows[:20]:
            by_site.add_metric([r["site_icao"]], float(r["n"]))
        other = sum(int(r["n"]) for r in site_rows[20:])
        if other:
            by_site.add_metric(["other"], float(other))
        yield by_site

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

    def _collect_dagster(self, pool: PostgresPool) -> Iterator[GaugeMetricFamily]:
        """Pipeline-run metrics from Dagster's run store (separate database).

        The label is ``pipeline`` (not ``job``) on purpose: ``job`` is a
        reserved Prometheus scrape label, so emitting our own ``job`` would
        collide and surface as ``exported_job`` after a scrape.
        """
        runs_24h = GaugeMetricFamily(
            "afm_dagster_runs_24h",
            "Dagster pipeline runs in the last 24 hours by pipeline and status.",
            labels=["pipeline", "status"],
        )
        for r in pool.fetchall(
            "SELECT coalesce(pipeline_name, 'unknown') AS pipeline, "
            "coalesce(status, 'unknown') AS status, count(*) AS n "
            "FROM runs WHERE create_timestamp > now() - interval '24 hours' "
            "GROUP BY 1, 2"
        ):
            runs_24h.add_metric([r["pipeline"], r["status"]], float(r["n"]))
        yield runs_24h

        # Per pipeline, the most recently *completed* run: how long it took and
        # how long ago it finished (freshness). start_time/end_time are epoch
        # doubles, so these are timezone-safe regardless of the session TZ.
        last_duration = GaugeMetricFamily(
            "afm_dagster_job_last_duration_seconds",
            "Duration of each pipeline's most recently completed run, in seconds.",
            labels=["pipeline"],
        )
        last_age = GaugeMetricFamily(
            "afm_dagster_job_last_age_seconds",
            "Seconds since each pipeline's most recently completed run finished.",
            labels=["pipeline"],
        )
        for r in pool.fetchall(
            "SELECT DISTINCT ON (pipeline_name) "
            "coalesce(pipeline_name, 'unknown') AS pipeline, "
            "start_time, end_time, "
            "extract(epoch FROM now()) - end_time AS age "
            "FROM runs WHERE end_time IS NOT NULL AND end_time > 0 "
            "ORDER BY pipeline_name, end_time DESC"
        ):
            if r["start_time"] is not None:
                last_duration.add_metric(
                    [r["pipeline"]], float(r["end_time"]) - float(r["start_time"])
                )
            last_age.add_metric([r["pipeline"]], float(r["age"]))
        yield last_duration
        yield last_age


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
