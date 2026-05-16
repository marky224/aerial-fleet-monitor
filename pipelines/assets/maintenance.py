"""Maintenance assets — bound unbounded operational tables.

``prune_stale_positions`` deletes rows from ``app.current_positions``
whose ``last_seen_at`` is older than the retention window. That table is
an upsert-only last-known-position store (``icao24`` PRIMARY KEY,
``INSERT … ON CONFLICT DO UPDATE`` in the ingestion path, *no* eviction),
so without this it grows without bound — every aircraft ever observed
keeps a row forever. It also propagates downstream: the Foundry sync
mirrors whatever the API returns, so an unbounded source means an
unbounded Ontology.

Retention is set comfortably above the widest reader window so pruning
never starves a consumer:

  - ``/v1/positions/live``         — 15 min (``query_service.LIVE_POSITION_WINDOW``)
  - single flight detail          — 30 min
  - site inbound / outbound       — 60 min
  - flight trail                  — reads the Parquet lakehouse, not this table

The widest need is 60 min; ``POSITION_RETENTION`` = 3 h leaves a wide
margin while bounding the table to hours of distinct aircraft instead of
days. Idempotent and safe to run on any cadence (scheduled hourly in
``definitions.py``). See the Phase 03 build-doc decision log.

NOTE: deliberately no ``from __future__ import annotations`` — it breaks
Dagster's ``@asset`` context-parameter validation.
"""

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from pipelines.resources.postgres import PostgresResource

# Must stay >= every current_positions reader window (widest: site
# in/outbound = 60 min) AND >= the API live window (15 min). 3 h is the
# safety-margined retention; tune here if a wider reader is ever added.
POSITION_RETENTION = "3 hours"


@asset(
    group_name="maintenance",
    description="Deletes app.current_positions rows older than the retention window.",
    metadata={"target": "Postgres: app.current_positions", "cadence": "hourly"},
)
def prune_stale_positions(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Bound the eviction-free current_positions table. Returns rows deleted.

    The interval literal is a code constant (not user input), inlined the
    same way ``query_service`` inlines its window constants.
    """
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM app.current_positions "
                f"WHERE last_seen_at < NOW() - INTERVAL '{POSITION_RETENTION}'"
            )
            deleted = int(cur.rowcount)
        conn.commit()
    context.log.info("pruned %d stale current_positions rows", deleted)
    return MaterializeResult(
        metadata={
            "rows_deleted": MetadataValue.int(deleted),
            "retention": MetadataValue.text(POSITION_RETENTION),
        }
    )
