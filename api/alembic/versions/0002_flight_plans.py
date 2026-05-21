"""flight_plans cache (Phase 05 — flight-plan enrichment)

Creates ``app.flight_plans`` — a per-icao24 cache of the most recent
flight's origin/destination, populated by the
``flight_plan_enrichment`` Dagster asset (Phase 05).

OpenSky's ``/states/all`` (used by the ingestion asset) does not carry
flight-plan data; pulling per-aircraft history from
``/flights/aircraft`` is rate-budget-bounded at the free tier. This
cache table lets the detector read origin/destination locally and lets
the enrichment asset skip fetches for icao24s with a fresh row
(``refreshed_at >= NOW() - INTERVAL '12h'``).

One row per icao24 keeps the schema simple and matches the rules'
needs: ``diversion`` and ``delay`` only care about the aircraft's
*current* flight plan, not history. When a new flight begins for the
same icao24, the row is updated in place.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-21

"""

from __future__ import annotations

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE app.flight_plans (
            icao24           TEXT PRIMARY KEY,
            origin_icao      TEXT,
            destination_icao TEXT,
            callsign         TEXT,
            departure_time   TIMESTAMPTZ,
            arrival_time     TIMESTAMPTZ,
            refreshed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            fetch_status     TEXT NOT NULL DEFAULT 'success'
        )
        """
    )
    # Partial index on stale rows — the enrichment asset scans
    # `WHERE refreshed_at < NOW() - INTERVAL '12h'` once per cycle and a
    # functional index over (refreshed_at) is the natural query plan.
    op.execute(
        "CREATE INDEX idx_flight_plans_refreshed ON app.flight_plans (refreshed_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.flight_plans CASCADE")
