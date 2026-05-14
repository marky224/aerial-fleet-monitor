"""initial schema

Creates the full Phase 01 schema in a single transaction: 10 tables in
`app`, 3 tables in `ref`, the case-ID sequence + generator function, and
all 21 indexes specified in docs/DATA_MODEL.md §2 + §3.

Schemas `app` and `ref` are created by infra/postgres/init.sql on first
container start — this migration assumes they exist.

Revision ID: 0001
Revises:
Create Date: 2026-05-12

"""

from __future__ import annotations

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # -----------------------------------------------------------------------
    # ref schema — static / slowly-changing reference data (§3)
    # -----------------------------------------------------------------------

    op.execute(
        """
        CREATE TABLE ref.airports (
            icao             TEXT PRIMARY KEY,
            iata             TEXT,
            name             TEXT NOT NULL,
            city             TEXT,
            state            TEXT,
            country          TEXT NOT NULL DEFAULT 'US',
            lat              NUMERIC(8,5) NOT NULL,
            lon              NUMERIC(9,5) NOT NULL,
            elevation_ft     INT,
            timezone         TEXT,
            is_watched       BOOLEAN NOT NULL DEFAULT FALSE,
            customer_regions TEXT[] NOT NULL DEFAULT '{}'
        )
        """
    )
    op.execute("CREATE INDEX idx_airports_state   ON ref.airports (state)")
    op.execute(
        "CREATE INDEX idx_airports_watched ON ref.airports (is_watched) "
        "WHERE is_watched = TRUE"
    )
    op.execute("CREATE INDEX idx_airports_iata    ON ref.airports (iata)")

    op.execute(
        """
        CREATE TABLE ref.aircraft_registry (
            icao24        TEXT PRIMARY KEY,
            registration  TEXT,
            type_code     TEXT,
            type_name     TEXT,
            operator      TEXT,
            operator_icao TEXT,
            country       TEXT
        )
        """
    )
    op.execute("CREATE INDEX idx_registry_op ON ref.aircraft_registry (operator_icao)")

    op.execute(
        """
        CREATE TABLE ref.runbook_index (
            runbook_id             TEXT PRIMARY KEY,
            title                  TEXT NOT NULL,
            case_types             TEXT[] NOT NULL,
            severity_floor         TEXT NOT NULL,
            tags                   TEXT[] NOT NULL DEFAULT '{}',
            salesforce_record_type TEXT,
            salesforce_template_id TEXT,
            salesforce_deeplink    TEXT,
            notion_page_id         TEXT,
            notion_url             TEXT,
            body_markdown          TEXT NOT NULL,
            last_synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            git_sha                TEXT NOT NULL
        )
        """
    )
    op.execute("CREATE INDEX idx_runbooks_case_types ON ref.runbook_index USING GIN (case_types)")

    # -----------------------------------------------------------------------
    # app schema — case-ID sequence + generator function (§2.1)
    # -----------------------------------------------------------------------

    op.execute("CREATE SEQUENCE app.case_id_seq START 1")
    op.execute(
        """
        CREATE FUNCTION app.next_case_id() RETURNS TEXT AS $$
            SELECT 'CASE-' || EXTRACT(YEAR FROM NOW())::text || '-' ||
                   LPAD(nextval('app.case_id_seq')::text, 6, '0');
        $$ LANGUAGE SQL
        """
    )

    # -----------------------------------------------------------------------
    # app schema — operational tables (§2)
    # -----------------------------------------------------------------------

    op.execute(
        """
        CREATE TABLE app.cases (
            case_id                TEXT PRIMARY KEY,
            salesforce_id          TEXT UNIQUE,
            flight_id              TEXT NOT NULL,
            site_icao              TEXT NOT NULL,
            customer_region        TEXT NOT NULL,
            case_type              TEXT NOT NULL,
            status                 TEXT NOT NULL DEFAULT 'open',
            severity               TEXT NOT NULL DEFAULT 'low',
            summary                TEXT,
            severity_justification TEXT,
            detection_facts        JSONB NOT NULL,
            runbook_refs           TEXT[] NOT NULL DEFAULT '{}',
            created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            resolved_at            TIMESTAMPTZ,
            sf_sync_status         TEXT NOT NULL DEFAULT 'pending',
            sf_sync_attempts       INT NOT NULL DEFAULT 0,
            sf_sync_last_error     TEXT
        )
        """
    )
    op.execute("CREATE INDEX idx_cases_status_severity ON app.cases (status, severity)")
    op.execute("CREATE INDEX idx_cases_site            ON app.cases (site_icao)")
    op.execute("CREATE INDEX idx_cases_flight          ON app.cases (flight_id)")
    op.execute("CREATE INDEX idx_cases_region_status   ON app.cases (customer_region, status)")
    op.execute("CREATE INDEX idx_cases_created_at      ON app.cases (created_at DESC)")
    op.execute(
        "CREATE INDEX idx_cases_sf_sync ON app.cases (sf_sync_status) "
        "WHERE sf_sync_status != 'synced'"
    )

    op.execute(
        """
        CREATE TABLE app.case_timeline (
            event_id    BIGSERIAL PRIMARY KEY,
            case_id     TEXT NOT NULL REFERENCES app.cases(case_id) ON DELETE CASCADE,
            event_type  TEXT NOT NULL,
            detail      JSONB NOT NULL DEFAULT '{}',
            source      TEXT NOT NULL,
            actor       TEXT,
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute("CREATE INDEX idx_timeline_case_time ON app.case_timeline (case_id, occurred_at)")

    op.execute(
        """
        CREATE TABLE app.site_metrics (
            site_icao               TEXT NOT NULL,
            period                  TEXT NOT NULL,
            inbound_count           INT  NOT NULL DEFAULT 0,
            outbound_count          INT  NOT NULL DEFAULT 0,
            on_time_arrival_pct     NUMERIC(5,2),
            on_time_departure_pct   NUMERIC(5,2),
            avg_arrival_delay_min   NUMERIC(6,2),
            avg_departure_delay_min NUMERIC(6,2),
            weather_impact          TEXT,
            flight_category         TEXT,
            active_cases            INT  NOT NULL DEFAULT 0,
            refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (site_icao, period)
        )
        """
    )
    op.execute("CREATE INDEX idx_site_metrics_refreshed ON app.site_metrics (refreshed_at DESC)")

    op.execute(
        """
        CREATE TABLE app.airport_conditions (
            site_icao         TEXT PRIMARY KEY,
            metar_raw         TEXT,
            metar_parsed      JSONB,
            taf_raw           TEXT,
            flight_category   TEXT,
            wind_kt           INT,
            wind_dir_deg      INT,
            visibility_sm     NUMERIC(4,1),
            ceiling_ft        INT,
            temperature_c     NUMERIC(4,1),
            altimeter_in_hg   NUMERIC(5,2),
            metar_observed_at TIMESTAMPTZ,
            fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    op.execute(
        """
        CREATE TABLE app.current_positions (
            icao24            TEXT PRIMARY KEY,
            callsign          TEXT,
            lat               NUMERIC(8,5),
            lon               NUMERIC(9,5),
            altitude_ft       INT,
            speed_kt          INT,
            heading_deg       INT,
            vertical_rate_fpm INT,
            on_ground         BOOLEAN,
            squawk            TEXT,
            origin_icao       TEXT,
            destination_icao  TEXT,
            aircraft_type     TEXT,
            customer_region   TEXT,
            last_seen_at      TIMESTAMPTZ NOT NULL,
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute("CREATE INDEX idx_positions_region    ON app.current_positions (customer_region)")
    op.execute(
        "CREATE INDEX idx_positions_dest ON app.current_positions (destination_icao) "
        "WHERE destination_icao IS NOT NULL"
    )
    op.execute("CREATE INDEX idx_positions_last_seen ON app.current_positions (last_seen_at DESC)")

    op.execute(
        """
        CREATE TABLE app.briefs (
            brief_id         BIGSERIAL PRIMARY KEY,
            region           TEXT NOT NULL,
            brief_date       DATE NOT NULL,
            timezone         TEXT NOT NULL,
            generated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            summary_md       TEXT NOT NULL,
            key_metrics      JSONB NOT NULL,
            notable_cases    TEXT[] NOT NULL DEFAULT '{}',
            chatter_post_id  TEXT,
            email_sent_count INT NOT NULL DEFAULT 0,
            UNIQUE (region, brief_date)
        )
        """
    )
    op.execute("CREATE INDEX idx_briefs_region_date ON app.briefs (region, brief_date DESC)")

    op.execute(
        """
        CREATE TABLE app.app_logs (
            log_id      BIGSERIAL PRIMARY KEY,
            level       TEXT NOT NULL,
            actor       TEXT,
            action      TEXT NOT NULL,
            target_type TEXT,
            target_id   TEXT,
            detail      JSONB NOT NULL DEFAULT '{}',
            occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_app_logs_target_time "
        "ON app.app_logs (target_type, target_id, occurred_at DESC)"
    )
    op.execute(
        "CREATE INDEX idx_app_logs_actor_time "
        "ON app.app_logs (actor, occurred_at DESC)"
    )

    op.execute(
        """
        CREATE TABLE app.subscribers (
            subscriber_id BIGSERIAL PRIMARY KEY,
            email         TEXT NOT NULL,
            region        TEXT NOT NULL,
            timezone      TEXT NOT NULL,
            enabled       BOOLEAN NOT NULL DEFAULT TRUE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (email, region)
        )
        """
    )

    op.execute(
        """
        CREATE TABLE app.user_sessions (
            session_id         TEXT PRIMARY KEY,
            user_handle        TEXT NOT NULL,
            salesforce_user_id TEXT,
            region             TEXT NOT NULL,
            custom_perms       TEXT[] NOT NULL DEFAULT '{}',
            issued_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at         TIMESTAMPTZ NOT NULL,
            last_activity_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute("CREATE INDEX idx_sessions_user   ON app.user_sessions (user_handle)")
    op.execute("CREATE INDEX idx_sessions_expiry ON app.user_sessions (expires_at)")

    op.execute(
        """
        CREATE TABLE app.sync_watermarks (
            sync_name    TEXT PRIMARY KEY,
            last_sync_at TIMESTAMPTZ NOT NULL,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def downgrade() -> None:
    # Reverse of upgrade(). Indexes drop with their tables; FK from
    # case_timeline → cases forces case_timeline first. CASCADE is
    # belt-and-suspenders against any future cross-table FKs.

    op.execute("DROP TABLE IF EXISTS app.sync_watermarks CASCADE")
    op.execute("DROP TABLE IF EXISTS app.user_sessions CASCADE")
    op.execute("DROP TABLE IF EXISTS app.subscribers CASCADE")
    op.execute("DROP TABLE IF EXISTS app.app_logs CASCADE")
    op.execute("DROP TABLE IF EXISTS app.briefs CASCADE")
    op.execute("DROP TABLE IF EXISTS app.current_positions CASCADE")
    op.execute("DROP TABLE IF EXISTS app.airport_conditions CASCADE")
    op.execute("DROP TABLE IF EXISTS app.site_metrics CASCADE")
    op.execute("DROP TABLE IF EXISTS app.case_timeline CASCADE")
    op.execute("DROP TABLE IF EXISTS app.cases CASCADE")

    op.execute("DROP FUNCTION IF EXISTS app.next_case_id()")
    op.execute("DROP SEQUENCE IF EXISTS app.case_id_seq")

    op.execute("DROP TABLE IF EXISTS ref.runbook_index CASCADE")
    op.execute("DROP TABLE IF EXISTS ref.aircraft_registry CASCADE")
    op.execute("DROP TABLE IF EXISTS ref.airports CASCADE")
