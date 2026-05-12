-- Aerial Fleet Monitor — Postgres bootstrap
--
-- Runs once, on first creation of the `postgres_data` volume, via the
-- Postgres image's docker-entrypoint mechanism (mounted at
-- /docker-entrypoint-initdb.d/01-init.sql by docker-compose.yml).
--
-- This file only creates the two schemas AFM uses. All tables, indexes,
-- and functions are managed by Alembic migrations in api/alembic/ from
-- Phase 01 onward — do not add table DDL here.
--
-- The default user (POSTGRES_USER, set in .env) owns the database and
-- both schemas. Alembic connects as this user and creates tables under
-- the appropriate schema via the search_path set by each migration.

-- Operational state: cases, timeline, site_metrics, briefs, audit logs,
-- user_sessions. Read by the API hot path; written by Dagster + the API.
CREATE SCHEMA IF NOT EXISTS app;

-- Reference data: airports, aircraft registry, runbook index. Loaded from
-- static sources (OurAirports CSV, GitHub) and refreshed infrequently.
CREATE SCHEMA IF NOT EXISTS ref;

-- Default search path so unqualified table names hit `app` first, then `ref`.
-- Migrations should still qualify tables explicitly; this is a convenience
-- for ad-hoc psql sessions via `make db-shell`.
DO $$
BEGIN
    EXECUTE format(
        'ALTER DATABASE %I SET search_path TO app, ref, public',
        current_database()
    );
END
$$;
