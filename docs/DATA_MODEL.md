# Aerial Fleet Monitor — Data Model

The full data model documents AFM's three-tier storage architecture — Postgres for operational state, Parquet+DuckDB for analytical history, Salesforce for the system-of-record — including complete DDL, indexing strategy, migration approach, and seed-data plan.

## Topics covered in the full specification

- Storage tier overview (three stores, distinct roles, why the split)
- Postgres `app` schema (operational tables): cases, case_timeline, site_metrics, airport_conditions, current_positions, briefs, app_logs, subscribers, user_sessions, sync_watermarks — full DDL with indexes, sequence definitions, and the `app.next_case_id()` generator function
- Postgres `ref` schema (reference tables): airports, aircraft_registry, runbook_index — including GIN indexes for tag/case-type lookups
- Parquet lakehouse: Hive partitioning layout, per-row position schema, DuckDB query patterns with predicate pushdown, 12-month rolling retention
- Salesforce data model summary: standard object usage, custom objects (`AFM_Site__c`, `AFM_Flight__c`), the full custom Case field set, custom permissions
- Cross-system identifiers: how `case_id`, `salesforce_id`, `external_id`, and `subject` correlate across the three stores
- Migration strategy: Alembic for Postgres, SFDX source format for Salesforce, pyarrow schema enforcement for Parquet
- Seed data plan: airport CSV import, aircraft registry, watchlist tagging

The full data model is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the full DDL, all indexes, the case-ID generator, and the cross-system identifier map, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
