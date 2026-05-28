"""case_detector — the anomaly -> Case orchestrator (Phase 05).

Per the decoupled SF-write design (user 2026-05-21), this asset only
*detects* and writes *local* cases. It reads the last hour of positions
(lakehouse), enriches them with flight-plan + nearest-watched-site
columns, loads weather + open cases + airport coords from Postgres, runs
every rule, dedups, and inserts the survivors into ``app.cases``
(``sf_sync_status='pending'``) + ``app.case_timeline``.

The Salesforce write is done separately by the case-sync push path
(slice 7) reading ``pending`` rows — the detector never touches SF, so an
SF outage can't block detection.

``summary`` and ``severity_justification`` stay NULL: per the
no-Anthropic decision the Phase-07 Agentforce agent fills the summary
post-insert and ``sf_case_sync`` pulls it back.
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import cast

import numpy as np
import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from psycopg2.extras import Json

from pipelines.assets.ingestion import noaa_weather, opensky_positions
from pipelines.assets.reference import static_reference
from pipelines.resources import LakehouseResource, PostgresResource
from pipelines.rules import ALL_RULES, AirportConditions, Anomaly, deduplicate
from pipelines.rules.base import Rule
from pipelines.services.baseline_provider import (
    BaselineProvider,
    build_baseline_provider,
    load_airport_coords,
)

_EARTH_RADIUS_NM = 3440.065
# Detector input window: positions in the last LOOKBACK_MINUTES are fed to
# every rule per tick. 30 (down from 60) cuts excessive_hold + go_around
# volume ~43% by capping the duration window and the valley-shape window.
# Coupled with rule constants — LOST_MAX_GAP (30) and HOLD_MIN_DURATION (30)
# both equal LOOKBACK_MINUTES, so cases at the edge of the window still fire.
# Going below 30 would start clipping real cases (HOLD_MIN_DURATION couldn't
# be met). Simulation (2026-05-28): 60→30 drops total volume 1,860→1,067/3h
# without losing operational signal — see PR description for the table.
LOOKBACK_MINUTES = 30


@dataclass(frozen=True)
class DetectionResult:
    in_scope_aircraft: int
    anomalies_detected: int
    cases_created: int
    by_rule: dict[str, int] = field(default_factory=dict)


# -- pure helpers ---------------------------------------------------------


def _haversine_matrix(
    lat: np.ndarray,
    lon: np.ndarray,
    wlat: np.ndarray,
    wlon: np.ndarray,
) -> np.ndarray:
    """(N,) positions x (M,) airports -> (N, M) great-circle nm matrix."""
    lat_r = np.radians(lat)[:, None]
    lon_r = np.radians(lon)[:, None]
    wlat_r = np.radians(wlat)[None, :]
    wlon_r = np.radians(wlon)[None, :]
    d_phi = wlat_r - lat_r
    d_lambda = wlon_r - lon_r
    a = np.sin(d_phi / 2) ** 2 + np.cos(lat_r) * np.cos(wlat_r) * np.sin(d_lambda / 2) ** 2
    return cast("np.ndarray", _EARTH_RADIUS_NM * 2 * np.arcsin(np.sqrt(a)))


def _nearest_sites(
    lats: pd.Series,
    lons: pd.Series,
    watched_coords: dict[str, tuple[float, float]],
) -> tuple[list[str | None], list[float | None]]:
    """Nearest *watched* airport ICAO + nm distance for each position row."""
    n = len(lats)
    if not watched_coords or n == 0:
        return [None] * n, [None] * n
    icaos = list(watched_coords)
    wlat = np.array([watched_coords[i][0] for i in icaos], dtype=float)
    wlon = np.array([watched_coords[i][1] for i in icaos], dtype=float)
    lat_arr = lats.to_numpy(dtype=float)
    lon_arr = lons.to_numpy(dtype=float)

    dist = _haversine_matrix(lat_arr, lon_arr, wlat, wlon)  # (N, M)
    valid = ~np.isnan(lat_arr) & ~np.isnan(lon_arr)
    nearest_icao: list[str | None] = [None] * n
    nearest_dist: list[float | None] = [None] * n
    for row in np.flatnonzero(valid):
        j = int(np.argmin(dist[row]))
        nearest_icao[row] = icaos[j]
        nearest_dist[row] = float(dist[row, j])
    return nearest_icao, nearest_dist


def enrich_positions(
    positions: pd.DataFrame,
    flight_plans: pd.DataFrame,
    watched_coords: dict[str, tuple[float, float]],
) -> pd.DataFrame:
    """Add origin/destination/departure (flight_plans) + nearest-site columns."""
    plan_cols = ["origin_icao", "destination_icao", "departure_time"]
    # enrich owns these columns; drop any pre-existing copies so the merge
    # branch can't suffix-collide (keeps it symmetric with the empty branch).
    df = positions.drop(columns=plan_cols, errors="ignore")
    if flight_plans.empty:
        df["origin_icao"] = None
        df["destination_icao"] = None
        df["departure_time"] = pd.NaT
    else:
        df = df.merge(
            flight_plans[["icao24", *plan_cols]],
            on="icao24",
            how="left",
        )
    nearest_icao, nearest_dist = _nearest_sites(df["lat"], df["lon"], watched_coords)
    df["nearest_site_icao"] = nearest_icao
    df["nearest_site_distance_nm"] = nearest_dist
    return df


def detect_and_dedup(
    positions: pd.DataFrame,
    weather: dict[str, AirportConditions],
    existing_cases: pd.DataFrame,
    baseline: BaselineProvider,
    rules: list[Rule],
    now: pd.Timestamp,
) -> list[Anomaly]:
    """Run every rule, then suppress repeats within each rule's window."""
    paired: list[tuple[Anomaly, Rule]] = []
    for rule in rules:
        for anomaly in rule.detect(positions, weather, existing_cases, baseline):
            paired.append((anomaly, rule))
    return deduplicate(paired, existing_cases, now.to_pydatetime())


# -- Postgres I/O ---------------------------------------------------------


def _load_flight_plans(postgres: PostgresResource) -> pd.DataFrame:
    cols = ["icao24", "origin_icao", "destination_icao", "departure_time"]
    sql = f"SELECT {', '.join(cols)} FROM app.flight_plans WHERE fetch_status = 'success'"
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _load_weather(postgres: PostgresResource) -> dict[str, AirportConditions]:
    sql = (
        "SELECT site_icao, flight_category, wind_kt, visibility_sm, ceiling_ft "
        "FROM app.airport_conditions"
    )
    out: dict[str, AirportConditions] = {}
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        for site, cat, wind, vis, ceiling in cur.fetchall():
            out[site] = AirportConditions(
                site_icao=site,
                flight_category=cat,
                wind_kt=wind,
                visibility_sm=float(vis) if vis is not None else None,
                ceiling_ft=ceiling,
            )
    return out


def _load_open_cases(postgres: PostgresResource) -> pd.DataFrame:
    cols = ["case_type", "flight_id", "site_icao", "detection_facts", "created_at"]
    sql = f"SELECT {', '.join(cols)} FROM app.cases WHERE status NOT IN ('resolved', 'closed')"
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _load_watched_coords(postgres: PostgresResource) -> dict[str, tuple[float, float]]:
    sql = (
        "SELECT icao, lat, lon FROM ref.airports "
        "WHERE is_watched = TRUE AND lat IS NOT NULL AND lon IS NOT NULL"
    )
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return {row[0]: (float(row[1]), float(row[2])) for row in cur.fetchall()}


def _load_runbook_refs(postgres: PostgresResource, case_type: str) -> list[str]:
    sql = "SELECT runbook_id FROM ref.runbook_index WHERE %s = ANY(case_types)"
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (case_type,))
        return [row[0] for row in cur.fetchall()]


def _next_case_id(postgres: PostgresResource) -> str:
    with postgres.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT app.next_case_id()")
        return str(cur.fetchone()[0])


def _insert_case(
    postgres: PostgresResource,
    anomaly: Anomaly,
    case_id: str,
    runbook_refs: list[str],
) -> None:
    # weather_impact (and any site-level rule) has no aircraft; satisfy the
    # NOT-NULL flight_id with a self-describing site sentinel.
    flight_id = anomaly.icao24 or f"WX-{anomaly.site_icao or 'UNKN'}"
    site_icao = anomaly.site_icao or ""
    severity = anomaly.severity_hint or "low"
    insert_case = """
        INSERT INTO app.cases (
            case_id, flight_id, site_icao, customer_region, case_type,
            status, severity, detection_facts, runbook_refs, sf_sync_status
        )
        VALUES (%s, %s, %s, %s, %s, 'open', %s, %s, %s, 'pending')
    """
    insert_timeline = """
        INSERT INTO app.case_timeline (case_id, event_type, detail, source)
        VALUES (%s, 'created', %s, 'detector')
    """
    with postgres.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                insert_case,
                (
                    case_id,
                    flight_id,
                    site_icao,
                    anomaly.customer_region,
                    anomaly.rule,
                    severity,
                    Json(anomaly.detection_facts),
                    runbook_refs,
                ),
            )
            cur.execute(insert_timeline, (case_id, Json({"rule": anomaly.rule})))
        conn.commit()


def run_case_detection(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    lakehouse: LakehouseResource,
) -> DetectionResult:
    positions = lakehouse.read_recent_positions(LOOKBACK_MINUTES)
    if positions.empty:
        return DetectionResult(0, 0, 0, {})
    # Out-of-scope traffic (no customer_region) is filtered before rules.
    in_scope = positions[positions["customer_region"].notna()].copy()
    if in_scope.empty:
        return DetectionResult(0, 0, 0, {})

    flight_plans = _load_flight_plans(postgres)
    watched_coords = _load_watched_coords(postgres)
    enriched = enrich_positions(in_scope, flight_plans, watched_coords)

    weather = _load_weather(postgres)
    existing_cases = _load_open_cases(postgres)
    baseline = build_baseline_provider(load_airport_coords(postgres))
    rules = ALL_RULES
    now = enriched["ts_polled"].max()

    anomalies = detect_and_dedup(enriched, weather, existing_cases, baseline, rules, now)

    by_rule: Counter[str] = Counter()
    for anomaly in anomalies:
        case_id = _next_case_id(postgres)
        runbook_refs = _load_runbook_refs(postgres, anomaly.rule)
        _insert_case(postgres, anomaly, case_id, runbook_refs)
        by_rule[anomaly.rule] += 1

    return DetectionResult(
        in_scope_aircraft=int(in_scope["icao24"].nunique()),
        anomalies_detected=len(anomalies),
        cases_created=len(anomalies),
        by_rule=dict(by_rule),
    )


def _result_metadata(result: DetectionResult) -> dict[str, MetadataValue]:
    md: dict[str, MetadataValue] = {
        "in_scope_aircraft": MetadataValue.int(result.in_scope_aircraft),
        "anomalies_detected": MetadataValue.int(result.anomalies_detected),
        "cases_created": MetadataValue.int(result.cases_created),
    }
    for rule_name, count in result.by_rule.items():
        md[f"rule.{rule_name}"] = MetadataValue.int(count)
    return md


@asset(
    group_name="detection",
    deps=[opensky_positions, noaa_weather, static_reference],
    description=(
        "Runs the anomaly rule engine over the last hour of positions and "
        "inserts detected cases into app.cases (sf_sync_status='pending'). "
        "The SF write is done separately by the case-sync push path."
    ),
    metadata={"target": "app.cases", "cadence": "5min"},
)
def case_detector(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    lakehouse: LakehouseResource,
) -> MaterializeResult:
    result = run_case_detection(context, postgres, lakehouse)
    context.log.info(
        "case_detector: in_scope=%d anomalies=%d created=%d by_rule=%s",
        result.in_scope_aircraft,
        result.anomalies_detected,
        result.cases_created,
        result.by_rule,
    )
    return MaterializeResult(metadata=_result_metadata(result))
