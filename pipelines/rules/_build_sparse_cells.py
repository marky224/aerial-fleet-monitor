"""Regenerate ``data/lost_signal_sparse_cells.json`` from ``app.cases``.

A "sparse cell" is a 1deg x 1deg lat/lon square where ADS-B coverage is
normally gappy enough that the ``lost_signal`` rule's 8-minute gap floor
fires on routine feed jitter rather than on a real signal loss. The
rule demotes one severity tier in these cells (see
``pipelines/rules/lost_signal.py`` for the gradation logic).

Source: ``app.cases WHERE case_type='lost_signal'`` — every historical
fire is exactly the event we're trying to flag, so the rule's own
output is the right training signal. The earlier lakehouse-positions
approach (read v1 in git history) computed per-cell inter-poll p-gaps
and consistently returned 0 cells; the diagnosis was that in-cell polls
are normal-cadence while the aircraft is in range, so per-cell gaps
capture tracking-while-here, not coverage sparsity. The rule's fires
ARE the coverage-sparsity signal, so query them directly.

Counts ALL severities (not just ``high``): once the rule's gradation
demotes hot-cell fires to ``medium``, a severity-filtered query would
self-erase the list on the next refresh. Counting every ``lost_signal``
fire keeps the signal stable across refresh cycles.

Refresh cadence: monthly. Sparse geography is a property of where
ADS-B ground receivers physically sit, which doesn't move on human
timescales. Re-run when a new round of false-positive Cases shows up
in a region not in the current list.

Run::

    docker exec afm-dagster-user-code-1 \\
        python -m pipelines.rules._build_sparse_cells

Output path is computed relative to this file so the JSON lands in the
repo regardless of whether the script runs from the container's
site-packages copy or the host checkout.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import psycopg2

logger = logging.getLogger(__name__)

CELL_DEGREES = 1
MIN_FIRES = 8
MIN_DISTINCT_CALLSIGNS = 6

_QUERY = """
SELECT
  FLOOR((detection_facts->>'last_lat')::float)::int  AS lat,
  FLOOR((detection_facts->>'last_lon')::float)::int  AS lon
FROM app.cases
WHERE case_type = 'lost_signal'
  AND detection_facts ? 'last_lat'
  AND detection_facts ? 'last_lon'
GROUP BY lat, lon
HAVING COUNT(*) >= %(min_fires)s
   AND COUNT(DISTINCT detection_facts->>'callsign') >= %(min_callsigns)s
ORDER BY lat, lon
"""


def _output_path() -> Path:
    """Where to write the JSON.

    When the script runs from the container's site-packages copy, the
    sibling ``data/`` is inside site-packages too; the bind-mounted host
    copy is at ``$AFM_REPO_ROOT/pipelines/rules/data/`` instead. Honor
    ``AFM_REPO_ROOT`` when set so monthly refresh runs from the
    container can still update the tracked file.
    """
    repo_root = os.environ.get("AFM_REPO_ROOT")
    if repo_root:
        return Path(repo_root) / "pipelines" / "rules" / "data" / "lost_signal_sparse_cells.json"
    return Path(__file__).parent / "data" / "lost_signal_sparse_cells.json"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not set")

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                _QUERY,
                {"min_fires": MIN_FIRES, "min_callsigns": MIN_DISTINCT_CALLSIGNS},
            )
            cells = [[int(lat), int(lon)] for lat, lon in cur.fetchall()]
    finally:
        conn.close()

    output_path = _output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 2,
        "cell_degrees": CELL_DEGREES,
        "min_fires": MIN_FIRES,
        "min_distinct_callsigns": MIN_DISTINCT_CALLSIGNS,
        "cells": cells,
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n")
    logger.info("wrote %d cells to %s", len(cells), output_path)


if __name__ == "__main__":
    main()
