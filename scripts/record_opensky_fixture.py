#!/usr/bin/env python3
"""Capture a real OpenSky ``/states/all`` snapshot for the detector e2e fixtures.

One-off developer tool (Phase 10). Hits OpenSky once (~4 credits at the CONUS
bbox) and writes the raw response JSON — ``{"time": <epoch>, "states": [[...]]}``,
exactly the shape ``OpenSkyResource`` parses — so ``pipelines/tests/
test_case_detector_e2e.py`` can replay it through the real ingestion +
detection path offline, with no network and no credentials.

The committed scenario fixtures under ``pipelines/tests/fixtures/opensky/`` are
built FROM a capture like this: icao24s are anonymised to a synthetic hex range
and the temporal anomaly scenarios (lost_signal, ...) are crafted as multi-tick
sequences — those can't be observed from a single live snapshot on demand.

Credentials come from ``OPENSKY_CLIENT_ID`` / ``OPENSKY_CLIENT_SECRET`` (the
same vars the stack's ``.env`` carries). Run from the pipelines venv::

    set -a && . ./.env && set +a
    python scripts/record_opensky_fixture.py --output /tmp/opensky_raw.json
    python scripts/record_opensky_fixture.py --output /tmp/opensky_raw.json --max-states 500
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import requests

from pipelines.resources.opensky import (
    DEFAULT_TIMEOUT,
    US_BBOX_LAMAX,
    US_BBOX_LAMIN,
    US_BBOX_LOMAX,
    US_BBOX_LOMIN,
    USER_AGENT,
    OpenSkyResource,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--output", required=True, help="Path to write the raw JSON snapshot."
    )
    parser.add_argument(
        "--max-states",
        type=int,
        default=None,
        help="Keep only the first N state rows (caps fixture size; default: all).",
    )
    args = parser.parse_args()

    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        print(
            "OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET must be set "
            "(source the stack's .env first).",
            file=sys.stderr,
        )
        return 2

    # Reuse the resource purely for its OAuth2 token handling; the bbox GET
    # below mirrors fetch_states() but keeps the *raw* payload (the resource
    # would otherwise hand back parsed OpenSkyState objects).
    resource = OpenSkyResource(client_id=client_id, client_secret=client_secret)
    token = resource._get_token()
    response = requests.get(
        f"{resource.base_url}/states/all",
        params={
            "lamin": US_BBOX_LAMIN,
            "lomin": US_BBOX_LOMIN,
            "lamax": US_BBOX_LAMAX,
            "lomax": US_BBOX_LOMAX,
        },
        headers={"User-Agent": USER_AGENT, "Authorization": f"Bearer {token}"},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()

    states = payload.get("states") or []
    if args.max_states is not None:
        states = states[: args.max_states]

    with open(args.output, "w") as fh:
        json.dump({"time": payload.get("time"), "states": states}, fh)
    print(
        f"wrote {len(states)} states to {args.output} (api_time={payload.get('time')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
