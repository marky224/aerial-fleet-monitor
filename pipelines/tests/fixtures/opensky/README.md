# OpenSky detector e2e fixtures

Recorded OpenSky `/states/all` snapshots that `test_case_detector_e2e.py` replays
through the real pipeline (OpenSky parse → ingestion transform → Parquet
lakehouse → `run_case_detection`).

## Provenance

A real contiguous-US snapshot was captured with
`scripts/record_opensky_fixture.py` (one authenticated `/states/all` call), then
each fixture was derived from it:

- **icao24s and callsigns are anonymised** to a synthetic `e…` / `AFMT…` range.
  Positions (lat/lon/altitude/etc.) are the real recorded values — public ADS-B
  data, no operator identity retained.
- Volume scenarios keep the real states filtered around the two watched airports
  the e2e tags against (KLAX → `west`, KJFK → `east`) plus a sample of
  out-of-scope traffic.
- **Anomaly scenarios are crafted**, because a single live snapshot cannot
  exhibit a temporal anomaly on demand:
  - `lost_signal_scenario` is a multi-tick sequence where one aircraft
    (`e9ff01`, 28 000 ft, level cruise near KLAX) is seen 16 min ago and then
    goes dark while the others keep transmitting.
  - `weather_event` carries a `seed_weather` block the test feeds to the
    detector's weather seam (the rule is site-level and position-independent).

## Format

```jsonc
{
  "scenario": "lost_signal",
  "description": "...",
  "seed_weather": { "site_icao": "KLAX", "flight_category": "LIFR", ... },  // weather_event only
  "ticks": [
    { "offset_minutes": -16.0, "states": [ [ <17-column OpenSky state vector> ], ... ] },
    { "offset_minutes":   0.0, "states": [ ... ] }
  ]
}
```

`offset_minutes` is relative to "now" at replay time (the latest tick is `0.0`);
the state rows are the raw OpenSky `/states/all` array shape that
`OpenSkyResource._row_to_state` parses.

## Refreshing

```bash
set -a && . ./.env && set +a            # OPENSKY_CLIENT_ID / _SECRET
python scripts/record_opensky_fixture.py --output /tmp/opensky_raw.json --max-states 800
```

Then re-derive the scenarios from the new capture (filter near the watched
airports, anonymise, re-craft the anomaly ticks).
