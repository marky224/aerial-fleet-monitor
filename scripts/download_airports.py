#!/usr/bin/env python3
"""Download the OurAirports `airports.csv` snapshot into `data/airports.csv`.

Mirror: David Megginson's GitHub Pages site, which OurAirports themselves
link to as the public download target. No upstream checksum is published,
so the script validates by file size and header sanity before committing
the bytes to the final path. Idempotent — re-running overwrites.

Usage::

    python scripts/download_airports.py
    python scripts/download_airports.py --out path/to/airports.csv
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import urllib.request
from pathlib import Path

SOURCE_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
MIN_BYTES = 1_000_000  # raw CSV is ~3.5 MB; anything under 1 MB is suspicious
REQUIRED_HEADER_TOKENS = ("ident", "iso_country", "latitude_deg", "longitude_deg")
USER_AGENT = "aerial-fleet-monitor/seed"

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "data" / "airports.csv"


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Unexpected HTTP status {response.status} from {url}")
        return response.read()


def validate(payload: bytes) -> None:
    if len(payload) < MIN_BYTES:
        raise RuntimeError(
            f"Downloaded payload is only {len(payload):,} bytes; "
            f"expected at least {MIN_BYTES:,}. Upstream may be serving an error page."
        )
    first_line = payload.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    missing = [token for token in REQUIRED_HEADER_TOKENS if token not in first_line]
    if missing:
        raise RuntimeError(
            f"Header line missing expected tokens {missing}. Got: {first_line[:200]!r}"
        )


def write_atomically(payload: bytes, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=destination.parent, prefix=".airports-", suffix=".csv.tmp", delete=False
    ) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)
    tmp_path.replace(destination)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help=f"Output path (default: {DEFAULT_OUT})"
    )
    parser.add_argument("--url", default=SOURCE_URL, help=f"Source URL (default: {SOURCE_URL})")
    args = parser.parse_args(argv)

    print(f"Downloading {args.url} ...", flush=True)
    payload = fetch(args.url)
    print(f"  received {len(payload):,} bytes", flush=True)

    validate(payload)
    write_atomically(payload, args.out)
    print(f"Wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
