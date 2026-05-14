#!/usr/bin/env python3
"""Download the OpenSky aircraft database snapshot into `data/aircraft.csv`.

OpenSky publishes the "latest" aircraft database at a stable URL that
302-redirects to the current S3-hosted CSV (~95 MB, ~600k rows).
Streams the response to a temp file to avoid loading the whole payload
into memory, validates the header, then atomic-renames into place.
Idempotent — re-running overwrites.

Usage::

    python scripts/download_aircraft.py
    python scripts/download_aircraft.py --out path/to/aircraft.csv
"""

import argparse
import sys
import tempfile
import urllib.request
from pathlib import Path

SOURCE_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
MIN_BYTES = 20_000_000  # actual is ~95 MB; anything under 20 MB is suspicious
REQUIRED_HEADER_TOKENS = ("icao24", "registration", "typecode", "operatoricao")
USER_AGENT = "aerial-fleet-monitor/seed"
CHUNK_BYTES = 64 * 1024
TIMEOUT_SECONDS = 120

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "data" / "aircraft.csv"


def fetch_to_tmp(url: str, destination_dir: Path) -> tuple[Path, int]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    destination_dir.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(
        dir=destination_dir, prefix=".aircraft-", suffix=".csv.tmp", delete=False
    )
    try:
        total = 0
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise RuntimeError(f"Unexpected HTTP status {response.status} from {url}")
            while True:
                chunk = response.read(CHUNK_BYTES)
                if not chunk:
                    break
                tmp.write(chunk)
                total += len(chunk)
        tmp.close()
        return Path(tmp.name), total
    except BaseException:
        tmp.close()
        Path(tmp.name).unlink(missing_ok=True)
        raise


def validate(tmp_path: Path, total_bytes: int) -> None:
    if total_bytes < MIN_BYTES:
        raise RuntimeError(
            f"Downloaded payload is only {total_bytes:,} bytes; "
            f"expected at least {MIN_BYTES:,}. Upstream may be serving an error page."
        )
    with tmp_path.open("rb") as fh:
        first_line = fh.readline().decode("utf-8", errors="replace")
    missing = [token for token in REQUIRED_HEADER_TOKENS if token not in first_line]
    if missing:
        raise RuntimeError(
            f"Header line missing expected tokens {missing}. Got: {first_line[:200]!r}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help=f"Output path (default: {DEFAULT_OUT})"
    )
    parser.add_argument("--url", default=SOURCE_URL, help=f"Source URL (default: {SOURCE_URL})")
    args = parser.parse_args(argv)

    print(f"Downloading {args.url} ...", flush=True)
    tmp_path, total = fetch_to_tmp(args.url, args.out.parent)
    print(f"  received {total:,} bytes", flush=True)

    try:
        validate(tmp_path, total)
        tmp_path.replace(args.out)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    print(f"Wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
