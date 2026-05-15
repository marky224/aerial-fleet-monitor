"""Dagster assets for AFM pipelines."""

from pipelines.assets.foundry_sync import (
    foundry_positions_sync,
    foundry_sites_sync,
)
from pipelines.assets.ingestion import noaa_weather, opensky_positions
from pipelines.assets.reference import static_reference

__all__ = [
    "foundry_positions_sync",
    "foundry_sites_sync",
    "noaa_weather",
    "opensky_positions",
    "static_reference",
]
