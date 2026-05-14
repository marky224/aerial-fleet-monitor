"""Dagster assets for AFM pipelines."""

from pipelines.assets.ingestion import noaa_weather, opensky_positions
from pipelines.assets.reference import static_reference

__all__ = ["noaa_weather", "opensky_positions", "static_reference"]
