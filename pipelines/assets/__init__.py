"""Dagster assets for AFM pipelines."""

from pipelines.assets.ingestion import opensky_positions
from pipelines.assets.reference import static_reference

__all__ = ["opensky_positions", "static_reference"]
