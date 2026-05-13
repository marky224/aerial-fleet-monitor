"""Dagster resources for AFM pipelines."""

from pipelines.resources.lakehouse import LakehouseResource
from pipelines.resources.opensky import OpenSkyResource
from pipelines.resources.postgres import PostgresResource
from pipelines.resources.watchlist import WatchlistResource

__all__ = [
    "LakehouseResource",
    "OpenSkyResource",
    "PostgresResource",
    "WatchlistResource",
]
