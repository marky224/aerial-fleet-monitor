"""FastAPI dependency providers for request-scoped wiring.

Heavy resources (Postgres pool, DuckDB wrapper, WatchedAirportsProvider)
are created once in app.main's lifespan and stashed on app.state. The
getters here read them off app.state per-request. Cheap composites
(QueryService, Scope) are constructed fresh per request.

Real OAuth-derived auth lands in Phase 04. For Phase 02, get_scope
returns a hardcoded internal-ops Scope populated with the live
watched-airports list — matches the /v1/auth/me stub.
"""

from __future__ import annotations

from typing import Annotated, cast

from fastapi import Depends, Request

from app.models.common import Scope
from app.services.lakehouse import LakehouseQuery
from app.services.postgres import PostgresPool, WatchedAirportsProvider
from app.services.query_service import QueryService


def get_postgres_pool(request: Request) -> PostgresPool:
    """The app-wide Postgres pool, created in lifespan startup."""
    return cast(PostgresPool, request.app.state.postgres)


def get_lakehouse(request: Request) -> LakehouseQuery:
    """The app-wide DuckDB lakehouse wrapper, created in lifespan startup."""
    return cast(LakehouseQuery, request.app.state.lakehouse)


def get_watched_airports(request: Request) -> WatchedAirportsProvider:
    """The app-wide WatchedAirportsProvider, created in lifespan startup."""
    return cast(WatchedAirportsProvider, request.app.state.watched_airports)


def get_query_service(
    postgres: Annotated[PostgresPool, Depends(get_postgres_pool)],
    lakehouse: Annotated[LakehouseQuery, Depends(get_lakehouse)],
) -> QueryService:
    """Fresh QueryService per request — composes pool + lakehouse references."""
    return QueryService(postgres=postgres, lakehouse=lakehouse)


def get_scope(
    watched_airports: Annotated[WatchedAirportsProvider, Depends(get_watched_airports)],
) -> Scope:
    """Phase 02 auth stub: hardcoded internal-ops scope.

    Phase 04 replaces this getter with JWT-derived scope from the
    afm_session cookie. Until then every caller gets full read access;
    sites_in_scope is populated from the live watched-airports list so
    the /v1/auth/me response always reflects current ref.airports state.
    """
    return Scope(
        user_handle="internal-ops",
        region="all",
        custom_perms=[],
        sites_in_scope=watched_airports.get(),
    )
