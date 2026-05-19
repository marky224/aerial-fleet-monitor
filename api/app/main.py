"""FastAPI application entrypoint.

Phase 02: wires up the four data routers, the auth stub, the request-ID
middleware, the global exception handlers (AFMException, Pydantic
validation, generic HTTPException), and the lifespan that owns the
Postgres pool + DuckDB lakehouse + WatchedAirportsProvider.

Per-request resources are checked out via the getters in
`app.dependencies`, which read off `app.state`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.responses import Response

from app.exceptions import AFMException
from app.logging import configure_logging, get_logger
from app.models.common import ErrorDetail, ErrorResponse
from app.routers import admin, auth_stub, flights, positions, sites
from app.services.lakehouse import LakehouseQuery
from app.services.postgres import PostgresPool, WatchedAirportsProvider
from app.services.salesforce import SalesforceService
from app.settings import settings


class HealthResponse(BaseModel):
    """Shape of /v1/health. Stable contract — front end and uptime checks read it."""

    status: str
    service: str
    version: str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Configure logging, create singletons, tear them down on shutdown.

    If Postgres is unreachable, `PostgresPool.__init__` raises
    UpstreamUnavailable and the app fails to start — preferable to
    silently failing every request later.
    """
    configure_logging()
    log = get_logger(__name__)
    log.info(
        "api.startup",
        service=settings.service_name,
        version=settings.service_version,
        environment=settings.environment,
    )

    app.state.postgres = PostgresPool(settings.database_url)
    log.info("pool.created")
    app.state.lakehouse = LakehouseQuery(settings.afm_lake_path)
    log.info("lakehouse.ready", path=settings.afm_lake_path)
    app.state.watched_airports = WatchedAirportsProvider(app.state.postgres)
    app.state.salesforce = SalesforceService(settings)
    log.info("salesforce.ready", configured=bool(settings.salesforce_client_id))

    yield

    app.state.postgres.close()
    log.info("pool.closed")
    log.info("api.shutdown")


app = FastAPI(
    title="Aerial Fleet Monitor API",
    version=settings.service_version,
    description="Real-time information console.",
    lifespan=lifespan,
    docs_url="/docs" if settings.expose_docs else None,
    redoc_url="/redoc" if settings.expose_docs else None,
    openapi_url="/openapi.json" if settings.expose_docs else None,
)


@app.middleware("http")
async def request_id_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Stamp every request with a unique req_<uuid4> id; echo on X-Request-Id."""
    request_id = f"req_{uuid4().hex}"
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


def _envelope(
    request: Request,
    status: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    """Build the standard API.md §1.3 error envelope as a JSONResponse."""
    request_id = getattr(request.state, "request_id", "unknown")
    body = ErrorResponse(
        error=ErrorDetail(
            code=code,
            message=message,
            request_id=request_id,
            details=details or {},
        )
    )
    return JSONResponse(status_code=status, content=body.model_dump())


@app.exception_handler(AFMException)
async def afm_exception_handler(request: Request, exc: AFMException) -> JSONResponse:
    """Map every AFMException subclass to its envelope."""
    return _envelope(request, exc.status_code, exc.code, exc.message, exc.details)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Reshape Pydantic 422 errors so clients see a single envelope shape.

    `details.errors` carries Pydantic's structured per-field error list,
    suitable for client-side form/query debugging.
    """
    return _envelope(
        request,
        422,
        "validation_failed",
        "Request validation failed",
        {"errors": exc.errors()},
    )


_HTTP_CODE_MAP: dict[int, str] = {404: "not_found", 405: "method_not_allowed"}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Reshape framework-issued HTTPExceptions into our envelope.

    Registered against Starlette's HTTPException (the parent class) so it
    also catches FastAPI's auto-raised 404/405 for unmatched routes —
    FastAPI's own HTTPException is a subclass and is caught as well.
    """
    code = _HTTP_CODE_MAP.get(exc.status_code, "http_error")
    return _envelope(request, exc.status_code, code, str(exc.detail))


app.include_router(auth_stub.router)
app.include_router(positions.router)
app.include_router(flights.router)
app.include_router(sites.router)

# Dev-only helpers (SF write smoke, acceptance #9). Mounted only in dev
# so the routes don't exist at all in any other environment.
if settings.environment == "dev":
    app.include_router(admin.router)


@app.get("/v1/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    """Liveness probe. Returns 200 as long as the process can serve requests."""
    return HealthResponse(
        status="ok",
        service=settings.service_name,
        version=settings.service_version,
    )
