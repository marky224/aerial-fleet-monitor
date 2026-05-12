"""FastAPI application entrypoint.

Phase 00 scope: a single GET /v1/health endpoint that returns
{"status": "ok", "service": "afm-api", "version": "1.0.0"} — enough to
satisfy the docker-compose healthcheck and the Cloudflare tunnel smoke test.

Routers, services, auth, and the QueryService all land in Phase 02+.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from app.logging import configure_logging, get_logger
from app.settings import settings


class HealthResponse(BaseModel):
    """Shape of /v1/health. Stable contract — front end and uptime checks read it."""

    status: str
    service: str
    version: str


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Configure logging before the first request, tear down nothing for now."""
    configure_logging()
    log = get_logger(__name__)
    log.info(
        "api.startup",
        service=settings.service_name,
        version=settings.service_version,
        environment=settings.environment,
    )
    yield
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


@app.get("/v1/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    """Liveness probe. Returns 200 as long as the process can serve requests."""
    return HealthResponse(
        status="ok",
        service=settings.service_name,
        version=settings.service_version,
    )
