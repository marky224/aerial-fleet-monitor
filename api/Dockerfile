# Aerial Fleet Monitor — API image
#
# Multi-stage build:
#   builder  — installs deps into a venv from pyproject.toml
#   runtime  — slim image, non-root user, only the venv + app code
#
# Build context is api/ — only api/ files are copied in.

# ----------------------------------------------------------------------------
# Stage 1: builder
# ----------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build-time system deps. psycopg/duckdb/etc. are added in later phases —
# Phase 00 only needs the basics to build the wheel set.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only the metadata first so dep installs cache across code changes.
COPY pyproject.toml ./
COPY app/__init__.py app/__init__.py

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install .

# ----------------------------------------------------------------------------
# Stage 2: runtime
# ----------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Non-root user. UID/GID 10001 to avoid collisions with host users.
RUN groupadd --system --gid 10001 afm \
    && useradd --system --uid 10001 --gid afm --no-create-home afm

# Copy the prebuilt venv from the builder stage.
COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv/api

# Application code. Owned by the non-root user so a future read-write
# bind-mount of `app/` for hot reload doesn't fight permissions.
COPY --chown=afm:afm app/ ./app/

USER afm

EXPOSE 8000

# `--proxy-headers` so X-Forwarded-* from Cloudflare Tunnel is trusted for
# scheme/host reconstruction inside FastAPI.
CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
