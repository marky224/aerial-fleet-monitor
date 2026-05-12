# Aerial Fleet Monitor — pipelines image (Dagster user-code + webserver + daemon)
#
# A single image is shared by all three Dagster services in docker-compose:
#   - dagster-user-code   (CMD: dagster api grpc ...)
#   - dagster-webserver   (CMD: dagster-webserver ...)
#   - dagster-daemon      (CMD: dagster-daemon run)
#
# The compose file overrides `command:` per service so we don't need
# separate Dockerfiles. Build context is pipelines/.

# ----------------------------------------------------------------------------
# Stage 1: builder
# ----------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Flat-layout package: the package source files sit alongside pyproject.toml.
# Copy ALL source before `pip install .` so site-packages ends up with the
# full package — without this, `dagster api grpc -m pipelines.definitions`
# fails at container start because definitions.py isn't installed.
# Phase 01+ will add COPYs for the assets/, jobs/, resources/ subpackages.
# Trade-off: any source change invalidates the dep-install cache layer.
COPY pyproject.toml ./
COPY __init__.py ./
COPY definitions.py ./

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install .

# ----------------------------------------------------------------------------
# Stage 2: runtime
# ----------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/dagster_home

# Non-root user. UID/GID match the API image's pattern for consistency.
RUN groupadd --system --gid 10001 afm \
    && useradd --system --uid 10001 --gid afm --no-create-home afm \
    && mkdir -p /opt/dagster/dagster_home \
    && chown -R afm:afm /opt/dagster

COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv/pipelines

# Copy the package source. The flat layout means everything in pipelines/
# (except files explicitly excluded by .dockerignore) lands here.
COPY --chown=afm:afm __init__.py ./
COPY --chown=afm:afm definitions.py ./

# Phase 01+ will add: assets/, jobs/, resources/, tests/

USER afm

# user-code: gRPC server (overridden per compose service)
EXPOSE 4000
# dagster-webserver: HTTP UI (overridden per compose service)
EXPOSE 3000

# Default command runs the user-code gRPC server. docker-compose.yml
# overrides this for the webserver and daemon services.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipelines.definitions"]
