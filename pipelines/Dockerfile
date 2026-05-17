# Aerial Fleet Monitor — pipelines image (Dagster user-code + webserver + daemon)
#
# A single image is shared by all three Dagster services in docker-compose:
#   - dagster-user-code   (CMD: dagster api grpc ...)
#   - dagster-webserver   (CMD: dagster-webserver ...)
#   - dagster-daemon      (CMD: dagster-daemon run)
#
# The compose file overrides `command:` per service so we don't need
# separate Dockerfiles.
#
# Build context is the REPO ROOT (not pipelines/). The Phase 03 Foundry
# sync assets `from afm_foundry_sync.sync_jobs import ...`, and that
# package is a sibling at foundry/sync/ — outside a pipelines/-only
# context. Root context lets this Dockerfile COPY + install both packages
# in one image, mirroring `make install`. compose passes
# `dockerfile: pipelines/Dockerfile` and a root `.dockerignore` keeps the
# context lean and keeps `_private/` + every `.env*` out of every layer
# (the Foundry token is injected at runtime via env_file, never baked).

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

# Sibling package. The Foundry sync assets import `afm_foundry_sync`; it
# is a separate src-layout package with its own pyproject, not on PyPI
# and deliberately not a PEP 508 dependency of afm-pipelines (relative
# paths aren't portably expressible in `dependencies` — see
# pipelines/pyproject.toml). Installed explicitly here, mirroring the
# `make install` step `pip install -e ../foundry/sync`.
COPY foundry/sync ./foundry-sync

# Flat-layout package: the package source files sit alongside pyproject.toml.
# Copy ALL source before `pip install .` so site-packages ends up with the
# full package — without this, `dagster api grpc -m pipelines.definitions`
# fails at container start because definitions.py isn't installed.
# Trade-off: any source change invalidates the dep-install cache layer.
COPY pipelines/pyproject.toml ./
COPY pipelines/__init__.py ./
COPY pipelines/definitions.py ./
COPY pipelines/assets ./assets
COPY pipelines/data ./data
COPY pipelines/resources ./resources

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install ./foundry-sync \
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
    && mkdir -p /opt/dagster/dagster_home /lake \
    && chown -R afm:afm /opt/dagster /lake

# Both packages (afm_foundry_sync + afm-pipelines) are installed into the
# venv in the builder stage; copying it forward is all the runtime needs.
COPY --from=builder /opt/venv /opt/venv

WORKDIR /srv/pipelines

# Copy the pipelines package source so `-m pipelines.definitions` resolves
# from the working dir as well as the installed venv (unchanged behavior;
# paths rebased onto the repo-root build context).
COPY --chown=afm:afm pipelines/__init__.py ./
COPY --chown=afm:afm pipelines/definitions.py ./
COPY --chown=afm:afm pipelines/assets ./assets
COPY --chown=afm:afm pipelines/data ./data
COPY --chown=afm:afm pipelines/resources ./resources

USER afm

# user-code: gRPC server (overridden per compose service)
EXPOSE 4000
# dagster-webserver: HTTP UI (overridden per compose service)
EXPOSE 3000

# Default command runs the user-code gRPC server. docker-compose.yml
# overrides this for the webserver and daemon services.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipelines.definitions"]
