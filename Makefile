# Aerial Fleet Monitor — Makefile
#
# `make help` prints every target from day one, marked [Phase NN] for ones
# whose real implementation lands later. Stubs print a phase pointer and
# exit non-zero so CI fails loud if a future-phase target is invoked early.
#
# Phase 00 real implementations: install, dev, down, logs, lint,
# db-migrate, db-shell, help. test-unit runs the API pytest suite as
# of Phase 02.

.DEFAULT_GOAL := help

# Detect docker compose v2 vs legacy. Most modern installs ship v2.
DOCKER_COMPOSE ?= docker compose

# Python interpreter used to create per-package venvs. Override if your
# system exposes 3.12 under a different name (e.g. `python`, `python3`,
# a pyenv shim). The venvs themselves still pin to whatever 3.12 this
# resolves to at install time.
PYTHON ?= python3.12

# Python venvs per package.
PY_API := api/.venv
PY_PIPELINES := pipelines/.venv

# Salesforce DX. SF_ORG is the org alias created by `sf org login`
# (Phase 04 — see docs/build/04_salesforce_setup.md). Override per env.
SF_ORG ?= afm-dev
SF_DIR := salesforce

# Base URL the contract suite (schemathesis) fires GET requests at. Defaults
# to the local stack; override to point at another running API.
AFM_CONTRACT_BASE_URL ?= http://localhost:8000

# ----------------------------------------------------------------------------
# Help
# ----------------------------------------------------------------------------

.PHONY: help
help:
	@echo "Aerial Fleet Monitor — make targets"
	@echo ""
	@echo "Setup:"
	@echo "  install                Install Python (pip) deps for all packages"
	@echo "  sf-auth                Authenticate to Salesforce DE org           [Phase 04]"
	@echo ""
	@echo "Development:"
	@echo "  dev                    docker compose up -d (dashboard lives in Foundry)"
	@echo "  down                   docker compose down"
	@echo "  logs                   Tail logs from the docker-compose stack"
	@echo "  api-shell              ipython with FastAPI app context loaded     [Phase 02]"
	@echo "  sf-deploy              Deploy Salesforce metadata                   [Phase 04]"
	@echo "  sf-validate            Validate metadata + Apex tests (no deploy)   [Phase 04]"
	@echo "  sf-seed-runbooks       Seed AFM_Runbook__mdt records + runbook Files [Phase 07]"
	@echo "  sf-publish-agent       Provision agent user + publish + activate     [Phase 07]"
	@echo "  sf-agent-up            Full agent stack: deploy + seed + publish     [Phase 07]"
	@echo ""
	@echo "Testing:"
	@echo "  test                   Full suite (unit + contract + integration)    [Phase 10]"
	@echo "  test-unit              Fast unit tests"
	@echo "  test-integration       Tests against live SF dev org                [Phase 04]"
	@echo "  test-e2e               (deprecated; no local frontend)              [n/a]"
	@echo "  test-contract          API contract tests (schemathesis)            [Phase 10]"
	@echo "  sf-test                Apex unit tests in DE org                    [Phase 04]"
	@echo "  lint                   ruff + mypy"
	@echo "  lint-runbooks          Validate runbook frontmatter + cross-links   [Phase 08]"
	@echo ""
	@echo "Database:"
	@echo "  db-migrate             alembic upgrade head"
	@echo "  db-seed                Seed reference data (run download_airports.py first)"
	@echo "  db-shell               psql into the running postgres container"
	@echo ""
	@echo "Salesforce + Ops:"
	@echo "  afm-issue-service-token  Mint a service JWT for the SF→AFM Named Cred [Phase 06]"
	@echo "  loom-record            Open the demo walkthrough script in \$$EDITOR  [Phase 11]"

# ----------------------------------------------------------------------------
# Real implementations (Phase 00)
# ----------------------------------------------------------------------------

.PHONY: install
install:
	@echo "→ Installing API Python deps (using $(PYTHON))"
	cd api && $(PYTHON) -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
	@echo "→ Installing pipelines Python deps (using $(PYTHON))"
	cd pipelines && $(PYTHON) -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
	@echo "→ Installing afm_foundry_sync into the pipelines venv (Phase 03 Foundry sync assets import it)"
	cd pipelines && . .venv/bin/activate && pip install -e ../foundry/sync
	@echo "→ Installing foundry/sync own venv with [dev] (its tests use respx; exercised by make test-unit)"
	cd foundry/sync && $(PYTHON) -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'

.PHONY: dev
dev:
	$(DOCKER_COMPOSE) up -d
	@echo "→ Backend up. Dashboard lives in Foundry — see _private/docs/full/FRONTEND.md"

.PHONY: down
down:
	$(DOCKER_COMPOSE) down

.PHONY: logs
logs:
	$(DOCKER_COMPOSE) logs -f

.PHONY: lint
lint:
	@echo "→ ruff (api)"
	cd api && . .venv/bin/activate && ruff check . && ruff format --check .
	@echo "→ mypy (api)"
	cd api && . .venv/bin/activate && mypy app
	@echo "→ ruff (pipelines)"
	cd pipelines && . .venv/bin/activate && ruff check . && ruff format --check .
	@echo "→ mypy (pipelines)"
	cd pipelines && . .venv/bin/activate && mypy .

# Each package enforces its own coverage floor (--cov-fail-under). Thresholds:
# api 75 / pipelines 70 / foundry 75 (Phase 10 decision; no Codecov, no badge —
# the gate lives here so CI fails loud on a regression). Source/omit live in
# each package's [tool.coverage.run]. Plain `cd <pkg> && pytest` stays fast
# (no coverage) for local iteration; the gate runs here + in CI.
.PHONY: test-unit
test-unit:
	@echo "→ test-unit (api): integration + contract excluded"
	cd api && . .venv/bin/activate && pytest -m "not integration and not contract" \
		--cov=app --cov-report=term-missing --cov-fail-under=75
	@echo "→ test-unit (pipelines)"
	cd pipelines && . .venv/bin/activate && pytest \
		--cov=pipelines --cov-report=term-missing --cov-fail-under=70
	@echo "→ test-unit (foundry/sync)"
	cd foundry/sync && . .venv/bin/activate && pytest \
		--cov=afm_foundry_sync --cov-report=term-missing --cov-fail-under=75

.PHONY: db-migrate
db-migrate:
	set -a; . ./.env; set +a; \
	export DATABASE_URL="$$(echo "$$DATABASE_URL" | sed 's|@postgres:|@127.0.0.1:|')"; \
	cd api && . .venv/bin/activate && alembic upgrade head

.PHONY: db-seed
db-seed:
	@if [ ! -f data/airports.csv ]; then \
		echo "data/airports.csv missing — run: python scripts/download_airports.py"; \
		exit 1; \
	fi
	@if [ ! -f data/aircraft.csv ]; then \
		echo "data/aircraft.csv missing — run: python scripts/download_aircraft.py"; \
		exit 1; \
	fi
	set -a; . ./.env; set +a; \
	export DATABASE_URL="$$(echo "$$DATABASE_URL" | sed 's|@postgres:|@127.0.0.1:|')"; \
	export AFM_AIRPORTS_CSV="$$(pwd)/data/airports.csv"; \
	export AFM_AIRCRAFT_CSV="$$(pwd)/data/aircraft.csv"; \
	export DAGSTER_HOME="$$(mktemp -d -t afm-db-seed-XXXXXX)"; \
	venv="$$(pwd)/pipelines/.venv"; \
	. "$$venv/bin/activate" && cd "$$DAGSTER_HOME" && \
	dagster asset materialize -m pipelines.definitions --select static_reference

.PHONY: db-shell
db-shell:
	$(DOCKER_COMPOSE) exec postgres sh -lc 'psql -U "$${POSTGRES_USER:-afm}" "$${POSTGRES_DB:-afm}"'

# ----------------------------------------------------------------------------
# Stubs — real implementation lands in the noted phase.
# Each prints a pointer and exits 1 so accidental early use is loud.
# ----------------------------------------------------------------------------

.PHONY: sf-auth
sf-auth:
	cd $(SF_DIR) && sf org login web --alias $(SF_ORG) --set-default \
		--instance-url https://login.salesforce.com

.PHONY: sf-deploy
sf-deploy:
	cd $(SF_DIR) && sf project deploy start --target-org $(SF_ORG) \
		--source-dir force-app --wait 30

.PHONY: sf-validate
sf-validate:
	cd $(SF_DIR) && sf project deploy start --dry-run --test-level RunLocalTests --target-org $(SF_ORG) \
		--source-dir force-app --wait 30

.PHONY: sf-test
sf-test:
	cd $(SF_DIR) && sf apex run test --target-org $(SF_ORG) \
		--code-coverage --result-format human --wait 30

# Seed the runbook data (CMDT records + reference Files). Run AFTER sf-deploy
# (which deploys the AFM_Runbook__mdt type+fields). Records go via the Apex
# Metadata API, not `sf project deploy` — afm-dev hits a known Salesforce GACK
# on CustomMetadata record deploys (see salesforce/.forceignore for the why).
.PHONY: sf-seed-runbooks
sf-seed-runbooks:
	./scripts/seed_runbook_cmdt.sh $(SF_ORG)
	./scripts/seed_runbook_files.sh $(SF_ORG)

# Provision the Agentforce agent run-as user (idempotent — query-first since
# `sf org create agent-user` is not), then publish + activate the agent. Run AFTER
# sf-deploy (the AiAuthoringBundle + AFM_Triage_Automation permset must be in the org).
.PHONY: sf-publish-agent
sf-publish-agent:
	./scripts/publish_agent.sh $(SF_ORG)

# One-shot reproduce of the whole Agentforce stack on a target org: deploy metadata,
# seed the runbook CMDT records + Files, then provision the agent user + publish +
# activate. Steps are ordered — run sequentially (don't `make -j` this).
.PHONY: sf-agent-up
sf-agent-up: sf-deploy sf-seed-runbooks sf-publish-agent
	@echo "✔ Full Agentforce stack deployed + published to $(SF_ORG)."

.PHONY: api-shell
api-shell:
	@echo "Target 'api-shell' available after Phase 02 — see docs/build/02_api_basic.md"
	@exit 1

.PHONY: test-contract
test-contract:
	@echo "→ test-contract: schemathesis vs the running API at $(AFM_CONTRACT_BASE_URL) (needs the stack up — make dev)"
	cd api && . .venv/bin/activate && AFM_CONTRACT_BASE_URL="$(AFM_CONTRACT_BASE_URL)" pytest -m contract

.PHONY: test-integration
test-integration:
	@echo "→ test-integration: running SF integration tests against afm-dev (auto-skipped if SF env vars missing)"
	cd api && . .venv/bin/activate && pytest -m integration

.PHONY: test-e2e
test-e2e:
	@echo "Target 'test-e2e' is deprecated — no local frontend after the Foundry pivot (2026-05-14). Workshop apps tested in Foundry's own framework."
	@exit 1

.PHONY: test
test: test-unit test-contract test-integration
	@echo "✔ Full test pyramid: unit + contract + integration (integration auto-skips without SF env)."

.PHONY: afm-issue-service-token
afm-issue-service-token:
	@echo "Target 'afm-issue-service-token' available after Phase 06 — see docs/build/06_lwc.md"
	@exit 1

.PHONY: lint-runbooks
lint-runbooks:
	@echo "Target 'lint-runbooks' available after Phase 08 — see docs/build/08_runbooks_notion.md"
	@exit 1

.PHONY: loom-record
loom-record:
	@echo "Target 'loom-record' available after Phase 11 — see docs/build/11_polish_demo.md"
	@exit 1
