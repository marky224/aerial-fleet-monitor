# Aerial Fleet Monitor — Makefile
#
# `make help` prints every target from day one, marked [Phase NN] for ones
# whose real implementation lands later. Stubs print a phase pointer and
# exit non-zero so CI fails loud if a future-phase target is invoked early.
#
# Phase 00 real implementations: install, dev, down, logs, lint,
# db-migrate, db-shell, help, test-unit (no-op pass).

.DEFAULT_GOAL := help

# Detect docker compose v2 vs legacy. Most modern installs ship v2.
DOCKER_COMPOSE ?= docker compose

# Python interpreter used to create per-package venvs. Override if your
# system exposes 3.12 under a different name (e.g. `python`, `python3`,
# a pyenv shim). The venvs themselves still pin to whatever 3.12 this
# resolves to at install time.
PYTHON ?= python3.12

# Python venvs per package; pnpm for the web workspace.
PY_API := api/.venv
PY_PIPELINES := pipelines/.venv

# ----------------------------------------------------------------------------
# Help
# ----------------------------------------------------------------------------

.PHONY: help
help:
	@echo "Aerial Fleet Monitor — make targets"
	@echo ""
	@echo "Setup:"
	@echo "  install                Install Python (pip) + Node (pnpm) deps for all packages"
	@echo "  sf-auth                Authenticate to Salesforce DE org           [Phase 04]"
	@echo ""
	@echo "Development:"
	@echo "  dev                    docker compose up -d + frontend dev server"
	@echo "  down                   docker compose down"
	@echo "  logs                   Tail logs from the docker-compose stack"
	@echo "  api-shell              ipython with FastAPI app context loaded     [Phase 02]"
	@echo "  sf-deploy              Deploy Salesforce metadata                   [Phase 04]"
	@echo "  sf-validate            Validate Salesforce metadata without deploy  [Phase 04]"
	@echo ""
	@echo "Testing:"
	@echo "  test                   Full suite (unit + integration + contract + e2e) [Phase 10]"
	@echo "  test-unit              Fast unit tests"
	@echo "  test-integration       Tests against live SF dev org                [Phase 04]"
	@echo "  test-e2e               Playwright tests                             [Phase 10]"
	@echo "  test-contract          API contract tests (schemathesis)            [Phase 02]"
	@echo "  sf-test                Apex unit tests in DE org                    [Phase 04]"
	@echo "  lint                   ruff + mypy + eslint"
	@echo "  lint-runbooks          Validate runbook frontmatter + cross-links   [Phase 08]"
	@echo ""
	@echo "Database:"
	@echo "  db-migrate             alembic upgrade head"
	@echo "  db-seed                Seed reference data                          [Phase 01]"
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
	@echo "→ Installing web Node deps"
	cd web && pnpm install

.PHONY: dev
dev:
	$(DOCKER_COMPOSE) up -d
	@echo "→ Backend up. Starting frontend dev server in web/"
	cd web && pnpm dev

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
	@echo "→ eslint (web)"
	cd web && pnpm lint

.PHONY: test-unit
test-unit:
	@echo "→ test-unit: no unit tests yet (Phase 00 stub — real impl in Phase 02)"
	@echo "  Exiting 0 so CI passes on the empty skeleton."

.PHONY: db-migrate
db-migrate:
	set -a; . ./.env; set +a; \
	export DATABASE_URL="$$(echo "$$DATABASE_URL" | sed 's|@postgres:|@127.0.0.1:|')"; \
	cd api && . .venv/bin/activate && alembic upgrade head

.PHONY: db-shell
db-shell:
	$(DOCKER_COMPOSE) exec postgres sh -lc 'psql -U "$${POSTGRES_USER:-afm}" "$${POSTGRES_DB:-afm}"'

# ----------------------------------------------------------------------------
# Stubs — real implementation lands in the noted phase.
# Each prints a pointer and exits 1 so accidental early use is loud.
# ----------------------------------------------------------------------------

.PHONY: sf-auth
sf-auth:
	@echo "Target 'sf-auth' available after Phase 04 — see docs/build/04_salesforce_setup.md"
	@exit 1

.PHONY: sf-deploy
sf-deploy:
	@echo "Target 'sf-deploy' available after Phase 04 — see docs/build/04_salesforce_setup.md"
	@exit 1

.PHONY: sf-validate
sf-validate:
	@echo "Target 'sf-validate' available after Phase 04 — see docs/build/04_salesforce_setup.md"
	@exit 1

.PHONY: sf-test
sf-test:
	@echo "Target 'sf-test' available after Phase 04 — see docs/build/04_salesforce_setup.md"
	@exit 1

.PHONY: api-shell
api-shell:
	@echo "Target 'api-shell' available after Phase 02 — see docs/build/02_api_basic.md"
	@exit 1

.PHONY: test-contract
test-contract:
	@echo "Target 'test-contract' available after Phase 02 — see docs/build/02_api_basic.md"
	@exit 1

.PHONY: test-integration
test-integration:
	@echo "Target 'test-integration' available after Phase 04 — see docs/build/04_salesforce_setup.md"
	@exit 1

.PHONY: test-e2e
test-e2e:
	@echo "Target 'test-e2e' available after Phase 10 — see docs/build/10_testing_ci.md"
	@exit 1

.PHONY: test
test:
	@echo "Target 'test' available after Phase 10 — see docs/build/10_testing_ci.md"
	@exit 1

.PHONY: db-seed
db-seed:
	@echo "Target 'db-seed' available after Phase 01 — see docs/build/01_ingestion.md"
	@exit 1

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
