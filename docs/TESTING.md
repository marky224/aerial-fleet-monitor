# Aerial Fleet Monitor — Testing Strategy

The full testing specification documents AFM's test pyramid (pytest unit + integration + contract, Playwright e2e, Salesforce Apex), the fixture strategy, the CI pipeline orchestration, and the coverage targets.

## Topics covered in the full specification

- The test pyramid (counts and wall-time targets per layer: ~80 unit, ~20 integration, ~12 e2e, plus contract and Apex)
- What each layer is for (unit = pure logic; integration = real Salesforce dev org; contract = OpenAPI drift detection via schemathesis; e2e = full deployed stack via Playwright; Apex = Salesforce test framework)
- Test organization (where tests live alongside their code in `api/tests/`, `pipelines/tests/`, `web/tests/`, and `salesforce/.../classes/`)
- Markers and selective runs (pytest markers, `make test-unit` / `test-integration` / `test-contract` / `test-e2e` targets, total wall time)
- Critical fixtures:
  - Transactional Postgres session (rollback per test)
  - Recorded OpenSky JSON fixtures across 5 scenarios
  - Per-user Salesforce session fixtures (`sf_internal_session`, `sf_west_session`, `sf_east_session`)
  - Mandatory cleanup discipline for any SF integration test
- CI pipeline (4 GitHub Actions workflows running in parallel; required secrets per environment, including per-username SF credentials)
- Coverage targets per component (Python ≥75% line, Apex organic 75%+, frontend components-focused ≥40%)
- Test authoring conventions (one assertion per test, AAA structure, fixtures over factories, frozen time for time-dependent logic)
- Flake handling (retries, quarantine policy, escape hatch)
- What we don't test in v1 (perf/load, mutation, visual regression, Salesforce metadata snapshots, chaos)

The full testing specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the conftest fixtures, the e2e specs, and the CI workflow files, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
