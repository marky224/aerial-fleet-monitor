# Aerial Fleet Monitor — Testing Strategy

The full testing specification documents AFM's test pyramid (pytest unit + integration + contract, Salesforce Apex), the fixture strategy, the CI pipeline orchestration, and the coverage targets.

## Topics covered in the full specification

- The test pyramid (counts and wall-time targets per layer: ~80 unit, ~20 integration, plus contract and Apex)
- What each layer is for (unit = pure logic; integration = real Salesforce dev org; contract = OpenAPI drift detection via schemathesis; Apex = Salesforce test framework)
- Test organization (where tests live alongside their code in `api/tests/`, `pipelines/tests/`, `foundry/sync/tests/`, and `salesforce/.../classes/`)
- Markers and selective runs (pytest markers, `make test-unit` / `test-integration` / `test-contract` targets, total wall time)
- Critical fixtures:
  - Transactional Postgres session (rollback per test)
  - Recorded OpenSky JSON fixtures across 5 scenarios
  - Per-user Salesforce session fixtures (`sf_internal_session`, `sf_west_session`, `sf_east_session`)
  - Mandatory cleanup discipline for any SF integration test
- CI pipeline (4 GitHub Actions workflows running in parallel; required secrets per environment, including per-username SF credentials)
- Coverage targets per component (Python ≥75% line, Apex organic 75%+; Foundry Workshop apps tested in Foundry's own framework)
- Test authoring conventions (one assertion per test, AAA structure, fixtures over factories, frozen time for time-dependent logic)
- Flake handling (retries, quarantine policy, escape hatch)
- What we don't test in v1 (perf/load, mutation, visual regression, Salesforce metadata snapshots, chaos)

The full testing specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the conftest fixtures and the CI workflow files, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
