# Aerial Fleet Monitor — Testing Strategy

The full testing specification documents AFM's test pyramid (pytest unit + integration + contract, Salesforce Apex), the fixture strategy, the CI pipeline orchestration, and the coverage targets.

## Topics covered in the full specification

- The test pyramid as built: ~524 unit (api · pipelines · foundry sync), Salesforce integration (run locally against the dev org), OpenAPI contract (schemathesis), and Apex
- What each layer is for (unit = pure logic; integration = real Salesforce dev org; contract = OpenAPI drift detection via schemathesis; Apex = Salesforce test framework)
- Test organization (where tests live alongside their code in `api/tests/`, `pipelines/tests/`, `foundry/sync/tests/`, and `salesforce/.../classes/`)
- Markers and selective runs (pytest markers, `make test-unit` / `test-integration` / `test-contract` targets, total wall time)
- Critical fixtures:
  - Transactional Postgres session (rollback per test)
  - Recorded OpenSky JSON fixtures across 5 scenarios
  - Per-user Salesforce session fixtures (`sf_internal_session`, `sf_west_session`, `sf_east_session`)
  - Mandatory cleanup discipline for any SF integration test
- CI pipeline (one GitHub Actions workflow: secret scan + lint + unit (with coverage gates) + OpenAPI contract, run in parallel; the Salesforce integration suite runs locally, not in CI)
- Coverage gates enforced in CI via `--cov-fail-under` (API ≥75%, pipelines ≥70%, Foundry sync ≥75%; Apex organic 75%+) — no third-party coverage service or badge
- Test authoring conventions (one assertion per test, AAA structure, fixtures over factories, frozen time for time-dependent logic)
- Flake handling (retries, quarantine policy, escape hatch)
- What we don't test in v1 (perf/load, mutation, visual regression, Salesforce metadata snapshots, chaos)

The full testing specification is available on request.

---

This stub exists so that automated reviewers (e.g., CodeRabbit) and human readers know this scope is documented. For the complete specification, including the conftest fixtures and the CI workflow files, contact the author.

**Mark Andrew Marquez** · mark@markandrewmarquez.com
