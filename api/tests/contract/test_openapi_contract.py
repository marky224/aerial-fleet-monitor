"""OpenAPI contract tests (Phase 10).

Schemathesis drives the API's GET endpoints with generated inputs and
asserts each response body conforms to the OpenAPI schema FastAPI
publishes — the drift detector between the Pydantic response models and
what the API actually returns at runtime.

Scope is deliberately GET reads only. The POST endpoints (admin SF-write
smoke, the two case-sync paths, the trail batch) mutate the database or
the live Salesforce org, and ``/v1/positions/stream`` is Server-Sent
Events; none are safe to fuzz against the running stack, so they are
excluded (``include(method="GET")`` plus the stream ``exclude`` below).

The schema is built offline from ``app.openapi()`` — no running server and
no database are touched at import — so collection stays safe under
``make test-unit`` (which deselects ``-m contract``). Execution targets a
RUNNING API at ``AFM_CONTRACT_BASE_URL`` (default the local stack); run it
via ``make test-contract`` with the stack up.
"""

from __future__ import annotations

import os

import pytest
import schemathesis
from hypothesis import HealthCheck, settings
from schemathesis.specs.openapi.checks import response_schema_conformance

BASE_URL = os.environ.get("AFM_CONTRACT_BASE_URL", "http://localhost:8000")

# FastAPI emits OpenAPI 3.1.0; schemathesis 3.x gates 3.1 behind this flag.
schemathesis.experimental.OPEN_API_3_1.enable()

# Build the schema from the app object (no network/DB at import), filtered to
# safe GET reads. Wrapped so a schema-build failure can never break collection
# of `make test-unit` (which only deselects -m contract, but still imports this
# module): on failure we skip the whole module instead of erroring out.
try:
    from app.main import app

    schema = (
        schemathesis.from_dict(app.openapi(), base_url=BASE_URL)
        .include(method="GET")
        .exclude(path="/v1/positions/stream")  # SSE — never completes
    )
except Exception as exc:  # defensive: a schema-build failure must never break the unit run
    pytest.skip(f"contract schema unavailable: {exc}", allow_module_level=True)


@pytest.mark.contract
@schema.parametrize()
@settings(
    max_examples=20,  # acceptance #3: >=20 examples per endpoint
    deadline=None,  # response time varies (lakehouse scans); assert shape, not latency
    suppress_health_check=[HealthCheck.too_slow],
)
def test_openapi_contract(case: schemathesis.Case) -> None:
    """Every generated GET response must match its documented schema.

    Only ``response_schema_conformance`` runs: we validate the body shape
    against the spec, not status-code completeness (FastAPI does not
    auto-document the 4xx error envelope — that is the integration suite's
    concern, not this layer's).
    """
    case.call_and_validate(checks=(response_schema_conformance,))
