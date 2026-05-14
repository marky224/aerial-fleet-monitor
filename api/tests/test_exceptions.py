"""Tests for the global exception handlers + _envelope helper in main.py.

Direct unit tests — no TestClient, no DB. Construct mock Request
objects, invoke the handlers, parse the JSONResponse body, assert the
envelope shape and codes.

Handlers are async (declared `async def` in main.py) so tests are too.
pytest-asyncio with asyncio_mode='auto' (set in pyproject.toml) handles
the event loop transparently.
"""

from __future__ import annotations

import json
import types
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from app.exceptions import ScopeViolation
from app.main import (
    _envelope,
    afm_exception_handler,
    http_exception_handler,
    validation_exception_handler,
)


def _decode(response: Any) -> dict[str, Any]:
    """Pull the JSON body out of a starlette JSONResponse."""
    return json.loads(response.body)  # type: ignore[no-any-return]


def _mock_request(request_id: str | None = "req_test123") -> MagicMock:
    """Build a request whose state either has or lacks a request_id."""
    request = MagicMock()
    if request_id is None:
        # SimpleNamespace has no request_id — getattr fallback should fire.
        request.state = types.SimpleNamespace()
    else:
        request.state.request_id = request_id
    return request


# === _envelope ===


def test_envelope_includes_request_id_from_state() -> None:
    response = _envelope(_mock_request("req_abc"), 404, "not_found", "missing site")
    body = _decode(response)
    assert response.status_code == 404
    assert body == {
        "error": {
            "code": "not_found",
            "message": "missing site",
            "request_id": "req_abc",
            "details": {},
        }
    }


def test_envelope_falls_back_to_unknown_when_no_request_id() -> None:
    response = _envelope(_mock_request(None), 500, "internal_error", "boom")
    body = _decode(response)
    assert body["error"]["request_id"] == "unknown"


# === afm_exception_handler ===


async def test_afm_exception_handler_uses_exc_fields() -> None:
    exc = ScopeViolation("nope", details={"region": "west"})
    response = await afm_exception_handler(_mock_request("req_xyz"), exc)
    body = _decode(response)
    assert response.status_code == 403
    assert body["error"]["code"] == "scope_insufficient"
    assert body["error"]["message"] == "nope"
    assert body["error"]["details"] == {"region": "west"}
    assert body["error"]["request_id"] == "req_xyz"


# === validation_exception_handler ===


async def test_validation_exception_handler_emits_422_envelope() -> None:
    errors = [{"type": "missing", "loc": ("body", "x"), "msg": "Field required", "input": None}]
    exc = RequestValidationError(errors)
    response = await validation_exception_handler(_mock_request("req_v"), exc)
    body = _decode(response)
    assert response.status_code == 422
    assert body["error"]["code"] == "validation_failed"
    assert body["error"]["message"] == "Request validation failed"
    assert "errors" in body["error"]["details"]
    assert body["error"]["details"]["errors"][0]["type"] == "missing"


# === http_exception_handler ===


@pytest.mark.parametrize(
    ("status_code", "expected_code"),
    [
        (404, "not_found"),
        (405, "method_not_allowed"),
        (418, "http_error"),
        (500, "http_error"),
    ],
)
async def test_http_exception_handler_maps_status_codes(
    status_code: int, expected_code: str
) -> None:
    exc = HTTPException(status_code=status_code, detail="something")
    response = await http_exception_handler(_mock_request("req_h"), exc)
    body = _decode(response)
    assert response.status_code == status_code
    assert body["error"]["code"] == expected_code
    assert body["error"]["message"] == "something"
