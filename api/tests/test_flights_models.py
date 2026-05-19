"""Validation tests for app.models.flights request models.

No fixtures, no I/O. Pins the TrailBatchRequest.icao24s validator
behaviour — in particular that a malformed icao24 raises a
JSON-serializable validation error (a bare ValueError used to embed the
exception object in the Pydantic error ctx, which main.py's
RequestValidationError handler could not serialize → HTTP 500).
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from app.models.flights import TrailBatchRequest


def test_trail_batch_request_normalizes_lowercases_and_dedupes() -> None:
    # Happy path unchanged by the defect-2 fix: case-folded, hex-validated,
    # de-duplicated, order preserved.
    req = TrailBatchRequest(icao24s=["ABC123", "abc123", " def456 "])
    assert req.icao24s == ["abc123", "def456"]


@pytest.mark.parametrize("bad", ["BADHEX!", "abc12", "abc1234", "zzzzzz", ""])
def test_trail_batch_request_rejects_malformed_icao24(bad: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        TrailBatchRequest(icao24s=[bad])
    errors = exc_info.value.errors()
    assert errors[0]["type"] == "invalid_icao24"
    assert "invalid icao24" in errors[0]["msg"]


def test_trail_batch_request_validation_error_is_json_serializable() -> None:
    # The exact regression: main.py's handler does `{"errors": exc.errors()}`
    # then JSON-serializes it. A bare ValueError put the exception object in
    # `ctx` → TypeError → 500. PydanticCustomError keeps ctx a plain dict, so
    # json.dumps must not raise.
    with pytest.raises(ValidationError) as exc_info:
        TrailBatchRequest(icao24s=["BADHEX!"])
    json.dumps({"errors": exc_info.value.errors()})  # must not raise
