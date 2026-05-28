"""Tests for `pipelines.rules.base` shared helpers."""

from __future__ import annotations

import pytest

from pipelines.rules.base import is_general_aviation_callsign


@pytest.mark.parametrize(
    "callsign,expected",
    [
        ("N816M", True),  # canonical US GA
        ("N5169E", True),
        ("N1", True),  # minimal valid form
        ("N12345", True),
        ("DAL1559", False),  # commercial 3-letter prefix
        ("UAL277", False),
        ("UPS2986", False),
        ("SKW3343", False),
        ("RCH123", False),  # military airlift
        ("NORTH", False),  # N followed by letter, not digit
        ("NW123", False),  # 2-letter operator, not GA
        ("ANA123", False),  # 3-letter operator starting with A
        ("", False),
        (None, False),
        ("N", False),  # too short
    ],
)
def test_is_general_aviation_callsign(callsign: str | None, expected: bool) -> None:
    """The N-prefix-then-digit pattern matches US GA registrations exactly."""
    assert is_general_aviation_callsign(callsign) is expected
