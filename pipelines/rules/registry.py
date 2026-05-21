"""Rule registry — the canonical list of detection rules.

``build_rules()`` returns fresh instances; the detector calls it once
per run. Rules are stateless, so instances are cheap and interchangeable.
"""

from __future__ import annotations

from pipelines.rules.base import Rule
from pipelines.rules.delay import DelayRule
from pipelines.rules.diversion import DiversionRule
from pipelines.rules.excessive_hold import ExcessiveHoldRule
from pipelines.rules.go_around import GoAroundRule
from pipelines.rules.lost_signal import LostSignalRule
from pipelines.rules.weather_impact import WeatherImpactRule

# Declaration order is the run order. No inter-rule dependencies.
ALL_RULE_CLASSES: list[type[Rule]] = [
    LostSignalRule,
    DiversionRule,
    ExcessiveHoldRule,
    WeatherImpactRule,
    GoAroundRule,
    DelayRule,
]


def build_rules() -> list[Rule]:
    """Instantiate every registered rule."""
    return [cls() for cls in ALL_RULE_CLASSES]


# Convenience for callers that just want the type list (e.g. tests
# asserting coverage). Built lazily-ish at import; stateless so safe.
ALL_RULES: list[Rule] = build_rules()
