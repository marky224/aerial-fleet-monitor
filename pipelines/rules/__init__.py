"""AFM anomaly-detection rules (Phase 05).

Each rule is a pure function of its inputs — it reads the enriched
positions frame + current weather + existing cases + a baseline
provider, and returns zero or more ``Anomaly`` records. Rules never do
I/O and never write: the orchestrating ``case_detector`` asset owns all
reads, writes, dedup, and Salesforce sync.
"""

from pipelines.rules.base import AirportConditions, Anomaly, Rule
from pipelines.rules.dedup import deduplicate
from pipelines.rules.registry import ALL_RULES, build_rules

__all__ = [
    "ALL_RULES",
    "AirportConditions",
    "Anomaly",
    "Rule",
    "build_rules",
    "deduplicate",
]
