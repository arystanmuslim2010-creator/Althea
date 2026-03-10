from __future__ import annotations

ALERTS_INGESTED = "alerts.ingested"
ALERTS_FEATURES_GENERATED = "alerts.features_generated"
ALERTS_SCORED = "alerts.scored"
ALERTS_PRIORITIZED = "alerts.prioritized"
CASES_CREATED = "cases.created"

STREAM_TOPICS = [
    ALERTS_INGESTED,
    ALERTS_FEATURES_GENERATED,
    ALERTS_SCORED,
    ALERTS_PRIORITIZED,
    CASES_CREATED,
]
