"""Action router — maps policy decision to queue action.

Determines the recommended analyst queue action for each alert
based on the combined priority score and governance status.

Actions:
    IMMEDIATE_REVIEW     — P0 / sanctions hold
    PRIORITY_QUEUE       — HIGH priority
    STANDARD_QUEUE       — MEDIUM / eligible
    DEFERRED_QUEUE       — LOW priority
    SUPPRESS             — below threshold
    MANDATORY_ANALYST    — mandatory review flag
"""
from __future__ import annotations

from typing import Any

import pandas as pd


_ACTION_MAP = {
    "sanctions_hold": "IMMEDIATE_REVIEW",
    "mandatory_review": "MANDATORY_ANALYST",
    "eligible": None,       # resolved by priority score
    "suppressed": "SUPPRESS",
}

_SCORE_TO_ACTION = [
    (85.0, "IMMEDIATE_REVIEW"),
    (65.0, "PRIORITY_QUEUE"),
    (40.0, "STANDARD_QUEUE"),
    (0.0,  "DEFERRED_QUEUE"),
]


class ActionRouter:
    """Resolve a recommended queue action from governance status + priority score."""

    def route(self, governance_status: str, priority_score: float) -> str:
        """Return the queue action for a single alert."""
        explicit = _ACTION_MAP.get(governance_status)
        if explicit is not None:
            return explicit

        for threshold, action in _SCORE_TO_ACTION:
            if priority_score >= threshold:
                return action
        return "DEFERRED_QUEUE"

    def route_batch(self, df: pd.DataFrame) -> pd.Series:
        """Apply routing to a full DataFrame.

        Requires columns: governance_status, priority_score
        """
        status = df.get("governance_status", pd.Series("eligible", index=df.index)).astype(str)
        score = pd.to_numeric(df.get("priority_score", 50.0), errors="coerce").fillna(0.0)
        return pd.Series(
            [self.route(s, float(p)) for s, p in zip(status, score)],
            index=df.index,
            name="queue_action",
        )

    def to_audit_record(
        self,
        alert_id: str,
        priority_score: float,
        governance_status: str,
        queue_action: str,
        signals: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build an immutable audit record for this decision."""
        return {
            "alert_id": alert_id,
            "priority_score": round(priority_score, 4),
            "governance_status": governance_status,
            "queue_action": queue_action,
            "signals": signals or {},
        }
