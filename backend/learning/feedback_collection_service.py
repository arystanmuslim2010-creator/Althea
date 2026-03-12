"""Feedback Collection Service — analyst outcome labeling for model retraining."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger("althea.learning.feedback")

VALID_DECISIONS = frozenset(
    {"true_positive", "false_positive", "escalated", "sar_filed", "benign_activity"}
)


class FeedbackCollectionService:
    """Collect analyst decisions on alert outcomes for ML model retraining pipeline."""

    def __init__(self, repository) -> None:
        self._repository = repository

    def record_outcome(
        self,
        tenant_id: str,
        alert_id: str,
        analyst_decision: str,
        decision_reason: str | None = None,
        analyst_id: str | None = None,
        model_version: str | None = None,
        risk_score_at_decision: float | None = None,
    ) -> dict[str, Any]:
        if analyst_decision not in VALID_DECISIONS:
            raise ValueError(
                f"Invalid decision '{analyst_decision}'. "
                f"Valid values: {sorted(VALID_DECISIONS)}"
            )

        outcome_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)

        logger.info(
            "Recording analyst outcome",
            extra={
                "tenant_id": tenant_id,
                "alert_id": alert_id,
                "decision": analyst_decision,
                "analyst_id": analyst_id,
            },
        )

        with self._repository.session(tenant_id=tenant_id) as session:
            # Upsert: if outcome already exists for this alert, update it
            existing = session.execute(
                text(
                    "SELECT id FROM alert_outcomes "
                    "WHERE tenant_id = :tenant_id AND alert_id = :alert_id"
                ),
                {"tenant_id": tenant_id, "alert_id": str(alert_id)},
            ).fetchone()

            if existing:
                session.execute(
                    text(
                        """
                        UPDATE alert_outcomes SET
                            analyst_decision = :decision,
                            decision_reason = :reason,
                            analyst_id = :analyst_id,
                            model_version = :model_version,
                            risk_score_at_decision = :risk_score,
                            timestamp = :ts
                        WHERE tenant_id = :tenant_id AND alert_id = :alert_id
                        """
                    ),
                    {
                        "decision": analyst_decision,
                        "reason": decision_reason,
                        "analyst_id": analyst_id,
                        "model_version": model_version,
                        "risk_score": risk_score_at_decision,
                        "ts": now,
                        "tenant_id": tenant_id,
                        "alert_id": str(alert_id),
                    },
                )
                outcome_id = str(existing[0])
            else:
                session.execute(
                    text(
                        """
                        INSERT INTO alert_outcomes (
                            id, tenant_id, alert_id, analyst_decision,
                            decision_reason, analyst_id, model_version,
                            risk_score_at_decision, timestamp
                        ) VALUES (
                            :id, :tenant_id, :alert_id, :decision,
                            :reason, :analyst_id, :model_version,
                            :risk_score, :ts
                        )
                        """
                    ),
                    {
                        "id": outcome_id,
                        "tenant_id": tenant_id,
                        "alert_id": str(alert_id),
                        "decision": analyst_decision,
                        "reason": decision_reason,
                        "analyst_id": analyst_id,
                        "model_version": model_version,
                        "risk_score": risk_score_at_decision,
                        "ts": now,
                    },
                )

        return {
            "outcome_id": outcome_id,
            "alert_id": alert_id,
            "analyst_decision": analyst_decision,
            "decision_reason": decision_reason,
            "analyst_id": analyst_id,
            "timestamp": now.isoformat(),
        }

    def get_outcome(self, tenant_id: str, alert_id: str) -> dict[str, Any] | None:
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                text(
                    """
                    SELECT id, alert_id, analyst_decision, decision_reason,
                           analyst_id, model_version, risk_score_at_decision, timestamp
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id AND alert_id = :alert_id
                    """
                ),
                {"tenant_id": tenant_id, "alert_id": str(alert_id)},
            ).fetchone()

        if not row:
            return None
        return {
            "outcome_id": str(row[0]),
            "alert_id": str(row[1]),
            "analyst_decision": str(row[2]),
            "decision_reason": row[3],
            "analyst_id": row[4],
            "model_version": row[5],
            "risk_score_at_decision": row[6],
            "timestamp": str(row[7]) if row[7] else None,
        }

    def list_outcomes(
        self, tenant_id: str, limit: int = 500
    ) -> list[dict[str, Any]]:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, alert_id, analyst_decision, decision_reason,
                           analyst_id, model_version, risk_score_at_decision, timestamp
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                    ORDER BY timestamp DESC
                    LIMIT :limit
                    """
                ),
                {"tenant_id": tenant_id, "limit": limit},
            ).fetchall()

        return [
            {
                "outcome_id": str(r[0]),
                "alert_id": str(r[1]),
                "analyst_decision": str(r[2]),
                "decision_reason": r[3],
                "analyst_id": r[4],
                "model_version": r[5],
                "risk_score_at_decision": r[6],
                "timestamp": str(r[7]) if r[7] else None,
            }
            for r in rows
        ]

    def get_outcome_statistics(self, tenant_id: str) -> dict[str, Any]:
        """Return aggregate outcome statistics useful for model retraining planning."""
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT analyst_decision, COUNT(*) as cnt
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                    GROUP BY analyst_decision
                    """
                ),
                {"tenant_id": tenant_id},
            ).fetchall()

        counts: dict[str, int] = {r[0]: int(r[1]) for r in rows}
        total = sum(counts.values())
        true_positives = counts.get("true_positive", 0) + counts.get("escalated", 0) + counts.get("sar_filed", 0)
        false_positives = counts.get("false_positive", 0) + counts.get("benign_activity", 0)
        precision = true_positives / total if total > 0 else 0.0

        return {
            "total_outcomes": total,
            "decision_counts": counts,
            "estimated_precision": round(precision, 4),
            "training_labels_available": total,
        }
