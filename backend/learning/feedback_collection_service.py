"""Feedback Collection Service — richer analyst outcome labeling.

Extended from the original to capture:
    - analyst_decision          (unchanged)
    - decision_reason           (unchanged)
    - sar_filed_flag            explicit SAR/STR boolean
    - qa_override               QA reviewed and overrode decision
    - investigation_start_time  when analyst started working the alert
    - investigation_end_time    when analyst completed the decision
    - touch_count               number of times analyst opened/edited the alert
    - notes_count               number of investigation notes added
    - final_label_status        "final" | "provisional" | "pending"
    - final_label_timestamp     when label became final/confirmed

These fields feed directly into:
    - Training labels (resolution_hours = end - start)
    - Investigation time model features
    - Business metrics (actual analyst hours)
    - Retraining scheduler (outcome finality check)
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger("althea.learning.feedback")

VALID_DECISIONS = frozenset(
    {"true_positive", "false_positive", "escalated", "sar_filed", "benign_activity", "confirmed_suspicious"}
)

FINAL_LABEL_STATUSES = frozenset({"final", "provisional", "pending"})


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
        # Extended fields
        sar_filed_flag: bool = False,
        qa_override: bool = False,
        investigation_start_time: datetime | None = None,
        investigation_end_time: datetime | None = None,
        touch_count: int | None = None,
        notes_count: int | None = None,
        final_label_status: str = "final",
    ) -> dict[str, Any]:
        """Record an analyst outcome with full investigation metadata."""
        if analyst_decision not in VALID_DECISIONS:
            raise ValueError(
                f"Invalid decision '{analyst_decision}'. "
                f"Valid values: {sorted(VALID_DECISIONS)}"
            )
        if final_label_status not in FINAL_LABEL_STATUSES:
            final_label_status = "final"

        # Auto-set sar_filed_flag from decision
        if analyst_decision == "sar_filed":
            sar_filed_flag = True

        outcome_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        final_label_timestamp = now if final_label_status == "final" else None

        # Compute resolution hours if both timestamps are provided
        resolution_hours: float | None = None
        if investigation_start_time and investigation_end_time:
            delta = (investigation_end_time - investigation_start_time).total_seconds()
            resolution_hours = max(delta / 3600.0, 0.0)

        logger.info(
            "Recording analyst outcome: alert_id=%s decision=%s sar=%s resolution_hours=%s",
            alert_id,
            analyst_decision,
            sar_filed_flag,
            resolution_hours,
        )

        with self._repository.session(tenant_id=tenant_id) as session:
            existing = session.execute(
                text(
                    "SELECT id FROM alert_outcomes "
                    "WHERE tenant_id = :tenant_id AND alert_id = :alert_id"
                ),
                {"tenant_id": tenant_id, "alert_id": str(alert_id)},
            ).fetchone()

            params: dict[str, Any] = {
                "decision": analyst_decision,
                "reason": decision_reason,
                "analyst_id": analyst_id,
                "model_version": model_version,
                "risk_score": risk_score_at_decision,
                "ts": now,
                "sar_filed": sar_filed_flag,
                "qa_override": qa_override,
                "inv_start": investigation_start_time,
                "inv_end": investigation_end_time,
                "resolution_hours": resolution_hours,
                "touch_count": touch_count,
                "notes_count": notes_count,
                "label_status": final_label_status,
                "label_ts": final_label_timestamp,
                "tenant_id": tenant_id,
                "alert_id": str(alert_id),
            }

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
                            timestamp = :ts,
                            sar_filed_flag = COALESCE(:sar_filed, sar_filed_flag),
                            qa_override = COALESCE(:qa_override, qa_override),
                            investigation_start_time = COALESCE(:inv_start, investigation_start_time),
                            investigation_end_time = COALESCE(:inv_end, investigation_end_time),
                            resolution_hours = COALESCE(:resolution_hours, resolution_hours),
                            touch_count = COALESCE(:touch_count, touch_count),
                            notes_count = COALESCE(:notes_count, notes_count),
                            final_label_status = :label_status,
                            final_label_timestamp = COALESCE(:label_ts, final_label_timestamp)
                        WHERE tenant_id = :tenant_id AND alert_id = :alert_id
                        """
                    ),
                    params,
                )
                outcome_id = str(existing[0])
            else:
                session.execute(
                    text(
                        """
                        INSERT INTO alert_outcomes (
                            id, tenant_id, alert_id, analyst_decision,
                            decision_reason, analyst_id, model_version,
                            risk_score_at_decision, timestamp,
                            sar_filed_flag, qa_override,
                            investigation_start_time, investigation_end_time,
                            resolution_hours, touch_count, notes_count,
                            final_label_status, final_label_timestamp
                        ) VALUES (
                            :id, :tenant_id, :alert_id, :decision,
                            :reason, :analyst_id, :model_version,
                            :risk_score, :ts,
                            :sar_filed, :qa_override,
                            :inv_start, :inv_end,
                            :resolution_hours, :touch_count, :notes_count,
                            :label_status, :label_ts
                        )
                        """
                    ),
                    {"id": outcome_id, **params},
                )

        return {
            "outcome_id": outcome_id,
            "alert_id": alert_id,
            "analyst_decision": analyst_decision,
            "decision_reason": decision_reason,
            "analyst_id": analyst_id,
            "sar_filed_flag": sar_filed_flag,
            "qa_override": qa_override,
            "resolution_hours": resolution_hours,
            "final_label_status": final_label_status,
            "timestamp": now.isoformat(),
        }

    def get_outcome(self, tenant_id: str, alert_id: str) -> dict[str, Any] | None:
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                text(
                    """
                    SELECT id, alert_id, analyst_decision, decision_reason,
                           analyst_id, model_version, risk_score_at_decision, timestamp,
                           sar_filed_flag, qa_override, resolution_hours,
                           touch_count, notes_count, final_label_status, final_label_timestamp
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
            "sar_filed_flag": bool(row[8]) if row[8] is not None else False,
            "qa_override": bool(row[9]) if row[9] is not None else False,
            "resolution_hours": float(row[10]) if row[10] is not None else None,
            "touch_count": row[11],
            "notes_count": row[12],
            "final_label_status": str(row[13] or "final"),
            "final_label_timestamp": str(row[14]) if row[14] else None,
        }

    def list_outcomes(self, tenant_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, alert_id, analyst_decision, decision_reason,
                           analyst_id, model_version, risk_score_at_decision, timestamp,
                           sar_filed_flag, resolution_hours, final_label_status
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
                "sar_filed_flag": bool(r[8]) if r[8] is not None else False,
                "resolution_hours": float(r[9]) if r[9] is not None else None,
                "final_label_status": str(r[10] or "final"),
            }
            for r in rows
        ]

    def get_outcome_statistics(self, tenant_id: str) -> dict[str, Any]:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT analyst_decision, COUNT(*) AS cnt,
                           AVG(resolution_hours) AS avg_hours,
                           SUM(CASE WHEN sar_filed_flag = TRUE THEN 1 ELSE 0 END) AS sar_count
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                    GROUP BY analyst_decision
                    """
                ),
                {"tenant_id": tenant_id},
            ).fetchall()

        counts: dict[str, int] = {}
        avg_hours_by_decision: dict[str, float | None] = {}
        total_sars = 0
        for r in rows:
            decision = str(r[0])
            counts[decision] = int(r[1])
            avg_hours_by_decision[decision] = float(r[2]) if r[2] is not None else None
            total_sars += int(r[3] or 0)

        total = sum(counts.values())
        true_positives = (
            counts.get("true_positive", 0)
            + counts.get("escalated", 0)
            + counts.get("sar_filed", 0)
            + counts.get("confirmed_suspicious", 0)
        )
        false_positives = counts.get("false_positive", 0) + counts.get("benign_activity", 0)
        precision = true_positives / total if total > 0 else 0.0

        return {
            "total_outcomes": total,
            "decision_counts": counts,
            "estimated_precision": round(precision, 4),
            "training_labels_available": total,
            "sar_filed_count": total_sars,
            "avg_resolution_hours_by_decision": avg_hours_by_decision,
        }
