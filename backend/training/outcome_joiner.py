"""Outcome joiner — links finalized analyst decisions back to training rows.

Called at retraining time to close the feedback loop:
    1. Fetch all alerts with finalized outcomes in a time window
    2. Join with stored feature snapshots
    3. Output point-in-time correct training rows

This is distinct from TrainingDatasetBuilder: the OutcomeJoiner is
designed for incremental updates (new outcomes since last training run)
whereas TrainingDatasetBuilder rebuilds from scratch with full history.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("althea.training.outcome_joiner")


class OutcomeJoiner:
    """Join new finalized outcomes with their feature snapshots."""

    def __init__(self, repository) -> None:
        self._repository = repository

    def fetch_new_labeled_rows(
        self,
        tenant_id: str,
        since: datetime,
        until: datetime | None = None,
    ) -> pd.DataFrame:
        """Return labeled rows for outcomes finalized between since and until.

        Each row contains:
            alert_id, analyst_decision, risk_score_at_decision, timestamp,
            all feature columns from the stored feature snapshot
        """
        cutoff = until or datetime.now(timezone.utc)

        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT
                        o.alert_id,
                        o.analyst_decision,
                        o.risk_score_at_decision,
                        o.model_version,
                        o.timestamp          AS outcome_timestamp,
                        a.payload_json,
                        a.created_at         AS alert_created_at
                    FROM alert_outcomes o
                    JOIN alerts a
                        ON a.alert_id = o.alert_id AND a.tenant_id = o.tenant_id
                    WHERE o.tenant_id = :tid
                      AND o.timestamp >= :since
                      AND o.timestamp < :until
                      AND o.analyst_decision IS NOT NULL
                    ORDER BY o.timestamp ASC
                    """
                ),
                {"tid": tenant_id, "since": since, "until": cutoff},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        import json
        records: list[dict[str, Any]] = []
        for r in rows:
            payload = r[5] or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            features = payload.get("features_json") or {}
            if isinstance(features, str):
                try:
                    features = json.loads(features)
                except Exception:
                    features = {}

            row: dict[str, Any] = {
                "alert_id": str(r[0]),
                "analyst_decision": str(r[1]),
                "risk_score_at_decision": float(r[2] or 0.0),
                "model_version": str(r[3] or ""),
                "outcome_timestamp": r[4],
                "alert_created_at": r[6],
            }
            # Merge feature snapshot
            row.update({k: v for k, v in payload.items() if k not in row and k != "features_json"})
            row.update({k: v for k, v in (features if isinstance(features, dict) else {}).items() if k not in row})
            records.append(row)

        df = pd.DataFrame(records)
        df["outcome_timestamp"] = pd.to_datetime(df["outcome_timestamp"], utc=True, errors="coerce")
        df["alert_created_at"] = pd.to_datetime(df["alert_created_at"], utc=True, errors="coerce")
        return df

    def count_new_outcomes(self, tenant_id: str, since: datetime) -> int:
        """Return the count of finalized outcomes since the given timestamp."""
        with self._repository.session(tenant_id=tenant_id) as session:
            result = session.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM alert_outcomes
                    WHERE tenant_id = :tid
                      AND timestamp >= :since
                      AND analyst_decision IS NOT NULL
                    """
                ),
                {"tid": tenant_id, "since": since},
            ).fetchone()
        return int(result[0]) if result else 0
