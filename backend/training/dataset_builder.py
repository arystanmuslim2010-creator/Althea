"""Training dataset builder with point-in-time correctness.

Joins alert records, feature snapshots, and finalized outcomes
to construct a leakage-free training dataset. Only outcomes
recorded BEFORE the dataset cutoff timestamp are included.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("althea.training.dataset_builder")


class TrainingDatasetBuilder:
    """Builds point-in-time correct supervised training datasets.

    The builder is intentionally read-only with respect to the repository.
    It never modifies stored data; it only assembles training rows.
    """

    # Minimum labeled rows required to attempt model training
    MIN_LABELED_ROWS = 50

    def __init__(self, repository) -> None:
        self._repository = repository

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_escalation_dataset(
        self,
        tenant_id: str,
        cutoff_timestamp: datetime | None = None,
    ) -> pd.DataFrame:
        """Build a dataset for escalation model training.

        Returns a DataFrame where each row is a labeled alert with its
        feature snapshot. The ``escalation_label`` column is the binary
        training target (0 = not suspicious, 1 = escalated/suspicious).

        Only finalized outcomes recorded strictly before ``cutoff_timestamp``
        are included to prevent future leakage. When ``cutoff_timestamp`` is
        None the current UTC time is used as the cutoff.
        """
        cutoff = cutoff_timestamp or datetime.now(timezone.utc)
        logger.info(
            json.dumps(
                {
                    "event": "dataset_build_start",
                    "dataset_type": "escalation",
                    "tenant_id": tenant_id,
                    "cutoff": cutoff.isoformat(),
                },
                ensure_ascii=True,
            )
        )

        outcomes_df = self._fetch_outcomes(tenant_id, cutoff)
        if outcomes_df.empty:
            raise ValueError(
                f"Insufficient labeled data: no finalized outcomes found for tenant '{tenant_id}' "
                f"before cutoff {cutoff.isoformat()}. "
                f"Minimum required rows: {self.MIN_LABELED_ROWS}."
            )

        alerts_df = self._fetch_alert_features(tenant_id, outcomes_df["alert_id"].tolist())
        if alerts_df.empty:
            raise ValueError(
                "Insufficient labeled data: no alert feature snapshots found for finalized outcomes."
            )

        merged = self._join_point_in_time(alerts_df, outcomes_df, cutoff)
        if len(merged) < self.MIN_LABELED_ROWS:
            raise ValueError(
                f"Insufficient labeled rows for training: got {len(merged)}, "
                f"minimum required is {self.MIN_LABELED_ROWS}."
            )

        logger.info(
            json.dumps(
                {
                    "event": "dataset_build_complete",
                    "dataset_type": "escalation",
                    "tenant_id": tenant_id,
                    "rows": len(merged),
                    "positive_rate": float(merged["escalation_label"].mean()) if "escalation_label" in merged.columns else None,
                },
                ensure_ascii=True,
            )
        )
        return merged

    def build_time_dataset(
        self,
        tenant_id: str,
        cutoff_timestamp: datetime | None = None,
    ) -> pd.DataFrame:
        """Build a dataset for investigation time model training.

        Returns a DataFrame where ``resolution_hours`` is the regression
        target (hours from alert creation to outcome decision). Rows where
        resolution time cannot be computed are excluded.
        """
        cutoff = cutoff_timestamp or datetime.now(timezone.utc)
        logger.info(
            json.dumps(
                {
                    "event": "dataset_build_start",
                    "dataset_type": "time",
                    "tenant_id": tenant_id,
                    "cutoff": cutoff.isoformat(),
                },
                ensure_ascii=True,
            )
        )

        outcomes_df = self._fetch_outcomes(tenant_id, cutoff)
        if outcomes_df.empty:
            raise ValueError(
                f"No finalized outcomes for tenant '{tenant_id}' before {cutoff.isoformat()}."
            )

        alerts_df = self._fetch_alert_features(tenant_id, outcomes_df["alert_id"].tolist())
        if alerts_df.empty:
            raise ValueError("No alert feature snapshots found for labeled outcomes.")

        merged = self._join_point_in_time(alerts_df, outcomes_df, cutoff)

        # Compute resolution time (hours) as the regression target
        if "alert_created_at" in merged.columns and "outcome_timestamp" in merged.columns:
            created = pd.to_datetime(merged["alert_created_at"], utc=True, errors="coerce")
            decided = pd.to_datetime(merged["outcome_timestamp"], utc=True, errors="coerce")
            delta_hours = (decided - created).dt.total_seconds() / 3600.0
            # Clamp to [0, 8760] hours (1 year) and drop negatives / missing
            merged["resolution_hours"] = delta_hours.clip(lower=0.0, upper=8760.0)
            merged = merged.dropna(subset=["resolution_hours"])
        else:
            raise ValueError(
                "Cannot compute resolution_hours: "
                "alert_created_at or outcome_timestamp columns are missing."
            )

        logger.info(
            json.dumps(
                {
                    "event": "dataset_build_complete",
                    "dataset_type": "time",
                    "tenant_id": tenant_id,
                    "rows": len(merged),
                    "median_resolution_hours": float(merged["resolution_hours"].median()) if len(merged) else None,
                },
                ensure_ascii=True,
            )
        )
        return merged

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_outcomes(self, tenant_id: str, cutoff: datetime) -> pd.DataFrame:
        """Fetch all finalized outcomes recorded before the cutoff."""
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT
                        id            AS outcome_id,
                        alert_id,
                        analyst_decision,
                        decision_reason,
                        analyst_id,
                        model_version,
                        risk_score_at_decision,
                        timestamp     AS outcome_timestamp
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                      AND timestamp < :cutoff
                      AND analyst_decision IS NOT NULL
                    ORDER BY timestamp ASC
                    """
                ),
                {"tenant_id": tenant_id, "cutoff": cutoff},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=[
                "outcome_id",
                "alert_id",
                "analyst_decision",
                "decision_reason",
                "analyst_id",
                "model_version",
                "risk_score_at_decision",
                "outcome_timestamp",
            ],
        )
        df["alert_id"] = df["alert_id"].astype(str)
        df["outcome_timestamp"] = pd.to_datetime(df["outcome_timestamp"], utc=True, errors="coerce")
        return df

    def _fetch_alert_features(self, tenant_id: str, alert_ids: list[str]) -> pd.DataFrame:
        """Fetch alert records with their stored feature snapshots.

        Alert features are stored inside ``payload_json`` as the ``features_json``
        sub-field, populated by the pipeline during initial scoring.
        """
        if not alert_ids:
            return pd.DataFrame()

        # Batch in groups of 500 to avoid overly large IN clauses
        all_records: list[dict[str, Any]] = []
        batch_size = 500
        for start in range(0, len(alert_ids), batch_size):
            batch = alert_ids[start : start + batch_size]
            with self._repository.session(tenant_id=tenant_id) as session:
                placeholders = ", ".join(f":id_{i}" for i in range(len(batch)))
                params: dict[str, Any] = {"tenant_id": tenant_id}
                params.update({f"id_{i}": aid for i, aid in enumerate(batch)})
                rows = session.execute(
                    text(
                        f"""
                        SELECT
                            alert_id,
                            risk_score,
                            risk_band,
                            priority,
                            status,
                            payload_json,
                            created_at AS alert_created_at
                        FROM alerts
                        WHERE tenant_id = :tenant_id
                          AND alert_id IN ({placeholders})
                        """
                    ),
                    params,
                ).fetchall()
                all_records.extend(
                    {
                        "alert_id": str(r[0]),
                        "risk_score": float(r[1] or 0.0),
                        "risk_band": str(r[2] or ""),
                        "priority": str(r[3] or ""),
                        "status": str(r[4] or ""),
                        "payload_json": r[5],
                        "alert_created_at": r[6],
                    }
                    for r in rows
                )

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        df["alert_created_at"] = pd.to_datetime(df["alert_created_at"], utc=True, errors="coerce")

        # Flatten feature snapshot stored inside payload_json.features_json
        feature_rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            payload = row["payload_json"] or {}
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
            flat: dict[str, Any] = {"alert_id": row["alert_id"]}
            flat.update(features)
            # Bring alert-level fields into the flat row
            for col in ("risk_score", "risk_band", "priority", "status", "alert_created_at"):
                flat[col] = row.get(col)
            # Carry through raw payload fields for behavioral signals
            for key in (
                "amount",
                "typology",
                "country",
                "segment",
                "source_system",
                "user_id",
                "customer_id",
                "account_id",
            ):
                if key not in flat and key in payload:
                    flat[key] = payload[key]
            feature_rows.append(flat)

        return pd.DataFrame(feature_rows)

    def _join_point_in_time(
        self,
        alerts_df: pd.DataFrame,
        outcomes_df: pd.DataFrame,
        cutoff: datetime,
    ) -> pd.DataFrame:
        """Inner-join alerts with outcomes ensuring temporal correctness.

        The outcome must have been recorded BEFORE the cutoff so that
        no information about future decisions leaks into training features.
        Alerts without a matching outcome (e.g. still open) are excluded.
        """
        merged = alerts_df.merge(outcomes_df, on="alert_id", how="inner")

        # Temporal guard: drop rows where the outcome post-dates the cutoff
        if "outcome_timestamp" in merged.columns:
            cutoff_ts = pd.Timestamp(cutoff).tz_localize("UTC") if cutoff.tzinfo is None else pd.Timestamp(cutoff)
            before_cutoff = pd.to_datetime(merged["outcome_timestamp"], utc=True, errors="coerce") < cutoff_ts
            merged = merged[before_cutoff].reset_index(drop=True)

        # Drop duplicate alert_id rows; keep the most recent outcome per alert
        merged = (
            merged
            .sort_values("outcome_timestamp", ascending=False)
            .drop_duplicates(subset=["alert_id"])
            .reset_index(drop=True)
        )
        return merged
