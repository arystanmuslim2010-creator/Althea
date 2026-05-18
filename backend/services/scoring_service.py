"""Scoring service — produces a structured scoring bundle per alert.

Replaces the previous single anomaly-score design with a full bundle:
    - escalation_score  [0, 100] from the escalation model
    - escalation_prob   [0, 1] calibrated probability
    - p50_hours         expected investigation time (hours, p50)
    - p90_hours         expected investigation time (hours, p90)
    - model_version     escalation model version
    - time_model_version time model version
    - explanations      per-row explanation dicts

Backward-compatible: ``run_anomaly_detection()`` still returns a DataFrame
with ``anomaly_score`` for any callers that haven't migrated yet.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from models.ml_model_service import MLModelService
from services.time_scoring_service import TimeScoringService

logger = logging.getLogger("althea.services.scoring")

RISK_BAND_THRESHOLDS: tuple[tuple[str, float], ...] = (
    ("High", 0.85),
    ("Medium", 0.60),
    ("Low", 0.0),
)


def normalize_risk_score(value: Any) -> float:
    raw = float(value or 0.0)
    normalized = raw / 100.0 if raw > 1.0 else raw
    return float(np.clip(normalized, 0.0, 1.0))


def derive_risk_band(value: Any) -> str:
    normalized = normalize_risk_score(value)
    for label, threshold in RISK_BAND_THRESHOLDS:
        if normalized >= threshold:
            return label
    return "Low"


def derive_score_method(model_version: Any, explicit_method: Any = None) -> str:
    requested = str(explicit_method or "").strip().lower()
    if requested in {"heuristic", "baseline_ml", "production_model"}:
        return requested

    version = str(model_version or "").strip().lower()
    if not version or version in {"none", "unknown"}:
        return "heuristic"
    if any(token in version for token in ("baseline", "bootstrap", "fallback", "dev")):
        return "baseline_ml"
    return "production_model"


def build_score_contract(
    record: dict[str, Any],
    *,
    priority_rank: int | None = None,
    score_created_at: str | None = None,
) -> dict[str, Any]:
    out = dict(record or {})
    risk_score = float(out.get("risk_score", 0.0) or 0.0)
    out["risk_score"] = risk_score
    out["risk_score_normalized"] = normalize_risk_score(risk_score)
    out["risk_band"] = str(out.get("risk_band") or derive_risk_band(risk_score))
    out["priority_rank"] = priority_rank if priority_rank is not None else out.get("priority_rank")
    out["score_version"] = str(
        out.get("score_version")
        or out.get("model_version")
        or out.get("time_model_version")
        or "unknown"
    )
    out["score_method"] = derive_score_method(out.get("model_version"), out.get("score_method"))
    out["score_created_at"] = str(
        out.get("score_created_at")
        or score_created_at
        or out.get("created_at")
        or out.get("timestamp")
        or datetime.now(timezone.utc).isoformat()
    )
    return out


class EnterpriseScoringService:
    """Bundle-based scoring: escalation + investigation time in one call."""

    def __init__(
        self,
        ml_service: MLModelService,
        time_scoring_service: TimeScoringService | None = None,
    ) -> None:
        self._ml_service = ml_service
        self._time_scoring_service = time_scoring_service

    def score_bundle(
        self,
        tenant_id: str,
        feature_matrix: pd.DataFrame,
        alerts_df: pd.DataFrame | None = None,
        strategy: str = "active_approved",
    ) -> dict[str, Any]:
        """Produce a complete scoring bundle for a batch of alerts.

        Returns a structured dict with escalation and time scores.
        """
        if feature_matrix is None or feature_matrix.empty:
            return {
                "scores": [],
                "escalation_probs": [],
                "p50_hours": [],
                "p90_hours": [],
                "model_version": "none",
                "time_model_version": "none",
                "explanations": [],
            }

        # Escalation scores
        inference = self._ml_service.predict(
            tenant_id=tenant_id,
            features=feature_matrix,
            strategy=strategy,
        )
        scores = list(inference.get("scores") or [])
        probs = [s / 100.0 for s in scores]

        # Investigation time scores
        p50_hours: list[float] = []
        p90_hours: list[float] = []
        time_model_version = "none"
        if self._time_scoring_service is not None:
            try:
                time_result = self._time_scoring_service.score(
                    tenant_id=tenant_id,
                    feature_matrix=feature_matrix,
                    alerts_df=alerts_df,
                )
                p50_hours = time_result.get("p50_hours") or [24.0] * len(scores)
                p90_hours = time_result.get("p90_hours") or [72.0] * len(scores)
                time_model_version = str(time_result.get("model_version") or "none")
            except Exception as exc:
                logger.warning("Time scoring failed (non-fatal): %s", exc)
                p50_hours = [24.0] * len(scores)
                p90_hours = [72.0] * len(scores)
        else:
            p50_hours = [24.0] * len(scores)
            p90_hours = [72.0] * len(scores)

        return {
            "scores": scores,
            "escalation_probs": probs,
            "p50_hours": p50_hours,
            "p90_hours": p90_hours,
            "model_version": str(inference.get("model_version") or "unknown"),
            "time_model_version": time_model_version,
            "explanations": list(inference.get("explanations") or []),
            "schema_validation": inference.get("schema_validation", {}),
        }

    def run_anomaly_detection(
        self,
        df: pd.DataFrame,
        feature_matrix: pd.DataFrame,
        tenant_id: str = "default",
        strategy: str = "active_approved",
    ) -> pd.DataFrame:
        """Backward-compatible anomaly detection entrypoint.

        Returns a DataFrame with ``anomaly_score`` and ``model_version``
        columns, matching the previous contract for existing pipeline code.
        """
        if feature_matrix.empty:
            out = df.copy()
            out["anomaly_score"] = 0.0
            return out

        bundle = self.score_bundle(
            tenant_id=tenant_id,
            feature_matrix=feature_matrix,
            alerts_df=df,
            strategy=strategy,
        )
        scores = pd.to_numeric(pd.Series(bundle.get("scores") or []), errors="coerce").fillna(0.0)
        if len(scores) < len(df):
            scores = pd.concat([scores, pd.Series(np.zeros(len(df) - len(scores)))], ignore_index=True)

        out = df.copy()
        out["anomaly_score"] = np.clip(scores.iloc[: len(df)].astype(float).to_numpy(), 0.0, 100.0)
        out["model_version"] = str(bundle.get("model_version") or "unknown")
        return out

    def predict(
        self,
        tenant_id: str,
        feature_matrix: pd.DataFrame,
        strategy: str = "active_approved",
    ) -> dict:
        """Thin wrapper around ml_service.predict for direct callers."""
        return self._ml_service.predict(
            tenant_id=tenant_id,
            features=feature_matrix,
            strategy=strategy,
        )

    def generate_explainability_drivers(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "top_feature_contributions_json" not in out.columns:
            out["top_feature_contributions_json"] = "[]"
        if "top_features_json" not in out.columns:
            out["top_features_json"] = "[]"
        return out
