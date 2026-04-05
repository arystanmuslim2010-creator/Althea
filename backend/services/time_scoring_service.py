"""Investigation time scoring service.

Wraps InvestigationTimeService and aligns time features from the
feature bundle for inference. Produces p50/p90 hour estimates that
are consumed downstream by the decision policy engine.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from models.investigation_time_service import InvestigationTimeService
from training.train_time_model import TIME_MODEL_FEATURE_COLS, MINIMAL_TIME_FEATURE_COLS

logger = logging.getLogger("althea.services.time_scoring")


class TimeScoringService:
    """Score investigation time from a feature bundle."""

    def __init__(self, time_service: InvestigationTimeService) -> None:
        self._time_service = time_service

    def score(
        self,
        tenant_id: str,
        feature_matrix: pd.DataFrame,
        alerts_df: pd.DataFrame | None = None,
    ) -> dict[str, Any]:
        """Produce time estimates for a batch of alerts.

        Parameters
        ----------
        tenant_id      : tenant identifier
        feature_matrix : numeric feature matrix from FeatureBundleService
        alerts_df      : optional original alerts DataFrame (for alert_ids)

        Returns
        -------
        dict with p50_hours, p90_hours lists (one per row) and model_version.
        """
        if feature_matrix is None or feature_matrix.empty:
            return {"p50_hours": [], "p90_hours": [], "model_version": "none"}

        # Select features that the time model expects
        aligned = self._align_features(feature_matrix)
        result = self._time_service.predict(
            tenant_id=tenant_id,
            feature_frame=aligned,
        )

        # Attach alert_ids if available
        if alerts_df is not None and "alert_id" in alerts_df.columns:
            result["alert_ids"] = [str(aid) for aid in alerts_df["alert_id"].tolist()]

        return result

    @staticmethod
    def _align_features(feature_matrix: pd.DataFrame) -> pd.DataFrame:
        """Select and order columns matching the time model feature schema."""
        available = set(feature_matrix.columns)
        full_cols = [c for c in TIME_MODEL_FEATURE_COLS if c in available]
        if full_cols:
            return feature_matrix[full_cols].copy()
        minimal = [c for c in MINIMAL_TIME_FEATURE_COLS if c in available]
        if minimal:
            return feature_matrix[minimal].copy()
        # Fall back to all numeric columns
        return feature_matrix.select_dtypes(include="number").copy()
