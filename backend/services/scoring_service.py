from __future__ import annotations

import numpy as np
import pandas as pd

from models.ml_model_service import MLModelService


class EnterpriseScoringService:
    def __init__(self, ml_service: MLModelService) -> None:
        self._ml_service = ml_service

    def run_anomaly_detection(
        self,
        df: pd.DataFrame,
        feature_matrix: pd.DataFrame,
        tenant_id: str = "default",
        strategy: str = "active_approved",
    ) -> pd.DataFrame:
        if feature_matrix.empty:
            out = df.copy()
            out["anomaly_score"] = 0.0
            return out

        inference = self.predict(tenant_id=tenant_id, feature_matrix=feature_matrix, strategy=strategy)
        scores = pd.to_numeric(pd.Series(inference.get("scores") or []), errors="coerce").fillna(0.0)
        if len(scores) < len(df):
            scores = pd.concat([scores, pd.Series(np.zeros(len(df) - len(scores)))], ignore_index=True)

        out = df.copy()
        out["anomaly_score"] = np.clip(scores.iloc[: len(df)].astype(float).to_numpy(), 0.0, 100.0)
        out["model_version"] = str(inference.get("model_version") or "unknown")
        return out

    def generate_explainability_drivers(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "top_feature_contributions_json" not in out.columns:
            out["top_feature_contributions_json"] = "[]"
        if "top_features_json" not in out.columns:
            out["top_features_json"] = "[]"
        return out

    def predict(self, tenant_id: str, feature_matrix: pd.DataFrame, strategy: str = "active_approved") -> dict:
        return self._ml_service.predict(tenant_id=tenant_id, features=feature_matrix, strategy=strategy)
