from __future__ import annotations

import numpy as np
import pandas as pd

from models.ml_model_service import MLModelService


class EnterpriseScoringService:
    def __init__(self, ml_service: MLModelService) -> None:
        self._ml_service = ml_service

    def run_anomaly_detection(self, df: pd.DataFrame, feature_matrix: pd.DataFrame) -> pd.DataFrame:
        if feature_matrix.empty:
            out = df.copy()
            out["anomaly_score"] = 0.0
            return out
        numeric = feature_matrix.select_dtypes(include=["number"]).fillna(0.0)
        z = (numeric - numeric.mean()) / (numeric.std(ddof=0) + 1e-6)
        out = df.copy()
        out["anomaly_score"] = np.clip(z.abs().mean(axis=1) * 25.0, 0.0, 100.0)
        return out

    def generate_explainability_drivers(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "top_feature_contributions_json" not in out.columns:
            out["top_feature_contributions_json"] = "[]"
        if "top_features_json" not in out.columns:
            out["top_features_json"] = "[]"
        return out

    def predict(self, tenant_id: str, feature_matrix: pd.DataFrame, strategy: str = "approved_latest") -> dict:
        return self._ml_service.predict(tenant_id=tenant_id, features=feature_matrix, strategy=strategy)
