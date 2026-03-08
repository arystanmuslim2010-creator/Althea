from __future__ import annotations

import pandas as pd

from models.ml_model_service import MLModelService
from src.services.scoring_service import ScoringService as LegacyScoringService


class EnterpriseScoringService:
    def __init__(self, ml_service: MLModelService) -> None:
        self._ml_service = ml_service
        self._legacy = LegacyScoringService()

    def run_anomaly_detection(self, df: pd.DataFrame, feature_matrix: pd.DataFrame) -> pd.DataFrame:
        return self._legacy.run_anomaly_detection(df, feature_matrix)

    def generate_explainability_drivers(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._legacy.generate_explainability_drivers(df)

    def predict(self, tenant_id: str, feature_matrix: pd.DataFrame, strategy: str = "approved_latest") -> dict:
        return self._ml_service.predict(tenant_id=tenant_id, features=feature_matrix, strategy=strategy)
