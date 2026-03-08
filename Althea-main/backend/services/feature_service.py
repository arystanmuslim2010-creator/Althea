from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
from typing import Any

import pandas as pd

from models.feature_schema import FeatureSchemaValidator
from src import config as legacy_config
from src import features as legacy_features
from src.services.feature_service import FeatureService as LegacyFeatureService


class EnterpriseFeatureService:
    def __init__(self, schema_validator: FeatureSchemaValidator) -> None:
        self._schema = schema_validator
        self._legacy = LegacyFeatureService()

    def _legacy_cfg(self) -> SimpleNamespace:
        return SimpleNamespace(**{name: getattr(legacy_config, name) for name in dir(legacy_config) if name.isupper()})

    def load_transactions_csv(self, payload: bytes) -> pd.DataFrame:
        return self._legacy.load_transactions_csv(BytesIO(payload))

    def build_training_features(self, df: pd.DataFrame) -> dict[str, Any]:
        engineered, feature_groups = legacy_features.compute_behavioral_features(df.copy(), self._legacy_cfg())
        matrix = legacy_features.build_feature_matrix(engineered, feature_groups["all_feature_cols"])
        schema = self._schema.from_frame(matrix)
        return {
            "alerts_df": engineered,
            "feature_groups": feature_groups,
            "feature_matrix": matrix,
            "feature_schema": schema,
        }

    def build_inference_features(self, df: pd.DataFrame) -> dict[str, Any]:
        return self.build_training_features(df)

    def validate_feature_schema(self, expected_schema: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        return self._schema.validate(expected_schema, df)
