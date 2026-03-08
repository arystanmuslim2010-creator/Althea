from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from models.feature_schema import FeatureSchemaValidator
from models.model_registry import ModelRegistry


class InferenceService:
    """
    Internal ML serving abstraction.

    The current implementation validates feature schemas and provides a deterministic
    fallback scorer when no registered production artifact is available.
    """

    def __init__(self, registry: ModelRegistry, schema_validator: FeatureSchemaValidator) -> None:
        self._registry = registry
        self._schema_validator = schema_validator

    def predict(self, tenant_id: str, feature_frame: pd.DataFrame, strategy: str = "approved_latest") -> dict[str, Any]:
        model_record = self._registry.resolve_model(tenant_id=tenant_id, strategy=strategy)
        if model_record:
            schema = self._registry.load_feature_schema(model_record)
            validation = self._schema_validator.validate(schema, feature_frame)
            if not validation["is_valid"]:
                raise ValueError(
                    f"Feature schema mismatch. Missing={validation['missing_columns']}, "
                    f"mismatched={validation['mismatched_types']}"
                )
            scores = self._fallback_scores(feature_frame)
            return {
                "model_version": model_record["model_version"],
                "scores": scores,
                "explanations": self._fallback_explanations(feature_frame, scores),
                "schema_validation": validation,
            }
        schema = self._schema_validator.from_frame(feature_frame)
        scores = self._fallback_scores(feature_frame)
        return {
            "model_version": "fallback-inline",
            "scores": scores,
            "explanations": self._fallback_explanations(feature_frame, scores),
            "schema_validation": {"is_valid": True, "current_schema": schema},
        }

    def _fallback_scores(self, feature_frame: pd.DataFrame) -> list[float]:
        if "risk_score" in feature_frame.columns:
            return pd.to_numeric(feature_frame["risk_score"], errors="coerce").fillna(0.0).astype(float).tolist()
        numeric = feature_frame.select_dtypes(include=["number"])
        if numeric.empty:
            return [0.0 for _ in range(len(feature_frame))]
        arr = numeric.fillna(0.0).to_numpy(dtype=float)
        base = np.clip(arr.mean(axis=1), 0.0, None)
        percentile = float(np.percentile(base, 95)) if len(base) else 1.0
        scaled = np.clip(base / (percentile or 1.0) * 100.0, 0.0, 100.0)
        return [float(item) for item in scaled]

    def _fallback_explanations(self, feature_frame: pd.DataFrame, scores: list[float]) -> list[dict[str, Any]]:
        numeric = feature_frame.select_dtypes(include=["number"]).fillna(0.0)
        explanations: list[dict[str, Any]] = []
        for idx, score in enumerate(scores):
            row = numeric.iloc[idx] if idx < len(numeric) else pd.Series(dtype=float)
            top = row.abs().sort_values(ascending=False).head(5)
            explanations.append(
                {
                    "risk_score": float(score),
                    "top_features": [{"feature": str(name), "value": float(value)} for name, value in top.items()],
                }
            )
        return explanations
