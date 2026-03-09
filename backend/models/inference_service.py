from __future__ import annotations

import io
import json
import logging
import pickle
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from joblib import load as joblib_load

from models.feature_schema import FeatureSchemaValidator
from models.model_registry import ModelRegistry

try:  # pragma: no cover - optional import
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

try:  # pragma: no cover - optional import
    import xgboost as xgb
except Exception:  # pragma: no cover
    xgb = None

logger = logging.getLogger("althea.inference")


class InferenceService:
    """ML inference service that serves registered model artifacts only."""

    def __init__(self, registry: ModelRegistry, schema_validator: FeatureSchemaValidator) -> None:
        self._registry = registry
        self._schema_validator = schema_validator
        self._model_cache: dict[str, Any] = {}
        self._max_artifact_bytes = 100 * 1024 * 1024

    def predict(self, tenant_id: str, feature_frame: pd.DataFrame, strategy: str = "active_approved") -> dict[str, Any]:
        model_record = self._registry.resolve_model(tenant_id=tenant_id, strategy=strategy)
        if not model_record:
            raise ValueError("No approved model artifact available for inference.")

        schema = self._registry.load_feature_schema(model_record)
        validation = self._schema_validator.validate(schema, feature_frame)
        if not validation["is_valid"]:
            raise ValueError(
                f"Feature schema mismatch. Missing={validation['missing_columns']}, "
                f"mismatched={validation['mismatched_types']}"
            )

        model = self._load_model(model_record)
        probabilities = self._predict_proba(model=model, feature_frame=feature_frame)
        scores = np.clip(probabilities * 100.0, 0.0, 100.0)
        explanations = self._build_explanations(feature_frame, scores.tolist())

        metadata = dict(model_record.get("training_metadata_json") or {})
        feature_schema_version = str(metadata.get("feature_schema_version") or schema.get("version") or "v1")
        model_id = str(model_record.get("id") or "")
        model_version = str(model_record.get("model_version") or "unknown")
        timestamp = datetime.now(timezone.utc).isoformat()

        logger.info(
            json.dumps(
                {
                    "event": "ml_inference",
                    "tenant_id": tenant_id,
                    "model_id": model_id,
                    "model_version": model_version,
                    "feature_schema_version": feature_schema_version,
                    "timestamp": timestamp,
                    "rows": int(len(feature_frame)),
                },
                ensure_ascii=True,
            )
        )

        return {
            "model_id": model_id,
            "model_version": model_version,
            "feature_schema_version": feature_schema_version,
            "timestamp": timestamp,
            "scores": [float(value) for value in scores.tolist()],
            "explanations": explanations,
            "schema_validation": validation,
        }

    def _load_model(self, model_record: dict[str, Any]) -> Any:
        cache_key = f"{model_record.get('tenant_id')}:{model_record.get('model_version')}"
        if cache_key in self._model_cache:
            return self._model_cache[cache_key]

        artifact_uri = str(model_record.get("artifact_uri") or "")
        if not artifact_uri.startswith("models/"):
            raise ValueError("Rejected model artifact URI: must be in models/ prefix.")
        artifact = self._registry.load_model_artifact(model_record)
        if len(artifact) <= 0 or len(artifact) > self._max_artifact_bytes:
            raise ValueError("Rejected model artifact due to invalid size.")

        metadata = dict(model_record.get("training_metadata_json") or {})
        artifact_format = str(metadata.get("artifact_format") or "").lower().strip()
        model = self._deserialize_model(artifact=artifact, artifact_format=artifact_format)
        self._assert_safe_model(model)
        self._model_cache[cache_key] = model
        return model

    def _deserialize_model(self, artifact: bytes, artifact_format: str) -> Any:
        if artifact_format in {"lightgbm_booster", "lgb_booster"} and lgb is not None:
            return lgb.Booster(model_str=artifact.decode("utf-8"))
        if artifact_format in {"xgboost_booster", "xgb_booster"} and xgb is not None:
            booster = xgb.Booster()
            booster.load_model(bytearray(artifact))
            return booster

        try:
            return joblib_load(io.BytesIO(artifact))
        except Exception:
            try:
                return pickle.loads(artifact)
            except Exception as exc:
                raise ValueError(f"Failed to deserialize model artifact: {exc}") from exc

    def _assert_safe_model(self, model: Any) -> None:
        class_path = f"{model.__class__.__module__}.{model.__class__.__name__}"
        allowed_prefixes = ("sklearn.", "lightgbm.", "xgboost.")
        if not class_path.startswith(allowed_prefixes):
            raise ValueError(f"Rejected model class for inference safety: {class_path}")
        if not hasattr(model, "predict_proba") and not hasattr(model, "predict"):
            raise ValueError(f"Model class does not expose prediction API: {class_path}")

    def _predict_proba(self, model: Any, feature_frame: pd.DataFrame) -> np.ndarray:
        if hasattr(model, "predict_proba"):
            pred = np.asarray(model.predict_proba(feature_frame))
            if pred.ndim == 1:
                return np.clip(pred.astype(float), 0.0, 1.0)
            if pred.shape[1] == 1:
                return np.clip(pred[:, 0].astype(float), 0.0, 1.0)
            return np.clip(pred[:, 1].astype(float), 0.0, 1.0)

        if lgb is not None and isinstance(model, lgb.Booster):
            pred = np.asarray(model.predict(feature_frame))
            if pred.ndim > 1:
                pred = pred[:, 1]
            return np.clip(pred.astype(float), 0.0, 1.0)

        if xgb is not None and isinstance(model, xgb.Booster):
            dmatrix = xgb.DMatrix(feature_frame)
            pred = np.asarray(model.predict(dmatrix))
            if pred.ndim > 1:
                pred = pred[:, 1]
            return np.clip(pred.astype(float), 0.0, 1.0)

        pred = np.asarray(model.predict(feature_frame))
        if pred.ndim > 1:
            pred = pred[:, 1]
        return np.clip(pred.astype(float), 0.0, 1.0)

    def _build_explanations(self, feature_frame: pd.DataFrame, scores: list[float]) -> list[dict[str, Any]]:
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
