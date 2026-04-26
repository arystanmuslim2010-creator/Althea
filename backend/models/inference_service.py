from __future__ import annotations

import hashlib
import io
import json
import logging
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from joblib import dump as joblib_dump
from joblib import load as joblib_load
from sklearn.ensemble import RandomForestClassifier

from models.feature_schema import FeatureSchemaValidator
from models.model_registry import ModelRegistry
from models.explainability_service import ExplainabilityService, get_explainability_service

try:  # pragma: no cover - optional import
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

try:  # pragma: no cover - optional import
    import xgboost as xgb
except Exception:  # pragma: no cover
    xgb = None

logger = logging.getLogger("althea.inference")


def verify_model_artifact_integrity(artifact_bytes: bytes, expected_sha256: str | None) -> bool:
    expected = str(expected_sha256 or "").strip().lower()
    if not expected:
        return True
    actual = hashlib.sha256(artifact_bytes or b"").hexdigest().lower()
    if actual != expected:
        raise ValueError("Model artifact integrity check failed.")
    return True


class InferenceService:
    """ML inference service that serves registered model artifacts only."""

    def __init__(
        self,
        registry: ModelRegistry,
        schema_validator: FeatureSchemaValidator,
        online_feature_store=None,
        feature_registry=None,
        explainability_service: ExplainabilityService | None = None,
        allow_dev_models: bool = False,
        max_cached_models: int = 5,
    ) -> None:
        self._registry = registry
        self._schema_validator = schema_validator
        self._online_feature_store = online_feature_store
        self._feature_registry = feature_registry
        self._model_cache: OrderedDict[str, Any] = OrderedDict()
        self._max_artifact_bytes = 100 * 1024 * 1024
        self._explainability_service = explainability_service or get_explainability_service()
        self._allow_dev_models = bool(allow_dev_models)
        self._max_cached_models = max(1, int(max_cached_models or 5))

    def predict(
        self,
        tenant_id: str,
        feature_frame: pd.DataFrame,
        strategy: str = "active_approved",
        alert_ids: list[str] | None = None,
        feature_version: str | None = None,
    ) -> dict[str, Any]:
        if (feature_frame is None or feature_frame.empty) and alert_ids:
            version = str(feature_version or "v1")
            if self._feature_registry is not None:
                # Use active registry version when available to support online inference versioning.
                try:
                    candidate = self._feature_registry.get_active_version(tenant_id=tenant_id, feature_name="feature_prior_score")
                    if candidate:
                        version = str(candidate)
                except Exception:
                    pass
            if self._online_feature_store is None:
                raise ValueError("Online feature store is not configured for alert-id based inference.")
            feature_frame = self._online_feature_store.get_many(
                tenant_id=tenant_id,
                alert_ids=[str(item) for item in alert_ids],
                version=version,
            )
            if feature_frame.empty:
                raise ValueError("No online features found for requested alert_ids.")

        resolved_alert_ids: list[str] | None = [str(item) for item in (alert_ids or []) if str(item)]
        if not resolved_alert_ids and feature_frame is not None and "alert_id" in feature_frame.columns:
            resolved_alert_ids = [str(item) for item in feature_frame["alert_id"].tolist()]

        model_record = self._registry.resolve_model(tenant_id=tenant_id, strategy=strategy)
        if not model_record:
            if not self._allow_dev_models:
                raise RuntimeError("Auto-bootstrap disabled in production")
            self._auto_bootstrap_model(tenant_id=tenant_id, feature_frame=feature_frame)
            model_record = self._registry.resolve_model(tenant_id=tenant_id, strategy=strategy)
        if not model_record:
            raise ValueError("No approved model artifact available for inference.")

        schema = self._registry.load_feature_schema(model_record)
        feature_frame, alignment = self._align_frame_to_schema(feature_frame, schema)
        validation = self._schema_validator.validate(schema, feature_frame)
        validation["imputed_columns"] = list(alignment.get("imputed_columns") or [])
        validation["dropped_columns"] = list(alignment.get("dropped_columns") or [])
        if not validation["is_valid"]:
            raise ValueError(
                f"Feature schema mismatch. Missing={validation['missing_columns']}, "
                f"mismatched={validation['mismatched_types']}"
            )
        if validation["imputed_columns"] or validation["dropped_columns"]:
            logger.warning(
                json.dumps(
                    {
                        "event": "feature_schema_alignment_applied",
                        "tenant_id": tenant_id,
                        "model_version": str(model_record.get("model_version") or "unknown"),
                        "imputed_columns": validation["imputed_columns"],
                        "dropped_columns": validation["dropped_columns"],
                    },
                    ensure_ascii=True,
                )
            )

        metadata = dict(model_record.get("training_metadata_json") or {})
        feature_schema_version = str(metadata.get("feature_schema_version") or schema.get("version") or "v1")
        model_id = str(model_record.get("id") or "")
        model_version = str(model_record.get("model_version") or "unknown")

        model = self._load_model(model_record)
        probabilities = self._predict_proba(model=model, feature_frame=feature_frame)
        scores = np.clip(probabilities * 100.0, 0.0, 100.0)
        explanations = self._build_explanations(
            tenant_id=tenant_id,
            model=model,
            feature_frame=feature_frame,
            model_version=model_version,
            feature_schema_version=feature_schema_version,
            alert_ids=resolved_alert_ids,
        )

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
            model = self._model_cache.pop(cache_key)
            self._model_cache[cache_key] = model
            return model

        artifact_uri = str(model_record.get("artifact_uri") or "")
        if not artifact_uri.startswith("models/"):
            raise ValueError("Rejected model artifact URI: must be in models/ prefix.")
        artifact = self._registry.load_model_artifact(model_record)
        if len(artifact) <= 0 or len(artifact) > self._max_artifact_bytes:
            raise ValueError("Rejected model artifact due to invalid size.")
        expected_sha256 = (
            model_record.get("artifact_sha256")
            or model_record.get("sha256")
            or dict(model_record.get("training_metadata_json") or {}).get("artifact_sha256")
        )
        try:
            verify_model_artifact_integrity(artifact, expected_sha256)
        except ValueError:
            logger.warning(
                "Model artifact integrity check failed",
                extra={
                    "tenant_id": str(model_record.get("tenant_id") or ""),
                    "model_version": str(model_record.get("model_version") or "unknown"),
                },
            )
            raise

        metadata = dict(model_record.get("training_metadata_json") or {})
        artifact_format = str(metadata.get("artifact_format") or "").lower().strip()
        model = self._deserialize_model(artifact=artifact, artifact_format=artifact_format)
        self._assert_safe_model(model)
        if len(self._model_cache) >= self._max_cached_models:
            self._model_cache.popitem(last=False)
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

    def _build_explanations(
        self,
        tenant_id: str,
        model: Any,
        feature_frame: pd.DataFrame,
        model_version: str = "unknown",
        feature_schema_version: str = "v1",
        alert_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Build model-based explanations using the unified explainability service.

        This method uses SHAP (SHapley Additive exPlanations) when available
        to compute faithful feature attribution. When SHAP is unavailable,
        it falls back to heuristic methods with explicit labeling.

        Args:
            model: Trained model object used for prediction
            feature_frame: Features used for scoring
            model_version: Version of the model

        Returns:
            List of explanation dicts, one per row in feature_frame.
            Each dict contains:
            - feature_attribution: List of {feature, value, shap_value} dicts
            - risk_reason_codes: Human-readable reason codes
            - explanation_method: "shap", "tree_shap", "numeric_fallback", or "unavailable"
            - explanation_status: "ok", "fallback", or "unavailable"
            - explanation_warning: Warning if fallback was used
            - explanation_warning_code: machine-readable warning code
        """
        explanations: list[dict[str, Any]] = []

        for idx in range(len(feature_frame)):
            row_frame = feature_frame.iloc[[idx]]  # Keep as DataFrame for SHAP
            alert_id = None
            if alert_ids and idx < len(alert_ids):
                alert_id = str(alert_ids[idx])
            explanation = self._explainability_service.generate_explanation(
                model=model,
                feature_frame=row_frame,
                model_version=model_version,
                tenant_id=tenant_id,
                alert_id=alert_id,
                feature_schema_version=feature_schema_version,
            )
            explanations.append(explanation)

        return explanations

    @staticmethod
    def _coerce_frame_to_schema(frame: pd.DataFrame, schema: dict[str, Any]) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame() if frame is None else frame
        out = frame.copy()
        expected = {str(item.get("name")): str(item.get("dtype")) for item in (schema.get("columns") or []) if item.get("name")}
        for name, dtype in expected.items():
            if name not in out.columns:
                continue
            raw = str(dtype).lower()
            if "float" in raw:
                out[name] = pd.to_numeric(out[name], errors="coerce").fillna(0.0).astype(float)
            elif "int" in raw:
                out[name] = pd.to_numeric(out[name], errors="coerce").fillna(0).astype(int)
            elif "bool" in raw:
                out[name] = out[name].astype(bool)
            elif "datetime" in raw or "timestamp" in raw:
                out[name] = pd.to_datetime(out[name], errors="coerce", utc=True)
            else:
                out[name] = out[name].astype(str)
        return out

    @staticmethod
    def _default_series_for_dtype(dtype: str, size: int, index: pd.Index) -> pd.Series:
        raw = str(dtype).lower()
        if "float" in raw:
            return pd.Series(np.zeros(size, dtype=float), index=index)
        if "int" in raw:
            return pd.Series(np.zeros(size, dtype=int), index=index)
        if "bool" in raw:
            return pd.Series([False] * size, index=index, dtype=bool)
        if "datetime" in raw or "timestamp" in raw:
            return pd.to_datetime(pd.Series(["1970-01-01T00:00:00Z"] * size, index=index), utc=True)
        return pd.Series([""] * size, index=index, dtype="object")

    def _align_frame_to_schema(self, frame: pd.DataFrame, schema: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, list[str]]]:
        if frame is None or frame.empty:
            return (pd.DataFrame() if frame is None else frame), {"imputed_columns": [], "dropped_columns": []}

        out = frame.copy()
        expected_items = [
            (str(item.get("name")), str(item.get("dtype")))
            for item in (schema.get("columns") or [])
            if item.get("name")
        ]
        expected_columns = [name for name, _ in expected_items]
        expected_set = set(expected_columns)

        imputed: list[str] = []
        for name, dtype in expected_items:
            if name in out.columns:
                continue
            out[name] = self._default_series_for_dtype(dtype=dtype, size=len(out), index=out.index)
            imputed.append(name)

        dropped = [str(column) for column in list(out.columns) if str(column) not in expected_set]
        if expected_columns:
            out = out[expected_columns]

        out = self._coerce_frame_to_schema(out, schema)
        return out, {"imputed_columns": imputed, "dropped_columns": dropped}

    def _auto_bootstrap_model(self, tenant_id: str, feature_frame: pd.DataFrame) -> None:
        if feature_frame is None or feature_frame.empty:
            return
        try:
            X = feature_frame.copy()
            numeric = X.select_dtypes(include=["number"]).fillna(0.0)
            if numeric.empty:
                return
            score_proxy = numeric.mean(axis=1)
            threshold = float(score_proxy.quantile(0.7))
            y = (score_proxy >= threshold).astype(int)
            if int(y.nunique()) < 2:
                y = pd.Series(np.where(np.arange(len(X)) % 2 == 0, 1, 0))
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X, y)

            buffer = io.BytesIO()
            joblib_dump(model, buffer)
            schema = self._schema_validator.from_frame(X)
            dataset_hash = hashlib.sha256(pd.util.hash_pandas_object(X, index=True).values.tobytes()).hexdigest()
            self._registry.register_model(
                tenant_id=tenant_id,
                artifact_bytes=buffer.getvalue(),
                training_dataset_hash=dataset_hash,
                feature_schema=schema,
                metrics={
                    "bootstrap": True,
                    "rows": int(len(X)),
                    "features": int(X.shape[1]),
                    "positive_rate": float(y.mean()),
                },
                training_metadata={
                    "artifact_format": "joblib",
                    "is_active": True,
                    "bootstrap_model": True,
                    "feature_schema_version": "v1",
                },
                approval_status="approved",
                approved_by="auto-bootstrap",
            )
        except Exception:
            logger.exception("Failed to auto-bootstrap baseline model for tenant %s", tenant_id)
