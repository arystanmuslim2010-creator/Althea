"""Investigation time inference service.

Loads the published time model(s) from the registry and produces
p50 and p90 resolution time estimates for a batch of alerts.

The time model is a LightGBM quantile regressor trained on
``resolution_hours_log`` (log1p hours). This service un-transforms
predictions back to hours.

Design mirrors InferenceService to maintain consistent patterns.
"""
from __future__ import annotations

import io
import json
import logging
from collections import OrderedDict
from typing import Any

import numpy as np
import pandas as pd
from joblib import load as joblib_load

logger = logging.getLogger("althea.models.time_service")

try:
    import lightgbm as lgb
except Exception:
    lgb = None


class InvestigationTimeService:
    """Produce investigation time estimates from a published time model.

    Resolves the time model artifact from the registry using a
    purpose-specific strategy (``time_model_latest``).

    Returns estimates in hours for both p50 (expected) and p90 (worst-case).
    """

    def __init__(
        self,
        registry,
        object_storage,
        max_cached_models: int = 3,
    ) -> None:
        self._registry = registry
        self._storage = object_storage
        self._model_cache: OrderedDict[str, Any] = OrderedDict()
        self._max_cached = max(1, int(max_cached_models))

    def predict(
        self,
        tenant_id: str,
        feature_frame: pd.DataFrame,
        model_version: str | None = None,
    ) -> dict[str, Any]:
        """Predict investigation time for a batch of alerts.

        Parameters
        ----------
        tenant_id    : tenant identifier
        feature_frame: features aligned to time model schema
        model_version: specific version to use; if None resolves 'latest time model'

        Returns
        -------
        dict with keys:
            p50_hours : list[float] — median resolution time estimate per alert
            p90_hours : list[float] — 90th-percentile estimate per alert
            model_version : str
            schema_valid : bool
        """
        if feature_frame is None or feature_frame.empty:
            return {"p50_hours": [], "p90_hours": [], "model_version": "none", "schema_valid": False}

        model_record = self._resolve_time_model(tenant_id, model_version)
        if not model_record:
            # No time model available; return safe defaults
            n = len(feature_frame)
            return {
                "p50_hours": [24.0] * n,
                "p90_hours": [72.0] * n,
                "model_version": "default_fallback",
                "schema_valid": False,
                "warning": "No published time model found; using default estimates.",
            }

        model_p50 = self._load_model(model_record, variant="p50")
        p50_log = self._raw_predict(model_p50, feature_frame)
        p50_hours = list(np.clip(np.expm1(p50_log), 0.0, 8760.0).round(2))

        # Optional p90 model
        p90_hours: list[float]
        p90_uri = (model_record.get("training_metadata_json") or {}).get("p90_model_uri")
        if p90_uri:
            model_p90 = self._load_artifact_direct(p90_uri, model_record)
            if model_p90 is not None:
                p90_log = self._raw_predict(model_p90, feature_frame)
                p90_hours = list(np.clip(np.expm1(p90_log), 0.0, 8760.0).round(2))
            else:
                p90_hours = [min(h * 2.5, 8760.0) for h in p50_hours]
        else:
            # Derive p90 from p50 with a scale factor (conservative approximation)
            p90_hours = [min(h * 2.5, 8760.0) for h in p50_hours]

        version = str(model_record.get("model_version") or "unknown")
        logger.info(
            json.dumps(
                {
                    "event": "time_model_inference",
                    "tenant_id": tenant_id,
                    "model_version": version,
                    "rows": len(feature_frame),
                    "median_p50_hours": float(np.median(p50_hours)) if p50_hours else 0.0,
                },
                ensure_ascii=True,
            )
        )

        return {
            "p50_hours": p50_hours,
            "p90_hours": p90_hours,
            "model_version": version,
            "schema_valid": True,
        }

    # ------------------------------------------------------------------

    def _resolve_time_model(
        self, tenant_id: str, model_version: str | None = None
    ) -> dict[str, Any] | None:
        """Find the latest time model for this tenant."""
        versions = self._registry._repository.list_model_versions(tenant_id)
        if not versions:
            return None

        # Filter to time models (stored under 'time-*' version prefix)
        time_models = [
            v for v in versions
            if str(v.get("model_version", "")).startswith("time-")
            or (v.get("training_metadata_json") or {}).get("model_purpose") == "investigation_time"
        ]

        if model_version:
            target = next((v for v in time_models if v.get("model_version") == model_version), None)
            return target

        if not time_models:
            return None

        return sorted(time_models, key=lambda v: v.get("created_at", ""), reverse=True)[0]

    def _load_model(self, model_record: dict[str, Any], variant: str = "p50") -> Any:
        key = f"{model_record.get('tenant_id')}:{model_record.get('model_version')}:{variant}"
        if key in self._model_cache:
            m = self._model_cache.pop(key)
            self._model_cache[key] = m
            return m

        artifact = self._registry.load_model_artifact(model_record)
        model = self._deserialize(artifact)
        if len(self._model_cache) >= self._max_cached:
            self._model_cache.popitem(last=False)
        self._model_cache[key] = model
        return model

    def _load_artifact_direct(self, uri: str, model_record: dict[str, Any]) -> Any | None:
        """Load a secondary model artifact (e.g. p90) from object storage."""
        try:
            artifact = self._storage.get_bytes(uri)
            return self._deserialize(artifact)
        except Exception as exc:
            logger.warning("Failed to load p90 model artifact from %s: %s", uri, exc)
            return None

    @staticmethod
    def _deserialize(artifact: bytes) -> Any:
        if lgb is not None:
            try:
                booster = lgb.Booster(model_str=artifact.decode("utf-8"))
                return booster
            except Exception:
                pass
        return joblib_load(io.BytesIO(artifact))

    @staticmethod
    def _raw_predict(model: Any, X: pd.DataFrame) -> np.ndarray:
        feature_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
        X_num = X[feature_cols].replace([np.inf, -np.inf], 0.0).fillna(0.0)
        if hasattr(model, "predict"):
            pred = model.predict(X_num)
        else:
            pred = np.zeros(len(X_num))
        return np.asarray(pred, dtype=float)
