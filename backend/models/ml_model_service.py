from __future__ import annotations

import time

import pandas as pd

from core.observability import record_ml_inference
from models.inference_service import InferenceService
from models.model_registry import ModelRegistry


class MLModelService:
    def __init__(self, registry: ModelRegistry, inference_service: InferenceService) -> None:
        self._registry = registry
        self._inference = inference_service

    def predict(self, tenant_id: str, features: pd.DataFrame, strategy: str = "active_approved") -> dict:
        started = time.perf_counter()
        result = self._inference.predict(tenant_id=tenant_id, feature_frame=features, strategy=strategy)
        record_ml_inference(
            model_version=str(result.get("model_version") or "unknown"),
            duration_seconds=time.perf_counter() - started,
        )
        return result

    def list_versions(self, tenant_id: str) -> list[dict]:
        return self._registry._repository.list_model_versions(tenant_id)
