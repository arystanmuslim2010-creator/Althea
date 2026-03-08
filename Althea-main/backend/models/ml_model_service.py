from __future__ import annotations

import pandas as pd

from models.inference_service import InferenceService
from models.model_registry import ModelRegistry


class MLModelService:
    def __init__(self, registry: ModelRegistry, inference_service: InferenceService) -> None:
        self._registry = registry
        self._inference = inference_service

    def predict(self, tenant_id: str, features: pd.DataFrame, strategy: str = "approved_latest") -> dict:
        return self._inference.predict(tenant_id=tenant_id, feature_frame=features, strategy=strategy)

    def list_versions(self, tenant_id: str) -> list[dict]:
        return self._registry._repository.list_model_versions(tenant_id)
