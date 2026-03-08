from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository


class ModelRegistry:
    def __init__(self, repository: EnterpriseRepository, object_storage: ObjectStorage) -> None:
        self._repository = repository
        self._object_storage = object_storage

    def register_model(
        self,
        tenant_id: str,
        artifact_bytes: bytes,
        training_dataset_hash: str,
        feature_schema: dict[str, Any],
        metrics: dict[str, Any],
        training_metadata: dict[str, Any] | None = None,
        approval_status: str = "pending",
        model_version: str | None = None,
        approved_by: str | None = None,
    ) -> dict[str, Any]:
        version = model_version or f"model-{uuid.uuid4().hex[:12]}"
        artifact_uri = f"models/{tenant_id}/{version}/artifact.bin"
        schema_uri = f"models/{tenant_id}/{version}/feature_schema.json"
        metrics_uri = f"models/{tenant_id}/{version}/metrics.json"
        self._object_storage.put_bytes(artifact_uri, artifact_bytes)
        self._object_storage.put_json(schema_uri, feature_schema)
        self._object_storage.put_json(metrics_uri, metrics)
        record = {
            "tenant_id": tenant_id,
            "model_version": version,
            "training_dataset_hash": training_dataset_hash,
            "feature_schema_uri": schema_uri,
            "feature_schema_hash": feature_schema.get("schema_hash", ""),
            "metrics_uri": metrics_uri,
            "approval_status": approval_status,
            "training_metadata_json": training_metadata or {},
            "approved_by": approved_by,
            "approved_at": datetime.now(timezone.utc).isoformat() if approval_status == "approved" else None,
            "artifact_uri": artifact_uri,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._repository.register_model_version(record)
        return record

    def resolve_model(self, tenant_id: str, strategy: str = "approved_latest") -> dict[str, Any] | None:
        versions = self._repository.list_model_versions(tenant_id)
        if not versions:
            return None
        approved = [item for item in versions if item.get("approval_status") == "approved"]
        candidates = approved or versions
        if strategy == "approved_latest":
            return sorted(candidates, key=lambda item: item.get("created_at", ""), reverse=True)[0]
        return candidates[0]

    def load_feature_schema(self, model_record: dict[str, Any]) -> dict[str, Any]:
        uri = model_record.get("feature_schema_uri")
        if not uri:
            return {}
        return self._object_storage.get_json(uri)

    def load_metrics(self, model_record: dict[str, Any]) -> dict[str, Any]:
        uri = model_record.get("metrics_uri")
        if not uri:
            return {}
        return self._object_storage.get_json(uri)
