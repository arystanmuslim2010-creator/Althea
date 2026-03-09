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
        approval_status: str = "draft",
        model_version: str | None = None,
        approved_by: str | None = None,
        feature_schema_version: str = "v1",
    ) -> dict[str, Any]:
        version = model_version or f"model-{uuid.uuid4().hex[:12]}"
        artifact_uri = f"models/{tenant_id}/{version}/artifact.bin"
        schema_uri = f"models/{tenant_id}/{version}/feature_schema.json"
        metrics_uri = f"models/{tenant_id}/{version}/metrics.json"
        self._object_storage.put_bytes(artifact_uri, artifact_bytes)
        self._object_storage.put_json(schema_uri, feature_schema)
        self._object_storage.put_json(metrics_uri, metrics)
        lifecycle = dict(training_metadata or {})
        lifecycle.setdefault("feature_schema_version", feature_schema_version)
        lifecycle.setdefault("is_active", False)
        lifecycle.setdefault("lifecycle", {"registered_at": datetime.now(timezone.utc).isoformat()})

        record = {
            "tenant_id": tenant_id,
            "model_version": version,
            "training_dataset_hash": training_dataset_hash,
            "feature_schema_uri": schema_uri,
            "feature_schema_hash": feature_schema.get("schema_hash", ""),
            "metrics_uri": metrics_uri,
            "approval_status": approval_status,
            "training_metadata_json": lifecycle,
            "approved_by": approved_by,
            "approved_at": datetime.now(timezone.utc).isoformat() if approval_status == "approved" else None,
            "artifact_uri": artifact_uri,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._repository.register_model_version(record)
        if approval_status == "approved":
            self.set_active_model(tenant_id=tenant_id, model_version=version, actor=approved_by or "system")
        return record

    def resolve_model(self, tenant_id: str, strategy: str = "active_approved") -> dict[str, Any] | None:
        versions = self._repository.list_model_versions(tenant_id)
        if not versions:
            return None
        approved = [item for item in versions if str(item.get("approval_status", "")).lower() == "approved"]
        if strategy == "active_approved":
            active = [
                item
                for item in approved
                if bool((item.get("training_metadata_json") or {}).get("is_active"))
            ]
            if active:
                return sorted(active, key=lambda item: item.get("created_at", ""), reverse=True)[0]
            return sorted(approved, key=lambda item: item.get("approved_at") or item.get("created_at", ""), reverse=True)[0] if approved else None
        if strategy == "approved_latest":
            return sorted(approved, key=lambda item: item.get("approved_at") or item.get("created_at", ""), reverse=True)[0] if approved else None
        if strategy == "latest_any":
            return sorted(versions, key=lambda item: item.get("created_at", ""), reverse=True)[0]
        return sorted(approved or versions, key=lambda item: item.get("created_at", ""), reverse=True)[0]

    def set_active_model(self, tenant_id: str, model_version: str, actor: str) -> dict[str, Any]:
        versions = self._repository.list_model_versions(tenant_id)
        target = next((item for item in versions if item.get("model_version") == model_version), None)
        if not target:
            raise ValueError(f"Model version not found: {model_version}")
        if str(target.get("approval_status", "")).lower() != "approved":
            raise ValueError("Only approved models can be marked active")
        now = datetime.now(timezone.utc).isoformat()
        for item in versions:
            metadata = dict(item.get("training_metadata_json") or {})
            metadata["is_active"] = item.get("model_version") == model_version
            metadata["last_activation"] = {"actor": actor, "timestamp": now}
            self._repository.update_model_version(
                tenant_id=tenant_id,
                model_version=str(item.get("model_version")),
                training_metadata_json=metadata,
            )
        updated = self._repository.get_model_version(tenant_id=tenant_id, model_version=model_version)
        return updated or target

    def update_approval_status(self, tenant_id: str, model_version: str, approval_status: str, actor: str) -> dict[str, Any]:
        normalized = str(approval_status).lower().strip()
        if normalized not in {"draft", "approved", "archived"}:
            raise ValueError("approval_status must be one of: draft, approved, archived")
        updates: dict[str, Any] = {"approval_status": normalized}
        if normalized == "approved":
            updates["approved_by"] = actor
            updates["approved_at"] = datetime.now(timezone.utc).isoformat()
        updated = self._repository.update_model_version(tenant_id=tenant_id, model_version=model_version, **updates)
        if updated is None:
            raise ValueError(f"Model version not found: {model_version}")
        if normalized == "approved":
            return self.set_active_model(tenant_id=tenant_id, model_version=model_version, actor=actor)
        if normalized == "archived":
            metadata = dict(updated.get("training_metadata_json") or {})
            if metadata.get("is_active"):
                metadata["is_active"] = False
                self._repository.update_model_version(
                    tenant_id=tenant_id,
                    model_version=model_version,
                    training_metadata_json=metadata,
                )
            active = self.resolve_model(tenant_id=tenant_id, strategy="active_approved")
            if active is None:
                fallback = self.resolve_model(tenant_id=tenant_id, strategy="approved_latest")
                if fallback:
                    self.set_active_model(tenant_id=tenant_id, model_version=str(fallback["model_version"]), actor=actor)
        return self._repository.get_model_version(tenant_id=tenant_id, model_version=model_version) or updated

    def rollback_to_version(self, tenant_id: str, target_version: str, actor: str, reason: str = "manual rollback") -> dict[str, Any]:
        target = self._repository.get_model_version(tenant_id=tenant_id, model_version=target_version)
        if not target:
            raise ValueError(f"Model version not found: {target_version}")
        if str(target.get("approval_status", "")).lower() != "approved":
            raise ValueError("Rollback target must be approved")
        current = self.resolve_model(tenant_id=tenant_id, strategy="active_approved")
        updated = self.set_active_model(tenant_id=tenant_id, model_version=target_version, actor=actor)
        metadata = dict(updated.get("training_metadata_json") or {})
        metadata["rollback"] = {
            "from_version": (current or {}).get("model_version"),
            "to_version": target_version,
            "actor": actor,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._repository.update_model_version(
            tenant_id=tenant_id,
            model_version=target_version,
            training_metadata_json=metadata,
        )
        return self._repository.get_model_version(tenant_id=tenant_id, model_version=target_version) or updated

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
