from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text


class FeatureRegistry:
    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def register_feature(
        self,
        tenant_id: str,
        name: str,
        dtype: str,
        description: str,
        owner: str = "feature-engineering",
        source: str = "alerts",
        tags: list[str] | None = None,
    ) -> str:
        feature_id = uuid.uuid4().hex
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO features (id, tenant_id, feature_name, feature_type, description, owner, source_system, tags_json, created_at)
                    VALUES (:id, :tenant_id, :feature_name, :feature_type, :description, :owner, :source_system, :tags_json, :created_at)
                    ON CONFLICT (tenant_id, feature_name)
                    DO UPDATE SET
                        feature_type = EXCLUDED.feature_type,
                        description = EXCLUDED.description,
                        owner = EXCLUDED.owner,
                        source_system = EXCLUDED.source_system,
                        tags_json = EXCLUDED.tags_json
                    """
                ),
                {
                    "id": feature_id,
                    "tenant_id": tenant_id,
                    "feature_name": name,
                    "feature_type": dtype,
                    "description": description,
                    "owner": owner,
                    "source_system": source,
                    "tags_json": json.dumps(list(tags or []), ensure_ascii=True),
                    "created_at": self._now(),
                },
            )
        return feature_id

    def register_feature_version(
        self,
        tenant_id: str,
        feature_name: str,
        version: str,
        transformation_sql: str,
        is_active: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        version_id = uuid.uuid4().hex
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO feature_versions (
                        id, tenant_id, feature_name, version, transformation_sql, is_active, metadata_json, created_at
                    ) VALUES (
                        :id, :tenant_id, :feature_name, :version, :transformation_sql, :is_active, :metadata_json, :created_at
                    )
                    ON CONFLICT (tenant_id, feature_name, version)
                    DO UPDATE SET
                        transformation_sql = EXCLUDED.transformation_sql,
                        is_active = EXCLUDED.is_active,
                        metadata_json = EXCLUDED.metadata_json
                    """
                ),
                {
                    "id": version_id,
                    "tenant_id": tenant_id,
                    "feature_name": feature_name,
                    "version": version,
                    "transformation_sql": transformation_sql,
                    "is_active": bool(is_active),
                    "metadata_json": json.dumps(dict(metadata or {}), ensure_ascii=True),
                    "created_at": self._now(),
                },
            )
        return version_id

    def register_dependency(self, tenant_id: str, feature_name: str, depends_on: str) -> str:
        dep_id = uuid.uuid4().hex
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO feature_dependencies (id, tenant_id, feature_name, depends_on, created_at)
                    VALUES (:id, :tenant_id, :feature_name, :depends_on, :created_at)
                    ON CONFLICT (tenant_id, feature_name, depends_on)
                    DO NOTHING
                    """
                ),
                {
                    "id": dep_id,
                    "tenant_id": tenant_id,
                    "feature_name": feature_name,
                    "depends_on": depends_on,
                    "created_at": self._now(),
                },
            )
        return dep_id

    def list_features(self, tenant_id: str) -> list[dict[str, Any]]:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT f.id, f.feature_name, f.feature_type, f.description, f.owner, f.source_system, f.tags_json,
                           fv.version, fv.is_active, fv.metadata_json
                    FROM features f
                    LEFT JOIN feature_versions fv
                      ON fv.tenant_id = f.tenant_id AND fv.feature_name = f.feature_name
                    WHERE f.tenant_id = :tenant_id
                    ORDER BY f.feature_name, fv.created_at DESC
                    """
                ),
                {"tenant_id": tenant_id},
            ).mappings().all()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            for field, fallback in (("tags_json", []), ("metadata_json", {})):
                value = item.get(field)
                if isinstance(value, str):
                    try:
                        item[field] = json.loads(value)
                    except Exception:
                        item[field] = fallback
                elif value is None:
                    item[field] = fallback
            out.append(item)
        return out

    def get_active_version(self, tenant_id: str, feature_name: str) -> str:
        with self._repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                text(
                    """
                    SELECT version
                    FROM feature_versions
                    WHERE tenant_id = :tenant_id
                      AND feature_name = :feature_name
                      AND is_active = TRUE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"tenant_id": tenant_id, "feature_name": feature_name},
            ).first()
        return str(row[0]) if row else "v1"

