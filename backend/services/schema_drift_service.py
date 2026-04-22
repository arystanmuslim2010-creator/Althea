from __future__ import annotations

import hashlib
import json
from typing import Any


class SchemaDriftService:
    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _flatten(payload: Any, prefix: str = "") -> dict[str, str]:
        out: dict[str, str] = {}
        if isinstance(payload, dict):
            for key, value in payload.items():
                current = f"{prefix}.{key}" if prefix else str(key)
                if isinstance(value, dict):
                    out.update(SchemaDriftService._flatten(value, current))
                elif isinstance(value, list):
                    out[current] = "list"
                elif value is None:
                    out[current] = "null"
                else:
                    out[current] = type(value).__name__
        return out

    def observe(self, source_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        observed = self._flatten(payload)
        schema_version = hashlib.sha256(
            json.dumps(observed, sort_keys=True, ensure_ascii=True).encode("utf-8")
        ).hexdigest()[:16]
        existing = self._repository.list_schema_registry(source_name=source_name)
        drift_status = "known"
        if existing and not any(item.get("schema_version") == schema_version for item in existing):
            drift_status = "drifted"
        return self._repository.upsert_schema_registry(
            source_name=source_name,
            schema_version=schema_version,
            observed_fields_json=observed,
            drift_status=drift_status,
        )

    def list_registry(self, source_name: str | None = None) -> list[dict[str, Any]]:
        return self._repository.list_schema_registry(source_name=source_name)
