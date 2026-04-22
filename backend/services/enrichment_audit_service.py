from __future__ import annotations

from typing import Any


class EnrichmentAuditService:
    def __init__(self, repository) -> None:
        self._repository = repository

    def record(
        self,
        *,
        tenant_id: str,
        action: str,
        source_name: str | None = None,
        actor_id: str | None = None,
        status: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._repository.append_audit_log(
            tenant_id,
            {
                "source_name": source_name,
                "action": action,
                "actor_id": actor_id,
                "status": status,
                "details_json": dict(details or {}),
            },
        )

    def list_logs(self, tenant_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return self._repository.list_audit_logs(tenant_id=tenant_id, limit=limit)
