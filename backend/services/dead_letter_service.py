from __future__ import annotations

from typing import Any


class DeadLetterService:
    def __init__(self, repository) -> None:
        self._repository = repository

    def capture(
        self,
        *,
        tenant_id: str,
        source_name: str,
        error_code: str,
        error_message: str,
        payload: dict[str, Any] | None = None,
        source_record_id: str | None = None,
    ) -> dict[str, Any]:
        return self._repository.append_dead_letter(
            tenant_id,
            {
                "source_name": source_name,
                "source_record_id": source_record_id,
                "error_code": error_code,
                "error_message": error_message,
                "payload_json": dict(payload or {}),
                "status": "pending",
            },
        )

    def list_items(self, tenant_id: str, source_name: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self._repository.list_dead_letters(tenant_id=tenant_id, source_name=source_name, limit=limit)

    def replay(self, tenant_id: str, item_ids: list[str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item_id in item_ids:
            updated = self._repository.mark_dead_letter_replayed(tenant_id=tenant_id, dead_letter_id=item_id)
            if updated is not None:
                results.append(updated)
        return results
