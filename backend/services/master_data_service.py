from __future__ import annotations

from typing import Any


class MasterDataService:
    def __init__(self, repository) -> None:
        self._repository = repository

    def upsert_payloads(
        self,
        *,
        tenant_id: str,
        customers: list[dict[str, Any]] | None = None,
        accounts: list[dict[str, Any]] | None = None,
        counterparties: list[dict[str, Any]] | None = None,
    ) -> dict[str, int]:
        return {
            "customers": self._repository.append_master_customers(tenant_id, list(customers or [])),
            "accounts": self._repository.append_master_accounts(tenant_id, list(accounts or [])),
            "counterparties": self._repository.append_master_counterparties(tenant_id, list(counterparties or [])),
        }

    def get_entity(self, tenant_id: str, entity_type: str, entity_id: str) -> dict[str, Any]:
        normalized_type = str(entity_type or "").strip().lower()
        normalized_id = str(entity_id or "").strip()
        if normalized_type == "customer":
            records = [item for item in self._repository.list_master_customers(tenant_id) if item.get("customer_id") == normalized_id]
        elif normalized_type == "account":
            records = [item for item in self._repository.list_master_accounts(tenant_id) if item.get("account_id") == normalized_id]
        elif normalized_type == "counterparty":
            records = [item for item in self._repository.list_master_counterparties(tenant_id) if item.get("counterparty_id") == normalized_id]
        else:
            raise ValueError("entity_type must be one of: customer, account, counterparty")
        return {
            "entity_type": normalized_type,
            "entity_id": normalized_id,
            "records": records,
            "aliases": self._repository.list_entity_aliases(tenant_id, canonical_id=normalized_id),
            "links": self._repository.list_entity_links(tenant_id, entity_id=normalized_id),
        }

    def create_override(self, tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._repository.create_master_data_override(tenant_id=tenant_id, payload=payload)

    def list_overrides(self, tenant_id: str) -> list[dict[str, Any]]:
        return self._repository.list_master_data_overrides(tenant_id=tenant_id)
