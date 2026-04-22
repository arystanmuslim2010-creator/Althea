from __future__ import annotations

from typing import Any


class EntityResolutionService:
    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _canonical_id(entity_type: str, source_name: str, source_id: str) -> str:
        clean_type = str(entity_type or "").strip().lower()
        clean_source = str(source_name or "").strip().lower()
        clean_id = str(source_id or "").strip()
        return f"{clean_type}:{clean_source}:{clean_id}"

    def rebuild_links(self, tenant_id: str) -> dict[str, Any]:
        customers = self._repository.list_master_customers(tenant_id)
        accounts = self._repository.list_master_accounts(tenant_id)
        counterparties = self._repository.list_master_counterparties(tenant_id)
        overrides = self._repository.list_master_data_overrides(tenant_id)

        aliases: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []
        blocked_pairs = {
            (
                str(item.get("left_entity_type") or ""),
                str(item.get("left_entity_id") or ""),
                str(item.get("right_entity_type") or ""),
                str(item.get("right_entity_id") or ""),
            )
            for item in overrides
            if str(item.get("override_type") or "").strip().lower() == "block_link"
        }

        for customer in customers:
            source_name = str(customer.get("source_name") or "unknown")
            customer_id = str(customer.get("customer_id") or "")
            canonical_id = self._canonical_id("customer", source_name, customer_id)
            aliases.append(
                {
                    "entity_type": "customer",
                    "canonical_id": canonical_id,
                    "source_name": source_name,
                    "external_id": customer_id,
                    "alias_type": "source_exact",
                    "confidence": 1.0,
                }
            )
            external_customer_id = str(customer.get("external_customer_id") or "").strip()
            if external_customer_id:
                aliases.append(
                    {
                        "entity_type": "customer",
                        "canonical_id": canonical_id,
                        "source_name": source_name,
                        "external_id": external_customer_id,
                        "alias_type": "external_exact",
                        "confidence": 0.8,
                    }
                )

        customer_canonical_by_source_id = {
            str(item.get("customer_id") or ""): self._canonical_id("customer", str(item.get("source_name") or "unknown"), str(item.get("customer_id") or ""))
            for item in customers
        }
        account_canonical_by_source_id: dict[str, str] = {}

        for account in accounts:
            source_name = str(account.get("source_name") or "unknown")
            account_id = str(account.get("account_id") or "")
            canonical_id = self._canonical_id("account", source_name, account_id)
            account_canonical_by_source_id[account_id] = canonical_id
            aliases.append(
                {
                    "entity_type": "account",
                    "canonical_id": canonical_id,
                    "source_name": source_name,
                    "external_id": account_id,
                    "alias_type": "source_exact",
                    "confidence": 1.0,
                }
            )
            external_account_id = str(account.get("external_account_id") or "").strip()
            if external_account_id:
                aliases.append(
                    {
                        "entity_type": "account",
                        "canonical_id": canonical_id,
                        "source_name": source_name,
                        "external_id": external_account_id,
                        "alias_type": "external_exact",
                        "confidence": 0.8,
                    }
                )
            customer_id = str(account.get("customer_id") or "").strip()
            customer_canonical = customer_canonical_by_source_id.get(customer_id)
            pair = ("customer", customer_canonical or "", "account", canonical_id)
            if customer_canonical and pair not in blocked_pairs:
                links.append(
                    {
                        "left_entity_type": "customer",
                        "left_entity_id": customer_canonical,
                        "right_entity_type": "account",
                        "right_entity_id": canonical_id,
                        "link_type": "owns_account",
                        "confidence": 1.0,
                        "source_name": source_name,
                        "metadata_json": {"rule": "account_customer_exact"},
                    }
                )

        for counterparty in counterparties:
            source_name = str(counterparty.get("source_name") or "unknown")
            counterparty_id = str(counterparty.get("counterparty_id") or "")
            canonical_id = self._canonical_id("counterparty", source_name, counterparty_id)
            aliases.append(
                {
                    "entity_type": "counterparty",
                    "canonical_id": canonical_id,
                    "source_name": source_name,
                    "external_id": counterparty_id,
                    "alias_type": "source_exact",
                    "confidence": 1.0,
                }
            )
            external_counterparty_id = str(counterparty.get("external_counterparty_id") or "").strip()
            if external_counterparty_id:
                aliases.append(
                    {
                        "entity_type": "counterparty",
                        "canonical_id": canonical_id,
                        "source_name": source_name,
                        "external_id": external_counterparty_id,
                        "alias_type": "external_exact",
                        "confidence": 0.8,
                    }
                )
            linked_account = account_canonical_by_source_id.get(counterparty_id) or account_canonical_by_source_id.get(external_counterparty_id)
            pair = ("counterparty", canonical_id, "account", linked_account or "")
            if linked_account and pair not in blocked_pairs:
                links.append(
                    {
                        "left_entity_type": "counterparty",
                        "left_entity_id": canonical_id,
                        "right_entity_type": "account",
                        "right_entity_id": linked_account,
                        "link_type": "matches_account_identifier",
                        "confidence": 0.5,
                        "source_name": source_name,
                        "metadata_json": {"rule": "counterparty_to_account_exact"},
                    }
                )

        for override in overrides:
            if str(override.get("override_type") or "").strip().lower() == "force_alias":
                source_name = str(override.get("source_name") or "manual")
                entity_type = str(override.get("target_entity_type") or "")
                canonical_id = str(override.get("target_entity_id") or "")
                external_id = str(override.get("external_id") or "")
                if source_name and entity_type and canonical_id and external_id:
                    aliases.append(
                        {
                            "entity_type": entity_type,
                            "canonical_id": canonical_id,
                            "source_name": source_name,
                            "external_id": external_id,
                            "alias_type": "manual_override",
                            "confidence": 1.0,
                        }
                    )
            if str(override.get("override_type") or "").strip().lower() == "force_link":
                links.append(
                    {
                        "left_entity_type": str(override.get("left_entity_type") or ""),
                        "left_entity_id": str(override.get("left_entity_id") or ""),
                        "right_entity_type": str(override.get("right_entity_type") or ""),
                        "right_entity_id": str(override.get("right_entity_id") or ""),
                        "link_type": "manual_override",
                        "confidence": 1.0,
                        "source_name": str(override.get("source_name") or "manual"),
                        "metadata_json": {"reason": override.get("reason")},
                    }
                )

        aliases_by_source: dict[str, list[dict[str, Any]]] = {}
        links_by_source: dict[str, list[dict[str, Any]]] = {}
        for alias in aliases:
            aliases_by_source.setdefault(str(alias.get("source_name") or "unknown"), []).append(alias)
        for link in links:
            links_by_source.setdefault(str(link.get("source_name") or "unknown"), []).append(link)

        alias_count = 0
        for source_name, items in aliases_by_source.items():
            alias_count += self._repository.replace_entity_aliases(tenant_id=tenant_id, source_name=source_name, aliases=items)
        link_count = 0
        for source_name, items in links_by_source.items():
            link_count += self._repository.replace_entity_links(tenant_id=tenant_id, source_name=source_name, links=items)

        return {
            "status": "ok",
            "alias_count": alias_count,
            "link_count": link_count,
            "sources": sorted(set(list(aliases_by_source) + list(links_by_source))),
        }
