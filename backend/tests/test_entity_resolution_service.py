from __future__ import annotations

from services.enrichment_repository import EnrichmentRepository
from services.entity_resolution_service import EntityResolutionService
from storage.postgres_repository import EnterpriseRepository


def test_entity_resolution_service_builds_rules_first_links_and_aliases(tmp_path) -> None:
    db_path = tmp_path / "entity_resolution.db"
    repository = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    enrichment_repository = EnrichmentRepository(repository)
    service = EntityResolutionService(enrichment_repository)
    tenant_id = "tenant-a"

    enrichment_repository.append_master_customers(
        tenant_id,
        [{"source_name": "kyc", "customer_id": "C1", "external_customer_id": "EXT-C1"}],
    )
    enrichment_repository.append_master_accounts(
        tenant_id,
        [{"source_name": "kyc", "account_id": "A1", "external_account_id": "EXT-A1", "customer_id": "C1"}],
    )
    enrichment_repository.append_master_counterparties(
        tenant_id,
        [{"source_name": "watchlist", "counterparty_id": "EXT-A1", "external_counterparty_id": "CP-EXT-A1"}],
    )

    result = service.rebuild_links(tenant_id)
    aliases = enrichment_repository.list_entity_aliases(tenant_id, source_name="kyc")
    links = enrichment_repository.list_entity_links(tenant_id)

    assert result["status"] == "ok"
    assert aliases
    assert any(item["link_type"] == "owns_account" for item in links)
