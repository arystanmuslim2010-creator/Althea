from __future__ import annotations

from services.enrichment_repository import EnrichmentRepository
from storage.postgres_repository import EnterpriseRepository


def test_enrichment_repository_sync_state_and_account_events_round_trip(tmp_path) -> None:
    db_path = tmp_path / "enrichment_repo.db"
    repository = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    enrichment_repository = EnrichmentRepository(repository)
    tenant_id = "tenant-a"

    written = enrichment_repository.append_account_events(
        tenant_id,
        [
            {
                "source_name": "internal_case",
                "source_record_id": "evt-1",
                "entity_id": "U1",
                "account_id": "AC1",
                "counterparty_id": "CP1",
                "amount": 100.0,
                "event_time": "2026-01-01T00:00:00Z",
            }
        ],
    )
    state = enrichment_repository.upsert_sync_state(
        tenant_id,
        "internal_case",
        status="completed",
        records_read=1,
        records_written=1,
    )
    events = enrichment_repository.list_account_events_before(
        tenant_id=tenant_id,
        entity_ids=["U1"],
        as_of_timestamp="2026-01-02T00:00:00Z",
    )

    assert written == 1
    assert state["status"] == "completed"
    assert len(events) == 1
    assert events[0]["source_name"] == "internal_case"


def test_enrichment_repository_context_snapshot_uses_canonical_tables(tmp_path) -> None:
    db_path = tmp_path / "enrichment_snapshot.db"
    repository = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    enrichment_repository = EnrichmentRepository(repository)
    tenant_id = "tenant-a"

    repository.save_alert_payloads(
        tenant_id=tenant_id,
        run_id="run-1",
        records=[
            {
                "alert_id": "A1",
                "user_id": "U1",
                "amount": 500.0,
                "timestamp": "2026-01-03T00:00:00Z",
            }
        ],
    )
    enrichment_repository.append_account_events(
        tenant_id,
        [
            {
                "source_name": "internal_case",
                "source_record_id": "evt-1",
                "entity_id": "U1",
                "amount": 100.0,
                "event_time": "2026-01-01T00:00:00Z",
            }
        ],
    )

    snapshot = enrichment_repository.get_enrichment_context_snapshot(tenant_id=tenant_id, alert_id="A1")

    assert snapshot["alert_payload"]["alert_id"] == "A1"
    assert snapshot["account_events"]
    assert snapshot["entity_ids"] == ["U1"]
