from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.enrichment_router import router as enrichment_router
from core.security import get_authenticated_tenant_id, get_current_user


class _StubQueue:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def set_status(self, job_id: str, payload: dict) -> None:
        self.calls.append({"job_id": job_id, "payload": payload, "kind": "status"})

    def enqueue(self, **kwargs) -> None:
        self.calls.append({"kind": "enqueue", **kwargs})


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(enrichment_router)
    app.state.settings = SimpleNamespace(
        queue_mode="rq",
        redis_url="redis://localhost:6379/0",
        rq_queue_name="althea-pipeline",
        enrichment_rq_queue_name="althea-enrichment",
        rq_job_timeout_seconds=900,
    )
    app.state.job_queue_service = _StubQueue()
    app.state.enrichment_sync_service = SimpleNamespace(
        list_sources=lambda: [
            {"source_name": "internal_case", "enabled": True, "kind": "internal"},
            {"source_name": "kyc", "enabled": False, "kind": "connector"},
        ]
    )
    app.state.enrichment_health_service = SimpleNamespace(status=lambda tenant_id: {"status": "healthy", "sources": []})
    app.state.enrichment_repository = SimpleNamespace(
        get_enrichment_context_snapshot=lambda tenant_id, alert_id: {"alert_id": alert_id, "account_events": []},
        list_entity_aliases=lambda tenant_id, source_name=None, external_id=None: [{"external_id": external_id}],
        list_coverage_snapshots=lambda tenant_id: [{"source_name": "internal_case"}],
    )
    app.state.master_data_service = SimpleNamespace(
        get_entity=lambda tenant_id, entity_type, entity_id: {"entity_type": entity_type, "entity_id": entity_id},
        create_override=lambda tenant_id, payload: {"status": "created", **payload},
        list_overrides=lambda tenant_id: [],
    )
    app.state.entity_resolution_service = SimpleNamespace(rebuild_links=lambda tenant_id: {"status": "ok", "link_count": 1})
    app.state.enrichment_audit_service = SimpleNamespace(record=lambda **kwargs: kwargs, list_logs=lambda tenant_id: [])
    app.state.dead_letter_service = SimpleNamespace(list_items=lambda tenant_id, source_name=None: [])
    app.state.schema_drift_service = SimpleNamespace(list_registry=lambda source_name=None: [{"source_name": source_name or "all"}])
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "admin-1",
        "role": "admin",
        "permissions": ["view_system_logs", "work_cases", "manager_approval"],
        "tenant_id": "tenant-a",
    }
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: "tenant-a"
    return TestClient(app)


def test_enrichment_status_and_context_endpoints_return_admin_contract() -> None:
    client = _client()

    status_res = client.get("/internal/enrichment/status")
    context_res = client.get("/internal/enrichment/context/A1")

    assert status_res.status_code == 200
    assert status_res.json()["status"] == "healthy"
    assert context_res.status_code == 200
    assert context_res.json()["alert_id"] == "A1"


def test_enrichment_sync_endpoint_enqueues_background_job() -> None:
    client = _client()

    response = client.post("/internal/enrichment/sources/internal_case/sync", json={"full_backfill": True})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"
    assert payload["job_id"].startswith("enrichment_sync_internal_case_")
    enqueue_call = next(item for item in client.app.state.job_queue_service.calls if item["kind"] == "enqueue")
    assert enqueue_call["queue_name"] == "althea-enrichment"
