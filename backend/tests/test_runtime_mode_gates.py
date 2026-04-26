from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.pipeline_router import router as pipeline_router
from core.config import Settings
from core.security import get_authenticated_tenant_id, get_current_user


class _Ingest:
    def generate_synthetic(self, tenant_id, user_scope, n_rows=400):
        return {"rows": n_rows}


def _client(mode: str):
    app = FastAPI()
    app.include_router(pipeline_router)
    app.state.settings = SimpleNamespace(
        runtime_mode=mode,
        demo_features_enabled=lambda: mode == "demo",
        ingestion_max_upload_bytes=10_000,
        enable_legacy_ingestion=False,
        enable_alert_jsonl_ingestion=True,
        alert_jsonl_max_upload_rows=1000,
        primary_ingestion_mode="alert_jsonl",
    )
    app.state.ingestion_service = _Ingest()
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "m1",
        "id": "m1",
        "role": "manager",
        "permissions": ["manager_approval"],
        "tenant_id": "tenant-a",
    }
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: "tenant-a"
    return TestClient(app)


def test_demo_generation_allowed_in_demo_mode():
    res = _client("demo").post("/api/data/generate-synthetic?n_rows=3")
    assert res.status_code == 200
    assert res.json()["rows"] == 3


def test_demo_generation_blocked_in_pilot_mode():
    res = _client("pilot").post("/api/data/generate-synthetic?n_rows=3")
    assert res.status_code == 403


def test_production_runtime_disables_dev_model_bootstrap():
    settings = Settings(
        jwt_secret="x" * 48,
        app_env="production",
        database_url="postgresql://u:p@db.example/althea",
        allowed_origins=["https://bank.example"],
        allowed_hosts=["bank.example"],
        refresh_cookie_secure=True,
    )
    settings.runtime_mode = "production"
    settings.validate()
    assert settings.allow_dev_models is False
