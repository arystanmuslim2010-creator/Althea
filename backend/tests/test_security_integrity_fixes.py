from __future__ import annotations

import uuid
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from api.routers.alerts_router import router as alerts_router
from api.routers.auth_router import _LOGIN_RATE_LIMITER
from api.routers.auth_router import router as auth_router
from core.config import Settings
from core.security import build_access_token
from core.security import get_authenticated_tenant_id
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from services.interpretation_service import InterpretationService
from storage.postgres_repository import AlertRecord, EnterpriseRepository
from main import create_app


def test_jwt_secret_validation_fails_for_missing_or_default_secret() -> None:
    with pytest.raises(RuntimeError, match="ALTHEA_JWT_SECRET must be securely set"):
        Settings(jwt_secret="", app_env="development").validate()
    with pytest.raises(RuntimeError, match="ALTHEA_JWT_SECRET must be securely set"):
        Settings(jwt_secret="change-me-in-production", app_env="development").validate()


def test_alerts_endpoint_rejects_invalid_json_field() -> None:
    app = FastAPI()
    app.include_router(alerts_router)
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: "tenant-a"
    app.state.pipeline_service = SimpleNamespace(get_run_info=lambda tenant_id, user_scope: {"run_id": "run-1"})
    app.state.repository = SimpleNamespace(
        list_alert_payloads_by_run=lambda tenant_id, run_id, limit=500000: [
            {
                "alert_id": "A1",
                "risk_score": 87.0,
                "top_features_json": "{bad-json",
                "top_feature_contributions_json": "[]",
                "risk_explain_json": "{}",
            }
        ]
    )
    client = TestClient(app)

    response = client.get("/api/alerts")
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid JSON input"


def test_duplicate_alert_insert_keeps_single_record(tmp_path) -> None:
    db_path = tmp_path / f"althea_repo_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    tenant_id = "tenant-a"
    run_id = "run-1"
    payload = {
        "alert_id": "ALERT-1",
        "timestamp": "2026-04-01T00:00:00Z",
        "risk_score": 75.0,
        "risk_band": "high",
        "status": "new",
    }
    repo.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=[payload])
    repo.save_alert_payloads(tenant_id=tenant_id, run_id="run-2", records=[payload])

    with repo.session(tenant_id=tenant_id) as session:
        count = session.execute(
            select(func.count()).select_from(AlertRecord).where(
                AlertRecord.tenant_id == tenant_id,
                AlertRecord.alert_id == "ALERT-1",
            )
        ).scalar_one()
    assert int(count or 0) == 1


def test_login_rate_limiting_enforces_5_attempts_per_minute() -> None:
    _LOGIN_RATE_LIMITER.clear("tenant-rate:testclient")
    app = FastAPI()
    app.include_router(auth_router)
    app.state.settings = SimpleNamespace(
        default_tenant_id="tenant-rate",
        tenant_header="X-Tenant-ID",
        refresh_token_minutes=60,
        access_token_minutes=15,
        jwt_secret="x" * 48,
        jwt_algorithm="HS256",
    )
    app.state.repository = SimpleNamespace(get_user_by_email=lambda tenant_id, email: None)
    client = TestClient(app)

    for _ in range(5):
        response = client.post("/api/auth/login", json={"email": "a@example.com", "password": "invalid"})
        assert response.status_code == 401
    blocked = client.post("/api/auth/login", json={"email": "a@example.com", "password": "invalid"})
    assert blocked.status_code == 429


class _NoModelRegistry:
    def resolve_model(self, tenant_id: str, strategy: str = "active_approved"):
        return None


def test_model_bootstrap_disabled_when_dev_models_not_allowed() -> None:
    service = InferenceService(
        registry=_NoModelRegistry(),  # type: ignore[arg-type]
        schema_validator=FeatureSchemaValidator(),
        allow_dev_models=False,
    )
    frame = pd.DataFrame({"amount": [100.0], "time_gap": [60.0]})
    with pytest.raises(RuntimeError, match="Auto-bootstrap disabled in production"):
        service.predict(tenant_id="tenant-a", feature_frame=frame)


def test_interpretation_output_format_contract() -> None:
    service = InterpretationService()
    raw = {
        "base_prob": 0.8,
        "risk_score": 88.0,
        "contributions": [{"feature": "time_gap", "value": -200.0, "magnitude": 0.2}],
        "explanation_method": "numeric_fallback",
        "explanation_status": "fallback",
    }
    out = service.build_human_explanation(raw, {"time_gap": 120})

    assert set(out.keys()) == {
        "summary_text",
        "key_reasons",
        "aml_patterns",
        "analyst_focus_points",
        "confidence_score",
        "technical_details",
    }
    assert isinstance(out["summary_text"], str)
    assert isinstance(out["key_reasons"], list)
    assert isinstance(out["aml_patterns"], list)
    assert isinstance(out["analyst_focus_points"], list)
    assert out["confidence_score"] is None or isinstance(out["confidence_score"], float)
    assert isinstance(out["technical_details"], dict)


def test_login_ignores_stale_authorization_tenant_override() -> None:
    app = create_app()
    settings = app.state.settings
    repository = app.state.repository
    tenant_id = settings.default_tenant_id
    email = "admin@althea.local"
    password = "Admin@12345"
    user = repository.get_user_by_email(tenant_id, email)
    assert user is not None

    stale_token = build_access_token(
        settings=settings,
        tenant_id="tenant-stale-other",
        user={"id": user["id"], "role": user["role"], "team": user.get("team", "default")},
        session_id="session-stale",
    )
    client = TestClient(app)
    response = client.post(
        "/api/auth/login",
        headers={"Authorization": f"Bearer {stale_token}"},
        json={"email": email, "password": password},
    )
    assert response.status_code == 200
