from __future__ import annotations

import ast
import hashlib
import inspect
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from api.routers.alerts_router import router as alerts_router
from api.routers.auth_router import _LOGIN_RATE_LIMITER
from api.routers.auth_router import router as auth_router
from api.routers.intelligence_router import router as intelligence_router
from core.dependencies import build_app_state
from core.config import Settings
from core.security import build_access_token
from core.security import build_refresh_token
from core.security import get_authenticated_tenant_id
from core.security import hash_password
from learning.feedback_collection_service import FeedbackCollectionService
from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from retrieval.retrieval_service import RetrievalService
from services.interpretation_service import InterpretationService
from services.model_monitoring_service import ModelMonitoringService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import AlertRecord, EnterpriseRepository
from workers import event_subscriber_worker
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


def test_build_app_state_covers_live_router_state_usage() -> None:
    router_dir = Path(__file__).resolve().parents[1] / "api" / "routers"
    required_keys: set[str] = set()
    for path in router_dir.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        required_keys.update(re.findall(r"request\.app\.state\.([A-Za-z_][A-Za-z0-9_]*)", text))

    function_source = inspect.getsource(build_app_state)
    module_ast = ast.parse(function_source)
    return_node = next(
        node for node in ast.walk(module_ast) if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)
    )
    provided_keys = {
        key.value
        for key in return_node.value.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    missing = sorted(required_keys - provided_keys)
    assert not missing, f"build_app_state is missing live router dependencies: {missing}"


def test_phase4_flag_defaults_reflect_primary_alert_jsonl_cutover(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_name in [
        "ALTHEA_ENABLE_ALERT_JSONL_INGESTION",
        "ALTHEA_ENABLE_LEGACY_INGESTION",
        "ALTHEA_ENABLE_IBM_AMLSIM_IMPORT",
        "ALTHEA_ENABLE_HUMAN_INTERPRETATION",
        "ALTHEA_STRICT_INGESTION_VALIDATION",
        "ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS",
        "ALTHEA_INGESTION_MAX_UPLOAD_BYTES",
        "ALTHEA_PRIMARY_INGESTION_MODE",
    ]:
        monkeypatch.delenv(env_name, raising=False)
    settings = Settings(jwt_secret="x" * 48, app_env="development")
    settings.validate()
    assert settings.enable_alert_jsonl_ingestion is True
    assert settings.enable_legacy_ingestion is False
    assert settings.enable_ibm_amlsim_import is False
    assert settings.enable_human_interpretation is True
    assert settings.strict_ingestion_validation is False
    assert settings.alert_jsonl_max_upload_rows == 1000
    assert settings.ingestion_max_upload_bytes == 10 * 1024 * 1024
    assert settings.primary_ingestion_mode == "alert_jsonl"


def test_phase1_flag_invalid_value_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_ENABLE_ALERT_JSONL_INGESTION", "maybe")
    with pytest.raises(RuntimeError, match="ALTHEA_ENABLE_ALERT_JSONL_INGESTION must be a boolean value"):
        Settings(jwt_secret="x" * 48, app_env="development").validate()


def test_phase2_row_limit_invalid_value_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS", "zero")
    with pytest.raises(RuntimeError, match="ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS must be an integer value"):
        Settings(jwt_secret="x" * 48, app_env="development").validate()


def test_removed_rollout_mode_flag_is_ignored_during_finalization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_ALERT_JSONL_ROLLOUT_MODE", "all")
    settings = Settings(jwt_secret="x" * 48, app_env="development")
    settings.validate()
    assert settings.enable_alert_jsonl_ingestion is True


def test_phase4_primary_mode_invalid_value_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_PRIMARY_INGESTION_MODE", "hybrid")
    with pytest.raises(RuntimeError, match="ALTHEA_PRIMARY_INGESTION_MODE must be one of"):
        Settings(jwt_secret="x" * 48, app_env="development").validate()


def test_phase5_legacy_ingestion_flag_invalid_value_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_ENABLE_LEGACY_INGESTION", "sometimes")
    with pytest.raises(RuntimeError, match="ALTHEA_ENABLE_LEGACY_INGESTION must be a boolean value"):
        Settings(jwt_secret="x" * 48, app_env="development").validate()


def test_phase5_upload_size_limit_invalid_value_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALTHEA_INGESTION_MAX_UPLOAD_BYTES", "tiny")
    with pytest.raises(RuntimeError, match="ALTHEA_INGESTION_MAX_UPLOAD_BYTES must be an integer value"):
        Settings(jwt_secret="x" * 48, app_env="development").validate()


def test_phase1_alert_columns_exist_for_legacy_sqlite_repo(tmp_path) -> None:
    db_path = tmp_path / f"althea_phase1_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    with repo.session(tenant_id="tenant-a") as session:
        rows = session.execute(text("PRAGMA table_info(alerts)")).all()
    columns = {str(row[1]) for row in rows}
    assert {
        "raw_payload_json",
        "source_system",
        "ingestion_run_id",
        "schema_version",
        "evaluation_label_is_sar",
        "ingestion_metadata_json",
    }.issubset(columns)


def test_phase5_alert_indexes_exist_for_ingestion_fields(tmp_path) -> None:
    db_path = tmp_path / f"althea_phase5_idx_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    with repo.session(tenant_id="tenant-a") as session:
        rows = session.execute(text("PRAGMA index_list('alerts')")).all()
    index_names = {str(row[1]) for row in rows}
    assert {
        "ix_alerts_ingestion_run_id",
        "ix_alerts_source_system",
    }.issubset(index_names)


def test_phase1_alert_metadata_persists_but_internal_label_is_not_exposed(tmp_path) -> None:
    db_path = tmp_path / f"althea_phase1_payload_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    tenant_id = "tenant-a"
    run_id = "run-phase1"
    repo.save_alert_payloads(
        tenant_id=tenant_id,
        run_id=run_id,
        records=[
            {
                "alert_id": "ALERT-PHASE1",
                "timestamp": "2026-04-05T00:00:00Z",
                "risk_score": 70.0,
                "source_system": "ibm_amlsim",
                "schema_version": "alert_jsonl.v1",
                "evaluation_label_is_sar": 1,
                "raw_payload_json": {"alert_id": "ALERT-PHASE1", "metadata": {"source_system": "ibm_amlsim"}},
                "ingestion_metadata_json": {"source_system": "ibm_amlsim", "warnings": []},
            }
        ],
    )

    payload = repo.get_alert_payload(tenant_id=tenant_id, alert_id="ALERT-PHASE1", run_id=run_id)
    assert payload is not None
    assert payload.get("source_system") == "ibm_amlsim"
    assert "evaluation_label_is_sar" not in payload

    with repo.session(tenant_id=tenant_id) as session:
        row = session.execute(
            select(AlertRecord).where(
                AlertRecord.tenant_id == tenant_id,
                AlertRecord.alert_id == "ALERT-PHASE1",
            )
        ).scalar_one()
    assert bool(row.evaluation_label_is_sar) is True
    assert isinstance(row.ingestion_metadata_json, dict)


def test_phase1_alert_payloads_remain_compatible_when_new_fields_missing(tmp_path) -> None:
    db_path = tmp_path / f"althea_phase1_missing_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    tenant_id = "tenant-a"
    run_id = "run-legacy"
    repo.save_alert_payloads(
        tenant_id=tenant_id,
        run_id=run_id,
        records=[
            {
                "alert_id": "ALERT-LEGACY",
                "timestamp": "2026-04-05T00:00:00Z",
                "risk_score": 44.0,
            }
        ],
    )
    payload = repo.get_alert_payload(tenant_id=tenant_id, alert_id="ALERT-LEGACY", run_id=run_id)
    assert payload is not None
    assert payload["alert_id"] == "ALERT-LEGACY"
    assert float(payload["risk_score"]) == 44.0


def test_login_rate_limiting_enforces_5_attempts_per_minute() -> None:
    _LOGIN_RATE_LIMITER.clear("tenant-rate:testclient:ip")
    _LOGIN_RATE_LIMITER.clear("tenant-rate:a@example.com:testclient")
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


def test_login_rejects_disabled_user() -> None:
    app = FastAPI()
    app.include_router(auth_router)
    app.state.settings = SimpleNamespace(
        default_tenant_id="tenant-disabled",
        tenant_header="X-Tenant-ID",
        refresh_token_minutes=60,
        access_token_minutes=15,
        jwt_secret="x" * 48,
        jwt_algorithm="HS256",
        trusted_proxy_headers=False,
        expose_refresh_token_in_response=True,
        refresh_cookie_name="althea_rt",
        refresh_cookie_path="/api/auth",
        refresh_cookie_domain=None,
        refresh_cookie_secure=False,
        refresh_cookie_samesite="strict",
    )
    disabled_user = {
        "id": "u-disabled",
        "email": "disabled@example.com",
        "password_hash": hash_password("Password123!"),
        "role": "analyst",
        "team": "default",
        "is_active": False,
    }
    app.state.repository = SimpleNamespace(
        get_user_by_email=lambda tenant_id, email: dict(disabled_user),
        append_auth_audit_log=lambda payload: payload,
    )
    client = TestClient(app)

    response = client.post("/api/auth/login", json={"email": "disabled@example.com", "password": "Password123!"})
    assert response.status_code == 403
    assert "disabled" in str(response.json().get("detail", "")).lower()


def test_refresh_rejects_disabled_user_and_revokes_session() -> None:
    app = FastAPI()
    app.include_router(auth_router)
    settings = SimpleNamespace(
        default_tenant_id="tenant-a",
        tenant_header="X-Tenant-ID",
        refresh_token_minutes=60,
        access_token_minutes=15,
        jwt_secret="x" * 48,
        jwt_algorithm="HS256",
        trusted_proxy_headers=False,
        allow_refresh_token_in_body=True,
        expose_refresh_token_in_response=True,
        refresh_cookie_name="althea_rt",
        refresh_cookie_path="/api/auth",
        refresh_cookie_domain=None,
        refresh_cookie_secure=False,
        refresh_cookie_samesite="strict",
    )
    app.state.settings = settings
    user = {"id": "u1", "role": "analyst", "team": "default"}
    refresh_token = build_refresh_token(Settings(jwt_secret="x" * 48, app_env="development"), "tenant-a", user, "sid-1")
    revoked = {"called": False}
    app.state.repository = SimpleNamespace(
        get_session=lambda tenant_id, session_id: {
            "session_id": "sid-1",
            "tenant_id": "tenant-a",
            "user_id": "u1",
            "refresh_token_hash": hashlib.sha256(refresh_token.encode("utf-8")).hexdigest(),
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=30),
            "revoked": False,
        },
        get_user_by_id=lambda tenant_id, user_id: {
            "id": "u1",
            "email": "u1@example.com",
            "role": "analyst",
            "team": "default",
            "is_active": False,
        },
        revoke_session=lambda tenant_id, session_id: revoked.__setitem__("called", True),
        append_auth_audit_log=lambda payload: payload,
    )
    client = TestClient(app)

    response = client.post("/api/auth/refresh", json={"refresh_token": refresh_token})
    assert response.status_code == 403
    assert revoked["called"] is True


def test_tenant_bootstrap_registration_requires_provisioning_token() -> None:
    app = FastAPI()
    app.include_router(auth_router)
    app.state.settings = SimpleNamespace(
        default_tenant_id="tenant-a",
        tenant_header="X-Tenant-ID",
        refresh_token_minutes=60,
        access_token_minutes=15,
        jwt_secret="x" * 48,
        jwt_algorithm="HS256",
        trusted_proxy_headers=False,
        expose_refresh_token_in_response=True,
        refresh_cookie_name="althea_rt",
        refresh_cookie_path="/api/auth",
        refresh_cookie_domain=None,
        refresh_cookie_secure=False,
        refresh_cookie_samesite="strict",
        enable_public_tenant_bootstrap=True,
        bootstrap_provisioning_secret="super-secret-bootstrap-token",
        sso_provisioning_secret="sso-secret",
    )
    app.state.repository = SimpleNamespace(
        count_users=lambda tenant_id: 0,
        get_user_by_email=lambda tenant_id, email: None,
    )
    client = TestClient(app)

    response = client.post(
        "/api/auth/register",
        json={
            "email": "bootstrap@bank.com",
            "password": "Password123!",
            "team": "ops",
            "provision_mode": "TENANT_BOOTSTRAP_USER",
        },
    )
    assert response.status_code == 403
    assert "bootstrap" in str(response.json().get("detail", "")).lower()


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


def test_decision_audit_records_persist_and_are_readable_via_api(tmp_path) -> None:
    db_path = tmp_path / f"althea_decision_audit_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    tenant_id = "tenant-a"
    repo.save_decision_audit_records(
        tenant_id=tenant_id,
        records=[
            {
                "alert_id": "A1",
                "run_id": "run-1",
                "model_version": "model-v1",
                "priority_score": 92.5,
                "escalation_prob": 0.91,
                "governance_status": "eligible",
                "queue_action": "queue",
                "priority_bucket": "critical",
                "signals_json": {"reason_codes": ["R1"]},
            }
        ],
    )

    app = FastAPI()
    app.include_router(intelligence_router)
    app.state.repository = repo
    app.state.pipeline_service = SimpleNamespace(get_run_info=lambda tenant_id, user_scope: {"run_id": "run-1"})
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: tenant_id
    client = TestClient(app)

    response = client.get("/api/alerts/A1/decision-audit")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["audit_records"][0]["alert_id"] == "A1"
    assert payload["audit_records"][0]["queue_action"] == "queue"


def test_retrieval_index_build_uses_latest_payload_for_alert_across_runs(tmp_path) -> None:
    db_path = tmp_path / f"althea_retrieval_{uuid.uuid4().hex}.db"
    storage_root = tmp_path / "objects"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    storage = ObjectStorage(storage_root)
    feedback = FeedbackCollectionService(repo)
    service = RetrievalService(repo, storage)
    tenant_id = "tenant-a"

    repo.save_alert_payloads(
        tenant_id=tenant_id,
        run_id="run-old",
        records=[
            {
                "alert_id": "A1",
                "timestamp": "2026-04-01T00:00:00Z",
                "risk_score": 30.0,
                "typology": "structuring",
                "features_json": {"amount_log1p": 2.0, "risk_score": 30.0},
            }
        ],
    )
    repo.save_alert_payloads(
        tenant_id=tenant_id,
        run_id="run-new",
        records=[
            {
                "alert_id": "A1",
                "timestamp": "2026-04-02T00:00:00Z",
                "risk_score": 88.0,
                "typology": "structuring",
                "features_json": {"amount_log1p": 5.0, "risk_score": 88.0},
            }
        ],
    )
    feedback.record_outcome(
        tenant_id=tenant_id,
        alert_id="A1",
        analyst_decision="true_positive",
        analyst_id="u1",
        risk_score_at_decision=88.0,
    )

    result = service.build_index(tenant_id=tenant_id)
    assert result["status"] == "ok"
    assert result["indexed_cases"] == 1

    retrieved = service.retrieve_similar_cases(
        tenant_id=tenant_id,
        alert_payload={"alert_id": "Q1", "features_json": {"amount_log1p": 5.0, "risk_score": 88.0}},
        top_k=1,
    )
    match = retrieved["similar_suspicious"][0]
    assert match["alert_id"] == "A1"
    assert match["risk_score"] == 88.0


def test_event_subscriber_does_not_duplicate_monitoring_records(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / f"althea_monitoring_{uuid.uuid4().hex}.db"
    repo = EnterpriseRepository(f"sqlite:///{db_path.as_posix()}")
    monitoring = ModelMonitoringService(repo)
    tenant_id = "tenant-a"
    run_id = "run-1"

    repo.create_pipeline_job(
        {
            "id": "job-1",
            "tenant_id": tenant_id,
            "source": "test",
            "dataset_hash": "hash-1",
            "status": "completed",
            "row_count": 1,
            "run_id": run_id,
        }
    )
    repo.save_alert_payloads(
        tenant_id=tenant_id,
        run_id=run_id,
        records=[{"alert_id": "A1", "timestamp": "2026-04-01T00:00:00Z", "risk_score": 42.0}],
    )

    monitoring.record_run_monitoring(
        tenant_id=tenant_id,
        run_id=run_id,
        model_version="model-v1",
        scores=[42.0],
    )
    monkeypatch.setattr(event_subscriber_worker, "get_repository", lambda: repo)
    monkeypatch.setattr(
        event_subscriber_worker,
        "get_pipeline_service",
        lambda: SimpleNamespace(compute_health=lambda run_id, tenant_id: {"status": "healthy"}),
    )

    event_subscriber_worker._handle_event(
        {
            "event_name": "alert_scored",
            "tenant_id": tenant_id,
            "payload": {"run_id": run_id},
        }
    )

    rows = repo.list_model_monitoring(tenant_id=tenant_id, limit=10)
    assert len(rows) == 1


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
