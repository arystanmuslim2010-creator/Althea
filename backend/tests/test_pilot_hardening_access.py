from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.alerts_router import router as alerts_router
from api.routers.auth_router import router as auth_router
from api.routers.investigation_router import router as investigation_router
from core.access_control import sanitize_training_run_dto, sanitize_user_dto
from core.security import get_authenticated_tenant_id, get_current_user


class _Repo:
    def __init__(self):
        self.alerts = {
            "A1": {"alert_id": "A1", "run_id": "run-1", "risk_score": 80, "assigned_to": "u1"},
            "A2": {"alert_id": "A2", "run_id": "run-1", "risk_score": 70},
        }
        self.users = [
            {
                "id": "admin",
                "email": "admin@example.com",
                "role": "admin",
                "team": "ops",
                "password_hash": "pbkdf2$secret",
                "refresh_token": "secret-token",
            }
        ]

    def get_alert_payload(self, tenant_id, alert_id, run_id=None):
        if tenant_id != "tenant-a":
            return None
        item = self.alerts.get(alert_id)
        if item and (not run_id or item.get("run_id") == run_id):
            return dict(item)
        return None

    def list_alert_payloads_by_run(self, tenant_id, run_id, limit=500000):
        if tenant_id != "tenant-a":
            return []
        return [dict(item) for item in self.alerts.values() if item.get("run_id") == run_id]

    def get_latest_assignment(self, tenant_id, alert_id):
        item = self.alerts.get(alert_id) or {}
        assigned_to = item.get("assigned_to")
        return {"alert_id": alert_id, "assigned_to": assigned_to, "status": "open"} if assigned_to else None

    def list_cases(self, tenant_id):
        return []

    def list_users(self, tenant_id):
        return list(self.users)


class _Pipeline:
    def get_run_info(self, tenant_id, user_scope):
        return {"run_id": "run-1"}


def _client(user):
    app = FastAPI()
    app.include_router(alerts_router)
    app.include_router(investigation_router)
    app.include_router(auth_router)
    app.state.repository = _Repo()
    app.state.pipeline_service = _Pipeline()
    app.state.settings = SimpleNamespace(runtime_mode="pilot")
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: user["tenant_id"]

    @app.middleware("http")
    async def _inject_user(request, call_next):
        request.state.current_user = user
        return await call_next(request)

    return TestClient(app)


def test_analyst_cannot_fetch_unassigned_alert_by_id():
    user = {"user_id": "u1", "id": "u1", "role": "analyst", "permissions": ["view_assigned_alerts"], "tenant_id": "tenant-a"}
    res = _client(user).get("/api/alerts/A2")
    assert res.status_code == 403


def test_analyst_can_fetch_assigned_alert_by_id():
    user = {"user_id": "u1", "id": "u1", "role": "analyst", "permissions": ["view_assigned_alerts"], "tenant_id": "tenant-a"}
    res = _client(user).get("/api/alerts/A1")
    assert res.status_code == 200
    assert res.json()["alert_id"] == "A1"


def test_manager_can_fetch_tenant_alert():
    user = {"user_id": "m1", "id": "m1", "role": "manager", "permissions": ["view_all_alerts"], "tenant_id": "tenant-a"}
    res = _client(user).get("/api/alerts/A2")
    assert res.status_code == 200


def test_cross_tenant_alert_fetch_is_safe_not_found():
    user = {"user_id": "m1", "id": "m1", "role": "manager", "permissions": ["view_all_alerts"], "tenant_id": "tenant-b"}
    res = _client(user).get("/api/alerts/A1")
    assert res.status_code == 404


def test_alert_list_filters_unassigned_alerts_for_analyst():
    user = {"user_id": "u1", "id": "u1", "role": "analyst", "permissions": ["view_assigned_alerts"], "tenant_id": "tenant-a"}
    res = _client(user).get("/api/alerts")
    assert res.status_code == 200
    ids = {item["alert_id"] for item in res.json()["alerts"]}
    assert ids == {"A1"}


def test_admin_user_list_is_sanitized():
    user = {"user_id": "admin", "id": "admin", "role": "admin", "permissions": ["manage_users"], "tenant_id": "tenant-a"}
    res = _client(user).get("/api/admin/users")
    assert res.status_code == 200
    payload = res.json()
    assert "password_hash" not in str(payload)
    assert "refresh_token" not in str(payload)


def test_sanitizers_remove_sensitive_fields():
    user = sanitize_user_dto({"id": "u1", "email": "a@b.test", "password_hash": "x", "token": "y"})
    assert user == {"id": "u1", "email": "a@b.test", "user_id": "u1"}
    run = sanitize_training_run_dto({"training_run_id": "r1", "dataset_hash": "secret", "error_message": "stack trace"}, full=False)
    assert "dataset_hash" not in run
    assert "error_message" not in run


def test_auth_response_never_exposes_refresh_token_json():
    from api.routers.auth_router import _build_auth_response

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                settings=SimpleNamespace(
                    expose_refresh_token_in_response=True,
                    refresh_token_minutes=60,
                    refresh_cookie_name="althea_rt",
                    refresh_cookie_path="/api/auth",
                    refresh_cookie_domain=None,
                    refresh_cookie_secure=False,
                    refresh_cookie_samesite="strict",
                )
            )
        )
    )
    response = _build_auth_response(
        request,
        access_token="access",
        refresh_token="refresh-secret",
        user_payload={"id": "u1"},
    )
    assert "refresh_token" not in response.body.decode("utf-8")
