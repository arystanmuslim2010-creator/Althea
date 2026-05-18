from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.intelligence_router import router as intelligence_router
from core.security import get_authenticated_tenant_id
from services.counterparty_intelligence_service import CounterpartyIntelligenceService

pytestmark = [pytest.mark.unit, pytest.mark.security]


class _Repo:
    def __init__(self) -> None:
        now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc).isoformat()
        self.alerts = {
            "A1": {
                "alert_id": "A1",
                "run_id": "run-1",
                "account_id": "ACCT-A",
                "timestamp": now,
                "assigned_to": "u1",
                "transactions": [{"timestamp": now, "amount": 100, "direction": "out", "counterparty_id": "CP-1"}],
            },
            "A2": {
                "alert_id": "A2",
                "run_id": "run-1",
                "account_id": "ACCT-B",
                "timestamp": now,
                "transactions": [{"timestamp": now, "amount": 200, "direction": "out", "counterparty_id": "CP-2"}],
            },
        }

    def get_alert_payload(self, tenant_id, alert_id, run_id=None):
        if tenant_id != "tenant-a":
            return None
        item = self.alerts.get(alert_id)
        if item and (not run_id or item.get("run_id") == run_id):
            return dict(item)
        return None

    def list_alert_payloads_by_run(self, tenant_id, run_id, limit=200000):
        if tenant_id != "tenant-a":
            return []
        return [dict(item) for item in self.alerts.values() if item.get("run_id") == run_id]

    def get_latest_assignment(self, tenant_id, alert_id):
        item = self.alerts.get(alert_id) or {}
        assigned_to = item.get("assigned_to")
        return {"alert_id": alert_id, "assigned_to": assigned_to, "status": "open"} if assigned_to else None

    def list_cases(self, tenant_id):
        return []


class _Pipeline:
    def get_run_info(self, tenant_id, user_scope):
        return {"run_id": "run-1"}


class _FailingCounterpartyService:
    def get_counterparty_intelligence(self, **_kwargs):
        raise RuntimeError(r"Traceback C:\secret\stack sqlalchemy token")


def _client(user: dict, service=None) -> TestClient:
    app = FastAPI()
    app.include_router(intelligence_router)
    app.state.repository = _Repo()
    app.state.pipeline_service = _Pipeline()
    app.state.settings = SimpleNamespace(runtime_mode="pilot")
    app.state.counterparty_intelligence_service = service or CounterpartyIntelligenceService(app.state.repository)
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: user["tenant_id"]

    @app.middleware("http")
    async def _inject_user(request, call_next):
        request.state.current_user = user
        return await call_next(request)

    return TestClient(app, raise_server_exceptions=False)


def test_analyst_cannot_access_unassigned_counterparty_intelligence():
    user = {"user_id": "u1", "id": "u1", "role": "analyst", "permissions": ["view_assigned_alerts"], "tenant_id": "tenant-a"}

    res = _client(user).get("/api/alerts/A2/counterparty-intelligence")

    assert res.status_code == 403


def test_manager_can_access_allowed_alert_counterparty_intelligence():
    user = {"user_id": "m1", "id": "m1", "role": "manager", "permissions": ["view_all_alerts"], "tenant_id": "tenant-a"}

    res = _client(user).get("/api/alerts/A2/counterparty-intelligence")

    assert res.status_code == 200
    payload = res.json()["counterparty_intelligence"]
    assert payload["alert_id"] == "A2"
    assert payload["summary"]["total_counterparties"] == 1


def test_cross_tenant_counterparty_intelligence_is_not_found():
    user = {"user_id": "m1", "id": "m1", "role": "manager", "permissions": ["view_all_alerts"], "tenant_id": "tenant-b"}

    res = _client(user).get("/api/alerts/A1/counterparty-intelligence")

    assert res.status_code == 404


def test_endpoint_does_not_leak_raw_internal_errors():
    user = {"user_id": "m1", "id": "m1", "role": "manager", "permissions": ["view_all_alerts"], "tenant_id": "tenant-a"}

    res = _client(user, service=_FailingCounterpartyService()).get("/api/alerts/A1/counterparty-intelligence")

    assert res.status_code == 500
    body = res.text.lower()
    assert "counterparty intelligence unavailable" in body
    assert "traceback" not in body
    assert "secret" not in body
    assert "sqlalchemy" not in body
