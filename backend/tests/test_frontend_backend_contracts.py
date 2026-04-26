from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.routers.alerts_router import router as alerts_router
from api.routers.intelligence_router import router as intelligence_router
from api.routers.investigation_router import router as investigation_router
from api.routers.pipeline_router import router as pipeline_router
from investigation.analyst_workspace_enrichment_service import AnalystWorkspaceEnrichmentService
from core.config import Settings
from core.security import (
    build_access_token,
    build_refresh_token,
    decode_token,
    get_authenticated_tenant_id,
    get_current_user,
    require_permissions,
)


class _FakeRepo:
    def __init__(self) -> None:
        self._assignment = {
            "id": "as1",
            "tenant_id": "tenant-a",
            "alert_id": "A1",
            "assigned_to": "u1",
            "assigned_by": "u1",
            "status": "open",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._outcome = None
        self._auth_logs = [
            {
                "id": "auth-1",
                "tenant_id": "tenant-a",
                "user_id": "u1",
                "actor_id": "u1",
                "action": "login_success",
                "details_json": {"request_id": "req-1"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]
        self._alert_payload = {
            "alert_id": "A1",
            "risk_score": 91.3,
            "risk_band": "critical",
            "priority": "high",
            "typology": "sanctions",
            "segment": "retail",
            "user_id": "U1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model_version": "model-v1",
            "governance_status": "eligible",
            "p50_hours": 8.5,
            "p90_hours": 28.0,
            "time_model_version": "time-fallback-v1",
        }

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000):
        return [
            dict(self._alert_payload),
            {**dict(self._alert_payload), "alert_id": "A2", "risk_score": 55.0},
        ]

    def get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        if alert_id == "A1":
            return dict(self._alert_payload)
        if alert_id == "A2":
            return {**dict(self._alert_payload), "alert_id": "A2", "risk_score": 55.0}
        return None

    def list_cases(self, tenant_id: str):
        return [{"case_id": "CASE_001", "alert_id": "A1", "status": "open", "assigned_to": "u1"}]

    def get_latest_assignment(self, tenant_id: str, alert_id: str):
        return dict(self._assignment) if alert_id == "A1" else None

    def upsert_assignment(self, payload: dict):
        self._assignment = dict(payload)
        return dict(self._assignment)

    def list_alert_notes(self, tenant_id: str, alert_id: str):
        return []

    def create_alert_note(self, payload: dict):
        return payload

    def append_investigation_log(self, payload: dict):
        return payload

    def list_investigation_logs(self, tenant_id: str, case_id: str | None = None, limit: int = 200):
        return [
            {
                "id": "inv-1",
                "tenant_id": tenant_id,
                "case_id": "CASE_001",
                "alert_id": "A1",
                "action": "case_created",
                "performed_by": "u1",
                "details_json": {"request_id": "req-1"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ][:limit]

    def list_auth_audit_logs(self, tenant_id: str, limit: int = 300):
        return list(self._auth_logs)[:limit]

    def get_case(self, tenant_id: str, case_id: str):
        return {
            "case_id": case_id,
            "tenant_id": tenant_id,
            "status": "open",
            "alert_id": "A1",
            "created_by": "u1",
            "assigned_to": "u1",
            "payload_json": {"status": "open"},
            "immutable_timeline_json": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def save_case(self, payload: dict):
        return payload

    def get_model_version(self, tenant_id: str, model_version: str):
        return {"model_version": model_version, "approval_status": "approved"}

    def list_model_monitoring(self, tenant_id: str, limit: int = 200):
        return [{"run_id": "run-1", "model_version": "model-v1", "created_at": datetime.now(timezone.utc).isoformat()}]

    def save_ai_summary(self, payload: dict):
        return {"summary": payload.get("summary"), "ts": datetime.now(timezone.utc).isoformat()}

    def get_ai_summary(self, tenant_id: str, entity_type: str, entity_id: str):
        return None

    def delete_ai_summary(self, tenant_id: str, entity_type: str, entity_id: str):
        return True

    def ping(self):
        return True

    def set_tenant_context(self, tenant_id: str):
        return None


class _FakePipeline:
    def __init__(self) -> None:
        self.last_ingestion_kwargs: dict[str, object] = {}
        self.primary_mode = "alert_jsonl"
        self.run_info_calls: list[str] = []

    def get_run_info(self, tenant_id: str, user_scope: str):
        self.run_info_calls.append(user_scope)
        return {"run_id": "run-1"}

    def list_runs(self, tenant_id: str):
        return [{"run_id": "run-1"}]

    def run_alert_ingestion_pipeline(
        self,
        file_path: str,
        run_id: str,
        tenant_id: str | None = None,
        user_scope: str = "public",
        canary_override: bool = False,
        upload_row_count: int | None = None,
    ):
        self.last_ingestion_kwargs = {
            "file_path": file_path,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "user_scope": user_scope,
            "canary_override": canary_override,
            "upload_row_count": upload_row_count,
        }
        return {
            "run_id": run_id,
            "total_rows": 2,
            "success_count": 2,
            "failed_count": 0,
            "warning_count": 0,
            "strict_mode_used": False,
            "source_system": "alert_jsonl",
            "elapsed_ms": 15,
            "status": "accepted",
            "failure_reason_category": "none",
            "ingested_alert_count": 2,
            "ingested_transaction_count": 3,
            "data_quality_inconsistency_count": 0,
            "data_quality_counts": {},
            "rollout_mode": "full",
            "rollout_decision": "GO",
        }

    def get_rollout_status(self, tenant_id: str, window_runs: int = 20):
        return {
            "primary_ingestion_mode": self.primary_mode,
            "rollout_mode": "full",
            "ingestion_enabled": True,
            "strict_validation_enabled": False,
            "max_upload_rows": 1000,
            "last_ingestion_result": {"run_id": "run-1", "status": "accepted"},
            "decision": "GO",
            "reasons": ["all_rollout_gates_passing"],
            "metrics_snapshot": {"run_count": 3},
            "thresholds": {"max_failure_rate_hold": 0.01},
            "legacy_ingestion_enabled": False,
            "legacy_disabled": True,
            "blocked_legacy_attempts_recent": 0,
            "legacy_access_attempts_recent": 0,
            "legacy_access_by_endpoint": {},
            "recent_ingestion_failure_runs": 0,
            "recent_ingestion_warning_count": 0,
            "new_ingestion_healthy": True,
        }

    def get_finalization_status(self, tenant_id: str, window_runs: int = 20):
        return self.get_rollout_status(tenant_id=tenant_id, window_runs=window_runs)

    def get_primary_ingestion_mode(self):
        return self.primary_mode

    def set_runtime_primary_ingestion_mode(self, mode: str):
        previous = self.primary_mode
        self.primary_mode = str(mode)
        return {
            "status": "ok",
            "primary_ingestion_mode": self.primary_mode,
            "previous_primary_ingestion_mode": previous,
            "source": "runtime_override",
        }


class _FakeIngestionService:
    def upload_transactions_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes):
        rows = sum(1 for line in raw_bytes.decode("utf-8", errors="ignore").splitlines() if line.strip()) - 1
        return {"rows": max(0, rows), "source": "LegacyCSV"}

    def upload_bank_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes):
        rows = sum(1 for line in raw_bytes.decode("utf-8", errors="ignore").splitlines() if line.strip()) - 1
        return {"rows": max(0, rows), "source": "BankCSV"}

    def generate_synthetic(self, tenant_id: str, user_scope: str, n_rows: int = 400):
        return {"rows": int(n_rows), "source": "Synthetic"}


class _FakeCaseService:
    def __init__(self) -> None:
        self.last_create_case: dict | None = None
        self.last_update_case: dict | None = None
        self._actors: dict[tuple[str, str], str] = {}

    def list_cases(self, tenant_id: str):
        return {"CASE_001": {"case_id": "CASE_001", "status": "OPEN", "alert_ids": ["A1"]}}

    def get_actor(self, tenant_id: str, user_scope: str):
        return self._actors.get((tenant_id, user_scope), "u1")

    def set_actor(self, tenant_id: str, user_scope: str, actor: str):
        self._actors[(tenant_id, user_scope)] = actor
        return {"tenant_id": tenant_id, "user_scope": user_scope, "actor": actor}

    def create_case(self, tenant_id: str, user_scope: str, alert_ids: list[str], run_id: str, actor: str):
        self.last_create_case = {
            "tenant_id": tenant_id,
            "user_scope": user_scope,
            "alert_ids": list(alert_ids),
            "run_id": run_id,
            "actor": actor,
        }
        return {
            "case_id": "CASE_001",
            "status": "OPEN",
            "assigned_to": actor,
            "created_by": actor,
            "alert_ids": list(alert_ids),
        }

    def update_case(self, **kwargs):
        self.last_update_case = dict(kwargs)
        return True, "ok", {
            "status": kwargs.get("status", "open"),
            "assigned_to": kwargs.get("assigned_to") or "u1",
            "closed_at": None,
        }

    def delete_case(self, tenant_id: str, case_id: str):
        return True

    def get_case_audit(self, case_id: str, tenant_id: str):
        return []


class _FakeWorkflow:
    def create_case_from_alert(self, tenant_id: str, alert_id: str, run_id: str, actor: str = "analyst"):
        return "CASE_001"

    def transition_case(self, tenant_id: str, case_id: str, to_state: str, actor: str, reason: str, escalation_level=None):
        return {"case_id": case_id, "from_state": "assigned", "to_state": to_state, "reason": reason}

    def escalate_case(self, tenant_id: str, case_id: str, actor: str):
        return {"case_id": case_id, "from_state": "investigating", "to_state": "escalated", "reason": "manual"}

    def monitor_sla(self, tenant_id: str):
        return []


class _FakeFeedback:
    def __init__(self):
        self._store = {}

    def record_outcome(self, tenant_id: str, alert_id: str, **kwargs):
        self._store[(tenant_id, alert_id)] = {"alert_id": alert_id, **kwargs}
        return self._store[(tenant_id, alert_id)]

    def get_outcome(self, tenant_id: str, alert_id: str):
        return self._store.get((tenant_id, alert_id))

    def list_outcomes(self, tenant_id: str, limit: int = 200):
        return list(self._store.values())[:limit]

    def get_outcome_statistics(self, tenant_id: str):
        return {"total_outcomes": len(self._store)}


class _FakeGraphService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def build_graph(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        self.calls.append({"tenant_id": tenant_id, "alert_id": alert_id, "run_id": run_id})
        if alert_id == "A1":
            return {
                "alert_id": alert_id,
                "nodes": [
                    {"id": "alert:A1", "label": "Alert A1", "type": "alert", "risk": "high", "meta": {}},
                    {"id": "customer:U1", "label": "Customer U1", "type": "customer", "risk": "medium", "meta": {}},
                ],
                "edges": [
                    {
                        "source": "alert:A1",
                        "target": "customer:U1",
                        "type": "associated_with",
                        "relation": "associated_with",
                        "weight": 1,
                        "meta": {},
                    }
                ],
                "summary": {"node_count": 2, "edge_count": 1, "high_risk_nodes": 1},
                "node_count": 2,
                "edge_count": 1,
                "risk_signals": ["customer"],
            }
        if alert_id == "A2":
            return {
                "alert_id": alert_id,
                "nodes": [{"id": "alert:A2", "label": "Alert A2", "type": "alert", "risk": "low", "meta": {}}],
                "edges": [],
                "summary": {"node_count": 1, "edge_count": 0, "high_risk_nodes": 0},
                "node_count": 1,
                "edge_count": 0,
                "risk_signals": [],
            }
        return {
            "alert_id": alert_id,
            "nodes": [],
            "edges": [],
            "summary": {"node_count": 0, "edge_count": 0, "high_risk_nodes": 0},
            "node_count": 0,
            "edge_count": 0,
            "risk_signals": [],
        }


class _FakeNarrativeService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def generate_draft(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        self.calls.append({"tenant_id": tenant_id, "alert_id": alert_id, "run_id": run_id})
        if alert_id == "A1":
            return {
                "alert_id": alert_id,
                "title": "Investigation Narrative Draft",
                "narrative": "Between review periods, account activity indicated elevated movement to linked counterparties.",
                "sections": {
                    "activity_summary": "Customer U1 moved funds to a linked destination account.",
                    "risk_indicators": ["Risk score above threshold", "High-risk typology signal"],
                    "recommended_follow_up": ["Validate counterparties", "Review source of funds evidence"],
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_signals": {"risk_score": 91.3, "reason_codes": ["R1"], "countries": ["US"]},
            }
        if alert_id == "A2":
            return {
                "alert_id": alert_id,
                "title": "Investigation Narrative Draft",
                "narrative": "Limited source details available; analyst review is required.",
                "sections": {
                    "activity_summary": "Only partial activity context was available.",
                    "risk_indicators": [],
                    "recommended_follow_up": ["Review transaction history"],
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_signals": {"risk_score": 55.0, "reason_codes": [], "countries": []},
            }
        return {
            "alert_id": alert_id,
            "title": "Investigation Narrative Draft",
            "narrative": "Draft unavailable due to limited source data; analyst validation is required.",
            "sections": {
                "activity_summary": "Insufficient transaction context available at draft time.",
                "risk_indicators": [],
                "recommended_follow_up": ["Review available alert and case history"],
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_signals": {"risk_score": None, "reason_codes": [], "countries": []},
        }


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(pipeline_router)
    app.include_router(alerts_router)
    app.include_router(investigation_router)
    app.include_router(intelligence_router)

    app.state.settings = SimpleNamespace(
        default_tenant_id="tenant-a",
        tenant_header="X-Tenant-ID",
        rq_queue_name="althea-pipeline",
        enable_alert_jsonl_ingestion=True,
        enable_legacy_ingestion=True,
        alert_jsonl_max_upload_rows=1000,
        ingestion_max_upload_bytes=10 * 1024 * 1024,
        primary_ingestion_mode="alert_jsonl",
    )
    app.state.repository = _FakeRepo()
    app.state.pipeline_service = _FakePipeline()
    app.state.ingestion_service = _FakeIngestionService()
    app.state.case_service = _FakeCaseService()
    app.state.workflow_engine = _FakeWorkflow()
    app.state.feedback_service = _FakeFeedback()
    app.state.investigation_summary_service = SimpleNamespace(generate_summary=lambda **_: {"summary": "ok"})
    app.state.risk_explanation_service = SimpleNamespace(generate_explanation=lambda **_: {"model_version": "model-v1"})
    app.state.relationship_graph_service = _FakeGraphService()
    app.state.guidance_service = SimpleNamespace(generate_steps=lambda **_: {"steps": [{"step": 1, "description": "Do X"}]})
    app.state.sar_generator = SimpleNamespace(generate_sar_draft=lambda **_: {"narrative": "draft"})
    app.state.narrative_service = _FakeNarrativeService()
    app.state.global_pattern_service = SimpleNamespace(get_signals_for_alert=lambda **_: [{"signal_type": "cross_tenant"}])
    app.state.analyst_workspace_enrichment_service = AnalystWorkspaceEnrichmentService(
        repository=app.state.repository,
        enrichment_repository=SimpleNamespace(
            get_enrichment_context_snapshot=lambda **_: {
                "alert_payload": dict(app.state.repository._alert_payload),
                "entity_ids": ["U1"],
                "account_events": [],
                "alert_outcomes": [],
                "case_actions": [],
            },
            list_master_customers=lambda tenant_id: [],
            list_master_accounts=lambda tenant_id: [],
            list_master_counterparties=lambda tenant_id: [],
            list_latest_source_health=lambda tenant_id: [],
        ),
    )
    app.state.event_bus = SimpleNamespace(publish=lambda **_: None)
    app.state.metrics = SimpleNamespace(
        set_gauge=lambda *args, **kwargs: None,
        prometheus=lambda: "# test_metrics 1\n",
    )
    app.state.ops_service = SimpleNamespace(compute_ops_metrics=lambda *_: {"precision_k": 0.0, "alerts_per_case": 0.0, "suppression_rate": 0.0})
    app.state.explain_service = SimpleNamespace(explain_alert=lambda **_: {"model_version": "model-v1"})
    app.state.ai_copilot_service = SimpleNamespace(generate_copilot_summary=lambda **_: {"summary": "copilot"})
    app.state.cache = SimpleNamespace(
        ping=lambda: True,
        get_json=lambda key, default=None: {"ts": datetime.now(timezone.utc).isoformat()}
        if key.startswith("heartbeat:worker")
        else default,
        increment_counter=lambda key, ttl_seconds: 1,
        delete=lambda key: None,
    )
    app.state.job_queue_service = SimpleNamespace(queue_depth=lambda _: 0)
    app.state.ml_service = SimpleNamespace(list_versions=lambda _: [{"model_version": "model-v1"}])
    app.state.feature_registry = SimpleNamespace(list_features=lambda tenant_id: [{"name": "f1"}])

    user = {
        "user_id": "u1",
        "id": "u1",
        "role": "admin",
        "permissions": [
            "change_alert_status",
            "reassign_alerts",
            "view_all_alerts",
            "view_team_queue",
            "approve_escalations",
            "manager_approval",
            "view_system_logs",
        ],
        "tenant_id": "tenant-a",
    }
    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: "tenant-a"

    return TestClient(app)


def test_health_endpoint_contract_shape(client: TestClient):
    res = client.get("/health")
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "alive"
    assert payload["ok"] is True

    internal = client.get("/internal/health")
    assert internal.status_code == 200
    internal_payload = internal.json()
    assert internal_payload["status"] in {"healthy", "degraded"}
    assert "worker_heartbeat" in internal_payload["checks"]
    assert "feature_store" in internal_payload["checks"]

    ready = client.get("/readyz")
    assert ready.status_code == 200
    ready_payload = ready.json()
    assert ready_payload["status"] == "ready"
    assert "checks" in ready_payload


def test_readiness_endpoint_returns_503_when_dependency_is_unhealthy(client: TestClient):
    client.app.state.cache = SimpleNamespace(
        ping=lambda: (_ for _ in ()).throw(RuntimeError("redis unavailable")),
        get_json=lambda key, default=None: default,
    )
    res = client.get("/readyz")
    assert res.status_code == 503
    payload = res.json()
    assert payload["status"] == "degraded"
    assert payload["ok"] is False


def test_alert_jsonl_upload_returns_structured_503_when_disabled(client: TestClient):
    client.app.state.settings.enable_alert_jsonl_ingestion = False
    res = client.post(
        "/api/data/upload-alert-jsonl",
        files={"file": ("alerts.jsonl", b'{"alert_id":"A1"}\n', "application/json")},
    )
    assert res.status_code == 503
    payload = res.json()
    assert payload["error"] == "alert_jsonl_ingestion_disabled"
    assert payload["message"] == "Alert JSONL ingestion is disabled by configuration."


def test_alert_jsonl_upload_enabled_returns_summary_payload(client: TestClient, tmp_path: Path):
    client.app.state.settings.enable_alert_jsonl_ingestion = True
    client.app.state.settings.strict_ingestion_validation = False
    client.app.state.settings.enable_ibm_amlsim_import = False
    client.app.state.settings.alert_jsonl_max_upload_rows = 1000
    client.app.state.settings.data_dir = tmp_path

    res = client.post(
        "/api/data/upload-alert-jsonl",
        files={"file": ("alerts.jsonl", b'{"alert_id":"A1"}\n{"alert_id":"A2"}\n', "application/json")},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "accepted"
    assert payload["success_count"] == 2
    assert payload["failed_count"] == 0
    assert payload["source_system"] == "alert_jsonl"
    assert "summary" in payload
    assert payload["summary"]["strict_mode_used"] is False
    upload_path = Path(str(client.app.state.pipeline_service.last_ingestion_kwargs["file_path"]))
    assert not upload_path.exists()


def test_finalization_status_endpoint_returns_stabilization_summary(client: TestClient):
    res = client.get("/internal/migration/finalization-status")
    assert res.status_code == 200
    payload = res.json()
    assert "legacy_disabled" in payload
    assert "blocked_legacy_attempts_recent" in payload
    assert "new_ingestion_healthy" in payload


def test_internal_rollout_status_endpoint_returns_operator_summary(client: TestClient):
    res = client.get("/internal/rollout/status")
    assert res.status_code == 200
    payload = res.json()
    assert payload["decision"] in {"GO", "HOLD", "ROLLBACK"}
    assert "metrics_snapshot" in payload


def test_unified_upload_routes_to_primary_alert_jsonl_mode(client: TestClient, tmp_path: Path):
    client.app.state.settings.enable_alert_jsonl_ingestion = True
    client.app.state.settings.data_dir = tmp_path
    client.app.state.pipeline_service.primary_mode = "alert_jsonl"

    res = client.post(
        "/api/data/upload",
        files={"file": ("alerts.jsonl", b'{"alert_id":"A1"}\n', "application/json")},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "accepted"
    assert payload["source_system"] == "alert_jsonl"


def test_unified_upload_routes_to_legacy_mode_when_primary_is_legacy(client: TestClient):
    client.app.state.pipeline_service.primary_mode = "legacy"
    csv_bytes = (
        "alert_id,user_id,amount,segment,country,typology,source_system,timestamp_utc\n"
        "ALT001,U1,1500.00,retail,US,structuring,core_bank,2026-04-05T00:00:00Z\n"
    ).encode("utf-8")
    res = client.post(
        "/api/data/upload",
        files={"file": ("alerts.csv", csv_bytes, "text/csv")},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["source"] == "BankCSV"
    assert payload["rows"] == 1


def test_legacy_endpoints_return_503_when_legacy_ingestion_disabled(client: TestClient):
    client.app.state.settings.enable_legacy_ingestion = False
    csv_bytes = (
        "alert_id,user_id,amount,segment,country,typology,source_system,timestamp_utc\n"
        "ALT001,U1,1500.00,retail,US,structuring,core_bank,2026-04-05T00:00:00Z\n"
    ).encode("utf-8")
    for path in ("/api/data/upload-csv", "/api/data/upload-bank-csv"):
        res = client.post(path, files={"file": ("alerts.csv", csv_bytes, "text/csv")})
        assert res.status_code == 503
        payload = res.json()
        assert payload["error"] == "legacy_ingestion_disabled"
        assert "finalization" in payload["message"].lower()


def test_unified_upload_returns_503_for_legacy_override_when_legacy_disabled(client: TestClient):
    client.app.state.settings.enable_legacy_ingestion = False
    client.app.state.pipeline_service.primary_mode = "alert_jsonl"
    csv_bytes = (
        "alert_id,user_id,amount,segment,country,typology,source_system,timestamp_utc\n"
        "ALT001,U1,1500.00,retail,US,structuring,core_bank,2026-04-05T00:00:00Z\n"
    ).encode("utf-8")
    res = client.post(
        "/api/data/upload?ingestion_mode=legacy",
        files={"file": ("alerts.csv", csv_bytes, "text/csv")},
    )
    assert res.status_code == 503
    assert res.json()["error"] == "legacy_ingestion_disabled"


def test_upload_returns_413_when_file_exceeds_size_limit(client: TestClient):
    client.app.state.settings.ingestion_max_upload_bytes = 4
    payload = b'{"alert_id":"A1"}\n'
    res = client.post(
        "/api/data/upload-alert-jsonl",
        files={"file": ("alerts.jsonl", payload, "application/json")},
    )
    assert res.status_code == 413
    detail = res.json()["detail"]
    assert detail["error"] == "upload_too_large"


def test_alert_jsonl_upload_rejects_non_jsonl_extension(client: TestClient, tmp_path: Path):
    client.app.state.settings.enable_alert_jsonl_ingestion = True
    client.app.state.settings.data_dir = tmp_path
    res = client.post(
        "/api/data/upload-alert-jsonl",
        files={"file": ("alerts.csv", b'{"alert_id":"A1"}\n', "application/json")},
    )
    assert res.status_code == 400
    assert "jsonl" in str(res.json().get("detail", "")).lower()


def test_metrics_endpoint_requires_view_system_logs_permission(client: TestClient):
    ok = client.get("/metrics")
    assert ok.status_code == 200

    client.app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "u1",
        "id": "u1",
        "role": "manager",
        "permissions": ["view_all_alerts"],
        "tenant_id": "tenant-a",
    }
    forbidden = client.get("/metrics")
    assert forbidden.status_code == 403


def test_internal_ml_predict_passes_runtime_enrichment_context(client: TestClient):
    captured: dict[str, object] = {}

    class _CapturingFeatureService:
        def generate_inference_features(self, frame, context=None):
            captured["context"] = context
            return {"feature_matrix": frame[["amount"]].copy()}

    class _StubEnrichmentService:
        def build_context(self, *, tenant_id: str, alerts_df, run_id: str | None = None):
            captured["tenant_id"] = tenant_id
            captured["run_id"] = run_id
            captured["row_count"] = int(len(alerts_df))
            return {"kind": "runtime_enrichment"}

    client.app.state.feature_service = _CapturingFeatureService()
    client.app.state.feature_enrichment_service = _StubEnrichmentService()
    client.app.state.inference_service = SimpleNamespace(
        predict=lambda **_: {
            "model_version": "model-v1",
            "scores": [55.0],
            "explanations": [{}],
            "schema_validation": {"is_valid": True},
        }
    )

    res = client.post(
        "/internal/ml/predict",
        json={
            "alert_ids": ["A1"],
            "rows": [
                {
                    "alert_id": "A1",
                    "user_id": "U1",
                    "amount": 1500.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
        },
    )

    assert res.status_code == 200
    assert captured["context"] == {"kind": "runtime_enrichment"}
    assert captured["tenant_id"] == "tenant-a"
    assert captured["run_id"] == "run-1"
    assert captured["row_count"] == 1


def test_primary_mode_runtime_switch_allows_fast_rollback(client: TestClient):
    to_legacy = client.post("/internal/ingestion/primary-mode", json={"mode": "legacy"})
    assert to_legacy.status_code == 200
    assert to_legacy.json()["primary_ingestion_mode"] == "legacy"

    to_alert = client.post("/internal/ingestion/primary-mode", json={"mode": "alert_jsonl"})
    assert to_alert.status_code == 200
    assert to_alert.json()["primary_ingestion_mode"] == "alert_jsonl"


def test_work_queue_includes_sla_fields(client: TestClient):
    res = client.get("/api/work/queue")
    assert res.status_code == 200
    item = res.json()["queue"][0]
    assert "alert_age_hours" in item
    assert "assignment_age_hours" in item
    assert "overdue_review" in item


def test_investigation_context_contract_for_ui(client: TestClient):
    res = client.get("/api/alerts/A1/investigation-context")
    assert res.status_code == 200
    payload = res.json()
    assert payload["alert_id"] == "A1"
    assert "network_graph" in payload
    assert "global_signals" in payload
    assert "model_metadata" in payload
    assert "customer_profile" in payload
    assert "account_profile" in payload
    assert "behavior_baseline" in payload
    assert "counterparty_summary" in payload
    assert "geography_payment_summary" in payload
    assert "screening_summary" in payload
    assert "data_availability" in payload
    assert payload["screening_summary"]["screening_status"] == "unavailable"


def test_network_graph_endpoint_contract_for_normal_alert(client: TestClient):
    res = client.get("/api/alerts/A1/network-graph")
    assert res.status_code == 200
    payload = res.json()
    assert payload["alert_id"] == "A1"
    assert payload["summary"]["node_count"] >= 1
    assert isinstance(payload["nodes"], list)
    assert isinstance(payload["edges"], list)


def test_network_graph_endpoint_handles_partial_data(client: TestClient):
    res = client.get("/api/alerts/A2/network-graph")
    assert res.status_code == 200
    payload = res.json()
    assert payload["alert_id"] == "A2"
    assert payload["summary"]["node_count"] == 1
    assert payload["summary"]["edge_count"] == 0


def test_network_graph_endpoint_returns_empty_graph_when_missing(client: TestClient):
    res = client.get("/api/alerts/A404/network-graph")
    assert res.status_code == 404


def test_narrative_draft_endpoint_contract_for_normal_alert(client: TestClient):
    res = client.get("/api/alerts/A1/narrative-draft")
    assert res.status_code == 200
    payload = res.json()
    assert payload["alert_id"] == "A1"
    assert payload["title"] == "Investigation Narrative Draft"
    assert "narrative" in payload
    assert "sections" in payload
    assert "source_signals" in payload


def test_narrative_draft_endpoint_minimal_fallback(client: TestClient):
    res = client.get("/api/alerts/A404/narrative-draft")
    assert res.status_code == 404


def test_graph_and_narrative_endpoints_respect_authenticated_tenant_context(client: TestClient):
    graph_res = client.get("/api/alerts/A1/network-graph", headers={"X-Tenant-ID": "tenant-b"})
    draft_res = client.get("/api/alerts/A1/narrative-draft", headers={"X-Tenant-ID": "tenant-b"})
    assert graph_res.status_code == 200
    assert draft_res.status_code == 200

    graph_call = client.app.state.relationship_graph_service.calls[-1]
    draft_call = client.app.state.narrative_service.calls[-1]
    assert graph_call["tenant_id"] == "tenant-a"
    assert draft_call["tenant_id"] == "tenant-a"


def test_bulk_status_endpoint_round_trip(client: TestClient):
    res = client.post("/api/alerts/bulk-status", json={"alert_ids": ["A1"], "status": "escalated"})
    assert res.status_code == 200
    assert res.json()["updated"] == 1
    assert res.json()["new_state"] == "escalated"


def test_alert_status_accepts_canonical_aliases_and_returns_unified_fields(client: TestClient):
    res = client.post("/api/alerts/A1/status", json={"status": "under_review"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "in_review"
    assert payload["case_status"] == "under_review"
    assert payload["workflow_state"] in {"investigating", "assigned", "escalated", "closed", "sar_candidate"}


def test_workflow_assign_alias_keeps_unified_case_status_shape(client: TestClient):
    res = client.post("/api/workflows/alerts/A1/assign", json={"assigned_to": "u1", "actor": "u1"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["status"] == "assigned"
    assert payload["case_status"] == "open"
    assert payload["workflow_state"] == "assigned"


def test_outcome_feedback_round_trip(client: TestClient):
    write = client.post("/api/alerts/A1/outcome", json={"analyst_decision": "true_positive", "analyst_id": "spoofed"})
    assert write.status_code == 200
    read = client.get("/api/alerts/A1/outcome")
    assert read.status_code == 200
    assert read.json()["analyst_decision"] == "true_positive"
    assert read.json()["analyst_id"] == "u1"


def test_case_creation_ignores_client_actor_and_uses_authenticated_user(client: TestClient):
    res = client.post("/api/cases", json={"alert_ids": ["A1"], "actor": "spoofed-user"})
    assert res.status_code == 200
    assert client.app.state.case_service.last_create_case is not None
    assert client.app.state.case_service.last_create_case["actor"] == "u1"
    assert client.app.state.case_service.last_create_case["user_scope"] == "u1"


def test_work_queue_ignores_spoofed_user_scope_header(client: TestClient):
    res = client.get("/api/work/queue", headers={"X-User-Scope": "spoofed-user"})
    assert res.status_code == 200
    assert client.app.state.pipeline_service.run_info_calls[-1] == "u1"


def test_case_access_denies_cross_user_read_for_non_elevated_permissions(client: TestClient):
    client.app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "u2",
        "id": "u2",
        "role": "analyst",
        "permissions": ["work_cases"],
        "tenant_id": "tenant-a",
    }
    res = client.get("/api/cases/CASE_001")
    assert res.status_code == 403


def test_case_delete_requires_manager_approval_permission(client: TestClient):
    client.app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "u1",
        "id": "u1",
        "role": "manager",
        "permissions": ["work_cases"],
        "tenant_id": "tenant-a",
    }
    res = client.delete("/api/cases/CASE_001")
    assert res.status_code == 403


def test_admin_logs_export_supports_jsonl_and_csv(client: TestClient):
    jsonl_response = client.get("/api/admin/logs/export?stream=auth&format=jsonl")
    assert jsonl_response.status_code == 200
    assert jsonl_response.headers["content-type"].startswith("application/x-ndjson")
    assert '"log_type": "auth"' in jsonl_response.text

    csv_response = client.get("/api/admin/logs/export?stream=all&format=csv")
    assert csv_response.status_code == 200
    assert csv_response.headers["content-type"].startswith("text/csv")
    assert "log_type" in csv_response.text
    assert "login_success" in csv_response.text


def test_time_estimate_endpoint_returns_scalar_values_for_model_predictions(client: TestClient):
    client.app.state.investigation_time_service = SimpleNamespace(
        predict=lambda **_: {
            "p50_hours": [4.0],
            "p90_hours": [11.5],
            "model_version": "time-v2",
        }
    )
    res = client.get("/api/alerts/A1/time-estimate")
    assert res.status_code == 200
    payload = res.json()
    assert payload["source"] == "time_model"
    assert payload["p50_hours"] == 4.0
    assert payload["p90_hours"] == 11.5


def test_time_estimate_endpoint_falls_back_to_cached_payload_when_model_not_wired(client: TestClient):
    if hasattr(client.app.state, "investigation_time_service"):
        delattr(client.app.state, "investigation_time_service")
    res = client.get("/api/alerts/A1/time-estimate")
    assert res.status_code == 200
    payload = res.json()
    assert payload["source"] == "cached_payload"
    assert payload["p50_hours"] == 8.5
    assert payload["p90_hours"] == 28.0


def test_copilot_summary_endpoint_keeps_contract(client: TestClient):
    res = client.get("/alerts/A1/copilot_summary")
    assert res.status_code == 200
    assert "summary" in res.json()


def test_token_and_tenant_security_guards():
    settings = Settings(jwt_secret="x" * 48, app_env="development")
    user = {"id": "u1", "role": "analyst", "team": "t1"}
    access = build_access_token(settings, "tenant-a", user, "session-1")
    refresh = build_refresh_token(settings, "tenant-a", user, "session-1")

    access_claims = decode_token(settings, access)
    refresh_claims = decode_token(settings, refresh)
    assert access_claims["tenant_id"] == "tenant-a"
    assert refresh_claims["type"] == "refresh"

    with pytest.raises(HTTPException):
        get_authenticated_tenant_id(request=SimpleNamespace(headers={}), user={"tenant_id": "tenant-a"}, x_tenant_id="tenant-b")


def test_rbac_permission_guard_enforced():
    dependency = require_permissions("manage_users")
    with pytest.raises(HTTPException):
        dependency(user={"role": "analyst", "permissions": []})
