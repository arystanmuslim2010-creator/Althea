from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers.alerts_router import router as alerts_router
from api.routers.intelligence_router import router as intelligence_router
from api.routers.pilot_router import router as pilot_router
from core.security import get_authenticated_tenant_id


class _Repo:
    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 500000):
        return [
            {
                "alert_id": "A2",
                "risk_score": 40.0,
                "risk_band": "Low",
                "status": "open",
                "user_id": "U2",
                "account_id": "ACC-2",
                "amount": 1200.0,
                "created_at": "2026-05-02T10:00:00Z",
                "source_system": "tm",
                "evaluation_label_is_sar": 0,
                "transactions_json": [
                    {"timestamp": "2026-05-02T11:00:00Z", "amount": 50},
                    {"timestamp": "2026-05-02T10:30:00Z", "amount": 25},
                ],
            },
            {
                "alert_id": "A1",
                "risk_score": 92.0,
                "risk_band": "High",
                "status": "open",
                "user_id": "U1",
                "account_id": "ACC-1",
                "amount": 9200.0,
                "created_at": "2026-05-02T12:00:00Z",
                "source_system": "tm",
                "evaluation_label_is_sar": 1,
                "top_features_json": '["time_gap"]',
                "transactions_json": [
                    {"timestamp": "2026-05-02T12:10:00Z", "amount": 120},
                    {"timestamp": "2026-05-02T12:01:00Z", "amount": 90},
                ],
            },
        ]

    def get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None = None):
        return next(
            (row for row in self.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id or "run-1") if row["alert_id"] == alert_id),
            None,
        )

    def list_cases(self, tenant_id: str):
        return [{"case_id": "CASE-1", "alert_id": "A1", "status": "under_review", "assigned_to": "u1"}]

    def get_model_version(self, tenant_id: str, model_version: str):
        return {"model_version": model_version, "approval_status": "approved"}

    def list_model_monitoring(self, tenant_id: str, limit: int = 200):
        return [{"run_id": "run-1", "model_version": "model-v1", "created_at": datetime.now(timezone.utc).isoformat()}]


def _app() -> TestClient:
    app = FastAPI()
    app.include_router(alerts_router)
    app.include_router(intelligence_router)
    app.include_router(pilot_router)
    app.state.pipeline_service = SimpleNamespace(get_run_info=lambda tenant_id, user_scope: {"run_id": "run-1"})
    app.state.repository = _Repo()
    app.state.investigation_summary_service = SimpleNamespace(
        generate_summary=lambda **_: {"alert_id": "A1", "key_observations": ["Prioritized for rapid movement of funds."]}
    )
    app.state.risk_explanation_service = SimpleNamespace(generate_explanation=lambda **_: {"model_version": "model-v1"})
    app.state.relationship_graph_service = SimpleNamespace(build_graph=lambda **_: {"alert_id": "A1", "nodes": [], "edges": [], "summary": {}})
    app.state.guidance_service = SimpleNamespace(generate_steps=lambda **_: {"steps": [{"description": "Review account activity."}]})
    app.state.sar_generator = SimpleNamespace(generate_sar_draft=lambda **_: {"narrative": "Draft."})
    app.state.narrative_service = SimpleNamespace(generate_draft=lambda **_: {"narrative": "Draft narrative."})
    app.state.global_pattern_service = SimpleNamespace(get_signals_for_alert=lambda **_: [])
    app.state.feedback_service = SimpleNamespace(get_outcome=lambda **_: None)
    app.state.analyst_workspace_enrichment_service = SimpleNamespace(
        generate_sections=lambda **_: {
            "customer_profile": {},
            "account_profile": {},
            "behavior_baseline": {},
            "counterparty_summary": {},
            "geography_payment_summary": {},
            "screening_summary": {"screening_status": "unavailable"},
            "data_availability": {"missing_sections": [], "coverage_status": "limited", "freshness_status": "legacy_only"},
        }
    )
    app.state.explain_service = SimpleNamespace(
        explain_alert=lambda **_: {
            "human_interpretation": {
                "summary_text": "This alert is prioritized because funds appear to move onward rapidly and may indicate a potential layering pattern.",
                "key_risk_drivers": ["Rapid outgoing movement after receipt of funds"],
                "aml_patterns": ["Potential layering"],
                "analyst_next_steps": ["Review whether funds were redistributed quickly"],
            }
        }
    )
    app.state.pilot_metrics_service = SimpleNamespace(
        summarize_run=lambda **_: {
            "total_alerts_ingested": 2,
            "evaluation_available": True,
            "evaluation_summary": "SAR capture: 50% at top 10%, 100% at top 20%, 100% at top 30%. Estimated workload reduction at target recall: 50%. Lift over baseline: 1.90x.",
        }
    )
    app.dependency_overrides[get_authenticated_tenant_id] = lambda: "tenant-a"
    return TestClient(app)


def test_queue_response_is_sorted_paginated_and_hides_evaluation_labels():
    client = _app()
    res = client.get("/api/alerts?response_mode=queue&limit=1")

    assert res.status_code == 200
    payload = res.json()
    assert payload["total_available"] == 2
    assert payload["alerts"][0]["alert_id"] == "A1"
    assert payload["alerts"][0]["priority_rank"] == 1
    assert "evaluation_label_is_sar" not in payload["alerts"][0]


def test_queue_invalid_risk_band_filter_returns_clear_error():
    client = _app()
    res = client.get("/api/alerts?response_mode=queue&risk_band=Critical")

    assert res.status_code == 400
    assert "Invalid risk_band filter" in str(res.json()["detail"])


def test_detail_context_includes_structured_detail_view_and_sorted_transactions():
    client = _app()
    res = client.get("/api/alerts/A1/investigation-context")

    assert res.status_code == 200
    payload = res.json()
    detail = payload["detail_view"]
    assert detail["investigation_summary"]
    assert detail["why_prioritized"]["summary_text"]
    assert detail["workflow"]["status"] == "under_review"
    assert detail["transactions"][0]["timestamp"] == "2026-05-02T12:01:00Z"
    assert detail["transactions"][1]["timestamp"] == "2026-05-02T12:10:00Z"


def test_pilot_summary_endpoint_returns_value_summary():
    client = _app()
    res = client.get("/api/pilot/summary")

    assert res.status_code == 200
    payload = res.json()
    assert payload["total_alerts_ingested"] == 2
    assert payload["evaluation_available"] is True
