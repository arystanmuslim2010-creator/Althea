from __future__ import annotations

from investigation.guidance_service import InvestigationGuidanceService
from investigation.narrative_service import InvestigationNarrativeService
from investigation.sar_generator import SARNarrativeGenerator


class _FakeRepository:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.saved: list[dict] = []

    def list_pipeline_runs(self, tenant_id: str, limit: int = 20):
        return [{"run_id": "run-1"}]

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 500000):
        return [self._payload]

    def get_ai_summary(self, tenant_id: str, entity_type: str, entity_id: str):
        return None

    def save_ai_summary(self, payload: dict):
        self.saved.append(payload)
        return payload


class _FakeExplainService:
    def explain_alert(self, tenant_id: str, alert_id: str, run_id: str):
        return {
            "risk_explanation": {"risk_reason_codes": ["R1", "R2"]},
            "feature_contributions": [{"feature": "amount", "shap_value": 0.8}],
        }


def test_narrative_draft_hides_unknown_placeholders_and_uses_preliminary_language():
    repo = _FakeRepository(
        {
            "alert_id": "A1",
            "user_id": "U1",
            "account_id": "",
            "counterparty_id": None,
            "typology": "structuring",
            "risk_score": 88,
            "amount": 12500,
            "rules_json": ["RULE-1"],
        }
    )
    service = InvestigationNarrativeService(repo, _FakeExplainService())

    draft = service.generate_draft("tenant-a", "A1", "run-1")

    assert "unknown account" not in draft["sections"]["activity_summary"].lower()
    assert "unknown counterparty" not in draft["sections"]["activity_summary"].lower()
    assert "preliminary narrative for analyst review" in draft["narrative"].lower()
    assert "does not determine suspicious activity" in draft["narrative"].lower()


def test_sar_draft_uses_non_conclusive_language_and_strong_disclaimer():
    repo = _FakeRepository(
        {
            "alert_id": "A1",
            "customer_id": "C1",
            "risk_score": 91,
            "typology": "structuring",
            "segment": "retail",
            "governance_status": "eligible",
            "country": "US",
            "amount": 15000,
            "rules_json": [{"rule_id": "RULE-1"}],
        }
    )
    service = SARNarrativeGenerator(repo, _FakeExplainService())

    draft = service.generate_sar_draft("tenant-a", "A1", "run-1")

    assert "reporting institution has determined" not in draft["narrative"].lower()
    assert "may warrant escalation review" in draft["narrative"].lower()
    assert "does not determine that suspicious activity occurred" in draft["disclaimer"].lower()


def test_guidance_service_uses_review_oriented_sar_language():
    repo = _FakeRepository(
        {
            "alert_id": "A1",
            "risk_score": 95,
            "risk_band": "CRITICAL",
            "typology": "structuring",
            "rules_json": ["RULE-1"],
        }
    )
    service = InvestigationGuidanceService(repo)

    guidance = service.generate_steps("tenant-a", "A1", "run-1")
    rendered_steps = [item["description"] for item in guidance["steps"]]

    assert any("SAR/STR draft for compliance review" in step for step in rendered_steps)
    assert any("may be warranted" in step for step in rendered_steps)
