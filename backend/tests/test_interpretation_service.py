from __future__ import annotations

import json

from services.explain_service import ExplainabilityService
from services.interpretation_service import InterpretationService


def test_interpretation_maps_technical_features_to_aml_semantics() -> None:
    service = InterpretationService()
    raw = {
        "base_prob": 0.83,
        "model_version": "model-v2",
        "explanation_method": "shap",
        "explanation_status": "ok",
        "contributions": [
            {"feature": "time_gap", "value": -500.0, "shap_value": 0.41},
            {"feature": "amount_log1p", "value": 10.4, "shap_value": 0.27},
            {"feature": "user_amount_std", "value": 6200.0, "shap_value": 0.21},
        ],
    }
    features = {
        "time_gap": 120,
        "amount": 18000,
        "amount_log1p": 9.8,
        "user_amount_mean": 4500,
        "user_amount_std": 6200,
        "num_transactions": 8,
        "incoming_counterparty_count": 4,
        "outgoing_counterparty_count": 1,
        "incoming_outgoing_time_delta_seconds": 300,
    }

    out = service.build_human_explanation(raw_explain_payload=raw, feature_dict=features)

    assert "consistent with" in out["summary_text"].lower()
    assert "warrant analyst review" in out["summary_text"].lower()
    assert "confirmed" not in out["summary_text"].lower()
    assert "Possible velocity spike" in out["aml_patterns"]
    assert "Possible structuring" in out["aml_patterns"]
    assert "Potential layering pattern" in out["aml_patterns"]
    assert "Possible fan-in" in out["aml_patterns"]
    assert isinstance(out["confidence_score"], float)
    assert 0.0 <= out["confidence_score"] <= 1.0
    assert out["technical_details"]["model_version"] == "model-v2"
    assert isinstance(out["technical_details"]["contributions"], list)


def test_interpretation_falls_back_to_generic_human_readable_text() -> None:
    service = InterpretationService()
    raw = {
        "base_prob": 0.51,
        "model_version": "model-v2",
        "explanation_method": "numeric_fallback",
        "explanation_status": "fallback",
        "contributions": [{"feature": "unknown_metric", "value": 0.3, "magnitude": 0.3}],
    }

    out = service.build_human_explanation(raw_explain_payload=raw, feature_dict={})

    assert out["summary_text"]
    assert out["key_reasons"]
    assert out["aml_patterns"]
    assert out["analyst_focus_points"]
    assert out["confidence_score"] < 0.9
    assert "atypical" in out["summary_text"].lower()


class _FakeRepo:
    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000):
        return [
            {
                "alert_id": "A-100",
                "risk_score": 91.2,
                "risk_prob": 0.91,
                "model_version": "model-v5",
                "governance_status": "eligible",
                "top_feature_contributions_json": json.dumps(
                    [
                        {"feature": "time_gap", "value": -320.0, "shap_value": 0.26},
                        {"feature": "amount_log1p", "value": 9.7, "shap_value": 0.21},
                    ],
                    ensure_ascii=True,
                ),
                "top_features_json": json.dumps(["time_gap", "amount_log1p"], ensure_ascii=True),
                "risk_explain_json": json.dumps(
                    {
                        "base_prob": 0.91,
                        "feature_attribution": [
                            {"feature": "time_gap", "value": -320.0, "shap_value": 0.26},
                            {"feature": "amount_log1p", "value": 9.7, "shap_value": 0.21},
                        ],
                        "risk_reason_codes": ["velocity:spike"],
                        "explanation_method": "shap",
                        "explanation_status": "ok",
                        "explanation_warning": None,
                        "explanation_warning_code": None,
                    },
                    ensure_ascii=True,
                ),
                "rules_json": "[]",
                "rule_evidence_json": "{}",
                "features_json": json.dumps(
                    {
                        "time_gap": 180,
                        "amount": 15000,
                        "amount_log1p": 9.7,
                        "user_amount_mean": 4200,
                        "user_amount_std": 5100,
                        "num_transactions": 6,
                    },
                    ensure_ascii=True,
                ),
            }
        ]


def test_explain_service_includes_human_interpretation_without_removing_raw_fields() -> None:
    service = ExplainabilityService(repository=_FakeRepo())  # type: ignore[arg-type]

    result = service.explain_alert(tenant_id="tenant-a", alert_id="A-100", run_id="run-x")

    assert result is not None
    # Existing raw/technical fields remain.
    assert "feature_contributions" in result
    assert "risk_explanation" in result
    assert "features" in result

    # New analyst-facing interpretation layer.
    assert "summary_text" in result
    assert "key_reasons" in result
    assert "aml_patterns" in result
    assert "analyst_focus_points" in result
    assert "confidence_score" in result
    assert "technical_details" in result
    assert isinstance(result["human_explanation"], dict)
    assert isinstance(result["technical_details"].get("contributions"), list)

