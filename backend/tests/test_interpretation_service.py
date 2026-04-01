from __future__ import annotations

import json

from services.explain_service import ExplainabilityService
from services.interpretation_service import InterpretationService


def _base_payload() -> dict:
    return {
        "base_prob": 0.82,
        "risk_score": 88.0,
        "risk_prob": 0.88,
        "model_version": "model-v7",
        "explanation_method": "shap",
        "explanation_status": "ok",
        "contributions": [],
    }


def test_numeric_fallback_payload_is_handled_conservatively() -> None:
    service = InterpretationService()
    payload = _base_payload()
    payload.update(
        {
            "explanation_method": "numeric_fallback",
            "explanation_status": "fallback",
            "contributions": [{"feature": "time_gap", "value": -200.0, "shap_value": None, "magnitude": 0.2}],
        }
    )
    features = {"time_gap": 120}

    out = service.build_human_explanation(payload, features)

    assert "may indicate" in out["summary_text"].lower()
    assert "confirmed" not in out["summary_text"].lower()
    assert out["aml_patterns"] == ["Velocity spike"]
    assert out["confidence_score"] is not None
    assert out["confidence_score"] < 0.95


def test_missing_contributions_uses_feature_driven_reasoning() -> None:
    service = InterpretationService()
    payload = _base_payload()
    features = {"amount": 22000, "amount_log1p": 10.0}

    out = service.build_human_explanation(payload, features)

    assert out["key_reasons"]
    assert "High-value anomaly" in out["aml_patterns"]
    assert len(out["key_reasons"]) <= 5
    assert len(out["aml_patterns"]) <= 3


def test_low_information_payload_falls_back_to_generic_human_text() -> None:
    service = InterpretationService()
    out = service.build_human_explanation(raw_explain_payload={}, feature_dict=None)

    assert "warrants review" in out["summary_text"].lower()
    assert out["key_reasons"]
    assert out["aml_patterns"] == []
    assert out["confidence_score"] is None


def test_duplicated_raw_features_are_deduplicated() -> None:
    service = InterpretationService()
    payload = _base_payload()
    payload["contributions"] = [
        {"feature": "time_gap", "value": -120.0, "shap_value": 0.08, "magnitude": 0.08},
        {"feature": "time_gap", "value": -900.0, "shap_value": 0.30, "magnitude": 0.30},
        {"feature": "time_gap", "value": -300.0, "shap_value": 0.10, "magnitude": 0.10},
    ]
    features = {"time_gap": 240}

    out = service.build_human_explanation(payload, features)

    assert out["aml_patterns"] == ["Velocity spike"]
    assert len(out["key_reasons"]) == 1


def test_unsupported_feature_names_do_not_crash_and_use_generic_reasoning() -> None:
    service = InterpretationService()
    payload = _base_payload()
    payload["contributions"] = [{"feature": "mystery_feature_xyz", "value": 1.0, "magnitude": 0.5}]

    out = service.build_human_explanation(payload, {})

    assert out["summary_text"]
    assert out["key_reasons"]
    assert "mystery_feature_xyz" in out["technical_details"]["contributions"][0]["feature"]
    assert "warrants review" in out["summary_text"].lower()


def test_high_risk_velocity_example_maps_to_velocity_pattern() -> None:
    service = InterpretationService()
    payload = _base_payload()
    payload["contributions"] = [
        {"feature": "time_gap", "value": -1000.0, "shap_value": 0.41, "magnitude": 0.41},
        {"feature": "num_transactions", "value": 9, "magnitude": 0.2},
    ]
    features = {"time_gap": 90, "num_transactions": 9}

    out = service.build_human_explanation(payload, features)

    assert "Velocity spike" in out["aml_patterns"]
    assert any("rapid transaction burst" in item.lower() for item in out["key_reasons"])


def test_structuring_like_example_maps_to_structuring_pattern() -> None:
    service = InterpretationService()
    payload = _base_payload()
    payload["contributions"] = [
        {"feature": "user_amount_std", "value": 7400.0, "shap_value": 0.28, "magnitude": 0.28},
        {"feature": "amount_log1p", "value": 9.2, "shap_value": 0.17, "magnitude": 0.17},
    ]
    features = {"user_amount_std": 7400.0, "num_transactions": 8}

    out = service.build_human_explanation(payload, features)

    assert "Structuring" in out["aml_patterns"]
    assert any("structuring" in item.lower() for item in out["key_reasons"])
    assert len(out["aml_patterns"]) <= 3


class _FakeRepo:
    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 200000):
        return [
            {
                "alert_id": "A-77",
                "risk_score": 93.4,
                "risk_prob": 0.93,
                "model_version": "model-v9",
                "governance_status": "eligible",
                "top_feature_contributions_json": json.dumps(
                    [
                        {"feature": "time_gap", "value": -280.0, "shap_value": 0.25, "magnitude": 0.25},
                        {"feature": "amount_log1p", "value": 9.8, "shap_value": 0.20, "magnitude": 0.20},
                    ],
                    ensure_ascii=True,
                ),
                "top_features_json": json.dumps(["time_gap", "amount_log1p"], ensure_ascii=True),
                "risk_explain_json": json.dumps(
                    {
                        "base_prob": 0.93,
                        "feature_attribution": [
                            {"feature": "time_gap", "value": -280.0, "shap_value": 0.25, "magnitude": 0.25},
                            {"feature": "amount_log1p", "value": 9.8, "shap_value": 0.20, "magnitude": 0.20},
                        ],
                        "explanation_method": "shap",
                        "explanation_status": "ok",
                    },
                    ensure_ascii=True,
                ),
                "rules_json": "[]",
                "rule_evidence_json": "{}",
                "features_json": json.dumps(
                    {
                        "time_gap": 120,
                        "amount": 18000,
                        "amount_log1p": 9.8,
                        "user_amount_mean": 4000,
                        "user_amount_std": 4200,
                        "num_transactions": 7,
                    },
                    ensure_ascii=True,
                ),
            }
        ]


def test_explain_service_returns_raw_and_human_interpretation_layers() -> None:
    service = ExplainabilityService(repository=_FakeRepo())  # type: ignore[arg-type]
    result = service.explain_alert(tenant_id="tenant-a", alert_id="A-77", run_id="run-z")

    assert result is not None
    # Existing technical output is preserved.
    assert "risk_explanation" in result
    assert "feature_contributions" in result
    assert "features" in result

    # New interpretation layer.
    assert "human_interpretation" in result
    human = result["human_interpretation"]
    assert "summary_text" in human
    assert "key_reasons" in human
    assert "aml_patterns" in human
    assert "analyst_focus_points" in human
    assert "technical_details" in human

    # UI-ready flattened format.
    assert "human_interpretation_view" in result
    assert "headline" in result["human_interpretation_view"]
    assert "reasons" in result["human_interpretation_view"]
    assert "patterns" in result["human_interpretation_view"]

