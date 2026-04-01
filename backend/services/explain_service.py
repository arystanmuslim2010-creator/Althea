from __future__ import annotations

import json
from typing import Any

from services.interpretation_service import InterpretationService
from storage.postgres_repository import EnterpriseRepository


class ExplainabilityService:
    def __init__(
        self,
        repository: EnterpriseRepository,
        interpretation_service: InterpretationService | None = None,
    ) -> None:
        self._repository = repository
        self._interpretation = interpretation_service or InterpretationService()

    @staticmethod
    def _parse(value: Any, default: Any):
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return default
            try:
                return json.loads(text)
            except Exception:
                return default
        return default

    @staticmethod
    def _normalize_contributions(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                feature = str(item.get("feature") or item.get("name") or "").strip()
                if not feature:
                    continue
                value = item.get("value", item.get("contribution", item.get("shap_value")))
                try:
                    cast_value = float(value) if value is not None else 0.0
                except Exception:
                    cast_value = 0.0
                shap_raw = item.get("shap_value", item.get("contribution"))
                try:
                    shap_value = float(shap_raw) if shap_raw is not None else None
                except Exception:
                    shap_value = None
                mag_raw = item.get("magnitude")
                try:
                    magnitude = float(mag_raw) if mag_raw is not None else None
                except Exception:
                    magnitude = None
                if magnitude is None:
                    base = shap_value if shap_value is not None else cast_value
                    magnitude = abs(float(base))
                normalized.append(
                    {
                        "feature": feature,
                        "value": cast_value,
                        "shap_value": shap_value,
                        "magnitude": magnitude,
                    }
                )
            elif isinstance(item, str) and item.strip():
                normalized.append({"feature": item.strip(), "value": 0.0, "shap_value": None, "magnitude": 0.0})
        return normalized

    def _normalize_risk_explanation(self, payload: dict[str, Any], contributions: list[dict[str, Any]]) -> dict[str, Any]:
        risk_explanation = self._parse(payload.get("risk_explain_json"), {})
        if not isinstance(risk_explanation, dict):
            risk_explanation = {}

        attribution = self._normalize_contributions(
            risk_explanation.get("feature_attribution") or risk_explanation.get("contributions") or contributions
        )
        reason_codes = risk_explanation.get("risk_reason_codes")
        if not isinstance(reason_codes, list):
            reason_codes = []
        method = str(risk_explanation.get("explanation_method") or "").strip().lower()
        if not method:
            has_shap_values = any(item.get("shap_value") is not None for item in attribution if isinstance(item, dict))
            method = "shap" if has_shap_values else "unknown"

        status = str(risk_explanation.get("explanation_status") or "").strip().lower()
        if not status:
            if method in {"shap", "tree_shap"}:
                status = "ok"
            elif method == "unavailable":
                status = "unavailable"
            elif method == "numeric_fallback":
                status = "fallback"
            else:
                status = "unknown"

        warning = risk_explanation.get("explanation_warning")
        if method == "numeric_fallback" and not warning:
            warning = "Heuristic feature highlights; not model contribution attribution."
        warning_code = risk_explanation.get("explanation_warning_code")

        return {
            **risk_explanation,
            "feature_attribution": attribution,
            "contributions": attribution,
            "risk_reason_codes": [str(code) for code in reason_codes if str(code).strip()],
            "explanation_method": method,
            "explanation_status": status,
            "explanation_warning": warning,
            "explanation_warning_code": warning_code,
        }

    def explain_alert(self, tenant_id: str, alert_id: str, run_id: str) -> dict[str, Any] | None:
        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        target = next((row for row in payloads if str(row.get("alert_id")) == str(alert_id)), None)
        if not target:
            return None

        contributions = self._normalize_contributions(self._parse(target.get("top_feature_contributions_json"), []))
        if not contributions:
            ml_explain = self._parse(target.get("ml_service_explain_json"), {})
            if isinstance(ml_explain, dict):
                contributions = self._normalize_contributions(ml_explain.get("top_features", []))

        risk_explanation = self._normalize_risk_explanation(target, contributions)
        top_features = self._parse(target.get("top_features_json"), [])
        if not isinstance(top_features, list) or not top_features:
            top_features = [item.get("feature") for item in contributions if isinstance(item, dict) and item.get("feature")]

        feature_dict = self._parse(target.get("features_json"), {})
        if not isinstance(feature_dict, dict):
            feature_dict = {}
        parsed_risk_explain = self._parse(target.get("risk_explain_json"), {})
        if not isinstance(parsed_risk_explain, dict):
            parsed_risk_explain = {}

        technical_payload = {
            "base_prob": parsed_risk_explain.get("base_prob"),
            "risk_score": float(target.get("risk_score", 0.0) or 0.0),
            "risk_prob": float(target.get("risk_prob", 0.0) or 0.0),
            "model_version": str(target.get("model_version", "unknown")),
            "contributions": contributions,
            "feature_attribution": risk_explanation.get("feature_attribution", contributions),
            "explanation_method": risk_explanation.get("explanation_method", "unknown"),
            "explanation_status": risk_explanation.get("explanation_status", "unknown"),
            "explanation_warning": risk_explanation.get("explanation_warning"),
            "explanation_warning_code": risk_explanation.get("explanation_warning_code"),
            "raw_explain_payload": parsed_risk_explain,
        }
        interpretation = self._interpretation.build_human_explanation(
            raw_explain_payload=technical_payload,
            feature_dict=feature_dict,
        )

        return {
            "alert_id": str(target.get("alert_id", "")),
            "run_id": run_id,
            "risk_score": float(target.get("risk_score", 0.0) or 0.0),
            "risk_prob": float(target.get("risk_prob", 0.0) or 0.0),
            "model_version": str(target.get("model_version", "unknown")),
            "governance_status": str(target.get("governance_status", "")),
            "feature_contributions": contributions,
            "top_features": top_features if isinstance(top_features, list) else [],
            "risk_explanation": risk_explanation,
            "explanation_method": risk_explanation.get("explanation_method", "unknown"),
            "explanation_status": risk_explanation.get("explanation_status", "unknown"),
            "explanation_warning": risk_explanation.get("explanation_warning"),
            "explanation_warning_code": risk_explanation.get("explanation_warning_code"),
            "rule_hits": self._parse(target.get("rules_json"), []),
            "rule_evidence": self._parse(target.get("rule_evidence_json"), {}),
            "features": feature_dict,
            # Additive analyst-facing interpretation layer.
            "summary_text": interpretation.get("summary_text", ""),
            "key_reasons": interpretation.get("key_reasons", []),
            "aml_patterns": interpretation.get("aml_patterns", []),
            "analyst_focus_points": interpretation.get("analyst_focus_points", []),
            "confidence_score": float(interpretation.get("confidence_score", 0.0) or 0.0),
            "technical_details": interpretation.get("technical_details", {}),
            "human_explanation": interpretation,
        }
