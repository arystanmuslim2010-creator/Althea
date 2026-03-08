from __future__ import annotations

import json
from typing import Any

from storage.postgres_repository import EnterpriseRepository


class ExplainabilityService:
    def __init__(self, repository: EnterpriseRepository) -> None:
        self._repository = repository

    def explain_alert(self, tenant_id: str, alert_id: str, run_id: str) -> dict[str, Any] | None:
        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        target = next((row for row in payloads if str(row.get("alert_id")) == str(alert_id)), None)
        if not target:
            return None

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

        contributions = _parse(target.get("top_feature_contributions_json"), [])
        if not contributions:
            ml_explain = _parse(target.get("ml_service_explain_json"), {})
            if isinstance(ml_explain, dict):
                contributions = ml_explain.get("top_features", [])

        return {
            "alert_id": str(target.get("alert_id", "")),
            "run_id": run_id,
            "risk_score": float(target.get("risk_score", 0.0) or 0.0),
            "risk_prob": float(target.get("risk_prob", 0.0) or 0.0),
            "model_version": str(target.get("model_version", "unknown")),
            "governance_status": str(target.get("governance_status", "")),
            "feature_contributions": contributions,
            "top_features": _parse(target.get("top_features_json"), []),
            "risk_explanation": _parse(target.get("risk_explain_json"), {}),
            "rule_hits": _parse(target.get("rules_json"), []),
            "rule_evidence": _parse(target.get("rule_evidence_json"), {}),
            "features": _parse(target.get("features_json"), {}),
        }
