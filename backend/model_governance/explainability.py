from __future__ import annotations

import json
import importlib
from typing import Any

import numpy as np
import pandas as pd


class GovernanceExplainabilityService:
    def __init__(self) -> None:
        self._shap_module = None
        self._shap_checked = False

    def _get_shap(self):
        if self._shap_checked:
            return self._shap_module
        self._shap_checked = True
        try:  # pragma: no cover - optional heavy dependency
            self._shap_module = importlib.import_module("shap")
        except Exception:  # pragma: no cover
            self._shap_module = None
        return self._shap_module

    @staticmethod
    def _reason_codes(contributions: list[dict[str, Any]]) -> list[str]:
        codes: list[str] = []
        for item in contributions[:5]:
            feature = str(item.get("feature") or "unknown")
            value = float(item.get("value", 0.0) or 0.0)
            direction = "increase" if value >= 0 else "decrease"
            codes.append(f"{feature}:{direction}")
        return codes

    def generate_explanation(
        self,
        model,
        feature_frame: pd.DataFrame,
        fallback_contributions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if feature_frame.empty:
            top = list(fallback_contributions or [])
            return {"feature_attribution": top, "risk_reason_codes": self._reason_codes(top)}

        numeric = feature_frame.select_dtypes(include=["number"]).fillna(0.0)

        def _numeric_top() -> list[dict[str, Any]]:
            if numeric.empty:
                return list(fallback_contributions or [])
            values = numeric.iloc[0].abs().sort_values(ascending=False).head(5)
            return [{"feature": str(name), "value": float(value)} for name, value in values.items()]

        # Governance path can call this service without a bound model object.
        # In that mode we still produce deterministic attributions from numeric feature magnitude.
        if model is None:
            top = _numeric_top()
            return {"feature_attribution": top, "risk_reason_codes": self._reason_codes(top)}

        shap_module = self._get_shap()
        if shap_module is None:
            top = _numeric_top()
            return {"feature_attribution": top, "risk_reason_codes": self._reason_codes(top)}

        try:
            explainer = shap_module.Explainer(model, feature_frame)
            shap_values = explainer(feature_frame)
            vals = shap_values.values[0]
            order = np.argsort(np.abs(vals))[::-1][:5]
            top = [
                {
                    "feature": str(feature_frame.columns[idx]),
                    "value": float(vals[idx]),
                }
                for idx in order
            ]
            return {"feature_attribution": top, "risk_reason_codes": self._reason_codes(top)}
        except Exception:
            top = _numeric_top()
            return {"feature_attribution": top, "risk_reason_codes": self._reason_codes(top)}

    def merge_into_alert_metadata(self, alert_payload: dict[str, Any], explanation: dict[str, Any]) -> dict[str, Any]:
        out = dict(alert_payload or {})
        existing = out.get("risk_explain_json")
        if isinstance(existing, str):
            try:
                existing = json.loads(existing)
            except Exception:
                existing = {}
        if not isinstance(existing, dict):
            existing = {}
        merged = {
            **existing,
            "feature_attribution": explanation.get("feature_attribution", []),
            "risk_reason_codes": explanation.get("risk_reason_codes", []),
        }
        out["risk_explain_json"] = json.dumps(merged, ensure_ascii=True)
        return out
