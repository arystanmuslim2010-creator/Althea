"""Risk Explanation Service — human-readable risk driver narratives."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.investigation.risk_explanation")


class RiskExplanationService:
    """Generate human-readable risk driver explanations from ML feature importance and rule codes."""

    # Rule-code → human description mappings
    _RULE_DESCRIPTIONS: dict[str, str] = {
        "TXN_VEL_HIGH": "unusual transaction velocity detected",
        "NEW_BENEF": "new beneficiary accounts opened recently",
        "HIGH_RISK_JUR": "counterparty in high-risk jurisdiction",
        "DEV_MISMATCH": "device fingerprint mismatch — potential account takeover",
        "STRUCT_PATTERN": "structuring pattern — repeated sub-threshold amounts",
        "SANCT_HIT": "sanctions screening match",
        "ROUND_AMT": "round-amount transactions indicative of structuring",
        "CROSS_BORDER": "unusual cross-border fund movement",
        "KYC_GAP": "KYC profile incomplete or outdated",
        "FLOW_THRU": "rapid flow-through — funds exit within 24 hours",
    }

    # Feature name → human description
    _FEATURE_DESCRIPTIONS: dict[str, str] = {
        "amount_zscore": "transaction amount significantly deviates from historical baseline",
        "tx_count": "unusually high transaction frequency",
        "log_amount": "large transaction amount",
        "hour_of_day": "transaction at unusual hour of day",
        "is_weekend": "transaction on weekend — outside normal business hours",
        "country_risk": "elevated country risk score",
        "amount_mean": "average transaction amount is elevated",
        "amount_std": "high transaction amount variability",
    }

    def __init__(self, repository, explain_service) -> None:
        self._repository = repository
        self._explain_service = explain_service

    @staticmethod
    def _parse_json(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw.strip()) if raw.strip() else default
            except Exception:
                return default
        return default

    def _feature_drivers(self, explanation: dict) -> list[dict[str, Any]]:
        drivers: list[dict[str, Any]] = []
        contrib = self._parse_json(
            explanation.get("feature_contributions")
            or explanation.get("top_feature_contributions_json"),
            [],
        )
        if not isinstance(contrib, list):
            return drivers
        for item in contrib[:8]:
            if not isinstance(item, dict):
                continue
            feat = str(item.get("feature") or "").strip()
            if not feat:
                continue
            impact = float(item.get("contribution", item.get("shap_value", item.get("value", 0.0))) or 0.0)
            description = self._FEATURE_DESCRIPTIONS.get(feat, feat.replace("_", " "))
            drivers.append(
                {
                    "type": "feature",
                    "name": feat,
                    "description": description,
                    "impact": round(impact, 4),
                    "direction": "risk-increasing" if impact > 0 else "risk-decreasing",
                }
            )
        # Sort by absolute impact descending
        drivers.sort(key=lambda d: abs(d["impact"]), reverse=True)
        return drivers

    def _rule_drivers(self, payload: dict) -> list[dict[str, Any]]:
        drivers: list[dict[str, Any]] = []
        rules = self._parse_json(payload.get("rules_json"), [])
        if not isinstance(rules, list):
            return drivers
        for item in rules[:6]:
            if isinstance(item, dict):
                rule_id = str(item.get("rule_id") or item.get("id") or "")
            else:
                rule_id = str(item)
            if not rule_id:
                continue
            description = self._RULE_DESCRIPTIONS.get(rule_id, f"rule {rule_id} triggered")
            drivers.append({"type": "rule", "rule_id": rule_id, "description": description})
        return drivers

    def _reason_code_drivers(self, explanation: dict) -> list[dict[str, Any]]:
        drivers: list[dict[str, Any]] = []
        risk_expl = explanation.get("risk_explanation") or {}
        codes = self._parse_json(risk_expl.get("risk_reason_codes"), [])
        if not isinstance(codes, list):
            return drivers
        for code in codes[:5]:
            code_str = str(code).strip()
            if not code_str:
                continue
            description = self._RULE_DESCRIPTIONS.get(code_str, code_str.replace("_", " ").lower())
            drivers.append({"type": "reason_code", "code": code_str, "description": description})
        return drivers

    def generate_explanation(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        logger.info(
            "Generating risk explanation",
            extra={"tenant_id": tenant_id, "alert_id": alert_id},
        )

        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})

        explanation = self._explain_service.explain_alert(
            tenant_id=tenant_id, alert_id=alert_id, run_id=run_id or ""
        ) or {}

        feature_drivers = self._feature_drivers(explanation)
        rule_drivers = self._rule_drivers(payload)
        reason_code_drivers = self._reason_code_drivers(explanation)

        # Build primary driver list for quick consumption
        all_drivers = feature_drivers[:4] + rule_drivers[:2] + reason_code_drivers[:2]
        primary_drivers = [d["description"] for d in all_drivers[:6]] or ["high model risk score"]

        result = {
            "alert_id": alert_id,
            "risk_score": float(payload.get("risk_score", 0.0) or 0.0),
            "risk_band": str(payload.get("risk_band") or "").upper(),
            "primary_drivers": primary_drivers,
            "feature_contributions": feature_drivers,
            "rule_hits": rule_drivers,
            "reason_codes": reason_code_drivers,
            "model_version": explanation.get("model_version", "unknown"),
            "explanation_method": explanation.get("explanation_method", "unknown"),
            "explanation_status": explanation.get("explanation_status", "unknown"),
            "explanation_warning": explanation.get("explanation_warning"),
            "explanation_warning_code": explanation.get("explanation_warning_code"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Persist explanation in ai_summaries for caching
        try:
            self._repository.save_ai_summary(
                {
                    "tenant_id": tenant_id,
                    "entity_type": "risk_explanation",
                    "entity_id": str(alert_id),
                    "summary": json.dumps(result, ensure_ascii=True),
                    "run_id": run_id or "",
                    "actor": "risk_explanation_service",
                }
            )
        except Exception:
            logger.warning("Failed to persist risk explanation", extra={"alert_id": alert_id})

        elapsed = time.perf_counter() - t0
        logger.info(
            "Risk explanation generated",
            extra={"alert_id": alert_id, "latency_s": round(elapsed, 3)},
        )
        return result
