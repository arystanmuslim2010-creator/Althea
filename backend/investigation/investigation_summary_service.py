"""Investigation Summary Service — structured alert summaries for analyst productivity."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.investigation.summary")

_METRICS: dict[str, float] = {}  # lightweight in-process counters


def _record_latency(key: str, elapsed: float) -> None:
    _METRICS[key] = elapsed


def get_summary_metrics() -> dict:
    return dict(_METRICS)


class InvestigationSummaryService:
    """Generate structured investigation summaries from alert data and ML explanations."""

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

    def _get_alert_payload(self, tenant_id: str, alert_id: str, run_id: str | None) -> dict:
        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")
        if not run_id:
            raise ValueError("No pipeline run context available")
        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id, limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), None)
        if not payload:
            raise ValueError(f"Alert {alert_id} not found in run {run_id}")
        return payload

    def _build_key_observations(self, payload: dict, explanation: dict) -> list[str]:
        observations: list[str] = []

        # Risk score context
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        risk_band = str(payload.get("risk_band") or "").upper()
        if risk_score >= 90:
            observations.append(f"Critical risk score of {risk_score:.1f} — immediate escalation recommended")
        elif risk_score >= 75:
            observations.append(f"High risk score of {risk_score:.1f} ({risk_band}) — priority investigation required")

        # Typology observation
        typology = str(payload.get("typology") or "").lower()
        typology_messages = {
            "sanctions": "Transaction involves sanctioned entity indicators",
            "structuring": "Transaction pattern consistent with structuring activity",
            "high_amount_outlier": "Transaction amount significantly exceeds customer baseline",
            "cross_border": "Cross-border activity flagged for unusual geographic flow",
            "flow_through": "Flow-through pattern detected — potential layering activity",
        }
        if typology in typology_messages:
            observations.append(typology_messages[typology])

        # Feature-driven observations
        contrib = self._parse_json(
            explanation.get("feature_contributions") or
            explanation.get("top_feature_contributions_json"), []
        )
        if isinstance(contrib, list):
            for item in contrib[:3]:
                if isinstance(item, dict) and item.get("feature"):
                    feat = str(item["feature"]).replace("_", " ")
                    impact = float(item.get("contribution", item.get("shap_value", 0.0)) or 0.0)
                    direction = "elevated" if impact > 0 else "anomalous"
                    observations.append(f"{feat} is {direction} (contribution: {impact:+.3f})")

        # Rule hits
        rules = self._parse_json(payload.get("rules_json"), [])
        if isinstance(rules, list) and rules:
            rule_ids = []
            for item in rules[:4]:
                if isinstance(item, dict):
                    rule_ids.append(str(item.get("rule_id") or item.get("id") or item))
                else:
                    rule_ids.append(str(item))
            if rule_ids:
                observations.append(f"Triggered rules: {', '.join(rule_ids)}")

        # Amount observation
        amount = payload.get("amount")
        if amount is not None:
            try:
                amt = float(amount)
                if amt >= 10000:
                    observations.append(f"Transaction amount ${amt:,.0f} triggers reporting threshold")
            except (TypeError, ValueError):
                pass

        # Country risk
        country = str(payload.get("country") or "").upper()
        high_risk_countries = {"IR", "KP", "RU", "SY", "CU", "VE", "UA", "BY"}
        if country in high_risk_countries:
            observations.append(f"Geographic risk: {country} is a high-risk jurisdiction")

        return observations[:8] if observations else ["No specific risk observations generated from available data"]

    def generate_summary(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        logger.info(
            "Generating investigation summary",
            extra={"tenant_id": tenant_id, "alert_id": alert_id},
        )

        payload = self._get_alert_payload(tenant_id, alert_id, run_id)
        explanation = self._explain_service.explain_alert(
            tenant_id=tenant_id, alert_id=alert_id, run_id=run_id or ""
        ) or {}

        customer = (
            payload.get("customer_name")
            or payload.get("customer_id")
            or payload.get("user_id")
            or "Unknown Customer"
        )
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        risk_band = str(payload.get("risk_band") or "").upper() or "UNKNOWN"
        segment = str(payload.get("segment") or "unknown")
        typology = str(payload.get("typology") or "N/A")
        governance_status = str(payload.get("governance_status") or payload.get("status") or "unknown")

        observations = self._build_key_observations(payload, explanation)

        summary = {
            "alert_id": alert_id,
            "tenant_id": tenant_id,
            "customer": customer,
            "risk_score": risk_score,
            "risk_band": risk_band,
            "segment": segment,
            "typology": typology,
            "governance_status": governance_status,
            "key_observations": observations,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Cache in ai_summaries
        try:
            self._repository.save_ai_summary(
                {
                    "tenant_id": tenant_id,
                    "entity_type": "investigation_summary",
                    "entity_id": str(alert_id),
                    "summary": json.dumps(summary, ensure_ascii=True),
                    "run_id": run_id or "",
                    "actor": "investigation_summary_service",
                }
            )
        except Exception:
            logger.warning("Failed to cache investigation summary", extra={"alert_id": alert_id})

        elapsed = time.perf_counter() - t0
        _record_latency("investigation_summary_latency_seconds", elapsed)
        logger.info(
            "Investigation summary generated",
            extra={"alert_id": alert_id, "latency_s": round(elapsed, 3)},
        )
        return summary
