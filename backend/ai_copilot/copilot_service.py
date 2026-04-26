from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


class AICopilotService:
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
            text = raw.strip()
            if not text:
                return default
            try:
                return json.loads(text)
            except Exception:
                return default
        return default

    def _build_network_graph(self, payload: dict[str, Any]) -> dict[str, Any]:
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []

        entity_fields = {
            "account_id": "account",
            "beneficiary_id": "beneficiary",
            "device_id": "device",
            "entity_id": "entity",
            "customer_id": "entity",
            "user_id": "entity",
        }

        anchor_id = str(payload.get("alert_id") or payload.get("id") or "alert")
        nodes[anchor_id] = {"id": anchor_id, "type": "alert"}

        for field, node_type in entity_fields.items():
            value = payload.get(field)
            if value is None or str(value).strip() == "":
                continue
            node_id = f"{node_type}:{value}"
            nodes[node_id] = {"id": node_id, "type": node_type, "label": str(value)}
            edges.append({"source": anchor_id, "target": node_id, "relation": field})

        return {"nodes": list(nodes.values()), "edges": edges}

    def _risk_drivers(self, explanation: dict[str, Any], payload: dict[str, Any]) -> list[str]:
        drivers: list[str] = []
        reason_codes = self._parse_json(explanation.get("risk_explanation", {}).get("risk_reason_codes"), [])
        if isinstance(reason_codes, list):
            drivers.extend([str(code) for code in reason_codes if str(code).strip()])

        contrib = explanation.get("feature_contributions") or []
        if isinstance(contrib, list):
            for item in contrib[:5]:
                if isinstance(item, dict) and item.get("feature"):
                    drivers.append(f"{item['feature']} impact")

        rules = self._parse_json(payload.get("rules_json"), [])
        if isinstance(rules, list):
            for item in rules[:3]:
                if isinstance(item, dict):
                    rule_id = item.get("rule_id") or item.get("id")
                    if rule_id:
                        drivers.append(f"rule:{rule_id}")
                else:
                    drivers.append(f"rule:{item}")

        if not drivers:
            drivers.append("high_model_score")
        return drivers[:8]

    def _investigation_steps(self, payload: dict[str, Any], risk_drivers: list[str]) -> list[str]:
        steps = [
            "Validate customer KYC profile and beneficial ownership.",
            "Review related transactions from the previous 30 days.",
        ]
        if float(payload.get("risk_score", 0.0) or 0.0) > 85:
            steps.append("Escalate to manager for priority review under high-risk policy.")
        if any("device" in item.lower() for item in risk_drivers):
            steps.append("Perform device-link analysis for mule account indicators.")
        steps.append("Document findings and determine whether human-reviewed escalation support is warranted.")
        return steps

    def _sar_draft(self, payload: dict[str, Any], risk_drivers: list[str]) -> str:
        customer = payload.get("customer_id") or payload.get("user_id") or "unknown customer"
        alert_id = payload.get("alert_id") or payload.get("id") or "unknown alert"
        amount = payload.get("amount")
        score = float(payload.get("risk_score", 0.0) or 0.0)
        drivers = ", ".join(risk_drivers[:5])
        return (
            f"Alert {alert_id} for {customer} was escalated due to a risk score of {score:.1f}. "
            f"Transaction activity{f' totaling {amount}' if amount is not None else ''} showed unusual patterns "
            f"driven by {drivers}. This draft supports analyst review only; any SAR/STR filing decision must be made by authorized compliance staff."
        )

    def generate_copilot_summary(self, tenant_id: str, alert_id: str, run_id: str | None = None) -> dict[str, Any]:
        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(item.get("run_id")) for item in runs if item.get("run_id")), "")
        if not run_id:
            raise ValueError("No run context available for copilot summary")

        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
        payload = next((row for row in payloads if str(row.get("alert_id")) == str(alert_id)), None)
        if not payload:
            raise ValueError("Alert not found")

        explanation = self._explain_service.explain_alert(tenant_id=tenant_id, alert_id=alert_id, run_id=run_id) or {}
        risk_drivers = self._risk_drivers(explanation=explanation, payload=payload)
        steps = self._investigation_steps(payload, risk_drivers)
        network_graph = self._build_network_graph(payload)

        summary = (
            f"Alert {alert_id} scored {float(payload.get('risk_score', 0.0) or 0.0):.1f} and is "
            f"prioritized as {str(payload.get('priority') or payload.get('risk_band') or 'unknown').upper()}. "
            f"Primary risk drivers include {', '.join(risk_drivers[:3])}."
        )

        response = {
            "summary": summary,
            "risk_drivers": risk_drivers,
            "investigation_steps": steps,
            "sar_draft": self._sar_draft(payload, risk_drivers),
            "network_graph": network_graph,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._repository.save_ai_summary(
            {
                "tenant_id": tenant_id,
                "entity_type": "copilot_alert",
                "entity_id": str(alert_id),
                "summary": json.dumps(response, ensure_ascii=True),
                "run_id": run_id,
                "actor": "ai_copilot",
            }
        )
        return response
