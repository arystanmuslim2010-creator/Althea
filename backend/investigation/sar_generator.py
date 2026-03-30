"""SAR Narrative Generator — Suspicious Activity Report draft generation."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.investigation.sar_generator")


class SARNarrativeGenerator:
    """Generate SAR narrative drafts from alert data, risk drivers, and customer profiles."""

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

    def _extract_risk_drivers(self, payload: dict, explanation: dict) -> list[str]:
        drivers: list[str] = []

        # From reason codes
        risk_expl = explanation.get("risk_explanation") or {}
        codes = self._parse_json(risk_expl.get("risk_reason_codes"), [])
        if isinstance(codes, list):
            drivers.extend([str(c) for c in codes[:3] if str(c).strip()])

        # From feature contributions
        contrib = self._parse_json(
            explanation.get("feature_contributions")
            or explanation.get("top_feature_contributions_json"),
            [],
        )
        if isinstance(contrib, list):
            for item in contrib[:3]:
                if isinstance(item, dict) and item.get("feature"):
                    feat = str(item["feature"]).replace("_", " ")
                    impact = float(item.get("contribution", item.get("shap_value", item.get("value", 0.0))) or 0.0)
                    if abs(impact) > 0.05:
                        drivers.append(f"{feat} anomaly")

        # From rules
        rules = self._parse_json(payload.get("rules_json"), [])
        if isinstance(rules, list):
            for item in rules[:2]:
                if isinstance(item, dict):
                    rule_id = str(item.get("rule_id") or item.get("id") or "")
                    if rule_id:
                        drivers.append(f"rule violation: {rule_id}")

        return drivers[:6] if drivers else ["elevated risk score", "anomalous transaction behavior"]

    def _build_narrative(self, payload: dict, risk_drivers: list[str]) -> str:
        customer = (
            payload.get("customer_name")
            or payload.get("customer_id")
            or payload.get("user_id")
            or "the subject"
        )
        alert_id = str(payload.get("alert_id") or "unknown")
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        typology = str(payload.get("typology") or "unknown activity")
        segment = str(payload.get("segment") or "unknown segment")
        country = str(payload.get("country") or "")
        amount = payload.get("amount")
        governance_status = str(payload.get("governance_status") or "eligible")

        # Date context
        created_at = str(payload.get("created_at") or payload.get("timestamp") or "")
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            date_str = dt.strftime("%B %d, %Y")
        except Exception:
            date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")

        # Amount context
        amount_clause = ""
        if amount is not None:
            try:
                amt_f = float(amount)
                amount_clause = f" totaling ${amt_f:,.2f}"
            except (TypeError, ValueError):
                pass

        country_clause = f" involving counterparties in {country}" if country else ""
        drivers_text = "; ".join(risk_drivers[:4])

        narrative = (
            f"On {date_str}, the account belonging to {customer} (Alert ID: {alert_id}) "
            f"was flagged by the AML monitoring system with a risk score of {risk_score:.1f}, "
            f"classified as {governance_status.upper()} under the {typology} typology "
            f"for the {segment} customer segment. "
            f"The subject account conducted transaction activity{amount_clause}{country_clause} "
            f"that was inconsistent with the customer's established behavioral profile. "
            f"Risk indicators identified include: {drivers_text}. "
            f"Based on the foregoing, the reporting institution has determined that the "
            f"described activity warrants filing of a Suspicious Activity Report."
        )
        return narrative

    def generate_sar_draft(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        logger.info(
            "Generating SAR narrative draft",
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

        risk_drivers = self._extract_risk_drivers(payload, explanation)
        narrative = self._build_narrative(payload, risk_drivers)

        result = {
            "alert_id": alert_id,
            "subject": (
                payload.get("customer_name")
                or payload.get("customer_id")
                or payload.get("user_id")
                or "Unknown"
            ),
            "risk_score": float(payload.get("risk_score", 0.0) or 0.0),
            "typology": str(payload.get("typology") or "N/A"),
            "risk_indicators": risk_drivers,
            "narrative": narrative,
            "disclaimer": (
                "This is a system-generated draft. It must be reviewed, verified, "
                "and approved by a qualified compliance officer before filing."
            ),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Cache the draft
        try:
            self._repository.save_ai_summary(
                {
                    "tenant_id": tenant_id,
                    "entity_type": "sar_draft",
                    "entity_id": str(alert_id),
                    "summary": json.dumps(result, ensure_ascii=True),
                    "run_id": run_id or "",
                    "actor": "sar_generator",
                }
            )
        except Exception:
            logger.warning("Failed to cache SAR draft", extra={"alert_id": alert_id})

        elapsed = time.perf_counter() - t0
        logger.info(
            "SAR draft generated",
            extra={"alert_id": alert_id, "latency_s": round(elapsed, 3)},
        )
        return result
