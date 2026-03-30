from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class InvestigationNarrativeService:
    """Deterministic narrative drafting from available investigation signals."""

    def __init__(self, repository, explain_service) -> None:
        self._repository = repository
        self._explain_service = explain_service

    @staticmethod
    def _to_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if value is None:
            return []
        text = str(value).strip()
        return [text] if text else []

    @staticmethod
    def _fallback(alert_id: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        narrative = (
            f"Investigation draft for alert {alert_id}. "
            "Available data is limited; analyst review is required before conclusions are documented."
        )
        return {
            "alert_id": alert_id,
            "title": "Investigation Narrative Draft",
            "narrative": narrative,
            "sections": {
                "activity_summary": "Insufficient transaction context available at draft time.",
                "risk_indicators": [],
                "recommended_follow_up": [
                    "Review full transaction history for the customer and linked accounts.",
                    "Validate KYC profile and any sanctions screening matches.",
                ],
            },
            "generated_at": now,
            "source_signals": {
                "risk_score": None,
                "reason_codes": [],
                "countries": [],
            },
        }

    def generate_draft(self, tenant_id: str, alert_id: str, run_id: str | None = None) -> dict[str, Any]:
        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(row.get("run_id")) for row in runs if row.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id or "", limit=500000)
        payload = next((row for row in payloads if str(row.get("alert_id")) == str(alert_id)), None)
        if not payload:
            return self._fallback(alert_id)

        explanation = {}
        if run_id:
            try:
                explanation = self._explain_service.explain_alert(
                    tenant_id=tenant_id,
                    alert_id=alert_id,
                    run_id=run_id,
                ) or {}
            except Exception:
                explanation = {}

        reason_codes = self._to_list((explanation.get("risk_explanation") or {}).get("risk_reason_codes"))
        if not reason_codes:
            reason_codes = self._to_list(payload.get("reason_codes"))

        countries = []
        for key in ("country", "beneficiary_country", "counterparty_country"):
            value = str(payload.get(key) or "").strip()
            if value:
                countries.append(value.upper())
        countries = sorted(set(countries))

        rules = self._to_list(payload.get("rules_json"))
        typology = str(payload.get("typology") or "unknown").strip()
        risk_score = payload.get("risk_score")
        customer = str(payload.get("customer_id") or payload.get("user_id") or "unknown customer").strip()
        source_account = str(payload.get("account_id") or "unknown account").strip()
        counterparty = str(
            payload.get("counterparty_account")
            or payload.get("counterparty_id")
            or payload.get("beneficiary_account")
            or payload.get("beneficiary_id")
            or "unknown counterparty"
        ).strip()
        amount = payload.get("amount")

        ai_summary = self._repository.get_ai_summary(tenant_id=tenant_id, entity_type="alert", entity_id=str(alert_id))
        ai_hint = str((ai_summary or {}).get("summary") or "").strip()

        activity_summary = (
            f"Alert {alert_id} involves customer {customer} using source account {source_account} "
            f"with counterparty {counterparty}."
        )
        if amount is not None:
            activity_summary += f" Observed transaction amount: {amount}."
        if typology and typology != "unknown":
            activity_summary += f" Detected typology: {typology}."

        risk_indicators: list[str] = []
        if risk_score is not None:
            risk_indicators.append(f"Risk score at alert time: {risk_score}.")
        for code in reason_codes[:5]:
            risk_indicators.append(f"Reason code: {code}.")
        for country in countries[:3]:
            risk_indicators.append(f"Country exposure: {country}.")
        if rules:
            risk_indicators.append("Rules triggered: " + ", ".join(rules[:3]) + ".")
        if not risk_indicators:
            risk_indicators.append("Limited explicit risk indicators were available in source signals.")

        follow_up = [
            "Validate the customer profile, expected account behavior, and source of funds.",
            "Review linked transactions and counterparties over the relevant monitoring period.",
        ]
        if countries:
            follow_up.append("Confirm jurisdictional risk relevance and sanctions exposure checks.")
        if reason_codes:
            follow_up.append("Trace each risk reason code to supporting evidence in transaction and rule data.")

        narrative_lines = [
            "Draft narrative for analyst review.",
            activity_summary,
            f"Key risk indicators include {', '.join(reason_codes[:3]) if reason_codes else 'available transaction and rule signals'}.",
            "This draft must be validated and edited by the assigned investigator before case submission.",
        ]
        if ai_hint:
            narrative_lines.append(f"Supporting AI summary context: {ai_hint}")

        return {
            "alert_id": str(alert_id),
            "title": "Investigation Narrative Draft",
            "narrative": " ".join(line for line in narrative_lines if line),
            "sections": {
                "activity_summary": activity_summary,
                "risk_indicators": risk_indicators,
                "recommended_follow_up": follow_up,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_signals": {
                "risk_score": risk_score,
                "reason_codes": reason_codes,
                "countries": countries,
            },
        }
