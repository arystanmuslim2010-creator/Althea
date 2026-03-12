"""Investigation Guidance Service — contextual investigation step recommendations."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.investigation.guidance")


class InvestigationGuidanceService:
    """Generate ordered investigation steps based on alert characteristics."""

    # Typology → additional steps
    _TYPOLOGY_STEPS: dict[str, list[str]] = {
        "sanctions": [
            "Run comprehensive sanctions screening against OFAC, UN, EU, and HMT lists",
            "Identify all counterparties and check for secondary sanctions exposure",
            "Freeze account pending compliance decision if sanctions match confirmed",
        ],
        "structuring": [
            "Review transaction history for sub-threshold patterns over 90 days",
            "Identify if amounts cumulatively exceed reporting threshold",
            "Check for structuring across multiple accounts or family members",
        ],
        "high_amount_outlier": [
            "Obtain supporting documentation for the large transaction (invoice, contract)",
            "Compare against customer's declared source of wealth",
            "Verify counterparty legitimacy through business registry checks",
        ],
        "cross_border": [
            "Verify the business purpose for cross-border fund movement",
            "Check correspondent banking risk for intermediary countries",
            "Review trade finance documentation if transaction is trade-related",
        ],
        "flow_through": [
            "Map complete fund flow: origin → transit → destination",
            "Identify if layering pattern matches known typologies",
            "Review all linked accounts for coordinated flow-through",
        ],
    }

    # Risk band → escalation steps
    _RISK_BAND_STEPS: dict[str, list[str]] = {
        "CRITICAL": [
            "Immediately escalate to compliance manager for urgent review",
            "Assess whether interim account restriction is warranted",
            "Prepare SAR draft for filing within regulatory deadline",
        ],
        "HIGH": [
            "Escalate to senior analyst or team lead if investigation is inconclusive after 24 hours",
            "Flag case for manager review before closing",
        ],
    }

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

    def __init__(self, repository) -> None:
        self._repository = repository

    def _base_steps(self, payload: dict) -> list[str]:
        steps = [
            "Validate customer KYC profile and confirm beneficial ownership structure",
            "Review transaction history for the past 90 days for anomalous patterns",
            "Check customer's declared business purpose against observed transaction behavior",
            "Verify counterparty identity and assess counterparty risk",
        ]

        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        if risk_score > 70:
            steps.append("Review all linked accounts and related-party transactions")

        rules = self._parse_json(payload.get("rules_json"), [])
        if isinstance(rules, list) and rules:
            steps.append("Examine each triggered rule and gather supporting evidence")

        steps.append("Run sanctions and PEP screening on customer and all counterparties")
        steps.append("Document findings with supporting evidence in the investigation case")
        steps.append("Determine SAR candidacy based on investigation findings")
        return steps

    def generate_steps(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        logger.info(
            "Generating investigation guidance",
            extra={"tenant_id": tenant_id, "alert_id": alert_id},
        )

        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})

        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        typology = str(payload.get("typology") or "").lower()
        risk_band = str(payload.get("risk_band") or "").upper()

        steps = self._base_steps(payload)

        # Add typology-specific steps
        typology_steps = self._TYPOLOGY_STEPS.get(typology, [])
        steps = typology_steps + steps  # put specific steps first

        # Add risk-band escalation steps
        band_steps = self._RISK_BAND_STEPS.get(risk_band, [])
        steps = steps + band_steps

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique_steps: list[str] = []
        for step in steps:
            if step not in seen:
                seen.add(step)
                unique_steps.append(step)

        return {
            "alert_id": alert_id,
            "risk_score": risk_score,
            "typology": typology or "N/A",
            "risk_band": risk_band or "UNKNOWN",
            "steps": [{"step": i + 1, "description": s} for i, s in enumerate(unique_steps[:12])],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
