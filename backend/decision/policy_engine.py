"""Policy engine — compliance constraints and governance enforcement.

This module is the ONLY place where compliance hard rules live.
It does NOT rank alerts (that is PriorityFormula's job).
It enforces rules that override or constrain ranking:

Hard rules:
    - Suppression: alerts below threshold are removed from the queue
    - Mandatory review: high-risk alerts cannot be auto-closed
    - Sanctions hold: sanction-flagged alerts must always enter queue
    - Regulatory escalation: alerts near CTR threshold get mandatory flag

All decisions are recorded in the audit trail.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class PolicyConfig:
    """Configurable thresholds for governance enforcement."""
    suppress_threshold: float = 30.0           # priority score below which alert is suppressed
    mandatory_review_threshold: float = 70.0   # above which analyst review is mandatory
    high_risk_typologies: frozenset[str] = frozenset({"sanctions", "terrorism_financing"})
    high_risk_countries: frozenset[str] = frozenset({"IR", "KP", "SY"})
    ctr_amount_threshold: float = 10_000.0     # Currency Transaction Report threshold
    policy_version: str = "3.0"


@dataclass
class PolicyDecision:
    governance_status: str   # "suppressed" | "eligible" | "mandatory_review" | "sanctions_hold"
    suppression_code: str
    suppression_reason: str
    in_queue: bool
    policy_version: str
    compliance_flags: list[str]
    audit_trail: dict[str, Any]


class PolicyEngine:
    """Apply governance policy rules to scored and ranked alerts.

    Takes a DataFrame with priority_score (from PriorityFormula) and
    applies compliance constraints to produce a final governance status.
    """

    def __init__(self, config: PolicyConfig | None = None) -> None:
        self._config = config or PolicyConfig()

    def apply(self, alerts_df: pd.DataFrame) -> pd.DataFrame:
        """Apply policy rules to all alerts in the DataFrame.

        Input DataFrame must contain at least:
            priority_score  : float [0, 100]

        Optional columns that activate additional rules:
            typology        : str
            country         : str
            amount          : float
            escalation_prob : float

        Returns the input DataFrame enriched with governance columns.
        """
        if alerts_df is None or alerts_df.empty:
            return pd.DataFrame()

        out = alerts_df.copy()
        cfg = self._config

        priority_raw = out.get("priority_score", out.get("risk_score", pd.Series(50.0, index=out.index)))
        priority = pd.to_numeric(priority_raw, errors="coerce").fillna(0.0)
        typology = out.get("typology", pd.Series("", index=out.index)).astype(str).str.lower().str.strip()
        country = out.get("country", pd.Series("", index=out.index)).astype(str).str.upper().str.strip()
        amount_raw = out.get("amount", pd.Series(0.0, index=out.index))
        amount = pd.to_numeric(amount_raw, errors="coerce").fillna(0.0)

        # ------------------------------------------------------------------
        # Sanctions hold (highest precedence — always queued regardless of score)
        # ------------------------------------------------------------------
        sanctions_flag = (
            typology.isin(self._config.high_risk_typologies)
            | country.isin(self._config.high_risk_countries)
        )

        # ------------------------------------------------------------------
        # CTR mandatory flag
        # ------------------------------------------------------------------
        ctr_flag = amount >= cfg.ctr_amount_threshold

        # ------------------------------------------------------------------
        # Governance status assignment (priority order)
        # ------------------------------------------------------------------
        def _status(idx: int) -> str:
            p = float(priority.iloc[idx])
            if sanctions_flag.iloc[idx]:
                return "sanctions_hold"
            if p >= cfg.mandatory_review_threshold:
                return "mandatory_review"
            if p < cfg.suppress_threshold:
                return "suppressed"
            return "eligible"

        governance_statuses = [_status(i) for i in range(len(out))]
        out["governance_status"] = governance_statuses
        out["in_queue"] = pd.Series([bool(s != "suppressed") for s in governance_statuses], dtype=object)
        out["suppression_code"] = [
            "LOW_PRIORITY" if s == "suppressed" else ""
            for s in governance_statuses
        ]
        out["suppression_reason"] = [
            "Priority score below suppression threshold" if s == "suppressed" else ""
            for s in governance_statuses
        ]
        out["policy_version"] = cfg.policy_version

        # Compliance flags JSON per row
        flags_list = []
        for i in range(len(out)):
            flags: list[str] = []
            if sanctions_flag.iloc[i]:
                flags.append("SANCTIONS_TYPOLOGY_OR_COUNTRY")
            if ctr_flag.iloc[i]:
                flags.append("CTR_AMOUNT_THRESHOLD")
            if float(priority.iloc[i]) >= cfg.mandatory_review_threshold:
                flags.append("HIGH_PRIORITY_MANDATORY_REVIEW")
            flags_list.append(json.dumps(flags, ensure_ascii=True))
        out["compliance_flags_json"] = flags_list

        # Backward-compatible risk_band column
        out["risk_band"] = priority.apply(self._risk_band)
        out["alert_priority"] = priority.apply(self._alert_priority)

        return out

    @staticmethod
    def _risk_band(score: float) -> str:
        if score >= 85:
            return "CRITICAL"
        if score >= 65:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _alert_priority(score: float) -> str:
        if score >= 85:
            return "P0"
        if score >= 65:
            return "P1"
        return "P2"

    def metadata(self) -> dict[str, Any]:
        return {
            "suppress_threshold": self._config.suppress_threshold,
            "mandatory_review_threshold": self._config.mandatory_review_threshold,
            "policy_version": self._config.policy_version,
        }
