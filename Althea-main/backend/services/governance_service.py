from __future__ import annotations

from typing import Any

import pandas as pd


class GovernanceService:
    """
    Enterprise governance policy adapter.

    Produces queue eligibility and suppression metadata from risk scores while
    preserving fields consumed by existing APIs/UI.
    """

    def __init__(
        self,
        suppress_threshold: float = 50.0,
        mandatory_review_threshold: float = 75.0,
        p0_threshold: float = 90.0,
    ) -> None:
        self._suppress_threshold = float(suppress_threshold)
        self._mandatory_review_threshold = float(mandatory_review_threshold)
        self._p0_threshold = float(p0_threshold)

    @staticmethod
    def _risk_band(score: float) -> str:
        if score >= 90:
            return "CRITICAL"
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _priority(score: float) -> str:
        if score >= 90:
            return "P0"
        if score >= 75:
            return "P1"
        return "P2"

    def apply_governance(self, alerts_df: pd.DataFrame) -> pd.DataFrame:
        if alerts_df is None or alerts_df.empty:
            return pd.DataFrame()

        out = alerts_df.copy()
        out["risk_score"] = pd.to_numeric(out.get("risk_score", 0.0), errors="coerce").fillna(0.0)
        out["risk_band"] = out["risk_score"].apply(self._risk_band)
        out["alert_priority"] = out["risk_score"].apply(self._priority)
        out["priority"] = out.get("priority", out["risk_band"].str.lower())

        def _status(score: float) -> str:
            if score < self._suppress_threshold:
                return "suppressed"
            if score >= self._mandatory_review_threshold:
                return "mandatory_review"
            return "eligible"

        out["governance_status"] = out["risk_score"].apply(_status)
        out["in_queue"] = (out["governance_status"] != "suppressed").map(lambda v: bool(v)).astype(object)
        out["suppression_code"] = out["governance_status"].apply(lambda s: "LOW_RISK" if s == "suppressed" else "")
        out["suppression_reason"] = out["governance_status"].apply(lambda s: "Risk below queue threshold" if s == "suppressed" else "")
        out["policy_version"] = out.get("policy_version", "2.0")
        return out

    def metadata(self) -> dict[str, Any]:
        return {
            "suppress_threshold": self._suppress_threshold,
            "mandatory_review_threshold": self._mandatory_review_threshold,
            "p0_threshold": self._p0_threshold,
            "policy_version": "2.0",
        }
