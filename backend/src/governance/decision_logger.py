"""Decision logging for AML scoring and governance outcomes."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


class DecisionLogger:
    """Append per-alert decision records to a JSONL audit file."""

    def __init__(self, log_path: str = "logs/decision_logs.jsonl") -> None:
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_decisions(self, df: pd.DataFrame, model_version: str) -> None:
        if df is None or df.empty:
            return

        timestamp = datetime.now(timezone.utc).isoformat()
        with self.log_path.open("a", encoding="utf-8") as handle:
            for _, row in df.iterrows():
                top_features = self._extract_top_features(row)
                governance_rules = self._governance_rules_triggered(row)
                record: Dict[str, Any] = {
                    "alert_id": str(row.get("alert_id", "")),
                    "timestamp": timestamp,
                    "model_version": str(row.get("model_version") or model_version),
                    "features_used": self._features_used(row),
                    "score": float(row.get("risk_score", 0.0) or 0.0),
                    "priority": str(row.get("priority") or self._priority_from_row(row)),
                    "top_features": top_features,
                    "governance_rules_triggered": governance_rules,
                }
                decision, reason = self._governance_decision_reason(row)
                if decision:
                    record["decision"] = decision
                if reason:
                    record["reason"] = reason
                handle.write(json.dumps(record, ensure_ascii=True) + "\n")

    @staticmethod
    def _priority_from_row(row: pd.Series) -> str:
        band = str(row.get("risk_band", "")).lower()
        return band if band else "low"

    @staticmethod
    def _parse_json(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, str):
            if not raw.strip():
                return default
            try:
                return json.loads(raw)
            except Exception:
                return default
        return raw

    def _extract_top_features(self, row: pd.Series) -> List[str]:
        direct = self._parse_json(row.get("top_features_json"), [])
        if isinstance(direct, list) and direct:
            return [str(x) for x in direct]

        contrib = self._parse_json(row.get("top_feature_contributions_json"), [])
        if isinstance(contrib, list):
            names: List[str] = []
            for item in contrib:
                if isinstance(item, dict) and item.get("feature"):
                    names.append(str(item.get("feature")))
            if names:
                return names
        return []

    def _features_used(self, row: pd.Series) -> List[str]:
        features = self._parse_json(row.get("features_json"), {})
        if isinstance(features, dict) and features:
            return [str(k) for k in features.keys()]
        return self._extract_top_features(row)

    @staticmethod
    def _governance_rules_triggered(row: pd.Series) -> List[str]:
        out: List[str] = []
        suppression_code = str(row.get("suppression_code", "") or "").strip()
        suppression_reason = str(row.get("suppression_reason", "") or "").strip()
        if suppression_code:
            out.append(suppression_code)
        if suppression_reason:
            out.extend([x.strip() for x in suppression_reason.split(";") if x.strip()])
        hard_code = str(row.get("hard_constraint_code", "") or "").strip()
        if hard_code:
            out.append(hard_code)
        return out

    def _governance_decision_reason(self, row: pd.Series) -> tuple[Optional[str], Optional[str]]:
        status = str(row.get("governance_status", "")).lower()
        reason = str(row.get("suppression_reason", "") or "").strip()
        if status == "suppressed":
            return "suppressed", reason or "low_expected_investigative_yield"

        priority = str(row.get("priority") or self._priority_from_row(row)).lower()
        if priority in ("low", "medium"):
            return "deprioritized", reason or "low_expected_investigative_yield"
        return None, None
