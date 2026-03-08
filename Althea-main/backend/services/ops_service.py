from __future__ import annotations

from typing import Any

import pandas as pd


class OpsService:
    def compute_ops_metrics(self, df: pd.DataFrame, analyst_capacity: int, minutes_per_case: int) -> dict[str, Any]:
        if df is None or df.empty:
            return {
                "precision_k": 0.0,
                "alerts_per_case": 0.0,
                "total_minutes": 0,
                "tp": 0,
                "suppression_rate": 0.0,
                "eligible_alerts_count": 0,
                "suppressed_alerts_count": 0,
            }

        ranked = df.copy()
        ranked["risk_score"] = pd.to_numeric(ranked.get("risk_score", 0.0), errors="coerce").fillna(0.0)
        ranked = ranked.sort_values("risk_score", ascending=False)
        topk = ranked.head(max(1, int(analyst_capacity)))

        tp = int((topk["risk_score"] >= 85).sum())
        precision_k = float(tp / max(1, len(topk)))
        alerts_per_case = float(len(topk) / max(1, tp)) if tp else 0.0

        if "governance_status" in df.columns:
            gov = df["governance_status"].astype(str).str.lower()
            eligible_count = int(gov.isin(["eligible", "mandatory_review"]).sum())
            suppressed_count = int((gov == "suppressed").sum())
        elif "in_queue" in df.columns:
            in_queue = df["in_queue"].fillna(False).astype(bool)
            eligible_count = int(in_queue.sum())
            suppressed_count = int((~in_queue).sum())
        else:
            eligible_count = int(len(df))
            suppressed_count = 0

        total_count = max(1, int(len(df)))
        suppression_rate = float(suppressed_count / total_count)
        return {
            "precision_k": precision_k,
            "alerts_per_case": alerts_per_case,
            "total_minutes": int(max(1, analyst_capacity) * max(1, minutes_per_case)),
            "tp": tp,
            "suppression_rate": suppression_rate,
            "eligible_alerts_count": eligible_count,
            "suppressed_alerts_count": suppressed_count,
            "label_source": "heuristic",
            "using_synthetic_labels": False,
            "synthetic_label_warning": "",
        }

