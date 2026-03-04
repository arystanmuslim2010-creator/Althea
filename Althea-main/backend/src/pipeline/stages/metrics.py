"""
METRICS stage: Pilot KPIs (suppression rate, prioritization lift, precision@capacity, queue pressure, analyst time saved).
Generate data/reports/<run_id>_pilot_report.json and CSV summary.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from ...observability.logging import get_logger

logger = get_logger("metrics")


def run_metrics(
    df: pd.DataFrame,
    run_id: str,
    reports_dir: Path,
    config: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Compute pilot KPIs and write data/reports/<run_id>_pilot_report.json and <run_id>_pilot_report.csv.
    """
    config = config or {}
    total = len(df)
    if total == 0:
        report = {
            "run_id": run_id,
            "total_alerts": 0,
            "in_queue": 0,
            "suppressed": 0,
            "mandatory_review": 0,
            "hard_constraints_count": 0,
            "suppression_rate": 0,
            "rule_hit_distribution": {},
            "top_typology_distribution": {},
            "precision_at_capacity": None,
            "recall_at_k": None,
            "lift": None,
            "queue_pressure": 0,
            "sla_risk": "low",
            "analyst_time_saved_mins": 0,
            "baseline_minutes_per_alert": config.get("baseline_minutes_per_alert") or config.get("minutes_per_alert", 10),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    else:
        in_queue = int(df["in_queue"].sum()) if "in_queue" in df.columns else 0
        suppressed = int((df["governance_status"].astype(str).str.lower() == "suppressed").sum()) if "governance_status" in df.columns else 0
        suppression_rate = suppressed / total if total else 0
        mandatory = int((df["governance_status"].astype(str).str.upper() == "MANDATORY_REVIEW").sum()) if "governance_status" in df.columns else 0

        # Labels column: risk_label or synthetic_true_suspicious
        labels_col = config.get("labels_column") or "risk_label"
        if labels_col not in df.columns and "synthetic_true_suspicious" in df.columns:
            labels_col = "synthetic_true_suspicious"
        has_labels = labels_col in df.columns
        precision_at_capacity = None
        recall_at_k = None
        lift = None
        if has_labels and in_queue > 0:
            queue_df = df[df["in_queue"] == True].copy()
            if "risk_score" in queue_df.columns:
                queue_df = queue_df.sort_values("risk_score", ascending=False)
            k = min(config.get("capacity", 100), len(queue_df))
            if k > 0:
                top = queue_df.head(k)
                tp = int((top[labels_col].astype(str).str.lower().isin(["yes", "true", "1"])).sum())
                total_positive = int((df[labels_col].astype(str).str.lower().isin(["yes", "true", "1"])).sum())
                precision_at_capacity = tp / k if k else 0
                recall_at_k = (tp / total_positive) if total_positive else 0
                base_rate = total_positive / total if total else 0
                lift = (precision_at_capacity / base_rate) if base_rate else 0

        # Queue pressure / SLA risk: in_queue vs capacity
        capacity = config.get("capacity", 400)
        queue_pressure = in_queue / capacity if capacity else 0
        sla_risk = "high" if queue_pressure > 1.0 else ("medium" if queue_pressure > 0.8 else "low")

        # Analyst time saved (configurable: baseline_minutes_per_alert or minutes_per_alert)
        mins_per_alert = config.get("baseline_minutes_per_alert") or config.get("minutes_per_alert", 10)
        time_saved_mins = suppressed * mins_per_alert

        # Rule hit distribution (top rules by hit count)
        rule_hit_distribution: Dict[str, int] = {}
        if "rule_hits" in df.columns:
            for hits in df["rule_hits"].dropna():
                if isinstance(hits, list):
                    for r in hits:
                        rule_hit_distribution[str(r)] = rule_hit_distribution.get(str(r), 0) + 1
        top_rules = dict(sorted(rule_hit_distribution.items(), key=lambda x: -x[1])[:10])

        # Hard constraints count (MANDATORY_REVIEW)
        hard_constraints_count = int(mandatory)

        # Top typology distribution (from context_json.typology_likelihood / top_typology)
        top_typology_distribution: Dict[str, int] = {}
        if "context_json" in df.columns:
            for ctx in df["context_json"].dropna():
                if isinstance(ctx, str) and ctx.strip():
                    try:
                        obj = json.loads(ctx)
                        top = obj.get("top_typology") or {}
                        name = str(top.get("name", "") or "").strip() or "unknown"
                        top_typology_distribution[name] = top_typology_distribution.get(name, 0) + 1
                    except (json.JSONDecodeError, TypeError):
                        top_typology_distribution["unknown"] = top_typology_distribution.get("unknown", 0) + 1

        report = {
            "run_id": run_id,
            "total_alerts": total,
            "in_queue": int(in_queue),
            "suppressed": int(suppressed),
            "mandatory_review": int(mandatory),
            "hard_constraints_count": hard_constraints_count,
            "suppression_rate": round(suppression_rate, 4),
            "rule_hit_distribution": top_rules,
            "top_typology_distribution": top_typology_distribution,
            "precision_at_capacity": precision_at_capacity,
            "recall_at_k": round(recall_at_k, 4) if recall_at_k is not None else None,
            "lift": round(float(lift), 4) if lift is not None else None,
            "queue_pressure": round(queue_pressure, 4),
            "sla_risk": sla_risk,
            "analyst_time_saved_mins": time_saved_mins,
            "baseline_minutes_per_alert": mins_per_alert,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"{run_id}_pilot_report.json"
    csv_path = reports_dir / f"{run_id}_pilot_report.csv"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    pd.DataFrame([report]).to_csv(csv_path, index=False)
