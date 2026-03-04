"""
PERSIST stage: Write alerts (with run_id, external_versions_json, decision_trace_json), run registry, rule_hit_stats, alert_daily_stats.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import pandas as pd

from ...observability.logging import get_logger

logger = get_logger("persist")


def run_persist(
    df: pd.DataFrame,
    run_id: str,
    source: str,
    dataset_hash: str,
    row_count: int,
    storage: Any,
    config: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Persist alerts (with run_id, external_versions_json, decision_trace_json, schema_version),
    run registry (run_id, source, dataset_hash, row_count, created_at, notes),
    and optionally rule_hit_stats, alert_daily_stats.
    """
    config = config or {}
    policy_version = str(config.get("policy_version", "1.0"))
    now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    storage.save_run(run_id, source, dataset_hash, row_count, notes=config.get("notes", ""))

    records = df.to_dict("records")
    for r in records:
        r["alert_id"] = str(r.get("alert_id", ""))
        r["user_id"] = str(r.get("user_id", ""))
        r["run_id"] = run_id
        r.setdefault("policy_version", policy_version)
        r.setdefault("external_versions_json", "{}")
        if "decision_trace_json" in r and not isinstance(r["decision_trace_json"], str):
            r["decision_trace_json"] = json.dumps(r["decision_trace_json"]) if r["decision_trace_json"] else "{}"
        for col in ["risk_explain_json", "rules_json", "rule_evidence_json", "ml_signals_json", "features_json", "context_json"]:
            if col in r and r[col] is not None and not isinstance(r[col], str):
                r[col] = json.dumps(r[col]) if r[col] != "" else "{}"
    storage.upsert_alerts(records, run_id=run_id)

    # rule_hit_stats
    if hasattr(storage, "upsert_rule_hit_stats") and "rule_hits" in df.columns:
        try:
            run_date = time.strftime("%Y-%m-%d", time.gmtime())
            stats_list = []
            for rule_id in df["rule_hits"].explode().dropna().unique():
                hit_count = (df["rule_hits"].apply(lambda x: rule_id in x if isinstance(x, list) else False)).sum()
                n_alerts = len(df)
                hit_rate = hit_count / n_alerts if n_alerts else 0
                stats_list.append({"run_date": run_date, "rule_id": rule_id, "hit_rate": hit_rate, "n_alerts": n_alerts, "n_hits": int(hit_count)})
            storage.upsert_rule_hit_stats(stats_list)
        except Exception:
            pass

    # alert_daily_stats
    if hasattr(storage, "upsert_daily_stats"):
        try:
            run_date = time.strftime("%Y-%m-%d", time.gmtime())
            in_queue = int(df["in_queue"].sum()) if "in_queue" in df.columns else 0
            mandatory = int((df["governance_status"].astype(str).str.upper() == "MANDATORY_REVIEW").sum()) if "governance_status" in df.columns else 0
            suppressed = int((df["governance_status"].astype(str).str.lower() == "suppressed").sum()) if "governance_status" in df.columns else 0
            storage.upsert_daily_stats({"date": run_date, "total_alerts": row_count, "in_queue": in_queue, "mandatory_review": mandatory, "suppressed": suppressed})
        except Exception:
            pass
