"""
EXPLAIN stage: Build decision_trace per alert (input -> features -> model -> rules -> governance -> outcome).
Interface: explain_alert(alert_id) -> DecisionTrace.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import pandas as pd

from ...domain.models import DecisionTrace
from ...observability.logging import get_logger

logger = get_logger("explain")


def _parse_json_field(val: Any, default: Any = None) -> Any:
    if val is None or (isinstance(val, str) and not val.strip()):
        return default if default is not None else {}
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return default if default is not None else {}
    return val


def _row_to_decision_trace(row: pd.Series, run_id: str) -> Dict[str, Any]:
    """Build decision_trace dict for one alert (input_summary, context_summary, ml, rules, governance, external_versions)."""
    features = _parse_json_field(row.get("features_json"), {})
    context_raw = row.get("context_json")
    context_summary = _parse_json_field(context_raw, {})
    typology_likelihood = context_summary.get("typology_likelihood", {})
    top_typology = context_summary.get("top_typology", {"name": "", "p": 0.0})
    rule_ev = row.get("rule_evidence_json")
    rules = _parse_json_field(rule_ev, []) if isinstance(rule_ev, (str, type(None))) else (rule_ev or [])
    external_versions = _parse_json_field(row.get("external_versions_json"), {})
    ml_signals = _parse_json_field(row.get("ml_signals_json"), {})
    risk_explain = _parse_json_field(row.get("risk_explain_json"), {})
    policy_ver = str(row.get("policy_version", "1.0"))
    return {
        "alert_id": str(row.get("alert_id", "")),
        "run_id": run_id,
        "input_summary": {"entity_id": str(row.get("user_id", row.get("entity_id", ""))), "typology": str(row.get("typology", "")), "segment": str(row.get("segment", ""))},
        "context_summary": context_summary,
        "typology_likelihood": typology_likelihood,
        "top_typology": top_typology,
        "features_summary": features,
        "model_output": {
            "risk_score": float(row.get("risk_score", 0)),
            "risk_prob": float(row.get("risk_prob", 0)),
            "risk_band": str(row.get("risk_band", "")),
            "ml_signals_json": ml_signals,
            "risk_explain_json": risk_explain,
        },
        "rules": rules,
        "governance": {
            "governance_status": str(row.get("governance_status", "")),
            "in_queue": bool(row.get("in_queue", False)),
            "suppression_code": str(row.get("suppression_code", "")),
            "suppression_reason": str(row.get("suppression_reason", "")),
            "policy_version": policy_ver,
        },
        "outcome": {"in_queue": bool(row.get("in_queue", False)), "risk_band": str(row.get("risk_band", ""))},
        "external_versions": external_versions,
        "schema_version": "1.0",
    }


def run_explain(
    df: pd.DataFrame,
    run_id: Optional[str] = None,
    storage: Any = None,
    config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Add decision_trace_json to each row."""
    run_id = run_id or ""
    out = df.copy()
    traces = []
    for idx in out.index:
        traces.append(_row_to_decision_trace(out.loc[idx], run_id))
    out["decision_trace_json"] = [json.dumps(t) for t in traces]
    return out


def explain_alert(alert_id: str, run_id: Optional[str], storage: Any) -> Optional[DecisionTrace]:
    """Return DecisionTrace for alert_id in given run. Used by UI 'Why?' drilldown."""
    if not run_id or not storage:
        return None
    try:
        df = storage.load_alerts_by_run(run_id)
        if df.empty:
            return None
        row = df[df["alert_id"].astype(str) == str(alert_id)]
        if row.empty:
            return None
        r = row.iloc[0]
        trace_dict = r.get("decision_trace_json")
        if isinstance(trace_dict, str):
            trace_dict = json.loads(trace_dict) if trace_dict else {}
        if not trace_dict:
            trace_dict = _row_to_decision_trace(r, run_id)
        return DecisionTrace(
            alert_id=str(trace_dict.get("alert_id", alert_id)),
            run_id=str(trace_dict.get("run_id", run_id)),
            input_summary=trace_dict.get("input_summary", {}),
            features_summary=trace_dict.get("features_summary", {}),
            model_output=trace_dict.get("model_output", {}),
            rules=trace_dict.get("rules", []),
            governance=trace_dict.get("governance", {}),
            outcome=trace_dict.get("outcome", {}),
            schema_version=str(trace_dict.get("schema_version", "1.0")),
        )
    except Exception:
        return None
