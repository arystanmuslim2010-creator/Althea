"""
Explain service: return decision_trace_json for an alert as a structured dict.
Used by API /api/alerts/{alert_id}/explain and UI "Why?" drilldown.
Returns full trace (context_summary, external_versions, ml, etc.) when stored.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional


def explain_alert(alert_id: str, run_id: Optional[str], storage: Any) -> Optional[Dict[str, Any]]:
    """
    Return decision trace for alert as dict (parsed decision_trace_json with context_summary, external_versions).
    Returns None if alert or run not found.
    """
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
        raw = r.get("decision_trace_json")
        if raw and isinstance(raw, str) and raw.strip():
            return json.loads(raw)
        # Build on the fly if missing
        from ..pipeline.stages.explain import _row_to_decision_trace
        return _row_to_decision_trace(r, run_id)
    except Exception:
        return None
