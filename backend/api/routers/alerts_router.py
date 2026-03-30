from __future__ import annotations

import json
import time
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request

from core.observability import record_copilot_generation, record_integration_error
from core.security import get_authenticated_tenant_id

router = APIRouter(tags=["alerts"])


def _user_scope(request: Request) -> str:
    return request.headers.get("X-User-Scope") or "public"


def _parse_json_field(raw, default):
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


def _make_json_serializable(val):
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return None
    if isinstance(val, np.floating):
        return float(val) if np.isfinite(val) else None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat() if pd.notna(val) else None
    if isinstance(val, dict):
        return {k: _make_json_serializable(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_make_json_serializable(v) for v in val]
    if pd.isna(val):
        return None
    return val


def _sanitize_record(record: dict) -> dict:
    return {key: _make_json_serializable(value) for key, value in record.items()}


def _apply_scoring_api_fields(record: dict) -> dict:
    out = dict(record)
    score = float(out.get("risk_score", 0.0) or 0.0)
    risk_band = str(out.get("risk_band", "") or "").lower()
    out["score"] = score
    out["priority"] = str(out.get("priority") or risk_band or "low")
    top_features = _parse_json_field(out.get("top_features_json"), [])
    if not isinstance(top_features, list) or not top_features:
        contrib = _parse_json_field(out.get("top_feature_contributions_json"), [])
        if isinstance(contrib, list):
            top_features = [str(item.get("feature")) for item in contrib if isinstance(item, dict) and item.get("feature")]
    out["top_features"] = top_features if isinstance(top_features, list) else []

    explain = _parse_json_field(out.get("risk_explain_json"), {})
    if isinstance(explain, dict):
        method = str(explain.get("explanation_method") or "").strip().lower()
        if not method:
            contrib = explain.get("feature_attribution") or explain.get("contributions") or []
            if isinstance(contrib, list) and any(isinstance(item, dict) and item.get("shap_value") is not None for item in contrib):
                method = "shap"
            else:
                method = "unknown"
        status = str(explain.get("explanation_status") or "").strip().lower()
        if not status:
            status = "fallback" if method in {"numeric_fallback", "unavailable"} else ("ok" if method in {"shap", "tree_shap"} else "unknown")
        out["explanation_method"] = method
        out["explanation_status"] = status
        out["explanation_warning"] = explain.get("explanation_warning")
        out["explanation_warning_code"] = explain.get("explanation_warning_code")
    else:
        out["explanation_method"] = "unknown"
        out["explanation_status"] = "unknown"
        out["explanation_warning"] = None
        out["explanation_warning_code"] = None
    return out


def _generate_alert_summary(row_dict: dict) -> str:
    segment = row_dict.get("segment") or "unknown"
    risk_score = float(row_dict.get("risk_score") or 0)
    typology = row_dict.get("typology") or "N/A"
    risk_band = (row_dict.get("risk_band") or "").upper()
    user_id = row_dict.get("user_id", "")
    rules_raw = row_dict.get("rules_json") or row_dict.get("rules") or []
    if isinstance(rules_raw, str):
        try:
            rules_raw = json.loads(rules_raw) if rules_raw else []
        except Exception:
            rules_raw = []
    rule_ids = []
    for item in rules_raw[:5]:
        if isinstance(item, dict):
            rule_ids.append(item.get("rule_id", item.get("id", str(item))))
        else:
            rule_ids.append(str(item))
    rule_hits = ", ".join(rule_ids) if rule_ids else "N/A"
    return (
        f"Segment: {segment} | User: {user_id}\n"
        f"Risk: {risk_score:.1f} ({risk_band}) | Typology: {typology}\n"
        f"Triggered rules: {rule_hits}\n\n"
        "AML analysis: Alert indicates anomaly in client behavior. Review rule evidence, transaction context, and governance outcome."
    )


def _active_run_id(request: Request, tenant_id: str) -> Optional[str]:
    info = request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))
    return info.get("run_id")


def _load_alerts_df(request: Request, tenant_id: str, run_id: str) -> pd.DataFrame:
    payloads = request.app.state.repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
    return pd.DataFrame(payloads)


@router.get("/api/alerts")
def get_alerts(
    request: Request,
    status_filter: str = "Eligible",
    min_risk: float = 0,
    typology: str = "All",
    segment: str = "All",
    search: str = "",
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
):
    run_id = run_id or _active_run_id(request, tenant_id)
    if not run_id:
        return {"alerts": [], "run_id": None, "total": 0}
    alerts_df = _load_alerts_df(request, tenant_id, run_id)
    if alerts_df.empty:
        return {"alerts": [], "run_id": run_id, "total": 0}
    queue = alerts_df.copy()
    if status_filter == "Eligible":
        if "governance_status" in queue.columns:
            gov = queue["governance_status"].astype(str).str.lower()
            queue = queue[gov.isin(["eligible", "mandatory_review"])]
        elif "in_queue" in queue.columns:
            queue = queue[queue["in_queue"] == True]
    elif status_filter == "Suppressed":
        if "governance_status" in queue.columns:
            queue = queue[queue["governance_status"].astype(str).str.lower() == "suppressed"]
        elif "in_queue" in queue.columns:
            queue = queue[queue["in_queue"] == False]
    if min_risk > 0 and "risk_score" in queue.columns:
        queue = queue[queue["risk_score"] >= min_risk]
    if segment != "All" and "segment" in queue.columns:
        queue = queue[queue["segment"].astype(str) == segment]
    if typology != "All" and "typology" in queue.columns:
        queue = queue[queue["typology"].astype(str) == typology]
    if search:
        term = search.lower()
        mask = queue["user_id"].astype(str).str.lower().str.contains(term, na=False) | queue["alert_id"].astype(str).str.lower().str.contains(term, na=False)
        queue = queue[mask]
    if "risk_score" in queue.columns:
        queue = queue.sort_values("risk_score", ascending=False)
    records = [_sanitize_record(_apply_scoring_api_fields(record)) for record in queue.head(200).to_dict("records")]
    return {"alerts": records, "run_id": run_id, "total": len(records)}


@router.get("/api/runs")
def list_runs(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {"runs": request.app.state.pipeline_service.list_runs(tenant_id)}


@router.get("/api/alerts/{alert_id}")
def get_alert(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    if not run_id:
        raise HTTPException(status_code=404, detail="No active run")
    alerts_df = _load_alerts_df(request, tenant_id, run_id)
    if "alert_id" not in alerts_df.columns:
        raise HTTPException(status_code=404, detail="Alert not found")
    row = alerts_df[alerts_df["alert_id"].astype(str) == str(alert_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _sanitize_record(_apply_scoring_api_fields(row.iloc[0].to_dict()))


@router.get("/api/alerts/{alert_id}/explain")
def get_alert_explain(alert_id: str, request: Request, run_id: Optional[str] = None, tenant_id: str = Depends(get_authenticated_tenant_id)):
    rid = run_id or _active_run_id(request, tenant_id)
    if not rid:
        raise HTTPException(status_code=404, detail="No active run")
    result = request.app.state.explain_service.explain_alert(tenant_id=tenant_id, alert_id=alert_id, run_id=rid)
    if result is None:
        raise HTTPException(status_code=404, detail="Alert or decision trace not found")
    return result


@router.get("/api/alerts/{alert_id}/ai-summary")
def get_ai_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    rec = request.app.state.repository.get_ai_summary(tenant_id=tenant_id, entity_type="alert", entity_id=alert_id)
    if rec:
        return {"summary": rec["summary"], "ts": rec.get("ts", "")}
    return {"summary": None, "ts": None}


@router.post("/api/alerts/{alert_id}/ai-summary")
def generate_ai_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    alerts_df = _load_alerts_df(request, tenant_id, run_id or "")
    if "alert_id" not in alerts_df.columns:
        raise HTTPException(status_code=404, detail="Alert not found")
    row = alerts_df[alerts_df["alert_id"].astype(str) == str(alert_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail="Alert not found")
    summary = _generate_alert_summary(row.iloc[0].to_dict())
    actor = request.headers.get("X-Actor") or request.app.state.case_service.get_actor(tenant_id, _user_scope(request))
    rec = request.app.state.repository.save_ai_summary(
        {
            "tenant_id": tenant_id,
            "entity_type": "alert",
            "entity_id": alert_id,
            "summary": summary,
            "run_id": run_id or "",
            "actor": actor,
        }
    )
    return {"summary": summary, "ts": rec.get("ts") or pd.Timestamp.utcnow().isoformat()}


@router.delete("/api/alerts/{alert_id}/ai-summary")
def clear_ai_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    request.app.state.repository.delete_ai_summary(tenant_id=tenant_id, entity_type="alert", entity_id=alert_id)
    return {"status": "cleared"}


@router.get("/api/queue-metrics")
def get_queue_metrics(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    safe = {"total_alerts": 0, "in_queue": 0, "suppressed": 0, "high_risk": 0}
    if not run_id:
        return safe
    alerts_df = _load_alerts_df(request, tenant_id, run_id)
    if alerts_df.empty:
        return safe
    total = len(alerts_df)
    eligible = 0
    suppressed = 0
    if "governance_status" in alerts_df.columns:
        gov = alerts_df["governance_status"].astype(str).str.lower()
        eligible = int(gov.isin(["eligible", "mandatory_review"]).sum())
        suppressed = int((gov == "suppressed").sum())
    elif "in_queue" in alerts_df.columns:
        eligible = int(alerts_df["in_queue"].fillna(False).astype(bool).sum())
        suppressed = int((~alerts_df["in_queue"].fillna(False).astype(bool)).sum())
    if "risk_band" in alerts_df.columns:
        bands = alerts_df["risk_band"].astype(str).str.upper()
        high_risk = int(bands.isin(["HIGH", "CRITICAL"]).sum())
    else:
        high_risk = int((alerts_df.get("risk_score", 0) >= 70).sum()) if "risk_score" in alerts_df.columns else 0
    request.app.state.metrics.set_gauge("althea_alert_queue_size", float(eligible))
    return {"total_alerts": total, "in_queue": eligible, "suppressed": suppressed, "high_risk": high_risk}


@router.get("/api/ops-metrics")
def get_ops_metrics(request: Request, analyst_capacity: int = 50, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    alerts_df = _load_alerts_df(request, tenant_id, run_id or "")
    if alerts_df.empty or "risk_score" not in alerts_df.columns:
        return {"precision_k": 0, "alerts_per_case": 0, "suppression_rate": 0}
    metrics = request.app.state.ops_service.compute_ops_metrics(alerts_df, analyst_capacity, 30)
    return {key: float(value) if isinstance(value, (int, float)) else value for key, value in metrics.items()}


@router.post("/internal/ml/predict")
def internal_ml_predict(payload: dict, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    alert_ids = [str(item) for item in (payload.get("alert_ids") or []) if str(item)]
    frame = pd.DataFrame(payload.get("rows", []))
    if not frame.empty:
        feature_bundle = request.app.state.feature_service.generate_inference_features(frame)
        feature_frame = feature_bundle["feature_matrix"]
    else:
        feature_frame = pd.DataFrame()
    result = request.app.state.inference_service.predict(
        tenant_id=tenant_id,
        feature_frame=feature_frame,
        alert_ids=alert_ids,
        feature_version=payload.get("feature_version"),
    )
    return {
        "model_version": result["model_version"],
        "scores": result["scores"],
        "explanations": result["explanations"],
        "schema_validation": result["schema_validation"],
    }


@router.get("/alerts/{alert_id}/copilot_summary")
def get_alert_copilot_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    started = time.perf_counter()
    try:
        payload = request.app.state.ai_copilot_service.generate_copilot_summary(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=run_id,
        )
        record_copilot_generation("copilot_summary", time.perf_counter() - started)
        return payload
    except ValueError as exc:
        record_integration_error("copilot_summary")
        raise HTTPException(status_code=404, detail=str(exc))
