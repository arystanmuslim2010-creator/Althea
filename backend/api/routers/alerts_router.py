from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Request

from core.access_control import filter_visible_alerts, require_alert_access
from core.observability import record_copilot_generation, record_integration_error
from core.security import get_authenticated_tenant_id, require_permissions
from services.scoring_service import build_score_contract, derive_risk_band

router = APIRouter(tags=["alerts"])
logger = logging.getLogger("althea.alerts")


def _user_scope(request: Request) -> str:
    current_user = getattr(request.state, "current_user", None) or {}
    return current_user.get("user_id") or "public"


def _request_user(request: Request, tenant_id: str) -> dict:
    user = getattr(request.state, "current_user", None)
    if isinstance(user, dict) and user:
        return user
    # Compatibility for isolated router tests that override tenant auth directly.
    return {
        "user_id": "test-admin",
        "id": "test-admin",
        "role": "admin",
        "roles": ["admin"],
        "permissions": ["view_all_alerts", "manager_approval", "view_system_logs"],
        "tenant_id": tenant_id,
    }


def _parse_json_field(raw, default, field_name: str = "json_field"):
    if raw is None:
        return default
    if isinstance(raw, str):
        if not raw.strip():
            return default
        try:
            return json.loads(raw)
        except Exception as exc:
            logger.warning(
                "Invalid JSON field",
                extra={"field_name": field_name, "error": str(exc)},
            )
            raise HTTPException(status_code=400, detail="Invalid JSON input")
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
    sanitized = {key: _make_json_serializable(value) for key, value in record.items()}
    sanitized.pop("evaluation_label_is_sar", None)
    return sanitized


def _parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _title_case(value: str) -> str:
    cleaned = str(value or "").strip().replace("_", " ").replace("-", " ")
    return " ".join(part.capitalize() for part in cleaned.split())


def _short_reason(record: dict) -> str:
    typology = str(record.get("typology") or "").strip().lower()
    if typology:
        return f"Pattern is consistent with possible {_title_case(typology).lower()} activity and warrants review."

    top_features = record.get("top_features") or []
    if isinstance(top_features, list) and top_features:
        first = str(top_features[0] or "").strip().lower()
        if "time_gap" in first:
            return "Rapid movement of funds may indicate unusual velocity and warrants review."
        if "amount" in first:
            return "Transaction size differs from expected activity and warrants review."
        if "counterparty" in first:
            return "Counterparty pattern may indicate concentration or dispersal risk and warrants review."

    rules_raw = _parse_json_field(record.get("rules_json"), [], field_name="rules_json")
    if isinstance(rules_raw, list) and rules_raw:
        return "Triggered rule activity may indicate elevated AML risk and warrants review."
    return "Prioritized for elevated risk and analyst review."


def _rank_queue(queue: pd.DataFrame) -> pd.DataFrame:
    ranked = queue.copy()
    if ranked.empty:
        ranked["priority_rank"] = []
        return ranked
    score_series = pd.to_numeric(ranked.get("priority_score", ranked.get("risk_score", 0.0)), errors="coerce").fillna(0.0)
    risk_series = pd.to_numeric(ranked.get("risk_score", 0.0), errors="coerce").fillna(0.0)
    ranked = ranked.assign(_priority_score=score_series, _risk_score=risk_series)
    ranked = ranked.sort_values(["_priority_score", "_risk_score"], ascending=[False, False], kind="mergesort")
    ranked["priority_rank"] = range(1, len(ranked) + 1)
    return ranked.drop(columns=["_priority_score", "_risk_score"])


def _apply_scoring_api_fields(record: dict) -> dict:
    out = build_score_contract(dict(record))
    score = float(out.get("risk_score", 0.0) or 0.0)
    risk_band = str(out.get("risk_band", "") or derive_risk_band(score)).lower()
    out["score"] = score
    out["priority"] = str(out.get("priority") or risk_band or "low")
    top_features = _parse_json_field(out.get("top_features_json"), [], field_name="top_features_json")
    if not isinstance(top_features, list) or not top_features:
        contrib = _parse_json_field(out.get("top_feature_contributions_json"), [], field_name="top_feature_contributions_json")
        if isinstance(contrib, list):
            top_features = [str(item.get("feature")) for item in contrib if isinstance(item, dict) and item.get("feature")]
    out["top_features"] = top_features if isinstance(top_features, list) else []

    explain = _parse_json_field(out.get("risk_explain_json"), {}, field_name="risk_explain_json")
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
    out["short_reason"] = _short_reason(out)
    return out


def _project_queue_record(record: dict) -> dict:
    created_at = record.get("created_at") or record.get("timestamp")
    return {
        "alert_id": record.get("alert_id"),
        "priority_rank": record.get("priority_rank"),
        "risk_score": record.get("risk_score"),
        "risk_score_normalized": record.get("risk_score_normalized"),
        "risk_band": record.get("risk_band"),
        "short_reason": record.get("short_reason"),
        "typology": record.get("typology"),
        "account_count": int(record.get("account_count") or (1 if record.get("user_id") or record.get("account_id") else 0)),
        "transaction_count": int(record.get("transaction_count") or record.get("num_transactions") or 0),
        "status": record.get("status") or record.get("governance_status") or "open",
        "assigned_to": record.get("assigned_to"),
        "created_at": created_at,
        "source_system": record.get("source_system"),
        "user_id": record.get("user_id"),
        "account_id": record.get("account_id"),
        "segment": record.get("segment"),
        "governance_status": record.get("governance_status"),
        "model_version": record.get("model_version"),
    }


def _build_detail_sections(record: dict) -> dict:
    transactions = _parse_json_field(record.get("transactions_json"), [], field_name="transactions_json")
    if not isinstance(transactions, list):
        transactions = []
    transactions = sorted(
        [item for item in transactions if isinstance(item, dict)],
        key=lambda item: str(item.get("timestamp") or item.get("created_at") or ""),
    )
    entities = _parse_json_field(record.get("entities_json"), [], field_name="entities_json")
    if not isinstance(entities, list):
        entities = []
    timeline = _parse_json_field(record.get("timeline_json"), transactions, field_name="timeline_json")
    if not isinstance(timeline, list):
        timeline = transactions
    timeline = sorted(
        [item for item in timeline if isinstance(item, dict)],
        key=lambda item: str(item.get("timestamp") or item.get("created_at") or ""),
    )
    workflow = {
        "status": record.get("status") or record.get("governance_status") or "open",
        "assigned_to": record.get("assigned_to"),
        "available_actions": ["open_case", "assign", "add_note", "change_status"],
    }
    return {
        "risk": {
            "risk_score": record.get("risk_score"),
            "risk_band": record.get("risk_band"),
            "priority_rank": record.get("priority_rank"),
            "score_method": record.get("score_method"),
            "score_version": record.get("score_version"),
        },
        "investigation_summary": record.get("short_reason"),
        "why_prioritized": {
            "summary_text": record.get("short_reason"),
            "key_risk_drivers": [],
            "aml_patterns": [],
            "analyst_next_steps": [],
        },
        "entities": entities,
        "transactions": transactions,
        "timeline": timeline,
        "workflow": workflow,
        "audit_activity": [],
        "technical_details": {
            "risk_explain_json": _parse_json_field(record.get("risk_explain_json"), {}, field_name="risk_explain_json"),
            "top_feature_contributions_json": _parse_json_field(
                record.get("top_feature_contributions_json"),
                [],
                field_name="top_feature_contributions_json",
            ),
        },
    }


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


def _build_feature_enrichment_context(request: Request, tenant_id: str, frame: pd.DataFrame):
    enrichment_service = getattr(request.app.state, "feature_enrichment_service", None)
    if enrichment_service is None or frame is None or frame.empty:
        return None
    run_id = _active_run_id(request, tenant_id)
    return enrichment_service.build_context(
        tenant_id=tenant_id,
        alerts_df=frame,
        run_id=run_id,
    )


@router.get("/api/alerts")
def get_alerts(
    request: Request,
    status_filter: str = "Eligible",
    min_risk: float = 0,
    typology: str = "All",
    segment: str = "All",
    risk_band: str = "All",
    workflow_status: str = "All",
    search: str = "",
    limit: int = 50,
    offset: int = 0,
    response_mode: str = "default",
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
):
    run_id = run_id or _active_run_id(request, tenant_id)
    if not run_id:
        return {"alerts": [], "run_id": None, "total": 0}
    alerts_df = _load_alerts_df(request, tenant_id, run_id)
    if alerts_df.empty:
        return {"alerts": [], "run_id": run_id, "total": 0}
    user = _request_user(request, tenant_id)
    visible = filter_visible_alerts(request, tenant_id, user, alerts_df.to_dict("records"))
    queue = pd.DataFrame(visible)
    if queue.empty:
        return {"alerts": [], "run_id": run_id, "total": 0, "total_available": 0, "limit": max(1, min(int(limit), 500)), "offset": max(0, int(offset))}
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
    normalized_risk_band = str(risk_band or "All").strip()
    if normalized_risk_band not in {"All", "High", "Medium", "Low"}:
        raise HTTPException(status_code=400, detail="Invalid risk_band filter. Use All, High, Medium, or Low.")
    if normalized_risk_band != "All":
        queue = queue[
            queue.get("risk_band", pd.Series("", index=queue.index)).astype(str).str.lower() == normalized_risk_band.lower()
        ]
    normalized_workflow_status = str(workflow_status or "All").strip()
    allowed_statuses = {"All", "open", "in_review", "escalated", "closed", "eligible", "suppressed"}
    if normalized_workflow_status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail="Invalid workflow_status filter. Use All, open, in_review, escalated, closed, eligible, or suppressed.",
        )
    if normalized_workflow_status != "All":
        status_series = queue.get("status", queue.get("governance_status", pd.Series("", index=queue.index))).astype(str)
        queue = queue[status_series.str.lower() == normalized_workflow_status.lower()]
    if segment != "All" and "segment" in queue.columns:
        queue = queue[queue["segment"].astype(str) == segment]
    if typology != "All" and "typology" in queue.columns:
        queue = queue[queue["typology"].astype(str) == typology]
    if search:
        term = search.lower()
        mask = (
            queue["user_id"].astype(str).str.lower().str.contains(term, na=False)
            | queue["alert_id"].astype(str).str.lower().str.contains(term, na=False)
            | queue.get("account_id", pd.Series("", index=queue.index)).astype(str).str.lower().str.contains(term, na=False)
        )
        queue = queue[mask]
    queue = _rank_queue(queue)
    safe_limit = max(1, min(int(limit), 500))
    safe_offset = max(0, int(offset))
    total_available = int(len(queue))
    page_df = queue.iloc[safe_offset : safe_offset + safe_limit]
    use_queue_projection = str(response_mode or "").strip().lower() == "queue"
    records = []
    for raw in page_df.to_dict("records"):
        enriched = _apply_scoring_api_fields(raw)
        if use_queue_projection:
            enriched = _project_queue_record(enriched)
        records.append(_sanitize_record(enriched))
    return {
        "alerts": records,
        "run_id": run_id,
        "total": len(records),
        "total_available": total_available,
        "limit": safe_limit,
        "offset": safe_offset,
    }


@router.get("/api/runs")
def list_runs(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {"runs": request.app.state.pipeline_service.list_runs(tenant_id)}


@router.get("/api/alerts/{alert_id}")
def get_alert(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    payload = require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), run_id)
    enriched = _apply_scoring_api_fields(payload)
    enriched.update(_build_detail_sections(enriched))
    explain_service = getattr(request.app.state, "explain_service", None)
    if explain_service is not None and run_id:
        explanation = explain_service.explain_alert(tenant_id=tenant_id, alert_id=alert_id, run_id=run_id) or {}
        human = explanation.get("human_interpretation") or explanation.get("human_explanation") or {}
        if isinstance(human, dict) and human:
            enriched["why_prioritized"] = {
                "summary_text": human.get("summary_text", enriched["why_prioritized"]["summary_text"]),
                "key_risk_drivers": list(human.get("key_risk_drivers") or human.get("key_reasons") or []),
                "aml_patterns": list(human.get("aml_patterns") or []),
                "analyst_next_steps": list(human.get("analyst_next_steps") or human.get("analyst_focus_points") or []),
            }
            enriched["investigation_summary"] = human.get("summary_text", enriched.get("investigation_summary"))
    return _sanitize_record(enriched)


@router.get("/api/alerts/{alert_id}/explain")
def get_alert_explain(alert_id: str, request: Request, run_id: Optional[str] = None, tenant_id: str = Depends(get_authenticated_tenant_id)):
    rid = run_id or _active_run_id(request, tenant_id)
    if not rid:
        raise HTTPException(status_code=404, detail="No active run")
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), rid)
    result = request.app.state.explain_service.explain_alert(tenant_id=tenant_id, alert_id=alert_id, run_id=rid)
    if result is None:
        raise HTTPException(status_code=404, detail="Alert or decision trace not found")
    return result


@router.get("/api/alerts/{alert_id}/ai-summary")
def get_ai_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), _active_run_id(request, tenant_id))
    rec = request.app.state.repository.get_ai_summary(tenant_id=tenant_id, entity_type="alert", entity_id=alert_id)
    if rec:
        return {"summary": rec["summary"], "ts": rec.get("ts", "")}
    return {"summary": None, "ts": None}


@router.post("/api/alerts/{alert_id}/ai-summary")
def generate_ai_summary(alert_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_id = _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), run_id)
    alerts_df = _load_alerts_df(request, tenant_id, run_id or "")
    if "alert_id" not in alerts_df.columns:
        raise HTTPException(status_code=404, detail="Alert not found")
    row = alerts_df[alerts_df["alert_id"].astype(str) == str(alert_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail="Alert not found")
    summary = _generate_alert_summary(row.iloc[0].to_dict())
    current_user = getattr(request.state, "current_user", None) or {}
    actor = str(current_user.get("user_id") or "") or request.app.state.case_service.get_actor(tenant_id, _user_scope(request))
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
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), _active_run_id(request, tenant_id))
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
def internal_ml_predict(
    payload: dict,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    alert_ids = [str(item) for item in (payload.get("alert_ids") or []) if str(item)]
    frame = pd.DataFrame(payload.get("rows", []))
    if not frame.empty:
        feature_bundle = request.app.state.feature_service.generate_inference_features(
            frame,
            context=_build_feature_enrichment_context(request, tenant_id, frame),
        )
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
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), str(alert_id), run_id)
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
        logger.warning("Copilot summary unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")
