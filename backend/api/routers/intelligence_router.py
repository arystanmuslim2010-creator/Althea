"""Intelligence Router — investigation intelligence and analyst productivity endpoints.

New endpoints:
  GET  /api/alerts/{id}/investigation-summary
  GET  /api/alerts/{id}/risk-explanation
  GET  /api/alerts/{id}/network-graph
  GET  /api/alerts/{id}/investigation-steps
  GET  /api/alerts/{id}/sar-draft
  GET  /api/alerts/{id}/narrative-draft
  POST /api/alerts/{id}/outcome
  GET  /api/alerts/{id}/global-signals
  POST /api/alerts/{id}/assign
  POST /api/alerts/{id}/escalate
  POST /api/alerts/{id}/close
  GET  /api/alerts/{id}/investigation-context
  GET  /api/investigation/outcomes
  GET  /api/investigation/outcome-stats
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from core.access_control import require_alert_access, require_governance_access
from core.observability import record_narrative_generation, record_narrative_generation_failure
from core.security import get_authenticated_tenant_id, require_permissions
from workflows.alert_workflow_service import apply_alert_assignment_transition

router = APIRouter(prefix="/api", tags=["intelligence"])
logger = logging.getLogger("althea.api.intelligence")


# ── Pydantic request models ──────────────────────────────────────────────────


class OutcomeRequest(BaseModel):
    analyst_decision: str = Field(
        ...,
        description="One of: true_positive, false_positive, escalated, sar_filed, benign_activity, confirmed_suspicious",
    )
    decision_reason: Optional[str] = Field(None, description="Free-text reason for the decision")
    analyst_id: Optional[str] = Field(None, description="Analyst user ID")
    model_version: Optional[str] = Field(None, description="Model version active at decision time")
    risk_score_at_decision: Optional[float] = Field(None, description="Risk score at time of decision")
    # Extended investigation tracking fields
    sar_filed_flag: bool = Field(False, description="Whether a SAR/STR was filed")
    qa_override: bool = Field(False, description="QA reviewed and overrode decision")
    investigation_start_time: Optional[str] = Field(None, description="ISO-8601 timestamp when analyst started the alert")
    investigation_end_time: Optional[str] = Field(None, description="ISO-8601 timestamp when analyst completed the decision")
    touch_count: Optional[int] = Field(None, description="Number of times analyst opened/edited the alert")
    notes_count: Optional[int] = Field(None, description="Number of investigation notes added")
    final_label_status: str = Field("final", description="One of: final, provisional, pending")


class AssignRequest(BaseModel):
    assigned_to: str = Field(..., description="Analyst user ID or name to assign")
    actor: Optional[str] = Field(None, description="User performing the assignment")


class EscalateRequest(BaseModel):
    actor: Optional[str] = Field("system", description="User performing escalation")
    reason: Optional[str] = Field(None, description="Escalation reason")


class CloseRequest(BaseModel):
    actor: Optional[str] = Field("system", description="User closing the case")
    reason: Optional[str] = Field(None, description="Closure reason")


# ── Helpers ──────────────────────────────────────────────────────────────────


def _active_run_id(request: Request, tenant_id: str) -> Optional[str]:
    current_user = getattr(request.state, "current_user", None) or {}
    raw_scope = str(current_user.get("user_id") or "public").strip()
    user_scope = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_scope).strip("._-") or "public"
    info = request.app.state.pipeline_service.get_run_info(
        tenant_id=tenant_id,
        user_scope=user_scope,
    )
    return info.get("run_id")


def _request_user(request: Request, tenant_id: str) -> dict:
    user = getattr(request.state, "current_user", None)
    if isinstance(user, dict) and user:
        return user
    return {
        "user_id": "test-admin",
        "id": "test-admin",
        "role": "admin",
        "roles": ["admin"],
        "permissions": ["view_all_alerts", "manager_approval", "view_model_governance"],
        "tenant_id": tenant_id,
    }


def _safe_get(service_fn):
    """Wrap a service call and return None rather than raise on missing data."""
    try:
        return service_fn()
    except (ValueError, KeyError):
        return None
    except Exception as exc:
        logger.warning("Service error suppressed in unified context: %s", exc)
        return None


# ── Investigation Summary ─────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/investigation-summary")
def get_investigation_summary(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        result = request.app.state.investigation_summary_service.generate_summary(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        logger.warning("Investigation summary unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")
    logger.info(
        "Investigation summary served",
        extra={"alert_id": alert_id, "latency_s": round(time.perf_counter() - t0, 3)},
    )
    return result


# ── Risk Explanation ──────────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/risk-explanation")
def get_risk_explanation(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        return request.app.state.risk_explanation_service.generate_explanation(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        logger.warning("Risk explanation unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")


# ── Relationship Graph ────────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/network-graph")
def get_network_graph(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        result = request.app.state.relationship_graph_service.build_graph(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        logger.warning("Network graph unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")
    logger.info(
        "Network graph served",
        extra={
            "alert_id": alert_id,
            "nodes": result.get("node_count"),
            "edges": result.get("edge_count"),
            "latency_s": round(time.perf_counter() - t0, 3),
        },
    )
    return result


# ── Investigation Steps ───────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/investigation-steps")
def get_investigation_steps(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        return request.app.state.guidance_service.generate_steps(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        logger.warning("Investigation steps unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")


# ── SAR Draft ─────────────────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/sar-draft")
def get_sar_draft(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    t0 = time.perf_counter()
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        result = request.app.state.sar_generator.generate_sar_draft(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        logger.warning("SAR/STR support draft unavailable", extra={"alert_id": alert_id, "error": str(exc)})
        raise HTTPException(status_code=404, detail="Resource not found")
    logger.info(
        "SAR draft served",
        extra={"alert_id": alert_id, "latency_s": round(time.perf_counter() - t0, 3)},
    )
    return result


@router.get("/alerts/{alert_id}/narrative-draft")
def get_narrative_draft(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    started = time.perf_counter()
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    try:
        payload = request.app.state.narrative_service.generate_draft(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
        record_narrative_generation(time.perf_counter() - started)
        logger.info(
            "Narrative draft served",
            extra={"alert_id": alert_id, "latency_s": round(time.perf_counter() - started, 3)},
        )
        return payload
    except Exception:
        record_narrative_generation_failure()
        logger.exception("Narrative draft generation failed", extra={"alert_id": alert_id})
        raise HTTPException(status_code=500, detail="Failed to generate narrative draft")


# ── Analyst Feedback / Outcome ────────────────────────────────────────────────


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


@router.post("/alerts/{alert_id}/outcome")
def record_alert_outcome(
    alert_id: str,
    body: OutcomeRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
) -> dict[str, Any]:
    require_alert_access(request, user["tenant_id"], user, alert_id, _active_run_id(request, user["tenant_id"]))
    try:
        return request.app.state.feedback_service.record_outcome(
            tenant_id=user["tenant_id"],
            alert_id=alert_id,
            analyst_decision=body.analyst_decision,
            decision_reason=body.decision_reason,
            analyst_id=user["user_id"],
            model_version=body.model_version,
            risk_score_at_decision=body.risk_score_at_decision,
            sar_filed_flag=body.sar_filed_flag,
            qa_override=body.qa_override,
            investigation_start_time=_parse_iso(body.investigation_start_time),
            investigation_end_time=_parse_iso(body.investigation_end_time),
            touch_count=body.touch_count,
            notes_count=body.notes_count,
            final_label_status=body.final_label_status,
        )
    except ValueError:
        logger.exception("Outcome recording rejected", extra={"alert_id": alert_id})
        raise HTTPException(status_code=400, detail="Invalid request")


@router.get("/alerts/{alert_id}/outcome")
def get_alert_outcome(
    alert_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, _active_run_id(request, tenant_id))
    result = request.app.state.feedback_service.get_outcome(
        tenant_id=tenant_id, alert_id=alert_id
    )
    if result is None:
        raise HTTPException(status_code=404, detail="No outcome recorded for this alert")
    return result


@router.get("/investigation/outcomes")
def list_outcomes(
    request: Request,
    limit: int = 200,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    outcomes = request.app.state.feedback_service.list_outcomes(tenant_id=tenant_id, limit=limit)
    return {"outcomes": outcomes, "total": len(outcomes)}


@router.get("/investigation/outcome-stats")
def outcome_statistics(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    return request.app.state.feedback_service.get_outcome_statistics(tenant_id=tenant_id)


# ── Global Cross-Tenant Signals ───────────────────────────────────────────────


@router.get("/alerts/{alert_id}/global-signals")
def get_global_signals(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    min_tenant_count: int = 2,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    signals = request.app.state.global_pattern_service.get_signals_for_alert(
        tenant_id=tenant_id,
        alert_id=alert_id,
        run_id=rid,
        min_tenant_count=min_tenant_count,
    )
    return {"alert_id": alert_id, "global_signals": signals, "total": len(signals)}


# ── Investigation Workflow (assign / escalate / close) ────────────────────────


@router.post("/workflows/alerts/{alert_id}/assign")
def assign_alert(
    alert_id: str,
    body: AssignRequest,
    request: Request,
    user: dict = Depends(require_permissions("reassign_alerts")),
) -> dict[str, Any]:
    require_alert_access(request, user["tenant_id"], user, alert_id, _active_run_id(request, user["tenant_id"]))
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=user["tenant_id"],
            alert_id=alert_id,
            actor=user["user_id"],
            user_scope=user["user_id"],
            assigned_to=body.assigned_to,
            status="open",
            reason="workflow_assign",
            strict_workflow=True,
        )
    except ValueError:
        logger.exception("Alert assignment rejected", extra={"alert_id": alert_id})
        raise HTTPException(status_code=400, detail="Invalid request")
    if not result.get("case_id"):
        raise HTTPException(status_code=500, detail="Failed to create or locate case for alert")
    return {
        "alert_id": alert_id,
        "case_id": result.get("case_id"),
        "assigned_to": body.assigned_to,
        "status": "assigned",
        "workflow_state": result.get("workflow_state"),
        "case_status": result.get("case_status"),
    }


@router.post("/workflows/alerts/{alert_id}/escalate")
def escalate_alert(
    alert_id: str,
    body: EscalateRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
) -> dict[str, Any]:
    require_alert_access(request, user["tenant_id"], user, alert_id, _active_run_id(request, user["tenant_id"]))
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=user["tenant_id"],
            alert_id=alert_id,
            actor=user["user_id"],
            user_scope=user["user_id"],
            status="escalated",
            reason=body.reason or "workflow_escalate",
            strict_workflow=True,
        )
    except ValueError:
        logger.exception("Alert escalation rejected", extra={"alert_id": alert_id})
        raise HTTPException(status_code=400, detail="Invalid request")
    if not result.get("case_id"):
        raise HTTPException(status_code=500, detail="No case found for alert")
    return {
        "alert_id": alert_id,
        "case_id": result.get("case_id"),
        "from_state": result.get("workflow_from_state"),
        "to_state": result.get("workflow_state"),
        "reason": result.get("workflow_reason"),
        "status": "escalated",
        "case_status": result.get("case_status"),
    }


@router.post("/workflows/alerts/{alert_id}/close")
def close_alert(
    alert_id: str,
    body: CloseRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
) -> dict[str, Any]:
    require_alert_access(request, user["tenant_id"], user, alert_id, _active_run_id(request, user["tenant_id"]))
    reason = body.reason or "analyst_closed"
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=user["tenant_id"],
            alert_id=alert_id,
            actor=user["user_id"],
            user_scope=user["user_id"],
            status="closed",
            reason=reason,
            strict_workflow=True,
        )
    except ValueError:
        logger.exception("Alert close rejected", extra={"alert_id": alert_id})
        raise HTTPException(status_code=400, detail="Invalid request")
    if not result.get("case_id"):
        raise HTTPException(status_code=500, detail="No case found for alert")
    return {
        "alert_id": alert_id,
        "case_id": result.get("case_id"),
        "from_state": result.get("workflow_from_state"),
        "to_state": result.get("workflow_state"),
        "reason": result.get("workflow_reason"),
        "status": "closed",
        "case_status": result.get("case_status"),
    }


# ── Similar Cases (nearest-neighbor retrieval) ───────────────────────────────


@router.get("/alerts/{alert_id}/similar-cases")
def get_similar_cases(
    alert_id: str,
    request: Request,
    top_k: int = Query(default=5, ge=1, le=20),
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    """Return the most similar historical cases using feature-based cosine retrieval."""
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)
    retrieval_service = getattr(request.app.state, "retrieval_service", None)
    if retrieval_service is None:
        return {"alert_id": alert_id, "similar_cases": [], "retrieval_available": False}

    # Fetch the alert payload to build the query vector
    payload: dict[str, Any] = {}
    if rid:
        try:
            payload = request.app.state.repository.get_alert_payload(
                tenant_id=tenant_id, alert_id=str(alert_id), run_id=rid
            ) or {}
        except Exception:
            pass

    try:
        result = retrieval_service.retrieve_similar_cases(
            tenant_id=tenant_id,
            alert_payload={"alert_id": alert_id, **payload},
            top_k=top_k,
        )
    except Exception:
        logger.exception("Similar-case retrieval failed", extra={"alert_id": alert_id})
        return {"alert_id": alert_id, "similar_cases": [], "error": "Similar-case retrieval unavailable"}

    return {
        "alert_id": alert_id,
        "similar_cases": result,
        "retrieval_available": True,
    }


# ── Decision Audit ────────────────────────────────────────────────────────────


@router.get("/alerts/{alert_id}/decision-audit")
def get_alert_decision_audit(
    alert_id: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    """Return the immutable decision audit trail for a single alert."""
    from sqlalchemy import text
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, _active_run_id(request, tenant_id))
    require_governance_access(_request_user(request, tenant_id), write=False)

    try:
        with request.app.state.repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, alert_id, run_id, model_version,
                           priority_score, escalation_prob, graph_risk_score,
                           similar_suspicious_strength, p50_hours, p90_hours,
                           governance_status, queue_action, priority_bucket,
                           compliance_flags_json, signals_json, decided_at
                    FROM decision_audit
                    WHERE tenant_id = :tenant_id AND alert_id = :alert_id
                    ORDER BY decided_at DESC
                    LIMIT :limit
                    """
                ),
                {"tenant_id": tenant_id, "alert_id": str(alert_id), "limit": limit},
            ).fetchall()
    except Exception as exc:
        logger.warning("decision_audit query failed: %s", exc)
        return {"alert_id": alert_id, "audit_records": [], "error": "decision_audit table unavailable"}

    records = [
        {
            "id": str(r[0]),
            "alert_id": str(r[1]),
            "run_id": r[2],
            "model_version": r[3],
            "priority_score": r[4],
            "escalation_prob": r[5],
            "graph_risk_score": r[6],
            "similar_suspicious_strength": r[7],
            "p50_hours": r[8],
            "p90_hours": r[9],
            "governance_status": r[10],
            "queue_action": r[11],
            "priority_bucket": r[12],
            "compliance_flags": r[13],
            "signals": r[14],
            "decided_at": str(r[15]) if r[15] else None,
        }
        for r in rows
    ]
    return {"alert_id": alert_id, "audit_records": records, "total": len(records)}


@router.get("/ml/decision-audit")
def list_decision_audit(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    queue_action: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    """Paginated decision audit log for compliance and model review."""
    from sqlalchemy import text
    require_governance_access(_request_user(request, tenant_id), write=False)

    filters = "WHERE tenant_id = :tenant_id"
    params: dict[str, Any] = {"tenant_id": tenant_id, "limit": limit, "offset": offset}
    if queue_action:
        filters += " AND queue_action = :queue_action"
        params["queue_action"] = queue_action

    try:
        with request.app.state.repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT id, alert_id, run_id, model_version,
                           priority_score, escalation_prob, governance_status,
                           queue_action, priority_bucket, decided_at
                    FROM decision_audit
                    {filters}
                    ORDER BY decided_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                params,
            ).fetchall()
    except Exception as exc:
        logger.warning("decision_audit list query failed: %s", exc)
        return {"audit_records": [], "error": "decision_audit table unavailable"}

    records = [
        {
            "id": str(r[0]),
            "alert_id": str(r[1]),
            "run_id": r[2],
            "model_version": r[3],
            "priority_score": r[4],
            "escalation_prob": r[5],
            "governance_status": r[6],
            "queue_action": r[7],
            "priority_bucket": r[8],
            "decided_at": str(r[9]) if r[9] else None,
        }
        for r in rows
    ]
    return {"audit_records": records, "total": len(records), "limit": limit, "offset": offset}


# ── Unified Investigation Context ─────────────────────────────────────────────


@router.get("/alerts/{alert_id}/investigation-context")
def get_investigation_context(
    alert_id: str,
    request: Request,
    run_id: Optional[str] = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    """Single endpoint returning the full investigation context for one alert.

    Powers the unified analyst investigation screen.
    """
    t0 = time.perf_counter()
    rid = run_id or _active_run_id(request, tenant_id)
    require_alert_access(request, tenant_id, _request_user(request, tenant_id), alert_id, rid)

    summary = _safe_get(
        lambda: request.app.state.investigation_summary_service.generate_summary(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    risk_explanation = _safe_get(
        lambda: request.app.state.risk_explanation_service.generate_explanation(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    network_graph = _safe_get(
        lambda: request.app.state.relationship_graph_service.build_graph(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    investigation_steps = _safe_get(
        lambda: request.app.state.guidance_service.generate_steps(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    sar_draft = _safe_get(
        lambda: request.app.state.sar_generator.generate_sar_draft(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    narrative_draft = _safe_get(
        lambda: request.app.state.narrative_service.generate_draft(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    global_signals = _safe_get(
        lambda: request.app.state.global_pattern_service.get_signals_for_alert(
            tenant_id=tenant_id, alert_id=alert_id, run_id=rid
        )
    )
    outcome = _safe_get(
        lambda: request.app.state.feedback_service.get_outcome(
            tenant_id=tenant_id, alert_id=alert_id
        )
    )

    model_metadata = {}
    try:
        payloads = request.app.state.repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=rid or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})
        model_version = str(
            (risk_explanation or {}).get("model_version")
            or payload.get("model_version")
            or "unknown"
        )
        monitoring = request.app.state.repository.list_model_monitoring(tenant_id=tenant_id, limit=200)
        latest_monitoring = next(
            (
                row for row in monitoring
                if str(row.get("run_id") or "") == str(rid or "")
                and str(row.get("model_version") or "") == model_version
            ),
            None,
        )
        model_record = request.app.state.repository.get_model_version(tenant_id=tenant_id, model_version=model_version)
        model_metadata = {
            "model_version": model_version,
            "approval_state": (model_record or {}).get("approval_status"),
            "scoring_timestamp": payload.get("timestamp") or payload.get("created_at"),
            "monitoring_timestamp": (latest_monitoring or {}).get("created_at"),
            "explanation_version": "v1",
        }
    except Exception:
        model_metadata = {}

    # Case status from existing case service
    case_status = None
    try:
        cases = request.app.state.repository.list_cases(tenant_id)
        for case in cases:
            if str(case.get("alert_id") or "") == str(alert_id):
                case_status = {
                    "case_id": case.get("case_id"),
                    "status": case.get("status"),
                    "assigned_to": case.get("assigned_to"),
                }
                break
    except Exception:
        pass

    analyst_enrichment = _safe_get(
        lambda: request.app.state.analyst_workspace_enrichment_service.generate_sections(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
            network_graph=network_graph,
            case_status=case_status,
        )
    ) or {}

    elapsed = time.perf_counter() - t0
    logger.info(
        "Investigation context assembled",
        extra={"alert_id": alert_id, "latency_s": round(elapsed, 3)},
    )

    return {
        "alert_id": alert_id,
        "investigation_summary": summary,
        "risk_explanation": risk_explanation,
        "network_graph": network_graph,
        "investigation_steps": investigation_steps,
        "sar_draft": sar_draft,
        "narrative_draft": narrative_draft,
        "global_signals": global_signals or [],
        "outcome": outcome,
        "case_status": case_status,
        "model_metadata": model_metadata,
        "customer_profile": analyst_enrichment.get("customer_profile"),
        "account_profile": analyst_enrichment.get("account_profile"),
        "behavior_baseline": analyst_enrichment.get("behavior_baseline"),
        "counterparty_summary": analyst_enrichment.get("counterparty_summary"),
        "geography_payment_summary": analyst_enrichment.get("geography_payment_summary"),
        "screening_summary": analyst_enrichment.get("screening_summary"),
        "data_availability": analyst_enrichment.get("data_availability"),
        "assembled_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "assembly_latency_seconds": round(elapsed, 3),
    }
