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
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.observability import record_narrative_generation, record_narrative_generation_failure
from core.security import get_authenticated_tenant_id
from workflows.alert_workflow_service import apply_alert_assignment_transition

router = APIRouter(prefix="/api", tags=["intelligence"])
logger = logging.getLogger("althea.api.intelligence")


# ── Pydantic request models ──────────────────────────────────────────────────


class OutcomeRequest(BaseModel):
    analyst_decision: str = Field(
        ...,
        description="One of: true_positive, false_positive, escalated, sar_filed, benign_activity",
    )
    decision_reason: Optional[str] = Field(None, description="Free-text reason for the decision")
    analyst_id: Optional[str] = Field(None, description="Analyst user ID")
    model_version: Optional[str] = Field(None, description="Model version active at decision time")
    risk_score_at_decision: Optional[float] = Field(None, description="Risk score at time of decision")


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
    info = request.app.state.pipeline_service.get_run_info(
        tenant_id=tenant_id,
        user_scope=request.headers.get("X-User-Scope") or "public",
    )
    return info.get("run_id")


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
    try:
        result = request.app.state.investigation_summary_service.generate_summary(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
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
    try:
        return request.app.state.risk_explanation_service.generate_explanation(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


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
    try:
        result = request.app.state.relationship_graph_service.build_graph(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
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
    try:
        return request.app.state.guidance_service.generate_steps(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


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
    try:
        result = request.app.state.sar_generator.generate_sar_draft(
            tenant_id=tenant_id,
            alert_id=alert_id,
            run_id=rid,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
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


@router.post("/alerts/{alert_id}/outcome")
def record_alert_outcome(
    alert_id: str,
    body: OutcomeRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    try:
        return request.app.state.feedback_service.record_outcome(
            tenant_id=tenant_id,
            alert_id=alert_id,
            analyst_decision=body.analyst_decision,
            decision_reason=body.decision_reason,
            analyst_id=body.analyst_id,
            model_version=body.model_version,
            risk_score_at_decision=body.risk_score_at_decision,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/alerts/{alert_id}/outcome")
def get_alert_outcome(
    alert_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
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
    signals = request.app.state.global_pattern_service.get_signals_for_alert(
        tenant_id=tenant_id,
        alert_id=alert_id,
        run_id=rid,
        min_tenant_count=min_tenant_count,
    )
    return {"alert_id": alert_id, "global_signals": signals, "total": len(signals)}


# ── Investigation Workflow (assign / escalate / close) ────────────────────────


@router.post("/alerts/{alert_id}/assign")
@router.post("/workflows/alerts/{alert_id}/assign")
def assign_alert(
    alert_id: str,
    body: AssignRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    actor = body.actor or body.assigned_to or "system"
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=tenant_id,
            alert_id=alert_id,
            actor=actor,
            assigned_to=body.assigned_to,
            status="open",
            reason="workflow_assign",
            strict_workflow=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
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


@router.post("/alerts/{alert_id}/escalate")
@router.post("/workflows/alerts/{alert_id}/escalate")
def escalate_alert(
    alert_id: str,
    body: EscalateRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    actor = body.actor or "system"
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=tenant_id,
            alert_id=alert_id,
            actor=actor,
            status="escalated",
            reason=body.reason or "workflow_escalate",
            strict_workflow=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
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


@router.post("/alerts/{alert_id}/close")
@router.post("/workflows/alerts/{alert_id}/close")
def close_alert(
    alert_id: str,
    body: CloseRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    actor = body.actor or "system"
    reason = body.reason or "analyst_closed"
    try:
        result = apply_alert_assignment_transition(
            request=request,
            tenant_id=tenant_id,
            alert_id=alert_id,
            actor=actor,
            status="closed",
            reason=reason,
            strict_workflow=True,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
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
        "assembled_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "assembly_latency_seconds": round(elapsed, 3),
    }
