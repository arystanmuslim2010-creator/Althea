from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from core.observability import record_integration_error, record_workflow_transition
from workflows.state_model import (
    case_state_from_assignment,
    normalize_assignment_status,
    normalize_case_state,
    workflow_state_from_case,
)


def _resolve_run_id(request, tenant_id: str, user_scope: str) -> str:
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id, user_scope)
    return str(run_info.get("run_id") or "")


def _safe_case_sync(
    request,
    tenant_id: str,
    case_id: str,
    case_state: str,
    actor: str,
    user_scope: str,
    run_id: str,
    assigned_to: str,
) -> None:
    try:
        request.app.state.case_service.update_case(
            tenant_id=tenant_id,
            user_scope=user_scope,
            case_id=case_id,
            run_id=run_id,
            actor=actor,
            status=case_state,
            assigned_to=assigned_to,
        )
    except Exception:
        record_integration_error("alert_case_sync")


def _transition_workflow(
    request,
    tenant_id: str,
    case_id: str,
    case_state: str,
    actor: str,
    reason: str,
    strict: bool = False,
) -> dict[str, Any] | None:
    target_state = workflow_state_from_case(case_state)
    if not target_state:
        return None
    try:
        result = request.app.state.workflow_engine.transition_case(
            tenant_id=tenant_id,
            case_id=case_id,
            to_state=target_state,
            actor=actor,
            reason=reason,
        )
        record_workflow_transition(
            from_state=result.get("from_state"),
            to_state=result.get("to_state"),
            result="success",
        )
        return result
    except Exception as exc:
        record_integration_error("alert_workflow_transition")
        if strict:
            raise ValueError(str(exc)) from exc
        return None


def apply_alert_assignment_transition(
    request,
    tenant_id: str,
    alert_id: str,
    actor: str,
    status: str,
    reason: str,
    assigned_to: str | None = None,
    user_scope: str | None = None,
    strict_workflow: bool = False,
) -> dict[str, Any]:
    assignment_status = normalize_assignment_status(status)
    if not assignment_status:
        raise ValueError("Invalid alert status")

    case_state = case_state_from_assignment(assignment_status)
    if not case_state:
        raise ValueError("Could not map alert status to case state")

    repo = request.app.state.repository
    existing = repo.get_latest_assignment(tenant_id, alert_id) or {}
    resolved_assignee = str(assigned_to or existing.get("assigned_to") or actor)
    resolved_user_scope = str(user_scope or actor or "").strip() or "public"
    now = datetime.now(timezone.utc)
    repo.upsert_assignment(
        {
            "id": existing.get("id") or uuid.uuid4().hex,
            "tenant_id": tenant_id,
            "alert_id": alert_id,
            "assigned_to": resolved_assignee,
            "assigned_by": existing.get("assigned_by") or actor,
            "status": assignment_status,
            "created_at": existing.get("created_at") or now,
            "updated_at": now,
        }
    )

    run_id = _resolve_run_id(request, tenant_id=tenant_id, user_scope=resolved_user_scope)
    case_id = request.app.state.workflow_engine.create_case_from_alert(
        tenant_id=tenant_id,
        alert_id=alert_id,
        run_id=run_id,
        actor=actor,
    )
    workflow_result = None
    if case_id:
        workflow_result = _transition_workflow(
            request=request,
            tenant_id=tenant_id,
            case_id=case_id,
            case_state=case_state,
            actor=actor,
            reason=reason,
            strict=strict_workflow,
        )
        _safe_case_sync(
            request=request,
            tenant_id=tenant_id,
            case_id=case_id,
            case_state=case_state,
            actor=actor,
            user_scope=resolved_user_scope,
            run_id=run_id,
            assigned_to=resolved_assignee,
        )
    else:
        record_integration_error("alert_case_creation")

    workflow_state = str((workflow_result or {}).get("to_state") or workflow_state_from_case(case_state) or "")
    canonical_case_status = normalize_case_state(case_state)
    return {
        "alert_id": alert_id,
        "assigned_to": resolved_assignee,
        "status": assignment_status,
        "case_id": case_id,
        "workflow_state": workflow_state or None,
        "workflow_from_state": (workflow_result or {}).get("from_state"),
        "workflow_reason": (workflow_result or {}).get("reason") or reason,
        "case_status": canonical_case_status,
    }
