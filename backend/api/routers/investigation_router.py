from __future__ import annotations

import csv
import io
import json
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from core.observability import record_integration_error, record_workflow_transition
from core.security import VALID_ROLES, get_authenticated_tenant_id, get_current_user, normalize_role, require_any_permission, require_permissions
from workflows.alert_workflow_service import apply_alert_assignment_transition
from workflows.state_model import ALLOWED_CASE_TRANSITIONS, normalize_assignment_status, normalize_case_state, workflow_state_from_case

router = APIRouter(prefix="/api", tags=["investigation"])

CASE_STATUSES = set(ALLOWED_CASE_TRANSITIONS.keys())


def _user_scope(request: Request, user: dict | None = None) -> str:
    return (user or {}).get("user_id") or "public"


def _load_alerts_df(request: Request, tenant_id: str, run_id: str):
    payloads = request.app.state.repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
    import pandas as pd

    return pd.DataFrame(payloads)


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _to_hours(delta_seconds: float | int | None) -> float | None:
    if delta_seconds is None:
        return None
    try:
        return round(float(delta_seconds) / 3600.0, 2)
    except Exception:
        return None


def _first_numeric(value):
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _log_event(
    request: Request,
    tenant_id: str,
    action: str,
    performed_by: str,
    alert_id: str | None = None,
    case_id: str | None = None,
    details: dict | None = None,
) -> None:
    entry = request.app.state.repository.append_investigation_log(
        {
            "id": uuid.uuid4().hex,
            "tenant_id": tenant_id,
            "case_id": case_id,
            "alert_id": alert_id,
            "action": action,
            "performed_by": performed_by,
            "details_json": details or {},
            "timestamp": datetime.now(timezone.utc),
        }
    )
    request.app.state.event_bus.publish(
        event_name=action,
        tenant_id=tenant_id,
        payload={
            "investigation_log_id": entry.get("id"),
            "case_id": case_id,
            "alert_id": alert_id,
            "performed_by": performed_by,
            "details": details or {},
        },
        correlation_id=getattr(request.state, "request_id", None),
        version="2.0",
    )


def _has_elevated_case_access(user: dict) -> bool:
    permissions = set(user.get("permissions") or [])
    role = normalize_role(user.get("role"))
    return role == "admin" or bool({"view_all_alerts", "view_team_queue", "manager_approval"} & permissions)


def _can_access_case(user: dict, case: dict | None) -> bool:
    if not case:
        return False
    if _has_elevated_case_access(user):
        return True
    user_id = str(user.get("user_id") or "").strip()
    if not user_id:
        return False
    payload = case.get("payload_json")
    payload = payload if isinstance(payload, dict) else {}
    owners = {
        str(case.get("assigned_to") or "").strip(),
        str(case.get("created_by") or "").strip(),
        str(payload.get("assigned_to") or "").strip(),
        str(payload.get("owner") or "").strip(),
        str(payload.get("created_by") or "").strip(),
    }
    return user_id in owners


def _require_case_access(user: dict, case: dict | None) -> dict:
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")
    if not _can_access_case(user, case):
        raise HTTPException(status_code=403, detail="Forbidden")
    return case


def _list_visible_cases(request: Request, user: dict) -> dict[str, dict]:
    records = request.app.state.repository.list_cases(user["tenant_id"])
    visible: dict[str, dict] = {}
    for case in records:
        if _can_access_case(user, case):
            payload = _serialize_case(case)
            visible[str(case.get("case_id"))] = payload
    return visible


def _serialize_case(case: dict | None) -> dict:
    if not case:
        return {}
    payload = dict(case.get("payload_json") or {})
    canonical_status = normalize_case_state(payload.get("status") or case.get("status")) or "open"
    alert_ids = payload.get("alert_ids") if isinstance(payload.get("alert_ids"), list) else []
    alert_id = case.get("alert_id") or (alert_ids[0] if alert_ids else None)
    legacy_status = payload.get("status") or case.get("status")
    return {
        **payload,
        "case_id": case.get("case_id") or payload.get("case_id"),
        "alert_id": alert_id,
        "alert_ids": alert_ids or ([alert_id] if alert_id else []),
        "assigned_to": payload.get("assigned_to") or case.get("assigned_to"),
        "created_by": payload.get("created_by") or case.get("created_by"),
        "created_at": payload.get("created_at") or case.get("created_at"),
        "updated_at": payload.get("updated_at") or case.get("updated_at"),
        "status": canonical_status,
        "case_status": canonical_status,
        "workflow_state": workflow_state_from_case(canonical_status),
        "legacy_status": legacy_status,
    }


def _create_case_record(request: Request, user: dict, alert_ids: list[str]) -> dict:
    clean_alert_ids = [str(item).strip() for item in (alert_ids or []) if str(item).strip()]
    if not clean_alert_ids:
        raise HTTPException(status_code=400, detail="alert_ids is required")
    run_info = request.app.state.pipeline_service.get_run_info(user["tenant_id"], _user_scope(request, user))
    if not run_info.get("run_id"):
        raise HTTPException(status_code=400, detail="No data loaded")
    case = request.app.state.case_service.create_case(
        tenant_id=user["tenant_id"],
        user_scope=_user_scope(request, user),
        alert_ids=clean_alert_ids,
        run_id=run_info["run_id"],
        actor=user["user_id"],
    )
    try:
        request.app.state.workflow_engine.transition_case(
            tenant_id=user["tenant_id"],
            case_id=case["case_id"],
            to_state="assigned",
            actor=user["user_id"],
            reason="manual_case_creation",
        )
    except Exception:
        pass
    _log_event(
        request,
        user["tenant_id"],
        "case_created",
        user["user_id"],
        alert_id=clean_alert_ids[0],
        case_id=case.get("case_id"),
        details={"alert_ids": clean_alert_ids, "run_id": run_info["run_id"]},
    )
    case_record = request.app.state.repository.get_case(user["tenant_id"], case["case_id"])
    if case_record:
        return _serialize_case(case_record)
    return _serialize_case(
        {
            "case_id": case.get("case_id"),
            "tenant_id": user["tenant_id"],
            "status": case.get("status"),
            "created_by": user["user_id"],
            "assigned_to": case.get("assigned_to") or user["user_id"],
            "alert_id": clean_alert_ids[0],
            "payload_json": case,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )


class AssignAlertRequest(BaseModel):
    assigned_to: str


class AlertStatusRequest(BaseModel):
    status: str


class BulkAssignRequest(BaseModel):
    alert_ids: list[str]
    assigned_to: str


class BulkStatusRequest(BaseModel):
    alert_ids: list[str]
    status: str


class AddNoteRequest(BaseModel):
    note_text: str = Field(min_length=1)


class CreateInvestigationCaseRequest(BaseModel):
    alert_id: str


class UpdateCaseStatusRequest(BaseModel):
    status: str


class UpdateUserRoleRequest(BaseModel):
    role: str


class UpdateUserStatusRequest(BaseModel):
    is_active: bool


class CreateCaseRequest(BaseModel):
    alert_ids: list[str]
    actor: str = "Analyst_1"


class SetActorRequest(BaseModel):
    actor: str


class UpdateCaseRequest(BaseModel):
    status: str | None = None
    assigned_to: str | None = None
    notes: str | None = None


class WorkflowStateTransitionRequest(BaseModel):
    to_state: str
    reason: str = "manual_transition"


def _serialize_export_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, default=str)
    return str(value)


def _build_log_export(rows: list[dict], *, log_type: str, export_format: str) -> tuple[str, str]:
    normalized_rows = [{"log_type": log_type, **dict(row or {})} for row in rows]
    if export_format == "jsonl":
        body = "\n".join(json.dumps(item, ensure_ascii=True, default=str) for item in normalized_rows)
        if body:
            body += "\n"
        return body, "application/x-ndjson"

    fieldnames = sorted({key for row in normalized_rows for key in row.keys()})
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in normalized_rows:
        writer.writerow({key: _serialize_export_value(row.get(key)) for key in fieldnames})
    return buffer.getvalue(), "text/csv; charset=utf-8"


@router.get("/alerts/{alert_id}/time-estimate")
def get_time_estimate(
    alert_id: str,
    request: Request,
    run_id: str | None = None,
    user: dict = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
) -> dict:
    """Return ML-estimated investigation time (p50, p90) for a single alert.

    Delegates to InvestigationTimeService if wired; falls back to cached
    priority_score data stored in the alert payload.
    """
    tenant_id: str = user["tenant_id"]

    # Resolve run_id
    rid = run_id
    if not rid:
        info = request.app.state.pipeline_service.get_run_info(tenant_id, _user_scope(request, user))
        rid = info.get("run_id")

    payload: dict = {}
    if rid:
        try:
            payload = request.app.state.repository.get_alert_payload(
                tenant_id=tenant_id, alert_id=str(alert_id), run_id=rid
            ) or {}
        except Exception:
            pass

    time_service = getattr(request.app.state, "investigation_time_service", None)
    if time_service is not None:
        import pandas as pd

        try:
            feature_frame = pd.DataFrame([payload]) if payload else pd.DataFrame()
            result = time_service.predict(tenant_id=tenant_id, feature_frame=feature_frame)
            return {
                "alert_id": alert_id,
                "p50_hours": _first_numeric(result.get("p50_hours")),
                "p90_hours": _first_numeric(result.get("p90_hours")),
                "model_version": result.get("model_version"),
                "source": "time_model",
            }
        except Exception as exc:
            import logging as _logging
            _logging.getLogger("althea.api.investigation").warning(
                "Time estimate model failed for alert %s: %s", alert_id, exc
            )

    # Fallback: return stored p50/p90 values from scored payload if present
    p50 = payload.get("p50_hours")
    p90 = payload.get("p90_hours")
    if p50 is not None or p90 is not None:
        return {
            "alert_id": alert_id,
            "p50_hours": float(p50) if p50 is not None else None,
            "p90_hours": float(p90) if p90 is not None else None,
            "model_version": payload.get("time_model_version"),
            "source": "cached_payload",
        }

    return {
        "alert_id": alert_id,
        "p50_hours": None,
        "p90_hours": None,
        "model_version": None,
        "source": "unavailable",
    }


@router.get("/work/queue")
def get_work_queue(
    request: Request,
    queue_view: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    user: dict = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
):
    # Resolve active run with compatibility fallbacks because different screens may persist
    # runtime context under different scopes ("public" vs user-specific).
    scope_candidates: list[str] = [str(user.get("user_id") or "").strip(), "public"]
    deduped_scopes: list[str] = []
    for scope in scope_candidates:
        if scope and scope not in deduped_scopes:
            deduped_scopes.append(scope)

    run_info = {}
    run_id = None
    for scope in deduped_scopes:
        info = request.app.state.pipeline_service.get_run_info(user["tenant_id"], scope)
        if info.get("run_id"):
            run_info = info
            run_id = info.get("run_id")
            break
    if not run_id:
        return {"queue": [], "count": 0}
    alerts_df = _load_alerts_df(request, user["tenant_id"], run_id)
    if alerts_df.empty:
        return {"queue": [], "count": 0}
    repo = request.app.state.repository
    cases = repo.list_cases(user["tenant_id"])
    records = []
    now = datetime.now(timezone.utc)
    normalized_view = str(queue_view or "").lower().strip()
    for record in alerts_df.to_dict("records"):
        alert_id = str(record.get("alert_id", ""))
        assignment = repo.get_latest_assignment(user["tenant_id"], alert_id)
        case_id = next((case["case_id"] for case in cases if case.get("alert_id") == alert_id), None)
        case_status = next((case.get("status") for case in cases if case.get("alert_id") == alert_id), None)

        created_at = (
            _parse_utc(record.get("created_at"))
            or _parse_utc(record.get("timestamp"))
            or _parse_utc(record.get("event_time"))
        )
        assignment_updated_at = _parse_utc((assignment or {}).get("updated_at"))
        alert_age_hours = _to_hours((now - created_at).total_seconds()) if created_at else None
        assignment_age_hours = _to_hours((now - assignment_updated_at).total_seconds()) if assignment_updated_at else None
        overdue_review = bool(alert_age_hours is not None and alert_age_hours >= 24.0 and (assignment or {}).get("status") != "closed")

        if normalized_view == "analyst" and (assignment or {}).get("status") == "escalated":
            continue
        if normalized_view == "manager" and user["role"] not in {"manager", "admin"}:
            continue
        if normalized_view == "compliance" and user["role"] not in {"manager", "admin"}:
            continue
        if normalized_view == "compliance" and str(record.get("typology") or "").lower() != "sanctions":
            continue

        if user["role"] in {"manager", "admin"}:
            pass
        elif user["role"] == "investigator":
            pass
        elif assignment and assignment.get("assigned_to") not in {None, "", user["user_id"]}:
            continue
        records.append(
            {
                "alert_id": alert_id,
                "priority": record.get("priority"),
                "risk_score": record.get("risk_score"),
                "assigned_to": assignment.get("assigned_to") if assignment else None,
                "status": assignment.get("status") if assignment else "open",
                "case_id": case_id,
                "case_status": case_status,
                "created_at": created_at.isoformat() if created_at else None,
                "alert_age_hours": alert_age_hours,
                "assignment_age_hours": assignment_age_hours,
                "overdue_review": overdue_review,
            }
        )
    safe_offset = max(0, int(offset or 0))
    if limit is None:
        page_records = records[safe_offset:]
        page_limit = None
    else:
        page_limit = max(1, min(int(limit), 500))
        page_records = records[safe_offset : safe_offset + page_limit]

    return {
        "queue": page_records,
        "count": len(page_records),
        "total_available": len(records),
        "limit": page_limit,
        "offset": safe_offset,
    }


@router.post("/alerts/{alert_id}/assign")
def assign_alert(
    alert_id: str,
    payload: AssignAlertRequest,
    request: Request,
    user: dict = Depends(require_permissions("reassign_alerts")),
):
    repo = request.app.state.repository
    existing = repo.get_latest_assignment(user["tenant_id"], alert_id) or {}
    previous_assignee = existing.get("assigned_to")
    result = apply_alert_assignment_transition(
        request=request,
        tenant_id=user["tenant_id"],
        alert_id=alert_id,
        actor=user["user_id"],
        user_scope=_user_scope(request, user),
        status="open",
        reason="assignment_updated",
        assigned_to=payload.assigned_to,
    )
    _log_event(
        request,
        user["tenant_id"],
        "alert_assigned",
        user["user_id"],
        alert_id=alert_id,
        case_id=result.get("case_id"),
        details={"assigned_to": payload.assigned_to, "previous_assigned_to": previous_assignee},
    )
    return {
        "status": "assigned",
        "alert_id": alert_id,
        "assigned_to": payload.assigned_to,
        "case_id": result.get("case_id"),
        "workflow_state": result.get("workflow_state"),
        "case_status": result.get("case_status"),
    }


@router.post("/alerts/{alert_id}/status")
def update_alert_status(
    alert_id: str,
    payload: AlertStatusRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
):
    status = normalize_assignment_status(payload.status)
    if not status:
        raise HTTPException(status_code=400, detail="Invalid status")
    repo = request.app.state.repository
    existing = repo.get_latest_assignment(user["tenant_id"], alert_id) or {}
    old_status = existing.get("status") or "open"
    result = apply_alert_assignment_transition(
        request=request,
        tenant_id=user["tenant_id"],
        alert_id=alert_id,
        actor=user["user_id"],
        user_scope=_user_scope(request, user),
        status=status,
        reason="alert_status_updated",
        assigned_to=existing.get("assigned_to") or user["user_id"],
    )
    _log_event(
        request,
        user["tenant_id"],
        "status_changed",
        user["user_id"],
        alert_id=alert_id,
        case_id=result.get("case_id"),
        details={"old_state": old_status, "new_state": status, "reason": "alert_status_updated"},
    )
    return {
        "alert_id": alert_id,
        "status": status,
        "workflow_state": result.get("workflow_state"),
        "case_id": result.get("case_id"),
        "case_status": result.get("case_status"),
    }


@router.post("/alerts/bulk-assign")
def bulk_assign_alerts(
    payload: BulkAssignRequest,
    request: Request,
    user: dict = Depends(require_permissions("reassign_alerts")),
):
    alert_ids = [str(item).strip() for item in (payload.alert_ids or []) if str(item).strip()]
    if not alert_ids:
        raise HTTPException(status_code=400, detail="alert_ids is required")
    updated = 0
    for alert_id in alert_ids:
        assign_alert(
            alert_id=alert_id,
            payload=AssignAlertRequest(assigned_to=payload.assigned_to),
            request=request,
            user=user,
        )
        updated += 1
    return {"status": "ok", "updated": updated, "assigned_to": payload.assigned_to}


@router.post("/alerts/bulk-status")
def bulk_update_alert_status(
    payload: BulkStatusRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
):
    alert_ids = [str(item).strip() for item in (payload.alert_ids or []) if str(item).strip()]
    if not alert_ids:
        raise HTTPException(status_code=400, detail="alert_ids is required")
    status = normalize_assignment_status(payload.status)
    if not status:
        raise HTTPException(status_code=400, detail="Invalid status")
    updated = 0
    for alert_id in alert_ids:
        update_alert_status(
            alert_id=alert_id,
            payload=AlertStatusRequest(status=status),
            request=request,
            user=user,
        )
        updated += 1
    return {"status": "ok", "updated": updated, "new_state": status}


@router.post("/alerts/{alert_id}/note")
def add_alert_note(
    alert_id: str,
    payload: AddNoteRequest,
    request: Request,
    user: dict = Depends(require_permissions("add_investigation_notes")),
):
    note = request.app.state.repository.create_alert_note(
        {
            "id": uuid.uuid4().hex,
            "tenant_id": user["tenant_id"],
            "alert_id": alert_id,
            "user_id": user["user_id"],
            "note_text": payload.note_text,
            "created_at": datetime.now(timezone.utc),
        }
    )
    _log_event(request, user["tenant_id"], "note_added", user["user_id"], alert_id=alert_id, details={"note_id": note["id"]})
    return {"status": "created", "note": note}


@router.get("/alerts/{alert_id}/notes")
def get_alert_notes(
    alert_id: str,
    request: Request,
    user: dict = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
):
    return {"alert_id": alert_id, "notes": request.app.state.repository.list_alert_notes(user["tenant_id"], alert_id)}


@router.post("/cases/create")
def create_investigation_case(
    payload: CreateInvestigationCaseRequest,
    request: Request,
    user: dict = Depends(require_permissions("work_cases")),
):
    repo = request.app.state.repository
    existing = [
        case
        for case in repo.list_cases(user["tenant_id"])
        if case.get("alert_id") == payload.alert_id and str(case.get("status") or "").lower() != "closed"
    ]
    if existing:
        raise HTTPException(status_code=409, detail="Case already exists for this alert")
    case = _create_case_record(request, user, [payload.alert_id])
    return {
        "case_id": case["case_id"],
        "status": case["status"],
        "case_status": case["case_status"],
        "workflow_state": case["workflow_state"],
    }


@router.get("/cases/{case_id}")
def get_case(
    case_id: str,
    request: Request,
    user: dict = Depends(require_any_permission("work_cases", "view_all_alerts", "view_team_queue")),
):
    repo = request.app.state.repository
    case = _require_case_access(user, repo.get_case(user["tenant_id"], case_id))
    notes = repo.list_alert_notes(user["tenant_id"], case.get("alert_id") or "")
    logs = repo.list_investigation_logs(user["tenant_id"], case_id=case_id, limit=200)
    return {"case": _serialize_case(case), "notes": notes, "timeline": logs}


@router.post("/cases/{case_id}/status")
def update_investigation_case_status(
    case_id: str,
    payload: UpdateCaseStatusRequest,
    request: Request,
    user: dict = Depends(require_permissions("work_cases")),
):
    new_status = normalize_case_state(payload.status)
    if not new_status or new_status not in CASE_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid case status")
    if new_status == "sar_filed" and user["role"] not in {"manager", "admin"}:
        raise HTTPException(status_code=403, detail="Only manager/admin can approve SAR cases")
    repo = request.app.state.repository
    case = _require_case_access(user, repo.get_case(user["tenant_id"], case_id))
    old_status = str(case.get("status") or "").lower().strip() or "open"
    run_info = request.app.state.pipeline_service.get_run_info(user["tenant_id"], _user_scope(request, user))
    actor = user["user_id"]
    ok, message, updated_case = request.app.state.case_service.update_case(
        tenant_id=user["tenant_id"],
        user_scope=_user_scope(request, user),
        case_id=case_id,
        run_id=run_info.get("run_id") or "",
        actor=actor,
        status=new_status,
    )
    if not ok or not updated_case:
        raise HTTPException(status_code=400, detail=message)

    workflow_target = workflow_state_from_case(new_status)
    workflow_result = None
    if workflow_target:
        try:
            workflow_result = request.app.state.workflow_engine.transition_case(
                tenant_id=user["tenant_id"],
                case_id=case_id,
                to_state=workflow_target,
                actor=actor,
                reason="case_status_api",
            )
        except Exception:
            workflow_result = None

    _log_event(
        request,
        user["tenant_id"],
        "case_closed" if new_status == "closed" else "status_changed",
        actor,
        alert_id=case.get("alert_id"),
        case_id=case_id,
        details={
            "old_state": old_status,
            "new_state": new_status,
            "workflow_state": (workflow_result or {}).get("to_state"),
            "reason": "case_status_api",
        },
    )
    if workflow_result:
        record_workflow_transition(
            from_state=workflow_result.get("from_state"),
            to_state=workflow_result.get("to_state"),
            result="success",
        )
    else:
        record_integration_error("case_status_workflow_sync")
    return {
        "case_id": case_id,
        "status": new_status,
        "closed_at": updated_case.get("closed_at"),
        "workflow_state": (workflow_result or {}).get("to_state"),
    }


@router.get("/admin/users")
def admin_list_users(request: Request, user: dict = Depends(require_permissions("manage_users"))):
    return {"users": request.app.state.repository.list_users(user["tenant_id"])}


@router.post("/admin/users/{user_id}/role")
def admin_update_role(
    user_id: str,
    payload: UpdateUserRoleRequest,
    request: Request,
    user: dict = Depends(require_permissions("manage_roles")),
):
    role = normalize_role(payload.role)
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    updated = request.app.state.repository.update_user_role(
        user["tenant_id"],
        user_id,
        role,
        actor_id=user["user_id"],
    )
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    _log_event(request, user["tenant_id"], "status_changed", user["user_id"], details={"target_user_id": user_id, "new_role": role})
    return {"user_id": user_id, "role": role}


@router.post("/admin/users/{user_id}/status")
def admin_update_user_status(
    user_id: str,
    payload: UpdateUserStatusRequest,
    request: Request,
    user: dict = Depends(require_permissions("manage_users")),
):
    updated = request.app.state.repository.set_user_active(
        tenant_id=user["tenant_id"],
        user_id=user_id,
        is_active=payload.is_active,
        actor_id=user["user_id"],
    )
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user_id": user_id, "is_active": bool(updated.get("is_active"))}


@router.get("/admin/logs")
def admin_logs(request: Request, user: dict = Depends(require_permissions("view_system_logs"))):
    return {
        "logs": request.app.state.repository.list_investigation_logs(user["tenant_id"], limit=300),
        "auth_audit_logs": request.app.state.repository.list_auth_audit_logs(user["tenant_id"], limit=300),
    }


@router.get("/admin/logs/export")
def admin_logs_export(
    request: Request,
    stream: str = Query(default="all", pattern="^(all|investigation|auth)$"),
    format: str = Query(default="jsonl", pattern="^(jsonl|csv)$"),
    limit: int = Query(default=1000, ge=1, le=5000),
    user: dict = Depends(require_permissions("view_system_logs")),
):
    repo = request.app.state.repository
    payloads: list[tuple[str, list[dict]]] = []
    if stream in {"all", "investigation"}:
        payloads.append(("investigation", repo.list_investigation_logs(user["tenant_id"], limit=limit)))
    if stream in {"all", "auth"}:
        payloads.append(("auth", repo.list_auth_audit_logs(user["tenant_id"], limit=limit)))

    if format == "jsonl":
        body = "".join(_build_log_export(rows, log_type=log_type, export_format="jsonl")[0] for log_type, rows in payloads)
        media_type = "application/x-ndjson"
    else:
        merged_rows: list[dict] = []
        for log_type, rows in payloads:
            merged_rows.extend({"log_type": log_type, **dict(row or {})} for row in rows)
        fieldnames = sorted({key for row in merged_rows for key in row.keys()})
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged_rows:
            writer.writerow({key: _serialize_export_value(row.get(key)) for key in fieldnames})
        body = buffer.getvalue()
        media_type = "text/csv; charset=utf-8"

    response = Response(content=body, media_type=media_type)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Content-Disposition"] = f'attachment; filename="althea-{stream}-audit.{format}"'
    return response


@router.get("/cases")
def get_cases(
    request: Request,
    user: dict = Depends(require_any_permission("work_cases", "view_all_alerts", "view_team_queue")),
):
    return {"cases": _list_visible_cases(request, user)}


@router.post("/cases")
def create_case(
    request: Request,
    payload: CreateCaseRequest,
    user: dict = Depends(require_permissions("work_cases")),
):
    case = _create_case_record(request, user, payload.alert_ids)
    return {
        "case_id": case["case_id"],
        "status": case["status"],
        "case_status": case["case_status"],
        "workflow_state": case["workflow_state"],
    }


@router.get("/actor")
def get_actor(request: Request, user: dict = Depends(get_current_user)):
    # Identity source of truth is authenticated user, not mutable client actor aliases.
    return {"actor": str(user.get("user_id") or "")}


@router.put("/actor")
def set_actor(request: Request, payload: SetActorRequest, user: dict = Depends(get_current_user)):
    requested_actor = str(payload.actor or "").strip()
    current_user_id = str(user.get("user_id") or "").strip()
    is_admin = normalize_role(user.get("role")) == "admin"
    if not requested_actor:
        raise HTTPException(status_code=400, detail="actor is required")
    if not is_admin and requested_actor != current_user_id:
        raise HTTPException(status_code=403, detail="Only admins can set actor aliases for other users")
    resolved_actor = requested_actor if is_admin else current_user_id
    request.app.state.case_service.set_actor(user["tenant_id"], _user_scope(request, user), resolved_actor)
    return {"actor": resolved_actor}


@router.put("/cases/{case_id}")
def update_case(
    case_id: str,
    request: Request,
    payload: UpdateCaseRequest,
    user: dict = Depends(require_permissions("work_cases")),
):
    tenant_id = user["tenant_id"]
    case_record = _require_case_access(user, request.app.state.repository.get_case(tenant_id, case_id))
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id, _user_scope(request, user))
    actor = user["user_id"]
    normalized_status = normalize_case_state(payload.status) if payload.status is not None else None
    if payload.status is not None and not normalized_status:
        raise HTTPException(status_code=400, detail="Invalid case status")
    permissions = set(user.get("permissions") or [])
    if payload.assigned_to is not None and "reassign_alerts" not in permissions and normalize_role(user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if normalized_status == "sar_filed" and normalize_role(user.get("role")) not in {"manager", "admin"}:
        raise HTTPException(status_code=403, detail="Only manager/admin can approve SAR cases")
    ok, message, case = request.app.state.case_service.update_case(
        tenant_id=tenant_id,
        user_scope=_user_scope(request, user),
        case_id=case_id,
        run_id=run_info.get("run_id") or "",
        actor=actor,
        status=normalized_status,
        assigned_to=payload.assigned_to,
        notes=payload.notes,
    )
    if not ok or not case:
        raise HTTPException(status_code=404 if "not found" in message.lower() else 400, detail=message)
    if normalized_status:
        target_state = workflow_state_from_case(normalized_status)
        try:
            request.app.state.workflow_engine.transition_case(
                tenant_id=tenant_id,
                case_id=case_id,
                to_state=target_state or str(normalized_status).lower(),
                actor=actor,
                reason="case_update_api",
            )
        except Exception:
            pass
    normalized_case = _serialize_case(
        {
            **case_record,
            "status": normalize_case_state(case.get("status") or normalized_status or case_record.get("status")) or "open",
            "assigned_to": case.get("assigned_to") or case_record.get("assigned_to"),
            "payload_json": case,
        }
    )
    return {
        "case_id": case_id,
        "status": normalized_case["status"],
        "case_status": normalized_case["case_status"],
        "workflow_state": normalized_case["workflow_state"],
        "assigned_to": normalized_case.get("assigned_to"),
    }


@router.delete("/cases/{case_id}")
def delete_case(case_id: str, request: Request, user: dict = Depends(require_permissions("manager_approval"))):
    case = request.app.state.repository.get_case(user["tenant_id"], case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")
    request.app.state.case_service.delete_case(user["tenant_id"], case_id)
    _log_event(
        request,
        user["tenant_id"],
        "case_closed",
        user["user_id"],
        alert_id=case.get("alert_id") if case else None,
        case_id=case_id,
        details={"reason": "deleted"},
    )
    return {"status": "deleted", "case_id": case_id}


@router.get("/cases/{case_id}/audit")
def get_case_audit(
    case_id: str,
    request: Request,
    user: dict = Depends(require_any_permission("work_cases", "view_all_alerts", "view_team_queue")),
):
    _require_case_access(user, request.app.state.repository.get_case(user["tenant_id"], case_id))
    return {"events": request.app.state.case_service.get_case_audit(case_id, user["tenant_id"])}


@router.get("/workflows/sla-breaches")
def get_sla_breaches(request: Request, user: dict = Depends(require_any_permission("view_team_queue", "view_all_alerts"))):
    breaches = request.app.state.workflow_engine.monitor_sla(tenant_id=user["tenant_id"])
    return {"breaches": breaches, "count": len(breaches), "sla_window_hours": 48}


@router.post("/workflows/cases/{case_id}/escalate")
def escalate_workflow_case(case_id: str, request: Request, user: dict = Depends(require_permissions("approve_escalations"))):
    try:
        result = request.app.state.workflow_engine.escalate_case(
            tenant_id=user["tenant_id"],
            case_id=case_id,
            actor=user["user_id"],
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return result


@router.post("/workflows/cases/{case_id}/state")
def transition_workflow_case_state(
    case_id: str,
    payload: WorkflowStateTransitionRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
):
    try:
        result = request.app.state.workflow_engine.transition_case(
            tenant_id=user["tenant_id"],
            case_id=case_id,
            to_state=str(payload.to_state or "").lower().strip(),
            actor=user["user_id"],
            reason=payload.reason or "manual_transition",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _log_event(
        request,
        user["tenant_id"],
        "workflow_transition",
        user["user_id"],
        case_id=case_id,
        details={
            "old_state": result.get("from_state"),
            "new_state": result.get("to_state"),
            "reason": result.get("reason"),
        },
    )
    record_workflow_transition(from_state=result.get("from_state"), to_state=result.get("to_state"), result="success")
    return result
