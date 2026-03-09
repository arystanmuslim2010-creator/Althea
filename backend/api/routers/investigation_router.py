from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.security import VALID_ROLES, get_authenticated_tenant_id, get_current_user, normalize_role, require_any_permission, require_permissions

router = APIRouter(prefix="/api", tags=["investigation"])

CASE_STATUSES = {"open", "under_review", "escalated", "sar_filed", "closed"}
ASSIGNMENT_STATUSES = {"open", "in_review", "escalated", "closed"}


def _user_scope(request: Request, user: dict | None = None) -> str:
    return (user or {}).get("user_id") or request.headers.get("X-User-Scope") or "public"


def _load_alerts_df(request: Request, tenant_id: str, run_id: str):
    payloads = request.app.state.repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
    import pandas as pd

    return pd.DataFrame(payloads)


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


class AssignAlertRequest(BaseModel):
    assigned_to: str


class AlertStatusRequest(BaseModel):
    status: str


class AddNoteRequest(BaseModel):
    note_text: str = Field(min_length=1)


class CreateInvestigationCaseRequest(BaseModel):
    alert_id: str


class UpdateCaseStatusRequest(BaseModel):
    status: str


class UpdateUserRoleRequest(BaseModel):
    role: str


class CreateCaseRequest(BaseModel):
    alert_ids: list[str]
    actor: str = "Analyst_1"


class SetActorRequest(BaseModel):
    actor: str


class UpdateCaseRequest(BaseModel):
    status: str | None = None
    assigned_to: str | None = None
    notes: str | None = None


@router.get("/work/queue")
def get_work_queue(
    request: Request,
    user: dict = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
):
    run_info = request.app.state.pipeline_service.get_run_info(user["tenant_id"], _user_scope(request, user))
    run_id = run_info.get("run_id")
    if not run_id:
        return {"queue": [], "count": 0}
    alerts_df = _load_alerts_df(request, user["tenant_id"], run_id)
    if alerts_df.empty:
        return {"queue": [], "count": 0}
    repo = request.app.state.repository
    cases = repo.list_cases(user["tenant_id"])
    records = []
    for record in alerts_df.to_dict("records"):
        alert_id = str(record.get("alert_id", ""))
        assignment = repo.get_latest_assignment(user["tenant_id"], alert_id)
        case_id = next((case["case_id"] for case in cases if case.get("alert_id") == alert_id), None)
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
            }
        )
    return {"queue": records, "count": len(records)}


@router.post("/alerts/{alert_id}/assign")
def assign_alert(
    alert_id: str,
    payload: AssignAlertRequest,
    request: Request,
    user: dict = Depends(require_permissions("reassign_alerts")),
):
    repo = request.app.state.repository
    existing = repo.get_latest_assignment(user["tenant_id"], alert_id)
    repo.upsert_assignment(
        {
            "id": existing["id"] if existing else uuid.uuid4().hex,
            "tenant_id": user["tenant_id"],
            "alert_id": alert_id,
            "assigned_to": payload.assigned_to,
            "assigned_by": user["user_id"],
            "status": "open",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
    )
    _log_event(request, user["tenant_id"], "alert_assigned", user["user_id"], alert_id=alert_id, details={"assigned_to": payload.assigned_to})
    return {"status": "assigned", "alert_id": alert_id, "assigned_to": payload.assigned_to}


@router.post("/alerts/{alert_id}/status")
def update_alert_status(
    alert_id: str,
    payload: AlertStatusRequest,
    request: Request,
    user: dict = Depends(require_permissions("change_alert_status")),
):
    status = payload.status.lower().strip()
    if status not in ASSIGNMENT_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status")
    repo = request.app.state.repository
    existing = repo.get_latest_assignment(user["tenant_id"], alert_id)
    repo.upsert_assignment(
        {
            "id": existing["id"] if existing else uuid.uuid4().hex,
            "tenant_id": user["tenant_id"],
            "alert_id": alert_id,
            "assigned_to": existing["assigned_to"] if existing else user["user_id"],
            "assigned_by": existing["assigned_by"] if existing else user["user_id"],
            "status": status,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
    )
    _log_event(request, user["tenant_id"], "status_changed", user["user_id"], alert_id=alert_id, details={"status": status})
    return {"alert_id": alert_id, "status": status}


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
    user: dict = Depends(require_permissions("change_alert_status")),
):
    repo = request.app.state.repository
    existing = [case for case in repo.list_cases(user["tenant_id"]) if case.get("alert_id") == payload.alert_id and case.get("status") != "closed"]
    if existing:
        raise HTTPException(status_code=409, detail="Case already exists for this alert")
    case_id = f"INV-{uuid.uuid4().hex[:12]}"
    created_at = datetime.now(timezone.utc)
    sla_due_at = (created_at + timedelta(hours=24)).isoformat()
    case = repo.save_case(
        {
            "case_id": case_id,
            "tenant_id": user["tenant_id"],
            "status": "open",
            "created_by": user["user_id"],
            "assigned_to": user["user_id"],
            "alert_id": payload.alert_id,
            "payload_json": {
                "case_id": case_id,
                "alert_id": payload.alert_id,
                "created_by": user["user_id"],
                "status": "open",
                "created_at": created_at.isoformat(),
                "sla_due_at": sla_due_at,
                "approval_chain": [
                    {"step": "analyst_review", "status": "completed", "actor": user["user_id"], "timestamp": created_at.isoformat()},
                    {"step": "manager_approval", "status": "pending", "actor": None, "timestamp": None},
                ],
            },
            "immutable_timeline_json": [],
            "created_at": created_at,
            "updated_at": created_at,
        }
    )
    _log_event(request, user["tenant_id"], "case_created", user["user_id"], alert_id=payload.alert_id, case_id=case_id, details={"status": "open"})
    return case["payload_json"]


@router.get("/cases/{case_id}")
def get_case(
    case_id: str,
    request: Request,
    user: dict = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
):
    repo = request.app.state.repository
    case = repo.get_case(user["tenant_id"], case_id)
    if case:
        notes = repo.list_alert_notes(user["tenant_id"], case.get("alert_id") or "")
        logs = repo.list_investigation_logs(user["tenant_id"], case_id=case_id, limit=200)
        return {"case": case["payload_json"], "notes": notes, "timeline": logs}
    raise HTTPException(status_code=404, detail="Case not found")


@router.post("/cases/{case_id}/status")
def update_investigation_case_status(
    case_id: str,
    payload: UpdateCaseStatusRequest,
    request: Request,
    user: dict = Depends(get_current_user),
):
    new_status = payload.status.lower().strip()
    if new_status not in CASE_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid case status")
    if new_status == "sar_filed" and user["role"] not in {"manager", "admin"}:
        raise HTTPException(status_code=403, detail="Only manager/admin can approve SAR cases")
    repo = request.app.state.repository
    case = repo.get_case(user["tenant_id"], case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")
    payload_json = dict(case["payload_json"])
    payload_json["status"] = new_status
    payload_json["closed_at"] = datetime.now(timezone.utc).isoformat() if new_status == "closed" else None
    approval_chain = list(payload_json.get("approval_chain") or [])
    if new_status in {"sar_filed", "closed"}:
        if not approval_chain:
            approval_chain = [{"step": "manager_approval", "status": "pending", "actor": None, "timestamp": None}]
        for step in approval_chain:
            if step.get("step") == "manager_approval":
                step["status"] = "completed"
                step["actor"] = user["user_id"]
                step["timestamp"] = datetime.now(timezone.utc).isoformat()
    payload_json["approval_chain"] = approval_chain
    timeline = list(case.get("immutable_timeline_json") or [])
    timeline.append(
        {
            "action": "case_closed" if new_status == "closed" else "status_changed",
            "performed_by": user["user_id"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": new_status,
        }
    )
    repo.save_case(
        {
            "case_id": case_id,
            "tenant_id": user["tenant_id"],
            "status": new_status,
            "created_by": case.get("created_by"),
            "assigned_to": case.get("assigned_to"),
            "alert_id": case.get("alert_id"),
            "payload_json": payload_json,
            "immutable_timeline_json": timeline,
            "created_at": datetime.fromisoformat(case["created_at"]),
            "updated_at": datetime.now(timezone.utc),
        }
    )
    _log_event(request, user["tenant_id"], "case_closed" if new_status == "closed" else "status_changed", user["user_id"], alert_id=case.get("alert_id"), case_id=case_id, details={"status": new_status})
    return {"case_id": case_id, "status": new_status, "closed_at": payload_json.get("closed_at")}


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
    updated = request.app.state.repository.update_user_role(user["tenant_id"], user_id, role)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    _log_event(request, user["tenant_id"], "status_changed", user["user_id"], details={"target_user_id": user_id, "new_role": role})
    return {"user_id": user_id, "role": role}


@router.get("/admin/logs")
def admin_logs(request: Request, user: dict = Depends(require_permissions("view_system_logs"))):
    return {"logs": request.app.state.repository.list_investigation_logs(user["tenant_id"], limit=300)}


@router.get("/cases")
def get_cases(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {"cases": request.app.state.case_service.list_cases(tenant_id)}


@router.post("/cases")
def create_case(request: Request, payload: CreateCaseRequest, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id, _user_scope(request))
    if not run_info.get("run_id"):
        raise HTTPException(status_code=400, detail="No data loaded")
    case = request.app.state.case_service.create_case(
        tenant_id=tenant_id,
        user_scope=_user_scope(request),
        alert_ids=payload.alert_ids,
        run_id=run_info["run_id"],
        actor=payload.actor,
    )
    request.app.state.event_bus.publish(
        event_name="case_created",
        tenant_id=tenant_id,
        payload={"case_id": case.get("case_id"), "alert_ids": payload.alert_ids, "actor": payload.actor},
        correlation_id=getattr(request.state, "request_id", None),
        version="2.0",
    )
    return {"case_id": case["case_id"], "status": case["status"]}


@router.get("/actor")
def get_actor(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {"actor": request.app.state.case_service.get_actor(tenant_id, _user_scope(request))}


@router.put("/actor")
def set_actor(request: Request, payload: SetActorRequest, tenant_id: str = Depends(get_authenticated_tenant_id)):
    request.app.state.case_service.set_actor(tenant_id, _user_scope(request), payload.actor)
    return {"actor": payload.actor}


@router.put("/cases/{case_id}")
def update_case(case_id: str, request: Request, payload: UpdateCaseRequest, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id, _user_scope(request))
    actor = request.app.state.case_service.get_actor(tenant_id, _user_scope(request))
    ok, message, case = request.app.state.case_service.update_case(
        tenant_id=tenant_id,
        user_scope=_user_scope(request),
        case_id=case_id,
        run_id=run_info.get("run_id") or "",
        actor=actor,
        status=payload.status,
        assigned_to=payload.assigned_to,
        notes=payload.notes,
    )
    if not ok or not case:
        raise HTTPException(status_code=404 if "not found" in message.lower() else 400, detail=message)
    return {"case_id": case_id, "status": case["status"], "assigned_to": case.get("assigned_to")}


@router.delete("/cases/{case_id}")
def delete_case(case_id: str, request: Request, user: dict = Depends(get_current_user)):
    case = request.app.state.repository.get_case(user["tenant_id"], case_id)
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
def get_case_audit(case_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {"events": request.app.state.case_service.get_case_audit(case_id, tenant_id)}
