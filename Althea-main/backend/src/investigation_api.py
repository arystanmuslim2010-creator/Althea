from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from .storage import Storage
from .services.case_service import InvestigationCaseService

router = APIRouter(prefix="/api", tags=["investigation"])
security = HTTPBearer(auto_error=False)

JWT_SECRET = os.getenv("ALTHEA_JWT_SECRET", "althea-dev-secret")
JWT_ALG = "HS256"
JWT_EXP_MINUTES = int(os.getenv("ALTHEA_JWT_EXP_MINUTES", "480"))

VALID_ROLES: Set[str] = {"analyst", "lead", "manager", "admin"}
ASSIGNMENT_STATUSES: Set[str] = {"open", "in_review", "escalated", "closed"}
CASE_STATUSES: Set[str] = {"open", "under_review", "escalated", "sar_filed", "closed"}

ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "analyst": {
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
    },
    "lead": {
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "reassign_alerts",
        "approve_escalations",
        "view_team_queue",
    },
    "manager": {
        "view_all_alerts",
        "view_dashboards",
        "approve_sar_cases",
    },
    "admin": {
        "manage_users",
        "manage_roles",
        "view_system_logs",
        "view_all_alerts",
        "view_team_queue",
    },
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _create_jwt(payload: Dict[str, Any]) -> str:
    header = {"alg": JWT_ALG, "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url_encode(signature)}"


def _decode_jwt(token: str) -> Dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("invalid token format")
        header_b64, payload_b64, signature_b64 = parts
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        expected_sig = hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
        actual_sig = _b64url_decode(signature_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            raise ValueError("invalid token signature")
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            raise ValueError("token expired")
        return payload
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Unauthorized: {exc}")


def _hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
    return f"pbkdf2_sha256${_b64url_encode(salt)}${_b64url_encode(digest)}"


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        _, salt_b64, digest_b64 = password_hash.split("$")
        salt = _b64url_decode(salt_b64)
        expected = _b64url_decode(digest_b64)
        check = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
        return hmac.compare_digest(expected, check)
    except Exception:
        return False


def _log_action(
    storage: Storage,
    action: str,
    performed_by: str,
    alert_id: Optional[str] = None,
    case_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    storage.append_investigation_log(
        {
            "id": str(uuid.uuid4()),
            "case_id": case_id,
            "alert_id": alert_id,
            "action": action,
            "performed_by": performed_by,
            "timestamp": _now_iso(),
            "details_json": json.dumps(details or {}),
        }
    )


class RegisterRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    role: str
    team: str = Field(min_length=1)


class LoginRequest(BaseModel):
    email: str
    password: str


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


def get_storage_from_request(request: Request) -> Storage:
    storage = getattr(request.app.state, "storage", None)
    if storage is None:
        raise HTTPException(status_code=500, detail="Storage not initialized")
    return storage


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    storage: Storage = Depends(get_storage_from_request),
) -> Dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    payload = _decode_jwt(credentials.credentials)
    user = storage.get_user_by_id(payload.get("user_id", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return {
        "user_id": user["id"],
        "email": user["email"],
        "role": payload.get("role", user["role"]),
        "team": payload.get("team", user["team"]),
    }


def require_permissions(*required: str):
    def dep(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        role = user["role"]
        granted = ROLE_PERMISSIONS.get(role, set())
        if role == "admin":
            return user
        if not set(required).issubset(granted):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return dep


def require_any_permission(*accepted: str):
    def dep(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        role = user["role"]
        if role == "admin":
            return user
        granted = ROLE_PERMISSIONS.get(role, set())
        if not any(p in granted for p in accepted):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return dep


def _build_token(user_id: str, role: str, team: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXP_MINUTES)
    payload = {
        "user_id": user_id,
        "role": role,
        "team": team,
        "exp": int(exp.timestamp()),
    }
    return _create_jwt(payload)


@router.post("/auth/register")
def register_user(payload: RegisterRequest, storage: Storage = Depends(get_storage_from_request)):
    role = payload.role.lower().strip()
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    existing = storage.get_user_by_email(payload.email)
    if existing:
        raise HTTPException(status_code=409, detail="User already exists")

    user_id = str(uuid.uuid4())
    user_record = {
        "id": user_id,
        "email": payload.email.lower(),
        "password_hash": _hash_password(payload.password),
        "role": role,
        "team": payload.team,
        "created_at": _now_iso(),
    }
    storage.create_user(user_record)
    token = _build_token(user_id=user_id, role=role, team=payload.team)
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user_id, "email": user_record["email"], "role": role, "team": payload.team},
    }


@router.post("/auth/login")
def login_user(payload: LoginRequest, storage: Storage = Depends(get_storage_from_request)):
    user = storage.get_user_by_email(payload.email)
    if not user or not _verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = _build_token(user_id=user["id"], role=user["role"], team=user["team"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user["id"], "email": user["email"], "role": user["role"], "team": user["team"]},
    }


@router.get("/auth/me")
def auth_me(user: Dict[str, Any] = Depends(get_current_user)):
    return user


@router.get("/work/queue")
def get_work_queue(
    request: Request,
    user: Dict[str, Any] = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
    storage: Storage = Depends(get_storage_from_request),
):
    role = user["role"]
    if role in {"manager", "admin"}:
        pass
    elif role == "lead" and "view_team_queue" not in ROLE_PERMISSIONS["lead"]:
        raise HTTPException(status_code=403, detail="Forbidden")

    app_state = getattr(request.app.state, "runtime_state", {})
    run_id = app_state.get("active_run_id") if isinstance(app_state, dict) else None
    service = InvestigationCaseService(storage=storage)
    queue = service.build_work_queue(run_id=run_id, user_id=user["user_id"], role=role, team=user["team"])
    return {"queue": queue, "count": len(queue)}


@router.post("/alerts/{alert_id}/assign")
def assign_alert(
    alert_id: str,
    payload: AssignAlertRequest,
    user: Dict[str, Any] = Depends(require_permissions("reassign_alerts")),
    storage: Storage = Depends(get_storage_from_request),
):
    assignment = {
        "id": str(uuid.uuid4()),
        "alert_id": alert_id,
        "assigned_to": payload.assigned_to,
        "assigned_by": user["user_id"],
        "status": "open",
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }
    storage.upsert_alert_assignment(assignment)
    _log_action(
        storage,
        action="alert_assigned",
        performed_by=user["user_id"],
        alert_id=alert_id,
        details={"assigned_to": payload.assigned_to, "assigned_by": user["user_id"]},
    )
    return {"status": "assigned", "alert_id": alert_id, "assigned_to": payload.assigned_to}


@router.post("/alerts/{alert_id}/status")
def update_alert_status(
    alert_id: str,
    payload: AlertStatusRequest,
    user: Dict[str, Any] = Depends(require_permissions("change_alert_status")),
    storage: Storage = Depends(get_storage_from_request),
):
    status = payload.status.lower().strip()
    if status not in ASSIGNMENT_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status")

    existing = storage.get_latest_assignment(alert_id)
    if not existing:
        assignment = {
            "id": str(uuid.uuid4()),
            "alert_id": alert_id,
            "assigned_to": user["user_id"],
            "assigned_by": user["user_id"],
            "status": status,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
    else:
        assignment = {
            "id": existing["id"],
            "alert_id": alert_id,
            "assigned_to": existing["assigned_to"],
            "assigned_by": existing["assigned_by"],
            "status": status,
            "created_at": existing["created_at"],
            "updated_at": _now_iso(),
        }
    storage.upsert_alert_assignment(assignment)
    _log_action(
        storage,
        action="status_changed",
        performed_by=user["user_id"],
        alert_id=alert_id,
        details={"status": status},
    )
    return {"alert_id": alert_id, "status": status}


@router.post("/alerts/{alert_id}/note")
def add_alert_note(
    alert_id: str,
    payload: AddNoteRequest,
    user: Dict[str, Any] = Depends(require_permissions("add_investigation_notes")),
    storage: Storage = Depends(get_storage_from_request),
):
    note = {
        "id": str(uuid.uuid4()),
        "alert_id": alert_id,
        "user_id": user["user_id"],
        "note_text": payload.note_text,
        "created_at": _now_iso(),
    }
    storage.create_alert_note(note)
    _log_action(
        storage,
        action="note_added",
        performed_by=user["user_id"],
        alert_id=alert_id,
        details={"note_id": note["id"]},
    )
    return {"status": "created", "note": note}


@router.post("/cases/create")
def create_case(
    payload: CreateInvestigationCaseRequest,
    user: Dict[str, Any] = Depends(require_permissions("change_alert_status")),
    storage: Storage = Depends(get_storage_from_request),
):
    existing_case = storage.get_investigation_case_by_alert(payload.alert_id)
    if existing_case and existing_case["status"] != "closed":
        raise HTTPException(status_code=409, detail="Case already exists for this alert")

    case_id = f"INV-{uuid.uuid4().hex[:12]}"
    case_data = {
        "case_id": case_id,
        "alert_id": payload.alert_id,
        "created_by": user["user_id"],
        "status": "open",
        "created_at": _now_iso(),
        "closed_at": None,
    }
    storage.create_investigation_case(case_data)
    _log_action(
        storage,
        action="case_created",
        performed_by=user["user_id"],
        alert_id=payload.alert_id,
        case_id=case_id,
        details={"status": "open"},
    )
    return case_data


@router.get("/cases/{case_id}")
def get_case(
    case_id: str,
    user: Dict[str, Any] = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
    storage: Storage = Depends(get_storage_from_request),
):
    case_data = storage.get_investigation_case(case_id)
    if not case_data:
        raise HTTPException(status_code=404, detail="Case not found")
    notes = storage.list_alert_notes(case_data["alert_id"])
    logs = storage.list_investigation_logs(case_id=case_id, limit=200)
    return {"case": case_data, "notes": notes, "timeline": logs}


@router.post("/cases/{case_id}/status")
def update_case_status(
    case_id: str,
    payload: UpdateCaseStatusRequest,
    user: Dict[str, Any] = Depends(get_current_user),
    storage: Storage = Depends(get_storage_from_request),
):
    new_status = payload.status.lower().strip()
    if new_status not in CASE_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid case status")

    role = user["role"]
    perms = ROLE_PERMISSIONS.get(role, set())
    if new_status == "sar_filed":
        if role not in {"manager", "admin"}:
            raise HTTPException(status_code=403, detail="Only manager/admin can approve SAR cases")
    elif role != "admin" and "change_alert_status" not in perms:
        raise HTTPException(status_code=403, detail="Forbidden")

    closed_at = _now_iso() if new_status == "closed" else None
    updated = storage.update_investigation_case_status(case_id=case_id, status=new_status, closed_at=closed_at)
    if not updated:
        raise HTTPException(status_code=404, detail="Case not found")

    case_data = storage.get_investigation_case(case_id)
    _log_action(
        storage,
        action="case_closed" if new_status == "closed" else "status_changed",
        performed_by=user["user_id"],
        alert_id=case_data["alert_id"] if case_data else None,
        case_id=case_id,
        details={"status": new_status},
    )
    return {"case_id": case_id, "status": new_status, "closed_at": closed_at}


@router.get("/admin/users")
def admin_list_users(
    user: Dict[str, Any] = Depends(require_permissions("manage_users")),
    storage: Storage = Depends(get_storage_from_request),
):
    return {"users": storage.list_users()}


@router.post("/admin/users/{user_id}/role")
def admin_update_role(
    user_id: str,
    payload: UpdateUserRoleRequest,
    user: Dict[str, Any] = Depends(require_permissions("manage_roles")),
    storage: Storage = Depends(get_storage_from_request),
):
    new_role = payload.role.lower().strip()
    if new_role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    updated = storage.update_user_role(user_id=user_id, role=new_role)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    _log_action(
        storage,
        action="status_changed",
        performed_by=user["user_id"],
        details={"target_user_id": user_id, "new_role": new_role},
    )
    return {"user_id": user_id, "role": new_role}


@router.get("/admin/logs")
def admin_logs(
    user: Dict[str, Any] = Depends(require_permissions("view_system_logs")),
    storage: Storage = Depends(get_storage_from_request),
):
    return {"logs": storage.list_investigation_logs(limit=300)}


@router.get("/alerts/{alert_id}/notes")
def get_alert_notes(
    alert_id: str,
    user: Dict[str, Any] = Depends(require_any_permission("view_assigned_alerts", "view_all_alerts")),
    storage: Storage = Depends(get_storage_from_request),
):
    return {"alert_id": alert_id, "notes": storage.list_alert_notes(alert_id)}
