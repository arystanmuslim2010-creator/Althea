from __future__ import annotations

import hashlib
import hmac
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from jose import JWTError, jwt

from core.config import Settings
from storage.postgres_repository import EnterpriseRepository

security_scheme = HTTPBearer(auto_error=False)

ROLE_ALIASES = {"lead": "investigator"}
VALID_ROLES = {"analyst", "investigator", "manager", "admin"}
ROLE_PERMISSIONS: dict[str, set[str]] = {
    "analyst": {
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "work_cases",
    },
    "investigator": {
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "reassign_alerts",
        "approve_escalations",
        "view_team_queue",
        "work_cases",
    },
    "manager": {
        "view_all_alerts",
        "view_dashboards",
        "approve_sar_cases",
        "manager_approval",
        "work_cases",
    },
    "admin": {
        "manage_users",
        "manage_roles",
        "view_system_logs",
        "view_all_alerts",
        "view_team_queue",
        "manager_approval",
        "work_cases",
    },
}


def normalize_role(role: str | None) -> str:
    normalized = str(role or "").lower().strip()
    return ROLE_ALIASES.get(normalized, normalized)


def _encode_token(payload: dict[str, Any], secret: str, algorithm: str) -> str:
    return jwt.encode(payload, secret, algorithm=algorithm)


def _decode_token(token: str, secret: str, algorithm: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, secret, algorithms=[algorithm])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Unauthorized: {exc}")


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
    return f"pbkdf2_sha256${salt.hex()}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        _, salt_hex, digest_hex = password_hash.split("$")
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        check = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
        return hmac.compare_digest(expected, check)
    except Exception:
        return False


def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_access_token(settings: Settings, tenant_id: str, user: dict[str, Any], session_id: str) -> str:
    now = datetime.now(timezone.utc)
    role = normalize_role(user.get("role"))
    payload = {
        "sub": user["id"],
        "tenant_id": tenant_id,
        "role": role,
        "team": user.get("team", "default"),
        "sid": session_id,
        "type": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.access_token_minutes)).timestamp()),
    }
    return _encode_token(payload, settings.jwt_secret, settings.jwt_algorithm)


def build_refresh_token(settings: Settings, tenant_id: str, user: dict[str, Any], session_id: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user["id"],
        "tenant_id": tenant_id,
        "sid": session_id,
        "type": "refresh",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.refresh_token_minutes)).timestamp()),
        "jti": uuid.uuid4().hex,
    }
    return _encode_token(payload, settings.jwt_secret, settings.jwt_algorithm)


def decode_token(settings: Settings, token: str) -> dict[str, Any]:
    return _decode_token(token, settings.jwt_secret, settings.jwt_algorithm)


def require_role(*roles: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        normalized = {normalize_role(role) for role in roles}
        if normalize_role(user["role"]) not in normalized:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _dep


def require_permissions(*permissions: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        role = normalize_role(user["role"])
        if role == "admin":
            return user
        granted = set(user.get("permissions") or [])
        if not set(permissions).issubset(granted):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _dep


def require_any_permission(*permissions: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        role = normalize_role(user["role"])
        if role == "admin":
            return user
        granted = set(user.get("permissions") or [])
        if not any(permission in granted for permission in permissions):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _dep


def get_tenant_id(
    request: Request,
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-ID"),
) -> str:
    state = request.app.state
    settings: Settings = state.settings
    request_scoped = getattr(request.state, "tenant_id", None)
    return request_scoped or x_tenant_id or settings.default_tenant_id


def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security_scheme),
) -> dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    settings: Settings = request.app.state.settings
    repository: EnterpriseRepository = request.app.state.repository
    payload = decode_token(settings, credentials.credentials)
    tenant_id = payload.get("tenant_id") or settings.default_tenant_id
    requested_tenant = request.headers.get(settings.tenant_header)
    if requested_tenant and requested_tenant != tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch between token and request header")
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    session_id = payload.get("sid")
    session = repository.get_session(tenant_id, session_id)
    if not session or session.get("revoked"):
        raise HTTPException(status_code=401, detail="Session revoked")
    user = repository.get_user_by_id(tenant_id, payload.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    if not bool(user.get("is_active", True)):
        raise HTTPException(status_code=403, detail="User disabled")
    role = normalize_role(user["role"])
    roles = repository.list_user_roles(tenant_id=tenant_id, user_id=user["id"]) or [role]
    permissions = repository.get_user_permissions(tenant_id=tenant_id, user_id=user["id"], fallback_role=role)
    return {
        "user_id": user["id"],
        "id": user["id"],
        "email": user["email"],
        "role": role,
        "roles": roles,
        "permissions": permissions,
        "team": user.get("team", "default"),
        "tenant_id": tenant_id,
        "session_id": session_id,
    }


def get_current_user_optional(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security_scheme),
) -> dict[str, Any] | None:
    if not credentials:
        return None
    try:
        return get_current_user(request=request, credentials=credentials)
    except HTTPException:
        return None


def get_authenticated_tenant_id(
    request: Request,
    user: dict[str, Any] = Depends(get_current_user),
    x_tenant_id: str | None = Header(default=None, alias="X-Tenant-ID"),
) -> str:
    tenant_id = str(user.get("tenant_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Missing tenant in authenticated context")
    if x_tenant_id and x_tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch between token and request header")
    return tenant_id
