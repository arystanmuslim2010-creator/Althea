from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

try:
    from jose import JWTError, jwt
except ImportError:  # pragma: no cover - optional runtime fallback
    JWTError = ValueError
    jwt = None

from core.config import Settings
from storage.postgres_repository import EnterpriseRepository

security_scheme = HTTPBearer(auto_error=False)

VALID_ROLES = {"analyst", "lead", "manager", "admin"}
ROLE_PERMISSIONS: dict[str, set[str]] = {
    "analyst": {
        "view_assigned_alerts",
        "add_investigation_notes",
        "change_alert_status",
        "view_explanations",
        "work_cases",
    },
    "lead": {
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


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _encode_token(payload: dict[str, Any], secret: str, algorithm: str) -> str:
    if jwt is not None:
        return jwt.encode(payload, secret, algorithm=algorithm)
    if algorithm != "HS256":
        raise HTTPException(status_code=500, detail="Fallback JWT encoder only supports HS256")
    header = {"alg": algorithm, "typ": "JWT"}
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url_encode(signature)}"


def _decode_token(token: str, secret: str, algorithm: str) -> dict[str, Any]:
    if jwt is not None:
        return jwt.decode(token, secret, algorithms=[algorithm])
    if algorithm != "HS256":
        raise HTTPException(status_code=500, detail="Fallback JWT decoder only supports HS256")
    try:
        header_b64, payload_b64, signature_b64 = token.split(".")
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        expected_sig = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        actual_sig = _b64url_decode(signature_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            raise HTTPException(status_code=401, detail="Unauthorized: invalid token signature")
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(datetime.now(timezone.utc).timestamp()):
            raise HTTPException(status_code=401, detail="Unauthorized: token expired")
        return payload
    except HTTPException:
        raise
    except Exception as exc:
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
    payload = {
        "sub": user["id"],
        "tenant_id": tenant_id,
        "role": user["role"],
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
    try:
        return _decode_token(token, settings.jwt_secret, settings.jwt_algorithm)
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Unauthorized: {exc}")


def require_role(*roles: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        if user["role"] not in roles:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _dep


def require_permissions(*permissions: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        role = user["role"]
        if role == "admin":
            return user
        granted = ROLE_PERMISSIONS.get(role, set())
        if not set(permissions).issubset(granted):
            raise HTTPException(status_code=403, detail="Forbidden")
        return user

    return _dep


def require_any_permission(*permissions: str):
    def _dep(user: dict[str, Any] = Depends(get_current_user)) -> dict[str, Any]:
        role = user["role"]
        if role == "admin":
            return user
        granted = ROLE_PERMISSIONS.get(role, set())
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
    return x_tenant_id or settings.default_tenant_id


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
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid access token")
    session_id = payload.get("sid")
    session = repository.get_session(tenant_id, session_id)
    if not session or session.get("revoked"):
        raise HTTPException(status_code=401, detail="Session revoked")
    user = repository.get_user_by_id(tenant_id, payload.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return {
        "user_id": user["id"],
        "id": user["id"],
        "email": user["email"],
        "role": user["role"],
        "team": user.get("team", "default"),
        "tenant_id": tenant_id,
        "session_id": session_id,
    }
