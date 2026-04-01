from __future__ import annotations

import uuid
import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.security import (
    VALID_ROLES,
    build_access_token,
    build_refresh_token,
    decode_token,
    get_current_user,
    get_current_user_optional,
    get_tenant_id,
    hash_password,
    hash_refresh_token,
    normalize_role,
    verify_password,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = logging.getLogger("althea.auth")


class _InMemoryRateLimiter:
    def __init__(self, max_attempts: int = 5, window_seconds: int = 60) -> None:
        self._max_attempts = int(max_attempts)
        self._window_seconds = int(window_seconds)
        self._lock = threading.Lock()
        self._events: dict[str, deque[float]] = defaultdict(deque)

    def _prune(self, key: str, now_monotonic: float) -> deque[float]:
        q = self._events[key]
        boundary = now_monotonic - float(self._window_seconds)
        while q and q[0] < boundary:
            q.popleft()
        return q

    def register(self, key: str) -> None:
        now_monotonic = time.monotonic()
        with self._lock:
            q = self._prune(key, now_monotonic)
            q.append(now_monotonic)

    def is_limited(self, key: str) -> bool:
        now_monotonic = time.monotonic()
        with self._lock:
            q = self._prune(key, now_monotonic)
            return len(q) >= self._max_attempts

    def clear(self, key: str) -> None:
        with self._lock:
            self._events.pop(key, None)


_LOGIN_RATE_LIMITER = _InMemoryRateLimiter(max_attempts=5, window_seconds=60)


def _client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    return str((request.client.host if request.client else "") or "unknown")


class RegisterRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    role: str | None = None
    team: str = Field(min_length=1)
    provision_mode: str = "admin_invite_user"


ALLOWED_PROVISION_MODES = {"ADMIN_INVITE_USER", "TENANT_BOOTSTRAP_USER", "SSO_PROVISION_USER"}
PROVISION_MODE_ALIASES = {
    "ADMIN_INVITE": "ADMIN_INVITE_USER",
    "TENANT_BOOTSTRAP": "TENANT_BOOTSTRAP_USER",
    "SSO_PROVISIONING": "SSO_PROVISION_USER",
}


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


@router.get("/providers")
def list_identity_providers(request: Request, tenant_id: str = Depends(get_tenant_id)):
    settings = request.app.state.settings
    configured = request.app.state.repository.list_identity_providers(tenant_id=tenant_id)
    return {
        "oidc": {
            "enabled": bool(settings.oidc_issuer_url),
            "issuer": settings.oidc_issuer_url,
            "client_id": settings.oidc_client_id,
        },
        "saml": {"enabled": bool(settings.saml_metadata_url), "metadata_url": settings.saml_metadata_url},
        "azure_ad": {
            "enabled": bool(settings.azure_ad_tenant_id and settings.azure_ad_client_id),
            "tenant_id": settings.azure_ad_tenant_id,
            "client_id": settings.azure_ad_client_id,
        },
        "okta": {
            "enabled": bool(settings.okta_domain and settings.okta_client_id),
            "domain": settings.okta_domain,
            "client_id": settings.okta_client_id,
        },
        "configured": configured,
    }


@router.post("/register")
def register_user(
    payload: RegisterRequest,
    request: Request,
    tenant_id: str = Depends(get_tenant_id),
    current_user: dict | None = Depends(get_current_user_optional),
):
    repository = request.app.state.repository
    settings = request.app.state.settings
    email_normalized = str(payload.email or "").strip().lower()
    if not email_normalized:
        raise HTTPException(status_code=400, detail="Email is required")
    provision_mode = str(payload.provision_mode or "").upper().strip()
    provision_mode = PROVISION_MODE_ALIASES.get(provision_mode, provision_mode)
    if provision_mode not in ALLOWED_PROVISION_MODES:
        raise HTTPException(status_code=400, detail="Unsupported provisioning mode")

    requested_role = normalize_role(payload.role)
    role = "analyst"

    if provision_mode == "TENANT_BOOTSTRAP_USER":
        if repository.count_users(tenant_id) > 0:
            raise HTTPException(status_code=403, detail="Tenant bootstrap is only allowed for empty tenants")
        # Bootstrap role is fixed and not user-selected.
        role = "admin"
    elif provision_mode == "ADMIN_INVITE_USER":
        if not current_user or normalize_role(current_user.get("role")) != "admin":
            raise HTTPException(status_code=403, detail="Admin invite registration requires admin authorization")
        role = requested_role or "analyst"
    elif provision_mode == "SSO_PROVISION_USER":
        provisioning_secret = (getattr(settings, "sso_provisioning_secret", None) or "").strip()
        provided_secret = (request.headers.get("X-SSO-Provisioning-Token") or "").strip()
        if not provisioning_secret or provided_secret != provisioning_secret:
            raise HTTPException(status_code=403, detail="Invalid SSO provisioning token")
        # SSO provisioning defaults to least-privilege role unless changed later by admins.
        role = "analyst"

    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    if provision_mode != "ADMIN_INVITE_USER" and role != "analyst" and provision_mode != "TENANT_BOOTSTRAP_USER":
        raise HTTPException(status_code=403, detail="Only admins can assign elevated roles")

    existing = repository.get_user_by_email(tenant_id, email_normalized)
    if existing:
        raise HTTPException(status_code=409, detail="User already exists")
    user = repository.create_user(
        {
            "id": uuid.uuid4().hex,
            "tenant_id": tenant_id,
            "email": email_normalized,
            "password_hash": hash_password(payload.password),
            "role": role,
            "team": payload.team,
            "created_at": datetime.now(timezone.utc),
            "created_by_actor_id": (current_user or {}).get("user_id"),
            "provision_mode": provision_mode,
        }
    )
    repository.assign_user_roles(
        tenant_id=tenant_id,
        user_id=user["id"],
        roles=[role],
        created_by=(current_user or {}).get("user_id"),
        replace=True,
    )
    session_id = uuid.uuid4().hex
    refresh_token = build_refresh_token(settings, tenant_id, user, session_id)
    repository.create_session(
        {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "user_id": user["id"],
            "refresh_token_hash": hash_refresh_token(refresh_token),
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=settings.refresh_token_minutes),
            "created_at": datetime.now(timezone.utc),
        }
    )
    access_token = build_access_token(settings, tenant_id, user, session_id)
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "user_id": user["id"],
            "email": user["email"],
            "role": normalize_role(user["role"]),
            "team": user["team"],
            "roles": repository.list_user_roles(tenant_id=tenant_id, user_id=user["id"]),
        },
    }


@router.post("/login")
def login_user(payload: LoginRequest, request: Request, tenant_id: str = Depends(get_tenant_id)):
    email_normalized = str(payload.email or "").strip().lower()
    if not email_normalized:
        raise HTTPException(status_code=400, detail="Email is required")

    limiter_key = f"{tenant_id}:{_client_ip(request)}"
    if _LOGIN_RATE_LIMITER.is_limited(limiter_key):
        logger.warning("Login rate limit exceeded", extra={"tenant_id": tenant_id, "client_ip": _client_ip(request)})
        raise HTTPException(status_code=429, detail="Too many login attempts. Please retry later.")
    _LOGIN_RATE_LIMITER.register(limiter_key)

    repository = request.app.state.repository
    user = repository.get_user_by_email(tenant_id, email_normalized)
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    _LOGIN_RATE_LIMITER.clear(limiter_key)

    session_id = uuid.uuid4().hex
    settings = request.app.state.settings
    refresh_token = build_refresh_token(settings, tenant_id, user, session_id)
    repository.create_session(
        {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "user_id": user["id"],
            "refresh_token_hash": hash_refresh_token(refresh_token),
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=settings.refresh_token_minutes),
            "created_at": datetime.now(timezone.utc),
        }
    )
    access_token = build_access_token(settings, tenant_id, user, session_id)
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user": {
            "id": user["id"],
            "user_id": user["id"],
            "email": user["email"],
            "role": normalize_role(user["role"]),
            "team": user["team"],
            "roles": repository.list_user_roles(tenant_id=tenant_id, user_id=user["id"]),
        },
    }


@router.post("/refresh")
def refresh_session(payload: RefreshRequest, request: Request):
    settings = request.app.state.settings
    repository = request.app.state.repository
    claims = decode_token(settings, payload.refresh_token)
    if claims.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    tenant_id = claims.get("tenant_id") or settings.default_tenant_id
    session = repository.get_session(tenant_id, claims.get("sid", ""))
    if not session or session.get("revoked"):
        raise HTTPException(status_code=401, detail="Session revoked")
    if session["refresh_token_hash"] != hash_refresh_token(payload.refresh_token):
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    user = repository.get_user_by_id(tenant_id, claims.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    rotated_refresh_token = build_refresh_token(settings, tenant_id, user, session["session_id"])
    repository.update_session_refresh_token(
        tenant_id=tenant_id,
        session_id=session["session_id"],
        refresh_token_hash=hash_refresh_token(rotated_refresh_token),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=settings.refresh_token_minutes),
    )
    access_token = build_access_token(settings, tenant_id, user, session["session_id"])
    return {"access_token": access_token, "refresh_token": rotated_refresh_token, "token_type": "bearer"}


@router.post("/logout")
def logout(request: Request, user: dict = Depends(get_current_user)):
    request.app.state.repository.revoke_session(user["tenant_id"], user["session_id"])
    return {"status": "revoked"}


@router.post("/logout-all")
def logout_all(request: Request, user: dict = Depends(get_current_user)):
    revoked = request.app.state.repository.revoke_all_user_sessions(user["tenant_id"], user["user_id"])
    return {"status": "revoked", "revoked_sessions": revoked}


@router.get("/me")
def auth_me(user: dict = Depends(get_current_user)):
    return user
