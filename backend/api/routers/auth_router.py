from __future__ import annotations

import uuid
import logging
import threading
import time
import hmac
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from core.security import (
    VALID_ROLES,
    build_access_token,
    build_refresh_token,
    decode_token,
    get_authenticated_tenant_id,
    get_current_user,
    get_current_user_optional,
    get_tenant_id,
    hash_password,
    hash_refresh_token,
    normalize_role,
    require_permissions,
    verify_password,
)
from core.access_control import sanitize_user_dto

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
_REFRESH_RATE_LIMITER = _InMemoryRateLimiter(max_attempts=30, window_seconds=60)


def _client_ip(request: Request) -> str:
    settings = request.app.state.settings
    if bool(getattr(settings, "trusted_proxy_headers", False)):
        forwarded_for = (request.headers.get("X-Forwarded-For") or "").strip()
        if forwarded_for:
            first = forwarded_for.split(",")[0].strip()
            if first:
                return first
    return str((request.client.host if request.client else "") or "unknown")


def _normalize_email(email: str | None) -> str:
    return str(email or "").strip().lower()


def _coerce_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _record_auth_event(
    request: Request,
    tenant_id: str,
    action: str,
    user_id: str | None = None,
    actor_id: str | None = None,
    details: dict | None = None,
) -> None:
    repository = request.app.state.repository
    details_payload = dict(details or {})
    request_id = str(getattr(request.state, "request_id", "") or "").strip()
    if request_id and "request_id" not in details_payload:
        details_payload["request_id"] = request_id
    try:
        repository.append_auth_audit_log(
            {
                "id": uuid.uuid4().hex,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "actor_id": actor_id,
                "action": action,
                "old_role": None,
                "new_role": None,
                "details_json": details_payload,
                "timestamp": datetime.now(timezone.utc),
            }
        )
    except Exception:
        logger.exception(
            "Failed to write auth audit event",
            extra={"tenant_id": tenant_id, "action": action},
        )


def _set_refresh_cookie(request: Request, response: Response, refresh_token: str) -> None:
    settings = request.app.state.settings
    response.set_cookie(
        key=str(getattr(settings, "refresh_cookie_name", "althea_rt")),
        value=refresh_token,
        max_age=max(1, int(settings.refresh_token_minutes) * 60),
        httponly=True,
        secure=bool(getattr(settings, "refresh_cookie_secure", False)),
        samesite=str(getattr(settings, "refresh_cookie_samesite", "strict")).lower(),
        path=str(getattr(settings, "refresh_cookie_path", "/api/auth")),
        domain=getattr(settings, "refresh_cookie_domain", None),
    )


def _clear_refresh_cookie(request: Request, response: Response) -> None:
    settings = request.app.state.settings
    response.delete_cookie(
        key=str(getattr(settings, "refresh_cookie_name", "althea_rt")),
        path=str(getattr(settings, "refresh_cookie_path", "/api/auth")),
        domain=getattr(settings, "refresh_cookie_domain", None),
    )


def _build_auth_response(
    request: Request,
    *,
    access_token: str,
    refresh_token: str,
    user_payload: dict | None = None,
) -> JSONResponse:
    settings = request.app.state.settings
    body: dict[str, object] = {
        "access_token": access_token,
        "token_type": "bearer",
    }
    # Refresh tokens are delivered only via HttpOnly cookie. Never expose them to JS/API response bodies.
    if user_payload is not None:
        body["user"] = user_payload
    response = JSONResponse(content=body)
    response.headers["Cache-Control"] = "no-store"
    _set_refresh_cookie(request, response, refresh_token)
    return response


def _extract_refresh_token(request: Request, payload: RefreshRequest | None) -> str:
    settings = request.app.state.settings
    cookie_name = str(getattr(settings, "refresh_cookie_name", "althea_rt"))
    cookie_token = str(request.cookies.get(cookie_name) or "").strip()
    body_token = str((payload.refresh_token if payload else "") or "").strip()
    if cookie_token and body_token and not hmac.compare_digest(cookie_token, body_token):
        raise HTTPException(status_code=401, detail="Conflicting refresh token sources")
    if cookie_token:
        return cookie_token
    if body_token:
        if not bool(getattr(settings, "allow_refresh_token_in_body", False)):
            raise HTTPException(status_code=401, detail="Refresh token in request body is disabled")
        return body_token
    raise HTTPException(status_code=401, detail="Missing refresh token")


def _rate_limit_cache(request: Request):
    return getattr(request.app.state, "cache", None)


def _consume_rate_limit(
    request: Request,
    *,
    bucket: str,
    key: str,
    max_attempts: int,
    window_seconds: int,
) -> bool:
    cache = _rate_limit_cache(request)
    counter_key = f"auth-rate-limit:{bucket}:{key}"
    if cache is not None and hasattr(cache, "increment_counter"):
        try:
            current = int(cache.increment_counter(counter_key, window_seconds))
            return current > max_attempts
        except Exception:
            logger.exception("Distributed auth rate limiter failed", extra={"bucket": bucket})
    limiter = _LOGIN_RATE_LIMITER if bucket == "login" else _REFRESH_RATE_LIMITER
    if limiter.is_limited(key):
        return True
    limiter.register(key)
    return False


def _clear_rate_limit(request: Request, *, bucket: str, key: str) -> None:
    cache = _rate_limit_cache(request)
    counter_key = f"auth-rate-limit:{bucket}:{key}"
    if cache is not None and hasattr(cache, "delete"):
        try:
            cache.delete(counter_key)
        except Exception:
            logger.exception("Distributed auth rate limiter cleanup failed", extra={"bucket": bucket})
    limiter = _LOGIN_RATE_LIMITER if bucket == "login" else _REFRESH_RATE_LIMITER
    limiter.clear(key)


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
    refresh_token: str | None = None


@router.get("/providers")
def list_identity_providers(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manage_roles")),
):
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
    email_normalized = _normalize_email(payload.email)
    if not email_normalized:
        raise HTTPException(status_code=400, detail="Email is required")
    provision_mode = str(payload.provision_mode or "").upper().strip()
    provision_mode = PROVISION_MODE_ALIASES.get(provision_mode, provision_mode)
    if provision_mode not in ALLOWED_PROVISION_MODES:
        raise HTTPException(status_code=400, detail="Unsupported provisioning mode")

    requested_role = normalize_role(payload.role)
    role = "analyst"

    if provision_mode == "TENANT_BOOTSTRAP_USER":
        if not bool(getattr(settings, "enable_public_tenant_bootstrap", False)):
            raise HTTPException(status_code=403, detail="Tenant bootstrap registration is disabled")
        bootstrap_secret = (getattr(settings, "bootstrap_provisioning_secret", None) or "").strip()
        provided_secret = (
            request.headers.get("X-Bootstrap-Provisioning-Token")
            or request.headers.get("X-SSO-Provisioning-Token")
            or ""
        ).strip()
        if not bootstrap_secret or not provided_secret or not hmac.compare_digest(provided_secret, bootstrap_secret):
            raise HTTPException(status_code=403, detail="Invalid bootstrap provisioning token")
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
        if not provisioning_secret or not provided_secret or not hmac.compare_digest(provided_secret, provisioning_secret):
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
    _record_auth_event(
        request=request,
        tenant_id=tenant_id,
        action="register_success",
        user_id=user["id"],
        actor_id=(current_user or {}).get("user_id"),
        details={
            "provision_mode": provision_mode,
            "email": user["email"],
            "role": normalize_role(user["role"]),
            "client_ip": _client_ip(request),
            "user_agent": str(request.headers.get("user-agent") or ""),
        },
    )
    return _build_auth_response(
        request=request,
        access_token=access_token,
        refresh_token=refresh_token,
        user_payload={
            **sanitize_user_dto(
                {
                    "id": user["id"],
                    "user_id": user["id"],
                    "email": user["email"],
                    "role": normalize_role(user["role"]),
                    "team": user["team"],
                    "is_active": user.get("is_active", True),
                    "created_at": user.get("created_at"),
                }
            ),
            "roles": repository.list_user_roles(tenant_id=tenant_id, user_id=user["id"]),
            "permissions": repository.get_user_permissions(
                tenant_id=tenant_id,
                user_id=user["id"],
                fallback_role=normalize_role(user["role"]),
            ),
        },
    )


@router.post("/login")
def login_user(payload: LoginRequest, request: Request, tenant_id: str = Depends(get_tenant_id)):
    email_normalized = _normalize_email(payload.email)
    if not email_normalized:
        raise HTTPException(status_code=400, detail="Email is required")

    client_ip = _client_ip(request)
    settings = request.app.state.settings
    limiter_key_ip = f"{tenant_id}:{client_ip}:ip"
    limiter_key_identity = f"{tenant_id}:{email_normalized}:{client_ip}"
    if _consume_rate_limit(
        request,
        bucket="login",
        key=limiter_key_ip,
        max_attempts=int(getattr(settings, "login_rate_limit_max_attempts", 5)),
        window_seconds=int(getattr(settings, "login_rate_limit_window_seconds", 60)),
    ) or _consume_rate_limit(
        request,
        bucket="login",
        key=limiter_key_identity,
        max_attempts=int(getattr(settings, "login_rate_limit_max_attempts", 5)),
        window_seconds=int(getattr(settings, "login_rate_limit_window_seconds", 60)),
    ):
        _record_auth_event(
            request=request,
            tenant_id=tenant_id,
            action="login_rate_limited",
            user_id=None,
            actor_id=None,
            details={
                "email": email_normalized,
                "client_ip": client_ip,
                "user_agent": str(request.headers.get("user-agent") or ""),
            },
        )
        logger.warning("Login rate limit exceeded", extra={"tenant_id": tenant_id, "client_ip": client_ip})
        raise HTTPException(status_code=429, detail="Too many login attempts. Please retry later.")

    repository = request.app.state.repository
    user = repository.get_user_by_email(tenant_id, email_normalized)
    if not user or not verify_password(payload.password, user["password_hash"]):
        _record_auth_event(
            request=request,
            tenant_id=tenant_id,
            action="login_failed",
            user_id=(user or {}).get("id"),
            actor_id=None,
            details={
                "email": email_normalized,
                "reason": "invalid_credentials",
                "client_ip": client_ip,
                "user_agent": str(request.headers.get("user-agent") or ""),
            },
        )
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not bool(user.get("is_active", True)):
        _record_auth_event(
            request=request,
            tenant_id=tenant_id,
            action="login_blocked_disabled_user",
            user_id=user.get("id"),
            actor_id=user.get("id"),
            details={
                "email": email_normalized,
                "client_ip": client_ip,
                "user_agent": str(request.headers.get("user-agent") or ""),
            },
        )
        raise HTTPException(status_code=403, detail="User disabled")
    _clear_rate_limit(request, bucket="login", key=limiter_key_ip)
    _clear_rate_limit(request, bucket="login", key=limiter_key_identity)

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
    _record_auth_event(
        request=request,
        tenant_id=tenant_id,
        action="login_success",
        user_id=user["id"],
        actor_id=user["id"],
        details={
            "email": email_normalized,
            "client_ip": client_ip,
            "user_agent": str(request.headers.get("user-agent") or ""),
        },
    )
    return _build_auth_response(
        request=request,
        access_token=access_token,
        refresh_token=refresh_token,
        user_payload={
            "id": user["id"],
            "user_id": user["id"],
            "email": user["email"],
            "role": normalize_role(user["role"]),
            "team": user["team"],
            "roles": repository.list_user_roles(tenant_id=tenant_id, user_id=user["id"]),
            "permissions": repository.get_user_permissions(
                tenant_id=tenant_id,
                user_id=user["id"],
                fallback_role=normalize_role(user["role"]),
            ),
        },
    )


@router.post("/refresh")
def refresh_session(request: Request, payload: RefreshRequest | None = None):
    settings = request.app.state.settings
    repository = request.app.state.repository
    refresh_token = _extract_refresh_token(request, payload)
    refresh_limiter_key = f"{_client_ip(request)}:refresh"
    if _consume_rate_limit(
        request,
        bucket="refresh",
        key=refresh_limiter_key,
        max_attempts=int(getattr(settings, "refresh_rate_limit_max_attempts", 30)),
        window_seconds=int(getattr(settings, "refresh_rate_limit_window_seconds", 60)),
    ):
        raise HTTPException(status_code=429, detail="Too many refresh attempts. Please retry later.")
    claims = decode_token(settings, refresh_token)
    if claims.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    tenant_id = str(claims.get("tenant_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    requested_tenant = request.headers.get(settings.tenant_header)
    if requested_tenant and requested_tenant != tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch between token and request header")
    session = repository.get_session(tenant_id, claims.get("sid", ""))
    if not session or session.get("revoked"):
        raise HTTPException(status_code=401, detail="Session revoked")
    if str(session.get("user_id") or "") != str(claims.get("sub") or ""):
        raise HTTPException(status_code=401, detail="Session/token user mismatch")
    session_expiry = _coerce_utc(session.get("expires_at"))
    if session_expiry and session_expiry <= datetime.now(timezone.utc):
        repository.revoke_session(tenant_id=tenant_id, session_id=session.get("session_id"))
        raise HTTPException(status_code=401, detail="Session expired")
    if session["refresh_token_hash"] != hash_refresh_token(refresh_token):
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    user = repository.get_user_by_id(tenant_id, claims.get("sub", ""))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    if not bool(user.get("is_active", True)):
        repository.revoke_session(tenant_id=tenant_id, session_id=session["session_id"])
        _record_auth_event(
            request=request,
            tenant_id=tenant_id,
            action="refresh_blocked_disabled_user",
            user_id=user.get("id"),
            actor_id=user.get("id"),
            details={
                "client_ip": _client_ip(request),
                "user_agent": str(request.headers.get("user-agent") or ""),
            },
        )
        raise HTTPException(status_code=403, detail="User disabled")
    rotated_refresh_token = build_refresh_token(settings, tenant_id, user, session["session_id"])
    repository.update_session_refresh_token(
        tenant_id=tenant_id,
        session_id=session["session_id"],
        refresh_token_hash=hash_refresh_token(rotated_refresh_token),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=settings.refresh_token_minutes),
    )
    access_token = build_access_token(settings, tenant_id, user, session["session_id"])
    _record_auth_event(
        request=request,
        tenant_id=tenant_id,
        action="refresh_success",
        user_id=user["id"],
        actor_id=user["id"],
        details={
            "client_ip": _client_ip(request),
            "user_agent": str(request.headers.get("user-agent") or ""),
        },
    )
    _clear_rate_limit(request, bucket="refresh", key=refresh_limiter_key)
    return _build_auth_response(
        request=request,
        access_token=access_token,
        refresh_token=rotated_refresh_token,
        user_payload=None,
    )


@router.post("/logout")
def logout(request: Request, user: dict = Depends(get_current_user)):
    request.app.state.repository.revoke_session(user["tenant_id"], user["session_id"])
    _record_auth_event(
        request=request,
        tenant_id=user["tenant_id"],
        action="logout_success",
        user_id=user["user_id"],
        actor_id=user["user_id"],
        details={
            "session_id": user.get("session_id"),
            "client_ip": _client_ip(request),
            "user_agent": str(request.headers.get("user-agent") or ""),
        },
    )
    response = JSONResponse(content={"status": "revoked"})
    _clear_refresh_cookie(request, response)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/logout-all")
def logout_all(request: Request, user: dict = Depends(get_current_user)):
    revoked = request.app.state.repository.revoke_all_user_sessions(user["tenant_id"], user["user_id"])
    _record_auth_event(
        request=request,
        tenant_id=user["tenant_id"],
        action="logout_all_success",
        user_id=user["user_id"],
        actor_id=user["user_id"],
        details={
            "revoked_sessions": int(revoked or 0),
            "client_ip": _client_ip(request),
            "user_agent": str(request.headers.get("user-agent") or ""),
        },
    )
    response = JSONResponse(content={"status": "revoked", "revoked_sessions": revoked})
    _clear_refresh_cookie(request, response)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/me")
def auth_me(user: dict = Depends(get_current_user)):
    response = JSONResponse(content=sanitize_user_dto(user))
    response.headers["Cache-Control"] = "no-store"
    return response
