from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.security import (
    VALID_ROLES,
    build_access_token,
    build_refresh_token,
    decode_token,
    get_current_user,
    get_tenant_id,
    hash_password,
    hash_refresh_token,
    verify_password,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: str
    password: str = Field(min_length=8)
    role: str
    team: str = Field(min_length=1)


class LoginRequest(BaseModel):
    email: str
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


@router.get("/providers")
def list_identity_providers(request: Request):
    settings = request.app.state.settings
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
    }


@router.post("/register")
def register_user(payload: RegisterRequest, request: Request, tenant_id: str = Depends(get_tenant_id)):
    repository = request.app.state.repository
    role = payload.role.lower().strip()
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    existing = repository.get_user_by_email(tenant_id, payload.email.lower())
    if existing:
        raise HTTPException(status_code=409, detail="User already exists")
    user = repository.create_user(
        {
            "id": uuid.uuid4().hex,
            "tenant_id": tenant_id,
            "email": payload.email.lower(),
            "password_hash": hash_password(payload.password),
            "role": role,
            "team": payload.team,
            "created_at": datetime.now(timezone.utc),
        }
    )
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
        "user": {"id": user["id"], "user_id": user["id"], "email": user["email"], "role": user["role"], "team": user["team"]},
    }


@router.post("/login")
def login_user(payload: LoginRequest, request: Request, tenant_id: str = Depends(get_tenant_id)):
    repository = request.app.state.repository
    user = repository.get_user_by_email(tenant_id, payload.email.lower())
    if not user or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
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
        "user": {"id": user["id"], "user_id": user["id"], "email": user["email"], "role": user["role"], "team": user["team"]},
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
