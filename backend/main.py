from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse

from api.routers.alerts_router import router as alerts_router
from api.routers.auth_router import router as auth_router
from api.routers.enrichment_router import router as enrichment_router
from api.routers.intelligence_router import router as intelligence_router
from api.routers.investigation_router import router as investigation_router
from api.routers.pipeline_router import router as pipeline_router
from core.dependencies import build_app_state
from core.observability import correlation_middleware
from core.security import decode_token
from core.telemetry import setup_telemetry
from services.ingestion_service import IngestionError

logger = logging.getLogger("althea-backend")


def create_app() -> FastAPI:
    app = FastAPI(title="ALTHEA Enterprise AML API")
    for key, value in build_app_state().items():
        setattr(app.state, key, value)

    setup_telemetry(app, app.state.settings)

    allowed_hosts = list(getattr(app.state.settings, "allowed_hosts", []) or [])
    if allowed_hosts:
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

    cors_kwargs = {
        "allow_origins": app.state.settings.allowed_origins,
        "allow_credentials": bool(getattr(app.state.settings, "cors_allow_credentials", True)),
        "allow_methods": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        "allow_headers": ["Authorization", "Content-Type", "X-Request-ID", "X-Tenant-ID"],
    }
    origin_regex = str(getattr(app.state.settings, "cors_allow_origin_regex", "") or "").strip()
    if origin_regex:
        cors_kwargs["allow_origin_regex"] = origin_regex
    app.add_middleware(
        CORSMiddleware,
        **cors_kwargs,
    )
    app.middleware("http")(correlation_middleware)

    @app.middleware("http")
    async def security_headers_middleware(request, call_next):
        response = await call_next(request)
        settings = request.app.state.settings
        if bool(getattr(settings, "security_headers_enabled", True)):
            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "DENY")
            response.headers.setdefault("Referrer-Policy", "no-referrer")
            response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
            response.headers.setdefault("Cache-Control", "no-store")
            if getattr(settings, "is_non_dev", False):
                response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return response

    @app.middleware("http")
    async def tenant_context_middleware(request, call_next):
        settings = request.app.state.settings
        repository = request.app.state.repository
        tenant_id = request.headers.get(settings.tenant_header) or settings.default_tenant_id

        # Do not let stale access tokens override tenant routing on auth bootstrap endpoints.
        # Frontend login/register flows may still carry old Authorization headers.
        request_path = str(request.url.path or "").lower().rstrip("/")
        auth_bootstrap_paths = {"/api/auth/login", "/api/auth/register", "/api/auth/refresh", "/api/auth/logout", "/api/auth/logout-all"}
        if request_path not in auth_bootstrap_paths:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.lower().startswith("bearer "):
                token = auth_header.split(" ", 1)[1].strip()
                if token:
                    try:
                        claims = decode_token(settings, token)
                        token_tenant = str(claims.get("tenant_id") or "").strip()
                        if token_tenant:
                            tenant_id = token_tenant
                    except Exception:
                        # Authentication dependencies handle token validity; middleware only sets context when available.
                        pass

        request.state.tenant_id = tenant_id
        try:
            repository.set_tenant_context(tenant_id)
        except Exception:
            # Avoid blocking request handling in case of transient context issues; repository sessions still set context.
            logger.exception("Failed to set request tenant DB context", extra={"tenant_id": tenant_id})

        return await call_next(request)

    @app.exception_handler(IngestionError)
    def ingestion_exception_handler(request, exc: IngestionError):
        response = JSONResponse(status_code=400, content={"detail": str(exc)})
        request_id = str(getattr(request.state, "request_id", "") or "").strip()
        if request_id:
            response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(Exception)
    def global_exception_handler(request, exc: Exception):
        if isinstance(exc, HTTPException):
            response = JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
            request_id = str(getattr(request.state, "request_id", "") or "").strip()
            if request_id:
                response.headers["X-Request-ID"] = request_id
            return response
        logger.exception("Unhandled exception", exc_info=exc)
        safe_detail = "Internal server error"
        try:
            settings = request.app.state.settings
            if getattr(settings, "is_dev", False):
                safe_detail = str(exc) or safe_detail
        except Exception:
            pass
        response = JSONResponse(status_code=500, content={"detail": safe_detail})
        request_id = str(getattr(request.state, "request_id", "") or "").strip()
        if request_id:
            response.headers["X-Request-ID"] = request_id
        return response

    app.include_router(pipeline_router)
    app.include_router(enrichment_router)
    app.include_router(alerts_router)
    app.include_router(auth_router)
    app.include_router(investigation_router)
    app.include_router(intelligence_router)
    return app


app = create_app()
