from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers.alerts_router import router as alerts_router
from api.routers.auth_router import router as auth_router
from api.routers.investigation_router import router as investigation_router
from api.routers.pipeline_router import router as pipeline_router
from core.dependencies import build_app_state
from core.observability import MetricsRegistry, correlation_middleware
from src.domain.schemas import OverlayInputError
from src.services import OpsService

logger = logging.getLogger("althea-backend")

app = FastAPI(title="AML Alert Prioritization API")

for key, value in build_app_state().items():
    setattr(app.state, key, value)
app.state.metrics = MetricsRegistry()
app.state.storage = app.state.pipeline_service._legacy_storage
app.state.ops_service = OpsService()

app.add_middleware(
    CORSMiddleware,
    allow_origins=app.state.settings.allowed_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware("http")(correlation_middleware)


@app.exception_handler(OverlayInputError)
def overlay_input_exception_handler(request, exc: OverlayInputError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
def global_exception_handler(request, exc: Exception):
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    logger.exception("Unhandled exception", exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": str(exc) or "Internal server error"})


app.include_router(pipeline_router)
app.include_router(alerts_router)
app.include_router(auth_router)
app.include_router(investigation_router)
