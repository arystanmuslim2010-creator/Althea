from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
import re

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.observability import (
    metrics_response,
    record_feature_retrieval,
    record_ingestion_path_used,
    record_legacy_path_access,
    record_legacy_ingestion_usage,
    record_integration_error,
)
from core.security import get_authenticated_tenant_id, require_permissions, require_role
from services.alert_ingestion_service import AlertIngestionValidationError
from services.ingestion_service import IngestionError

router = APIRouter(tags=["pipeline"])
logger = logging.getLogger("althea.pipeline_router")

_JSONL_ALLOWED_EXTENSIONS = {".jsonl", ".ndjson"}
_JSONL_ALLOWED_CONTENT_TYPES = {
    "application/json",
    "application/x-ndjson",
    "application/ndjson",
    "text/plain",
}
_CSV_ALLOWED_EXTENSIONS = {".csv"}
_CSV_ALLOWED_CONTENT_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
    "text/plain",
}


class StreamReplayRequest(BaseModel):
    topic: str
    start_event_id: str = "0-0"
    batch_size: int = 500


class StreamBackfillRequest(BaseModel):
    topic: str
    events: list[dict]


class ModelRescoreRequest(BaseModel):
    run_id: str
    alert_ids: list[str]
    model_version: str | None = None


class GovernanceApprovalRequest(BaseModel):
    model_version: str
    stage: str
    decision: str
    notes: str = ""


class PrimaryIngestionModeRequest(BaseModel):
    mode: str


def _user_scope(request: Request) -> str:
    current_user = getattr(request.state, "current_user", None) or {}
    raw = str(current_user.get("user_id") or request.headers.get("X-User-Scope") or "public").strip()
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
    return clean[:128] if clean else "public"


def _is_debug_env(request: Request) -> bool:
    env = str(getattr(request.app.state.settings, "app_env", "") or "").strip().lower()
    return env in {"dev", "development", "local", "test", "testing"}


def _error_detail(request: Request, exc: Exception, fallback: str = "Internal server error") -> str:
    if _is_debug_env(request):
        return str(exc)
    return fallback


def _max_upload_bytes(request: Request) -> int:
    return int(getattr(request.app.state.settings, "ingestion_max_upload_bytes", 10 * 1024 * 1024))


def _ensure_upload_size_within_limit(request: Request, payload: bytes) -> None:
    max_bytes = max(1, _max_upload_bytes(request))
    actual = len(payload or b"")
    if actual > max_bytes:
        raise HTTPException(
            status_code=413,
            detail={
                "error": "upload_too_large",
                "message": "Uploaded file exceeds configured size limit.",
                "max_bytes": max_bytes,
                "actual_bytes": actual,
            },
        )


def _count_non_empty_lines(raw_bytes: bytes) -> int:
    return sum(1 for line in (raw_bytes or b"").splitlines() if line.strip())


def _validate_uploaded_file_metadata(
    file: UploadFile,
    *,
    allowed_extensions: set[str],
    allowed_content_types: set[str],
    label: str,
) -> None:
    raw_name = str(file.filename or "").strip()
    safe_name = Path(raw_name).name
    extension = Path(safe_name).suffix.lower()
    content_type = str(file.content_type or "").strip().lower()

    if not safe_name:
        raise HTTPException(status_code=400, detail=f"{label} file name is required.")
    if extension not in allowed_extensions:
        allowed_ext = ", ".join(sorted(allowed_extensions))
        raise HTTPException(status_code=400, detail=f"{label} must use one of: {allowed_ext}.")
    if content_type and content_type not in allowed_content_types:
        allowed_types = ", ".join(sorted(allowed_content_types))
        raise HTTPException(status_code=400, detail=f"{label} content type must be one of: {allowed_types}.")


def _legacy_ingestion_enabled(request: Request) -> bool:
    return bool(getattr(request.app.state.settings, "enable_legacy_ingestion", False))


def _log_deprecated_path_access(
    request: Request,
    endpoint: str,
    blocked: bool,
    tenant_id: str | None = None,
) -> None:
    logger.warning(
        "deprecated_path_access",
        extra={
            "category": "deprecated_path_access",
            "endpoint": endpoint,
            "blocked": bool(blocked),
            "tenant_id": str(tenant_id or ""),
            "path": str(request.url.path),
            "method": str(request.method),
        },
    )


def _resolve_primary_ingestion_mode(request: Request, override_mode: str | None = None) -> str:
    if override_mode:
        raw = str(override_mode).strip().lower()
        if raw in {"legacy", "alert_jsonl"}:
            return raw
        raise HTTPException(status_code=400, detail="ingestion_mode must be one of: legacy, alert_jsonl")
    pipeline = getattr(request.app.state, "pipeline_service", None)
    if pipeline is not None and hasattr(pipeline, "get_primary_ingestion_mode"):
        return str(pipeline.get_primary_ingestion_mode() or "legacy")
    settings = request.app.state.settings
    raw = str(getattr(settings, "primary_ingestion_mode", "legacy") or "legacy").strip().lower()
    return raw if raw in {"legacy", "alert_jsonl"} else "legacy"


@router.get("/")
def root() -> dict:
    return {"status": "ok", "app": "AML Alert Prioritization API"}


def _compute_detailed_health(request: Request) -> dict:
    checks = {
        "database": False,
        "redis": False,
        "worker_queue": False,
        "worker_heartbeat": False,
        "model_registry": False,
        "feature_store": False,
    }
    details: dict[str, str] = {}

    try:
        request.app.state.repository.ping()
        checks["database"] = True
    except Exception as exc:
        details["database"] = str(exc) if _is_debug_env(request) else "unavailable"

    try:
        request.app.state.cache.ping()
        checks["redis"] = True
    except Exception as exc:
        details["redis"] = str(exc) if _is_debug_env(request) else "unavailable"

    try:
        depth = request.app.state.job_queue_service.queue_depth(request.app.state.settings.rq_queue_name)
        checks["worker_queue"] = depth >= 0
    except Exception as exc:
        details["worker_queue"] = str(exc) if _is_debug_env(request) else "unavailable"
        depth = -1

    try:
        heartbeat_keys = (
            "heartbeat:worker:pipeline",
            "heartbeat:worker:event",
            "heartbeat:worker:streaming",
            "heartbeat:worker:all_in_one",
        )
        checks["worker_heartbeat"] = any(
            bool(request.app.state.cache.get_json(key, default=None)) for key in heartbeat_keys
        )
        if not checks["worker_heartbeat"]:
            details["worker_heartbeat"] = "No active worker heartbeat keys found."
    except Exception as exc:
        details["worker_heartbeat"] = str(exc) if _is_debug_env(request) else "unavailable"

    try:
        tenant_id = request.app.state.settings.default_tenant_id
        request.app.state.ml_service.list_versions(tenant_id)
        checks["model_registry"] = True
    except Exception as exc:
        details["model_registry"] = str(exc) if _is_debug_env(request) else "unavailable"

    try:
        tenant_id = request.app.state.settings.default_tenant_id
        request.app.state.feature_registry.list_features(tenant_id=tenant_id)
        checks["feature_store"] = True
    except Exception as exc:
        details["feature_store"] = str(exc) if _is_debug_env(request) else "unavailable"

    ok = all(checks.values())
    status = "healthy" if ok else "degraded"
    return {"ok": ok, "status": status, "checks": checks, "queue_depth": depth, "details": details}


@router.get("/health")
def health_check() -> dict:
    # Public liveness only; dependency diagnostics are restricted to /internal/health.
    return {"ok": True, "status": "alive"}


@router.get("/internal/health")
def internal_health_check(
    request: Request,
    _tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
) -> dict:
    return _compute_detailed_health(request)


@router.get("/metrics")
def metrics(
    request: Request,
    _tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return metrics_response(request.app.state.metrics)


@router.get("/internal/rollout/status")
def get_rollout_status(
    request: Request,
    window_runs: int = 20,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_role("admin")),
) -> dict:
    bounded_window = max(1, min(int(window_runs), 200))
    return request.app.state.pipeline_service.get_rollout_status(tenant_id=tenant_id, window_runs=bounded_window)


@router.get("/internal/migration/finalization-status")
def get_finalization_status(
    request: Request,
    window_runs: int = 20,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_role("admin")),
) -> dict:
    bounded_window = max(1, min(int(window_runs), 200))
    pipeline = request.app.state.pipeline_service
    if hasattr(pipeline, "get_finalization_status"):
        return pipeline.get_finalization_status(tenant_id=tenant_id, window_runs=bounded_window)
    return pipeline.get_rollout_status(tenant_id=tenant_id, window_runs=bounded_window)


@router.post("/internal/ingestion/primary-mode")
def set_primary_ingestion_mode(
    payload: PrimaryIngestionModeRequest,
    request: Request,
    _: dict = Depends(require_role("admin")),
) -> dict:
    target_mode = str(payload.mode or "").strip().lower()
    if target_mode not in {"legacy", "alert_jsonl"}:
        raise HTTPException(status_code=400, detail="mode must be one of: legacy, alert_jsonl")
    pipeline = request.app.state.pipeline_service
    if not hasattr(pipeline, "set_runtime_primary_ingestion_mode"):
        raise HTTPException(status_code=503, detail="runtime primary mode switching not configured")
    return pipeline.set_runtime_primary_ingestion_mode(target_mode)


@router.post("/api/data/generate-synthetic")
def generate_synthetic(
    n_rows: int = 400,
    request: Request = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    return request.app.state.ingestion_service.generate_synthetic(tenant_id=tenant_id, user_scope=_user_scope(request), n_rows=n_rows)


@router.post("/api/data/upload-csv")
async def upload_csv(
    request: Request,
    file: UploadFile = File(...),
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    primary_mode = _resolve_primary_ingestion_mode(request)
    if not _legacy_ingestion_enabled(request):
        record_legacy_ingestion_usage(endpoint="upload_csv", status="disabled")
        record_legacy_path_access(endpoint="upload_csv", caller="api", blocked=True)
        _log_deprecated_path_access(request=request, endpoint="upload_csv", blocked=True, tenant_id=tenant_id)
        return JSONResponse(
            status_code=503,
            content={
                "error": "legacy_ingestion_disabled",
                "message": "Legacy ingestion has been disabled after alert-centric migration finalization.",
            },
        )
    if primary_mode == "alert_jsonl":
        # Phase 5 deprecation marker: legacy path remains supported but is no longer the primary target.
        logger.warning("Legacy CSV ingestion used while primary mode is alert_jsonl", extra={"tenant_id": tenant_id})
    record_legacy_path_access(endpoint="upload_csv", caller="api", blocked=False)
    record_legacy_ingestion_usage(endpoint="upload_csv", status="attempt")
    try:
        _validate_uploaded_file_metadata(
            file,
            allowed_extensions=_CSV_ALLOWED_EXTENSIONS,
            allowed_content_types=_CSV_ALLOWED_CONTENT_TYPES,
            label="CSV upload",
        )
        contents = await file.read()
        _ensure_upload_size_within_limit(request, contents)
        out = request.app.state.ingestion_service.upload_transactions_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
        rows = int(out.get("rows") or 0) if isinstance(out, dict) else 0
        record_ingestion_path_used(
            ingestion_path="legacy",
            primary_mode=primary_mode,
            status="accepted",
            alerts_ingested=rows,
        )
        record_legacy_ingestion_usage(endpoint="upload_csv", status="accepted")
        return out
    except IngestionError as exc:
        if str(exc) == "legacy_ingestion_disabled":
            record_legacy_ingestion_usage(endpoint="upload_csv", status="disabled")
            record_legacy_path_access(endpoint="upload_csv", caller="service", blocked=True)
            _log_deprecated_path_access(request=request, endpoint="upload_csv", blocked=True, tenant_id=tenant_id)
            return JSONResponse(
                status_code=503,
                content={
                    "error": "legacy_ingestion_disabled",
                    "message": "Legacy ingestion has been disabled after alert-centric migration finalization.",
                },
            )
        record_ingestion_path_used(ingestion_path="legacy", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        record_legacy_ingestion_usage(endpoint="upload_csv", status="failed")
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        record_legacy_ingestion_usage(endpoint="upload_csv", status="failed")
        raise
    except Exception as exc:
        record_ingestion_path_used(ingestion_path="legacy", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        record_legacy_ingestion_usage(endpoint="upload_csv", status="failed")
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))


@router.post("/api/data/generate-bank-csv")
def generate_bank_csv(
    n_rows: int = 1000,
    seed: int = 42,
    request: Request = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    try:
        out = request.app.state.ingestion_service.generate_bank_alerts_csv(n_rows=max(1, min(n_rows, 10000)), seed=seed)
        out_path = request.app.state.settings.data_dir / f"bank_alerts_{len(out)}.csv"
        out.to_csv(out_path, index=False, encoding="utf-8")
        return {
            "rows": len(out),
            "artifact": out_path.name,
            "message": "Bank CSV generated successfully. Upload this file via Upload bank CSV.",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))


@router.post("/api/data/upload-bank-csv")
async def upload_bank_csv(
    request: Request,
    file: UploadFile = File(...),
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    primary_mode = _resolve_primary_ingestion_mode(request)
    if not _legacy_ingestion_enabled(request):
        record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="disabled")
        record_legacy_path_access(endpoint="upload_bank_csv", caller="api", blocked=True)
        _log_deprecated_path_access(request=request, endpoint="upload_bank_csv", blocked=True, tenant_id=tenant_id)
        return JSONResponse(
            status_code=503,
            content={
                "error": "legacy_ingestion_disabled",
                "message": "Legacy ingestion has been disabled after alert-centric migration finalization.",
            },
        )
    if primary_mode == "alert_jsonl":
        # Phase 5 deprecation marker: legacy path remains supported but is no longer the primary target.
        logger.warning("Legacy bank CSV ingestion used while primary mode is alert_jsonl", extra={"tenant_id": tenant_id})
    record_legacy_path_access(endpoint="upload_bank_csv", caller="api", blocked=False)
    record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="attempt")
    try:
        _validate_uploaded_file_metadata(
            file,
            allowed_extensions=_CSV_ALLOWED_EXTENSIONS,
            allowed_content_types=_CSV_ALLOWED_CONTENT_TYPES,
            label="Bank CSV upload",
        )
        contents = await file.read()
        _ensure_upload_size_within_limit(request, contents)
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        out = request.app.state.ingestion_service.upload_bank_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
        rows = int(out.get("rows") or 0) if isinstance(out, dict) else 0
        record_ingestion_path_used(
            ingestion_path="legacy",
            primary_mode=primary_mode,
            status="accepted",
            alerts_ingested=rows,
        )
        record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="accepted")
        return out
    except IngestionError as exc:
        if str(exc) == "legacy_ingestion_disabled":
            record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="disabled")
            record_legacy_path_access(endpoint="upload_bank_csv", caller="service", blocked=True)
            _log_deprecated_path_access(request=request, endpoint="upload_bank_csv", blocked=True, tenant_id=tenant_id)
            return JSONResponse(
                status_code=503,
                content={
                    "error": "legacy_ingestion_disabled",
                    "message": "Legacy ingestion has been disabled after alert-centric migration finalization.",
                },
            )
        record_ingestion_path_used(ingestion_path="legacy", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="failed")
        raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="failed")
        raise
    except Exception as exc:
        record_ingestion_path_used(ingestion_path="legacy", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="failed")
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))


@router.post("/api/data/upload-alert-jsonl")
async def upload_alert_jsonl(
    request: Request,
    file: UploadFile = File(...),
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    primary_mode = _resolve_primary_ingestion_mode(request)
    if not bool(getattr(request.app.state.settings, "enable_alert_jsonl_ingestion", False)):
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="disabled", alerts_ingested=0)
        return JSONResponse(
            status_code=503,
            content={
                "error": "alert_jsonl_ingestion_disabled",
                "message": "Alert JSONL ingestion is disabled by configuration.",
            },
        )
    run_id = f"run_{uuid.uuid4().hex[:16]}"
    try:
        _validate_uploaded_file_metadata(
            file,
            allowed_extensions=_JSONL_ALLOWED_EXTENSIONS,
            allowed_content_types=_JSONL_ALLOWED_CONTENT_TYPES,
            label="Alert JSONL upload",
        )
        contents = await file.read()
        _ensure_upload_size_within_limit(request, contents)
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded JSONL file is empty.")

        upload_dir = request.app.state.settings.data_dir / "uploads" / "alert_jsonl"
        upload_dir.mkdir(parents=True, exist_ok=True)
        safe_name = Path(file.filename or "alerts.jsonl").name
        file_path = upload_dir / f"{run_id}_{safe_name}"
        file_path.write_bytes(contents)
        upload_row_count = _count_non_empty_lines(contents)

        result = request.app.state.pipeline_service.run_alert_ingestion_pipeline(
            file_path=str(file_path),
            run_id=run_id,
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            canary_override=False,
            upload_row_count=upload_row_count,
        )
        payload = {
            "run_id": run_id,
            "total_alerts": int(result.get("total_rows", 0) or 0),
            "success_count": int(result.get("success_count", 0) or 0),
            "failed_count": int(result.get("failed_count", 0) or 0),
            "warning_count": int(result.get("warning_count", 0) or 0),
            "strict_mode_used": bool(result.get("strict_mode_used", False)),
            "source_system": str(result.get("source_system") or "unknown"),
            "elapsed_ms": int(result.get("elapsed_ms", 0) or 0),
            "status": str(result.get("status") or "accepted"),
            "failure_reason_category": str(result.get("failure_reason_category") or "none"),
            "ingested_alert_count": int(result.get("ingested_alert_count", result.get("success_count", 0)) or 0),
            "ingested_transaction_count": int(result.get("ingested_transaction_count", 0) or 0),
            "data_quality_inconsistency_count": int(result.get("data_quality_inconsistency_count", 0) or 0),
            "data_quality_counts": dict(result.get("data_quality_counts") or {}),
            "rollout_mode": str(result.get("rollout_mode") or "full"),
            "rollout_decision": str(result.get("rollout_decision") or "HOLD"),
            "summary": {
                "total_rows": int(result.get("total_rows", 0) or 0),
                "success_count": int(result.get("success_count", 0) or 0),
                "failed_count": int(result.get("failed_count", 0) or 0),
                "warning_count": int(result.get("warning_count", 0) or 0),
                "strict_mode_used": bool(result.get("strict_mode_used", False)),
                "source_system": str(result.get("source_system") or "unknown"),
                "elapsed_ms": int(result.get("elapsed_ms", 0) or 0),
                "status": str(result.get("status") or "accepted"),
                "rollout_mode": str(result.get("rollout_mode") or "full"),
            },
        }
        if str(payload.get("status") or "") == "rejected":
            return JSONResponse(
                status_code=400,
                content={
                    "error": "alert_jsonl_ingestion_rejected",
                    "message": "Alert JSONL ingestion rejected by validation checks.",
                    **payload,
                },
            )
        return payload
    except HTTPException:
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        raise
    except AlertIngestionValidationError as exc:
        summary = dict(getattr(exc, "summary", {}) or {})
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="failed_validation", alerts_ingested=0)
        return JSONResponse(
            status_code=400,
            content={
                "error": "alert_jsonl_failed_validation",
                "message": str(exc) or "Alert JSONL ingestion failed validation.",
                "run_id": run_id,
                "summary": summary,
                "status": str(summary.get("status") or "failed_validation"),
            },
        )
    except ValueError as exc:
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        if str(exc) == "alert_jsonl_ingestion_disabled":
            record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="disabled", alerts_ingested=0)
            return JSONResponse(
                status_code=503,
                content={
                    "error": "alert_jsonl_ingestion_disabled",
                    "message": "Alert JSONL ingestion is disabled by configuration.",
                },
            )
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))
    except Exception as exc:
        record_ingestion_path_used(ingestion_path="alert_jsonl", primary_mode=primary_mode, status="failed", alerts_ingested=0)
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))


@router.post("/api/data/upload")
async def upload_data_default(
    request: Request,
    file: UploadFile = File(...),
    ingestion_mode: str | None = Query(default=None, description="Optional override: legacy|alert_jsonl"),
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    primary_mode = _resolve_primary_ingestion_mode(request)
    selected_mode = _resolve_primary_ingestion_mode(request, override_mode=ingestion_mode) if ingestion_mode else primary_mode
    if selected_mode == "alert_jsonl":
        return await upload_alert_jsonl(request=request, file=file, tenant_id=tenant_id)

    if not _legacy_ingestion_enabled(request):
        record_legacy_ingestion_usage(endpoint="upload", status="disabled")
        record_legacy_path_access(endpoint="upload", caller="api", blocked=True)
        _log_deprecated_path_access(request=request, endpoint="upload", blocked=True, tenant_id=tenant_id)
        return JSONResponse(
            status_code=503,
            content={
                "error": "legacy_ingestion_disabled",
                "message": "Legacy ingestion has been disabled after alert-centric migration finalization.",
            },
        )
    if primary_mode == "alert_jsonl":
        logger.warning(
            "Unified upload route selected legacy path while primary mode is alert_jsonl",
            extra={"tenant_id": tenant_id, "override_mode": str(ingestion_mode or "")},
        )
    return await upload_bank_csv(request=request, file=file, tenant_id=tenant_id)


@router.post("/api/pipeline/run")
def run_pipeline(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_permissions("manager_approval")),
):
    try:
        initiated_by = str(user.get("user_id") or "system")
        return request.app.state.pipeline_service.enqueue_pipeline_run(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            initiated_by=initiated_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))


@router.get("/api/pipeline/jobs/{job_id}")
def get_pipeline_job(job_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.pipeline_service.get_job_status(tenant_id=tenant_id, job_id=job_id)


@router.post("/api/pipeline/clear")
def clear_run(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("manager_approval")),
):
    return request.app.state.pipeline_service.clear_active_run(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/run-info")
def get_run_info(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/health")
def get_model_health(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))
    return request.app.state.pipeline_service.compute_health(run_info.get("run_id") or "", tenant_id=tenant_id)


@router.post("/api/stream/replay")
def replay_stream(
    payload: StreamReplayRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_role("admin")),
):
    try:
        count = request.app.state.streaming_orchestrator.replay_topic(
            topic=payload.topic,
            start_event_id=payload.start_event_id,
            batch_size=max(1, min(payload.batch_size, 5000)),
        )
        return {"replayed_events": count, "topic": payload.topic}
    except Exception:
        record_integration_error("stream_replay")
        raise


@router.post("/api/stream/backfill")
def backfill_stream(
    payload: StreamBackfillRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_role("admin")),
):
    try:
        ids = request.app.state.streaming_backbone.backfill(
            topic=payload.topic,
            tenant_id=tenant_id,
            payloads=payload.events,
        )
        request.app.state.streaming_orchestrator.process_once(batch_size=1000)
        return {"published_events": len(ids), "topic": payload.topic}
    except Exception:
        record_integration_error("stream_backfill")
        raise


@router.post("/api/stream/rescore")
def rescore_alerts(
    payload: ModelRescoreRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_role("admin")),
):
    try:
        event_id = request.app.state.streaming_backbone.rescore(
            tenant_id=tenant_id,
            run_id=payload.run_id,
            alert_ids=payload.alert_ids,
            model_version=payload.model_version,
        )
        request.app.state.streaming_orchestrator.process_once(batch_size=1000)
        return {"event_id": event_id, "rescore_alerts": len(payload.alert_ids)}
    except Exception:
        record_integration_error("stream_rescore")
        raise


@router.get("/api/features/registry")
def list_feature_registry(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    started = time.perf_counter()
    features = request.app.state.feature_registry.list_features(tenant_id)
    record_feature_retrieval("feature_registry", time.perf_counter() - started)
    return {"features": features}


@router.post("/api/model-governance/approve")
def submit_model_governance_approval(
    payload: GovernanceApprovalRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_permissions("manager_approval")),
):
    actor = str(user.get("user_id") or "system")
    result = request.app.state.model_governance_lifecycle.submit_approval(
        tenant_id=tenant_id,
        model_version=payload.model_version,
        stage=payload.stage,
        actor_id=actor,
        decision=payload.decision,
        notes=payload.notes,
    )
    return result


@router.get("/api/model-governance/monitoring/{model_version}")
def get_model_governance_monitoring(model_version: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return {
        "items": request.app.state.model_governance_lifecycle.list_monitoring(
            tenant_id=tenant_id,
            model_version=model_version,
            limit=200,
        )
    }


# ── ML Training & Retraining ──────────────────────────────────────────────────


class TrainingTriggerRequest(BaseModel):
    initiated_by: str = "api"
    force: bool = False


class RetrainingEvaluateRequest(BaseModel):
    last_training_time: str
    model_version: str | None = None
    reference_pr_auc: float | None = None
    reference_ece: float | None = None
    force: bool = False
    initiated_by: str = "api"


@router.post("/api/ml/training/trigger")
def trigger_training_run(
    payload: TrainingTriggerRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_permissions("manager_approval")),
) -> dict:
    """Manually trigger a supervised training run for the tenant's models."""
    training_service = getattr(request.app.state, "training_run_service", None)
    if training_service is None:
        raise HTTPException(status_code=503, detail="Training service not configured")
    try:
        result = training_service.run(
            tenant_id=tenant_id,
            initiated_by=str(user.get("user_id") or "system"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))
    return result


@router.get("/api/ml/training-runs")
def list_training_runs(
    request: Request,
    limit: int = 50,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    """List recent training runs from the training_runs table."""
    from sqlalchemy import text

    limit = max(1, min(int(limit), 200))
    try:
        with request.app.state.repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, training_run_id, snapshot_id, status,
                           initiated_by, dataset_hash, row_count,
                           feature_schema_version, escalation_model_version,
                           time_model_version, pr_auc, roc_auc,
                           suspicious_capture_top_20pct, ece_after_calibration,
                           error_message, started_at, completed_at
                    FROM training_runs
                    WHERE tenant_id = :tenant_id
                    ORDER BY COALESCE(completed_at, started_at) DESC NULLS LAST
                    LIMIT :limit
                    """
                ),
                {"tenant_id": tenant_id, "limit": limit},
            ).fetchall()
    except Exception as exc:
        return {"training_runs": [], "error": "training_runs table unavailable: " + str(exc)}

    runs = [
        {
            "id": str(r[0]),
            "training_run_id": r[1],
            "snapshot_id": r[2],
            "status": r[3],
            "initiated_by": r[4],
            "dataset_hash": r[5],
            "row_count": r[6],
            "feature_schema_version": r[7],
            "escalation_model_version": r[8],
            "time_model_version": r[9],
            "pr_auc": float(r[10]) if r[10] is not None else None,
            "roc_auc": float(r[11]) if r[11] is not None else None,
            "suspicious_capture_top_20pct": float(r[12]) if r[12] is not None else None,
            "ece_after_calibration": float(r[13]) if r[13] is not None else None,
            "error_message": r[14],
            "started_at": str(r[15]) if r[15] else None,
            "completed_at": str(r[16]) if r[16] else None,
        }
        for r in rows
    ]
    return {"training_runs": runs, "total": len(runs)}


@router.get("/api/ml/training-runs/{run_id}")
def get_training_run(
    run_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    """Fetch the full record for a single training run including metadata_json."""
    from sqlalchemy import text

    try:
        with request.app.state.repository.session(tenant_id=tenant_id) as session:
            row = session.execute(
                text(
                    """
                    SELECT id, training_run_id, snapshot_id, status,
                           initiated_by, dataset_hash, row_count,
                           feature_schema_version, cutoff_timestamp,
                           escalation_model_version, time_model_version,
                           pr_auc, roc_auc, suspicious_capture_top_20pct,
                           ece_after_calibration, metadata_json,
                           error_message, started_at, completed_at
                    FROM training_runs
                    WHERE tenant_id = :tenant_id
                      AND (id = :run_id OR training_run_id = :run_id)
                    LIMIT 1
                    """
                ),
                {"tenant_id": tenant_id, "run_id": run_id},
            ).fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="training_runs table unavailable: " + str(exc))

    if not row:
        raise HTTPException(status_code=404, detail="Training run not found")

    return {
        "id": str(row[0]),
        "training_run_id": row[1],
        "snapshot_id": row[2],
        "status": row[3],
        "initiated_by": row[4],
        "dataset_hash": row[5],
        "row_count": row[6],
        "feature_schema_version": row[7],
        "cutoff_timestamp": str(row[8]) if row[8] else None,
        "escalation_model_version": row[9],
        "time_model_version": row[10],
        "pr_auc": float(row[11]) if row[11] is not None else None,
        "roc_auc": float(row[12]) if row[12] is not None else None,
        "suspicious_capture_top_20pct": float(row[13]) if row[13] is not None else None,
        "ece_after_calibration": float(row[14]) if row[14] is not None else None,
        "metadata": row[15],
        "error_message": row[16],
        "started_at": str(row[17]) if row[17] else None,
        "completed_at": str(row[18]) if row[18] else None,
    }


@router.post("/api/ml/retraining/evaluate")
def evaluate_retraining(
    payload: RetrainingEvaluateRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_permissions("manager_approval")),
) -> dict:
    """Evaluate retraining triggers and optionally launch a training run.

    Pass force=true to bypass checks and retrain immediately.
    """
    import datetime as _dt

    scheduler = getattr(request.app.state, "retraining_scheduler", None)
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Retraining scheduler not configured")

    try:
        last_training_time = _dt.datetime.fromisoformat(
            str(payload.last_training_time).replace("Z", "+00:00")
        )
        if last_training_time.tzinfo is None:
            last_training_time = last_training_time.replace(tzinfo=_dt.timezone.utc)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid last_training_time; expected ISO-8601 datetime")

    try:
        result = scheduler.evaluate_and_trigger(
            tenant_id=tenant_id,
            last_training_time=last_training_time,
            model_version=payload.model_version,
            reference_pr_auc=payload.reference_pr_auc,
            reference_ece=payload.reference_ece,
            force=payload.force,
            initiated_by=str(user.get("user_id") or "system"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_error_detail(request, exc))

    return result


@router.get("/api/ml/monitoring/performance")
def get_performance_report(
    request: Request,
    model_version: str | None = None,
    lookback_days: int = 90,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    """Return PR-AUC, precision@k, capture rate and breakdown by typology."""
    monitoring_service = getattr(request.app.state, "model_monitoring_service", None)
    if monitoring_service is None:
        raise HTTPException(status_code=503, detail="Monitoring service not configured")
    return monitoring_service.compute_performance_report(
        tenant_id=tenant_id,
        model_version=model_version,
        lookback_days=lookback_days,
    )


@router.get("/api/ml/monitoring/calibration")
def get_calibration_report(
    request: Request,
    model_version: str | None = None,
    lookback_days: int = 90,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    """Return ECE, reliability diagram, and calibration drift."""
    monitoring_service = getattr(request.app.state, "model_monitoring_service", None)
    if monitoring_service is None:
        raise HTTPException(status_code=503, detail="Monitoring service not configured")
    return monitoring_service.compute_calibration_report(
        tenant_id=tenant_id,
        model_version=model_version,
        lookback_days=lookback_days,
    )


@router.get("/api/ml/monitoring/business")
def get_business_report(
    request: Request,
    model_version: str | None = None,
    lookback_days: int = 90,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    """Return queue compression, analyst hours saved, SAR capture rate, and typology breakdown."""
    monitoring_service = getattr(request.app.state, "model_monitoring_service", None)
    if monitoring_service is None:
        raise HTTPException(status_code=503, detail="Monitoring service not configured")
    return monitoring_service.compute_business_report(
        tenant_id=tenant_id,
        model_version=model_version,
        lookback_days=lookback_days,
    )
