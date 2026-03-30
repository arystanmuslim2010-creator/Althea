from __future__ import annotations

import time

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel

from core.observability import metrics_response, record_feature_retrieval, record_integration_error
from core.security import get_authenticated_tenant_id
from services.ingestion_service import IngestionError

router = APIRouter(tags=["pipeline"])


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


def _user_scope(request: Request) -> str:
    return request.headers.get("X-User-Scope") or "public"


@router.get("/")
def root() -> dict:
    return {"status": "ok", "app": "AML Alert Prioritization API"}


@router.get("/health")
def health_check(request: Request) -> dict:
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
        details["database"] = str(exc)

    try:
        request.app.state.cache.ping()
        checks["redis"] = True
    except Exception as exc:
        details["redis"] = str(exc)

    try:
        depth = request.app.state.job_queue_service.queue_depth(request.app.state.settings.rq_queue_name)
        checks["worker_queue"] = depth >= 0
    except Exception as exc:
        details["worker_queue"] = str(exc)
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
        details["worker_heartbeat"] = str(exc)

    try:
        tenant_id = request.app.state.settings.default_tenant_id
        request.app.state.ml_service.list_versions(tenant_id)
        checks["model_registry"] = True
    except Exception as exc:
        details["model_registry"] = str(exc)

    try:
        tenant_id = request.app.state.settings.default_tenant_id
        request.app.state.feature_registry.list_features(tenant_id=tenant_id)
        checks["feature_store"] = True
    except Exception as exc:
        details["feature_store"] = str(exc)

    ok = all(checks.values())
    status = "healthy" if ok else "degraded"
    return {"ok": ok, "status": status, "checks": checks, "queue_depth": depth, "details": details}


@router.get("/metrics")
def metrics(request: Request):
    return metrics_response(request.app.state.metrics)


@router.post("/api/data/generate-synthetic")
def generate_synthetic(n_rows: int = 400, request: Request = None, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.ingestion_service.generate_synthetic(tenant_id=tenant_id, user_scope=_user_scope(request), n_rows=n_rows)


@router.post("/api/data/upload-csv")
async def upload_csv(request: Request, file: UploadFile = File(...), tenant_id: str = Depends(get_authenticated_tenant_id)):
    try:
        contents = await file.read()
        return request.app.state.ingestion_service.upload_transactions_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
    except IngestionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/data/generate-bank-csv")
def generate_bank_csv(
    n_rows: int = 1000,
    seed: int = 42,
    request: Request = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
):
    try:
        out = request.app.state.ingestion_service.generate_bank_alerts_csv(n_rows=max(1, min(n_rows, 10000)), seed=seed)
        out_path = request.app.state.settings.data_dir / f"bank_alerts_{len(out)}.csv"
        out.to_csv(out_path, index=False, encoding="utf-8")
        return {"rows": len(out), "path": str(out_path), "message": f"Saved to {out_path}. Upload this file via Upload bank CSV."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/data/upload-bank-csv")
async def upload_bank_csv(request: Request, file: UploadFile = File(...), tenant_id: str = Depends(get_authenticated_tenant_id)):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        return request.app.state.ingestion_service.upload_bank_csv(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            raw_bytes=contents,
        )
    except IngestionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/pipeline/run")
def run_pipeline(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    try:
        initiated_by = request.headers.get("X-Actor")
        return request.app.state.pipeline_service.enqueue_pipeline_run(
            tenant_id=tenant_id,
            user_scope=_user_scope(request),
            initiated_by=initiated_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/pipeline/jobs/{job_id}")
def get_pipeline_job(job_id: str, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.pipeline_service.get_job_status(tenant_id=tenant_id, job_id=job_id)


@router.post("/api/pipeline/clear")
def clear_run(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.pipeline_service.clear_active_run(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/run-info")
def get_run_info(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    return request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))


@router.get("/api/health")
def get_model_health(request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
    run_info = request.app.state.pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=_user_scope(request))
    return request.app.state.pipeline_service.compute_health(run_info.get("run_id") or "", tenant_id=tenant_id)


@router.post("/api/stream/replay")
def replay_stream(payload: StreamReplayRequest, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
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
def backfill_stream(payload: StreamBackfillRequest, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
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
def rescore_alerts(payload: ModelRescoreRequest, request: Request, tenant_id: str = Depends(get_authenticated_tenant_id)):
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
):
    actor = request.headers.get("X-Actor") or "system"
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
