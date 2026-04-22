from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from core.security import get_authenticated_tenant_id, require_permissions, require_role

router = APIRouter(tags=["enrichment"])


class EnrichmentSyncRequest(BaseModel):
    full_backfill: bool = False


class InternalBackfillRequest(BaseModel):
    targets: list[str]


class MasterDataOverrideRequest(BaseModel):
    override_type: str
    left_entity_type: str | None = None
    left_entity_id: str | None = None
    right_entity_type: str | None = None
    right_entity_id: str | None = None
    target_entity_type: str | None = None
    target_entity_id: str | None = None
    source_name: str | None = None
    external_id: str | None = None
    reason: str | None = None


class DeadLetterReplayRequest(BaseModel):
    item_ids: list[str]


def _enqueue_job(request: Request, *, import_path: str, kwargs: dict, job_name: str) -> dict:
    job_id = f"{job_name}_{uuid.uuid4().hex[:16]}"
    settings = request.app.state.settings
    queue_name = str(
        getattr(settings, "enrichment_rq_queue_name", None)
        or getattr(settings, "rq_queue_name", "althea-pipeline")
    )
    request.app.state.job_queue_service.set_status(job_id, {"job_id": job_id, "status": "queued"})
    request.app.state.job_queue_service.enqueue(
        import_path=import_path,
        kwargs=kwargs,
        queue_mode=settings.queue_mode,
        redis_url=settings.redis_url,
        queue_name=queue_name,
        job_timeout_seconds=settings.rq_job_timeout_seconds,
    )
    return {"job_id": job_id, "status": "queued"}


@router.get("/internal/enrichment/sources")
def list_enrichment_sources(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"sources": request.app.state.enrichment_sync_service.list_sources()}


@router.get("/internal/enrichment/status")
def get_enrichment_status(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return request.app.state.enrichment_health_service.status(tenant_id=tenant_id)


@router.post("/internal/enrichment/sources/{source_name}/sync")
def sync_enrichment_source(
    source_name: str,
    payload: EnrichmentSyncRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_role("admin")),
):
    valid_sources = {item["source_name"] for item in request.app.state.enrichment_sync_service.list_sources()}
    if source_name not in valid_sources:
        raise HTTPException(status_code=404, detail="Unknown enrichment source")
    return _enqueue_job(
        request,
        import_path="workers.enrichment_worker.sync_enrichment_source_job",
        kwargs={
            "tenant_id": tenant_id,
            "source_name": source_name,
            "full_backfill": bool(payload.full_backfill),
            "actor_id": str(user.get("user_id") or "system"),
        },
        job_name=f"enrichment_sync_{source_name}",
    )


@router.post("/internal/enrichment/backfill/internal")
def backfill_internal_enrichment(
    payload: InternalBackfillRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_role("admin")),
):
    targets = {str(item or "").strip() for item in payload.targets}
    jobs = []
    if "case_actions" in targets:
        jobs.append(
            _enqueue_job(
                request,
                import_path="workers.enrichment_worker.backfill_internal_case_history_job",
                kwargs={"tenant_id": tenant_id, "actor_id": str(user.get("user_id") or "system")},
                job_name="enrichment_backfill_case_actions",
            )
        )
    if "alert_outcomes" in targets:
        jobs.append(
            _enqueue_job(
                request,
                import_path="workers.enrichment_worker.backfill_internal_alert_outcomes_job",
                kwargs={"tenant_id": tenant_id, "actor_id": str(user.get("user_id") or "system")},
                job_name="enrichment_backfill_alert_outcomes",
            )
        )
    return {"status": "queued", "jobs": jobs}


@router.get("/internal/enrichment/context/{alert_id}")
def get_enrichment_context(
    alert_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return request.app.state.enrichment_repository.get_enrichment_context_snapshot(tenant_id=tenant_id, alert_id=alert_id)


@router.post("/internal/master-data/rebuild-links")
def rebuild_master_data_links(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_role("admin")),
):
    result = request.app.state.entity_resolution_service.rebuild_links(tenant_id=tenant_id)
    request.app.state.enrichment_audit_service.record(
        tenant_id=tenant_id,
        source_name="master_data",
        action="rebuild_links",
        actor_id=str(user.get("user_id") or "system"),
        status="completed",
        details=result,
    )
    return result


@router.get("/internal/master-data/entity/{entity_type}/{entity_id}")
def get_master_data_entity(
    entity_type: str,
    entity_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    try:
        return request.app.state.master_data_service.get_entity(tenant_id=tenant_id, entity_type=entity_type, entity_id=entity_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/internal/master-data/aliases/{source_name}/{external_id}")
def get_master_data_aliases(
    source_name: str,
    external_id: str,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {
        "aliases": request.app.state.enrichment_repository.list_entity_aliases(
            tenant_id=tenant_id,
            source_name=source_name,
            external_id=external_id,
        )
    }


@router.post("/internal/master-data/overrides")
def create_master_data_override(
    payload: MasterDataOverrideRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_role("admin")),
):
    return request.app.state.master_data_service.create_override(
        tenant_id=tenant_id,
        payload={**payload.model_dump(), "created_by": str(user.get("user_id") or "system")},
    )


@router.get("/internal/master-data/overrides")
def list_master_data_overrides(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"overrides": request.app.state.master_data_service.list_overrides(tenant_id=tenant_id)}


@router.get("/internal/enrichment/audit")
def list_enrichment_audit(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"items": request.app.state.enrichment_audit_service.list_logs(tenant_id=tenant_id)}


@router.get("/internal/enrichment/dead-letter")
def list_enrichment_dead_letter(
    request: Request,
    source_name: str | None = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"items": request.app.state.dead_letter_service.list_items(tenant_id=tenant_id, source_name=source_name)}


@router.post("/internal/enrichment/dead-letter/replay")
def replay_enrichment_dead_letter(
    payload: DeadLetterReplayRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    user: dict = Depends(require_role("admin")),
):
    return _enqueue_job(
        request,
        import_path="workers.enrichment_worker.replay_enrichment_dead_letter_job",
        kwargs={"tenant_id": tenant_id, "item_ids": payload.item_ids},
        job_name="enrichment_dead_letter_replay",
    )


@router.get("/internal/enrichment/schema-drift")
def get_enrichment_schema_drift(
    request: Request,
    source_name: str | None = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"items": request.app.state.schema_drift_service.list_registry(source_name=source_name)}


@router.get("/internal/enrichment/coverage")
def get_enrichment_coverage(
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
    _: dict = Depends(require_permissions("view_system_logs")),
):
    return {"items": request.app.state.enrichment_repository.list_coverage_snapshots(tenant_id=tenant_id)}
