from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from core.security import get_authenticated_tenant_id

router = APIRouter(prefix="/api/pilot", tags=["pilot"])


@router.get("/summary")
def get_pilot_summary(
    request: Request,
    dataset_name: str | None = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict:
    service = getattr(request.app.state, "pilot_metrics_service", None)
    pipeline_service = getattr(request.app.state, "pipeline_service", None)
    current_user = getattr(request.state, "current_user", None) or {}
    user_scope = current_user.get("user_id") or "public"
    run_info = pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=user_scope) if pipeline_service else {}
    run_id = run_info.get("run_id")
    if service is None or not run_id:
        raise HTTPException(status_code=404, detail="No active run available for pilot summary")
    return service.summarize_run(
        tenant_id=tenant_id,
        run_id=run_id,
        dataset_name=dataset_name or str(run_id),
    )
