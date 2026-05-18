from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from core.security import get_authenticated_tenant_id

router = APIRouter(prefix="/api/evaluation", tags=["evaluation"])


class EvaluationRequest(BaseModel):
    dataset_name: str = Field(default="ad_hoc_dataset", min_length=1)
    records: list[dict[str, Any]] = Field(default_factory=list)
    label_field: str = Field(default="evaluation_label_is_sar", min_length=1)
    althea_score_field: str = Field(default="risk_score", min_length=1)


@router.post("/ranking")
def evaluate_ranking(
    body: EvaluationRequest,
    request: Request,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    service = getattr(request.app.state, "evaluation_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="Evaluation service unavailable")
    return service.evaluate_records(
        dataset_name=body.dataset_name,
        records=body.records,
        label_field=body.label_field,
        althea_score_field=body.althea_score_field,
    )


@router.get("/current-run")
def evaluate_current_run(
    request: Request,
    label_field: str = "evaluation_label_is_sar",
    althea_score_field: str = "risk_score",
    dataset_name: str | None = None,
    tenant_id: str = Depends(get_authenticated_tenant_id),
) -> dict[str, Any]:
    service = getattr(request.app.state, "evaluation_service", None)
    pipeline_service = getattr(request.app.state, "pipeline_service", None)
    current_user = getattr(request.state, "current_user", None) or {}
    user_scope = current_user.get("user_id") or "public"
    run_info = pipeline_service.get_run_info(tenant_id=tenant_id, user_scope=user_scope) if pipeline_service else {}
    run_id = run_info.get("run_id")
    if service is None or not run_id:
        raise HTTPException(status_code=404, detail="No active run available for evaluation")
    return service.evaluate_run(
        tenant_id=tenant_id,
        run_id=run_id,
        dataset_name=dataset_name or str(run_id),
        label_field=label_field,
        althea_score_field=althea_score_field,
    )
