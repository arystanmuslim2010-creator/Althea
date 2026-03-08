from __future__ import annotations

from core.dependencies import get_pipeline_service


def run_pipeline_job(job_id: str, tenant_id: str, user_scope: str) -> dict:
    return get_pipeline_service().execute_pipeline_job(job_id=job_id, tenant_id=tenant_id, user_scope=user_scope)
