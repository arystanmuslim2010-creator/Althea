from __future__ import annotations

import json
import os
import time
from typing import Any

import pandas as pd

from core.observability import record_pipeline_run
from core.dependencies import get_pipeline_service


def run_pipeline_job(job_id: str, tenant_id: str, user_scope: str) -> dict:
    service = get_pipeline_service()
    # Worker jobs carry tenant_id in payload; set DB context before any query for RLS-safe execution.
    service._repository.set_tenant_context(tenant_id)
    job = service._repository.get_pipeline_job(tenant_id=tenant_id, job_id=job_id)
    if not job:
        # Stale Redis queue entries can outlive DB records during local restarts.
        payload = {
            "job_id": job_id,
            "status": "discarded",
            "detail": f"Pipeline job {job_id} is missing for tenant {tenant_id}; likely stale queue item.",
        }
        service._job_queue.set_status(job_id, payload)
        return payload
    return service.execute_pipeline_job(job_id=job_id, tenant_id=tenant_id, user_scope=user_scope)


def execute_pipeline_job(service: Any, job_id: str, tenant_id: str, user_scope: str) -> dict[str, Any]:
    """
    Worker-side pipeline execution entrypoint.
    Heavy pipeline processing must stay in workers and never block API handlers.
    """
    existing = service._repository.get_pipeline_job(tenant_id=tenant_id, job_id=job_id)
    if existing and str(existing.get("status", "")).lower() == "completed" and existing.get("run_id"):
        return {
            "job_id": job_id,
            "status": "completed",
            "run_id": existing.get("run_id"),
            "alerts": int(existing.get("row_count") or 0),
        }
    service._repository.set_tenant_context(tenant_id)

    started = time.perf_counter()
    service._repository.update_pipeline_job(
        job_id,
        tenant_id=tenant_id,
        status="running",
        started_at=pd.Timestamp.utcnow().to_pydatetime(),
    )
    service._job_queue.set_status(job_id, {"job_id": job_id, "status": "running"})
    try:
        context = service.get_runtime_context(tenant_id, user_scope)
        run_source = str(context.get("run_source") or "")
        runtime_stream = service._ingestion_service.stream_runtime_dataset(
            context=context,
            batch_size=service._settings.pipeline_batch_size,
        )
        completed_chunks = service._repository.get_completed_pipeline_chunk_indexes(tenant_id=tenant_id, job_id=job_id)
        run_id = str(existing.get("run_id") or f"run_{job_id.replace('job_', '')}")
        persisted_count = 0
        model_version = "unknown"
        score_values: list[float] = []
        max_monitoring_scores = 50000

        for chunk_index, chunk in enumerate(runtime_stream):
            if chunk_index in completed_chunks:
                continue
            chunk_run_id, chunk_persisted, chunk_model_version, chunk_scores = service._run_pipeline(
                chunk,
                tenant_id=tenant_id,
                run_id=run_id,
                run_source=run_source,
            )
            run_id = chunk_run_id
            persisted_count += int(chunk_persisted)
            model_version = chunk_model_version or model_version
            if len(score_values) < max_monitoring_scores:
                remaining = max_monitoring_scores - len(score_values)
                score_values.extend(list(chunk_scores or [])[:remaining])
            service._repository.upsert_pipeline_checkpoint(
                tenant_id=tenant_id,
                job_id=job_id,
                chunk_index=chunk_index,
                processed_rows=int(len(chunk)),
                run_id=run_id,
                status="completed",
            )

        if persisted_count <= 0 and not completed_chunks:
            raise ValueError("Pipeline produced no alerts from streamed dataset.")
        if persisted_count <= 0 and completed_chunks:
            persisted_count = int(existing.get("row_count") or 0)

        monitoring = service._model_monitoring_service.record_run_monitoring(
            tenant_id=tenant_id,
            run_id=run_id,
            model_version=model_version,
            scores=score_values,
        )
        service._repository.update_pipeline_job(
            job_id,
            tenant_id=tenant_id,
            status="completed",
            run_id=run_id,
            row_count=persisted_count,
            completed_at=pd.Timestamp.utcnow().to_pydatetime(),
        )
        service._repository.upsert_runtime_context(
            tenant_id=tenant_id,
            user_scope=user_scope,
            active_run_id=run_id,
            active_job_id=job_id,
        )
        service._publish_pipeline_events(
            tenant_id=tenant_id,
            job_id=job_id,
            run_id=run_id,
            alert_count=persisted_count,
            model_version=model_version,
            monitoring_metrics=monitoring.get("metrics", {}),
        )
        record_pipeline_run(
            status="completed",
            duration_seconds=time.perf_counter() - started,
            alerts_processed=persisted_count,
        )
        payload = {"job_id": job_id, "status": "completed", "run_id": run_id, "alerts": persisted_count}
        service._job_queue.set_status(job_id, payload)
        return payload
    except Exception as exc:
        service._repository.upsert_pipeline_checkpoint(
            tenant_id=tenant_id,
            job_id=job_id,
            chunk_index=-1,
            processed_rows=0,
            run_id=existing.get("run_id") if existing else None,
            status="failed",
        )
        record_pipeline_run(status="failed", duration_seconds=time.perf_counter() - started, alerts_processed=0)
        dead_letter_path = service._settings.dead_letter_dir / f"{job_id}_dead_letter.json"
        dead_letter_path.parent.mkdir(parents=True, exist_ok=True)
        dead_letter_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "tenant_id": tenant_id,
                    "user_scope": user_scope,
                    "error": str(exc),
                    "ts": pd.Timestamp.utcnow().isoformat(),
                },
                ensure_ascii=True,
            ),
            encoding="utf-8",
        )
        service._repository.update_pipeline_job(
            job_id,
            tenant_id=tenant_id,
            status="failed",
            error_message=str(exc),
            completed_at=pd.Timestamp.utcnow().to_pydatetime(),
        )
        payload = {"job_id": job_id, "status": "failed", "detail": str(exc)}
        service._job_queue.set_status(job_id, payload)
        raise


def run_rq_worker() -> None:
    import redis
    from rq import SimpleWorker, Worker
    from rq.timeouts import TimerDeathPenalty

    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379/0")
    queue_name = os.getenv("ALTHEA_RQ_QUEUE", "althea-pipeline")
    print(f"[pipeline_worker] starting queue={queue_name} redis={redis_url}", flush=True)
    connection = redis.Redis.from_url(redis_url)
    worker_cls = SimpleWorker if os.name == "nt" else Worker
    worker = worker_cls([queue_name], connection=connection)
    if os.name == "nt":
        worker.death_penalty_class = TimerDeathPenalty
    worker.work(with_scheduler=True)


if __name__ == "__main__":
    run_rq_worker()
