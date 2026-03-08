from __future__ import annotations

import os

from core.dependencies import get_pipeline_service


def run_pipeline_job(job_id: str, tenant_id: str, user_scope: str) -> dict:
    return get_pipeline_service().execute_pipeline_job(job_id=job_id, tenant_id=tenant_id, user_scope=user_scope)


def run_rq_worker() -> None:
    import redis
    from rq import Worker

    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379/0")
    queue_name = os.getenv("ALTHEA_RQ_QUEUE", "althea-pipeline")
    connection = redis.Redis.from_url(redis_url)
    worker = Worker([queue_name], connection=connection)
    worker.work(with_scheduler=True)


if __name__ == "__main__":
    run_rq_worker()
