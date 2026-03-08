from __future__ import annotations

import os

import pandas as pd

from core.dependencies import get_inference_service


def score_feature_batch(tenant_id: str, feature_rows: list[dict]) -> dict:
    feature_frame = pd.DataFrame(feature_rows)
    return get_inference_service().predict(tenant_id=tenant_id, feature_frame=feature_frame)


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
