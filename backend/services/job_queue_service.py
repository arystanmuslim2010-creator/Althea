from __future__ import annotations

import importlib
import inspect
import logging
from typing import Any

from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache

logger = logging.getLogger("althea.job_queue")


class JobQueueService:
    def __init__(self, repository: EnterpriseRepository, cache: RedisCache) -> None:
        self._repository = repository
        self._cache = cache

    def set_status(self, job_id: str, payload: dict[str, Any]) -> None:
        self._cache.set_json(f"job:{job_id}", payload, ttl_seconds=60 * 60 * 24)

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        return self._cache.get_json(f"job:{job_id}")

    def queue_depth(self, queue_name: str) -> int:
        return self._cache.queue_depth(queue_name)

    def enqueue(
        self,
        import_path: str,
        kwargs: dict[str, Any],
        queue_mode: str,
        redis_url: str,
        queue_name: str,
        job_timeout_seconds: int = 900,
        queue_depth_warning_threshold: int = 5000,
    ) -> None:
        normalized = (queue_mode or "").lower().strip()
        if normalized != "rq":
            raise RuntimeError("Only RQ queue mode is supported.")

        try:
            depth = int(self.queue_depth(queue_name))
            if depth >= int(queue_depth_warning_threshold):
                logger.warning(
                    "Queue depth above warning threshold before enqueue",
                    extra={
                        "queue_name": queue_name,
                        "queue_depth": depth,
                        "threshold": int(queue_depth_warning_threshold),
                    },
                )
        except Exception as exc:
            logger.warning("Unable to read queue depth before enqueue", extra={"queue_name": queue_name, "error": str(exc)})

        module_name, func_name = import_path.rsplit(".", 1)
        target_module = importlib.import_module(module_name)
        target = getattr(target_module, func_name)
        try:
            import redis
            from rq import Queue as RQQueue
            from rq import Retry
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"RQ queue mode import failed: {exc}") from exc

        connection = redis.Redis.from_url(redis_url)
        queue = RQQueue(queue_name, connection=connection)
        pipeline_job_id = kwargs.get("job_id")
        timeout_seconds = max(60, int(job_timeout_seconds or 900))
        enqueue_kwargs: dict[str, Any] = {
            "func": target,
            "kwargs": kwargs,
            "job_id": str(pipeline_job_id) if pipeline_job_id else None,
            "retry": Retry(max=3, interval=[10, 30, 60]),
            "failure_ttl": 60 * 60 * 24 * 7,
        }
        timeout_param = (
            "job_timeout"
            if "job_timeout" in inspect.signature(queue.enqueue_call).parameters
            else "timeout"
        )
        enqueue_kwargs[timeout_param] = timeout_seconds
        queue.enqueue_call(**enqueue_kwargs)
