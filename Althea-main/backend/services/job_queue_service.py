from __future__ import annotations

import importlib
from typing import Any

from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache


class JobQueueService:
    def __init__(self, repository: EnterpriseRepository, cache: RedisCache) -> None:
        self._repository = repository
        self._cache = cache

    def set_status(self, job_id: str, payload: dict[str, Any]) -> None:
        self._cache.set_json(f"job:{job_id}", payload, ttl_seconds=60 * 60 * 24)

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        return self._cache.get_json(f"job:{job_id}")

    def enqueue(self, import_path: str, kwargs: dict[str, Any], queue_mode: str, redis_url: str, queue_name: str) -> None:
        normalized = (queue_mode or "").lower().strip()
        if normalized != "rq":
            raise RuntimeError("Only RQ queue mode is supported.")

        module_name, func_name = import_path.rsplit(".", 1)
        target_module = importlib.import_module(module_name)
        target = getattr(target_module, func_name)
        try:
            import redis
            from rq import Queue as RQQueue
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("RQ queue mode requested but rq/redis are not installed") from exc

        connection = redis.Redis.from_url(redis_url)
        queue = RQQueue(queue_name, connection=connection)
        queue.enqueue(target, **kwargs)

