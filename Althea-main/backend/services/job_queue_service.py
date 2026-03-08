from __future__ import annotations

import importlib
import logging
import threading
import time
from queue import Empty, Queue
from typing import Any

from core.observability import record_worker_task
from storage.postgres_repository import EnterpriseRepository
from storage.redis_cache import RedisCache

logger = logging.getLogger("althea.job_queue")


class JobQueueService:
    def __init__(self, repository: EnterpriseRepository, cache: RedisCache) -> None:
        self._repository = repository
        self._cache = cache
        self._local_jobs: Queue[tuple[str, dict[str, Any]]] = Queue()
        self._local_threads: list[threading.Thread] = []
        self._local_started = False

    def set_status(self, job_id: str, payload: dict[str, Any]) -> None:
        self._cache.set_json(f"job:{job_id}", payload, ttl_seconds=60 * 60 * 24)

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        return self._cache.get_json(f"job:{job_id}")

    def start_threaded_workers(self, worker_count: int = 2) -> None:
        if self._local_started:
            return
        self._local_started = True
        for index in range(max(1, worker_count)):
            thread = threading.Thread(target=self._threaded_worker_loop, name=f"althea-local-worker-{index+1}", daemon=True)
            thread.start()
            self._local_threads.append(thread)

    def _threaded_worker_loop(self) -> None:
        while True:
            try:
                import_path, kwargs = self._local_jobs.get(timeout=1.0)
            except Empty:
                continue
            started = time.perf_counter()
            status = "completed"
            try:
                module_name, func_name = import_path.rsplit(".", 1)
                target_module = importlib.import_module(module_name)
                target = getattr(target_module, func_name)
                target(**kwargs)
            except Exception:
                status = "failed"
                logger.exception("Threaded worker failed while executing %s", import_path)
            finally:
                record_worker_task(
                    worker_name=threading.current_thread().name,
                    status=status,
                    duration_seconds=time.perf_counter() - started,
                )
                self._local_jobs.task_done()

    def enqueue(self, import_path: str, kwargs: dict[str, Any], queue_mode: str, redis_url: str, queue_name: str) -> None:
        normalized = (queue_mode or "").lower().strip()
        if normalized == "rq":
            module_name, func_name = import_path.rsplit(".", 1)
            target_module = importlib.import_module(module_name)
            target = getattr(target_module, func_name)
            try:
                import redis
                from rq import Queue as RQQueue
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("RQ queue mode requested but rq/redis are not installed") from exc
            connection = redis.Redis.from_url(redis_url)
            queue = RQQueue(queue_name, connection=connection)
            queue.enqueue(target, **kwargs)
            return

        # Threaded mode (default for local dev when RQ is unavailable).
        self.start_threaded_workers()
        self._local_jobs.put((import_path, kwargs))
