from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from core.observability import (
    record_enrichment_records_failed,
    record_enrichment_records_written,
    record_enrichment_sync_attempt,
    record_enrichment_sync_duration,
    record_worker_task,
)
from core.dependencies import (
    get_cache,
    get_dead_letter_service,
    get_enrichment_health_service,
    get_enrichment_sync_service,
)


def sync_enrichment_source_job(tenant_id: str, source_name: str, full_backfill: bool = False, actor_id: str | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    record_enrichment_sync_attempt(source_name, "started")
    try:
        result = get_enrichment_sync_service().sync_source(
            tenant_id=tenant_id,
            source_name=source_name,
            full_backfill=bool(full_backfill),
            actor_id=actor_id,
        )
        record_enrichment_sync_attempt(source_name, "completed")
        record_enrichment_records_written(source_name, int(result.get("records_written") or 0))
        record_enrichment_sync_duration(source_name, time.perf_counter() - started)
        record_worker_task("enrichment", "completed", time.perf_counter() - started)
        return result
    except Exception:
        record_enrichment_sync_attempt(source_name, "failed")
        record_enrichment_records_failed(source_name, 1)
        record_worker_task("enrichment", "failed", time.perf_counter() - started)
        raise


def rebuild_enrichment_health_job(tenant_id: str, source_name: str | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        snapshots = get_enrichment_health_service().rebuild(tenant_id=tenant_id, source_name=source_name)
        record_worker_task("enrichment-health", "completed", time.perf_counter() - started)
        return {"status": "completed", "snapshots": snapshots}
    except Exception:
        record_worker_task("enrichment-health", "failed", time.perf_counter() - started)
        raise


def backfill_internal_case_history_job(tenant_id: str, actor_id: str | None = None) -> dict[str, Any]:
    return get_enrichment_sync_service().backfill_internal_targets(
        tenant_id=tenant_id,
        targets=["case_actions"],
        actor_id=actor_id,
    )


def backfill_internal_alert_outcomes_job(tenant_id: str, actor_id: str | None = None) -> dict[str, Any]:
    return get_enrichment_sync_service().backfill_internal_targets(
        tenant_id=tenant_id,
        targets=["alert_outcomes"],
        actor_id=actor_id,
    )


def replay_enrichment_dead_letter_job(tenant_id: str, item_ids: list[str]) -> dict[str, Any]:
    items = get_dead_letter_service().replay(tenant_id=tenant_id, item_ids=item_ids)
    return {"status": "completed", "replayed": len(items), "items": items}


def run_enrichment_worker() -> None:
    import redis
    from rq import SimpleWorker, Worker
    from rq.timeouts import TimerDeathPenalty

    redis_url = os.getenv("ALTHEA_REDIS_URL", "redis://localhost:6379/0")
    queue_name = os.getenv("ALTHEA_ENRICHMENT_RQ_QUEUE") or os.getenv("ALTHEA_RQ_QUEUE", "althea-enrichment")
    connection = redis.Redis.from_url(redis_url)
    cache = get_cache()

    def _heartbeat_loop() -> None:
        while True:
            cache.set_json(
                "heartbeat:worker:enrichment",
                {
                    "worker": "enrichment",
                    "queue": queue_name,
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
                ttl_seconds=30,
            )
            time.sleep(10)

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="enrichment-worker-heartbeat")
    heartbeat_thread.start()

    worker_cls = SimpleWorker if os.name == "nt" else Worker
    worker = worker_cls([queue_name], connection=connection)
    if os.name == "nt":
        worker.death_penalty_class = TimerDeathPenalty
    worker.work(with_scheduler=True)


if __name__ == "__main__":
    run_enrichment_worker()
