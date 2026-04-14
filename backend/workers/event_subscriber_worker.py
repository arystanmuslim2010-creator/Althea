from __future__ import annotations

import logging
import math
import time
import threading
from datetime import datetime, timezone
from typing import Any

from core.dependencies import get_cache, get_event_bus, get_pipeline_service, get_repository
from core.observability import record_worker_task

logger = logging.getLogger("althea.event_worker")

SUBSCRIBED_EVENTS = {
    "alert_ingested",
    "features_generated",
    "alert_scored",
    "alert_governed",
    "case_created",
    "case_closed",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def _handle_event(envelope: dict[str, Any]) -> None:
    event_name = str(envelope.get("event_name", ""))
    tenant_id = str(envelope.get("tenant_id", ""))
    payload = envelope.get("payload", {}) or {}
    if event_name not in SUBSCRIBED_EVENTS:
        return

    logger.info("event_received name=%s tenant=%s payload=%s", event_name, tenant_id, payload)

    # Scoring is exclusively executed in PipelineService.
    # Event subscriber only reacts, enriches monitoring state, and notifies downstream systems.
    if event_name == "features_generated":
        return

    if event_name == "alert_scored":
        run_id = payload.get("run_id")
        if tenant_id and run_id:
            get_repository().set_tenant_context(tenant_id)
            health = get_pipeline_service().compute_health(run_id=run_id, tenant_id=tenant_id)
            logger.info(
                "alert_scored event observed without secondary monitoring write",
                extra={"tenant_id": tenant_id, "run_id": run_id, "health": health},
            )


def run_event_subscriber(start_from: str = "0-0") -> None:
    bus = get_event_bus()
    cache = get_cache()
    cursor = start_from
    logger.info("Starting event subscriber worker from %s", cursor)

    def _heartbeat_loop() -> None:
        while True:
            cache.set_json(
                "heartbeat:worker:event",
                {"worker": "event", "ts": datetime.now(timezone.utc).isoformat()},
                ttl_seconds=30,
            )
            time.sleep(10)

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="event-worker-heartbeat")
    heartbeat_thread.start()

    while True:
        events = bus.consume_after(last_event_id=cursor, limit=200, block_ms=1000)
        if not events:
            time.sleep(0.2)
            continue
        for event in events:
            cursor = event.get("id", cursor)
            envelope = event.get("payload", {})
            started = time.perf_counter()
            status = "completed"
            try:
                _handle_event(envelope)
            except Exception:
                status = "failed"
                logger.exception("Failed to process event envelope=%s", envelope)
            finally:
                record_worker_task(
                    worker_name="event_subscriber",
                    status=status,
                    duration_seconds=time.perf_counter() - started,
                )
