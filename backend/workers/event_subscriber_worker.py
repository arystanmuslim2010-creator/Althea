from __future__ import annotations

import logging
import time
from typing import Any

from core.dependencies import get_event_bus, get_pipeline_service, get_repository
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
            # Snapshot latest model health at scoring completion.
            health = get_pipeline_service().compute_health(run_id=run_id, tenant_id=tenant_id)
            get_repository().save_model_monitoring(
                {
                    "tenant_id": tenant_id,
                    "run_id": run_id,
                    "model_version": str(payload.get("model_version") or "unknown"),
                    "psi_score": float(payload.get("psi_score") or 0.0),
                    "drift_score": float(payload.get("drift_score") or 0.0),
                    "degradation_flag": str(health.get("status", "")).lower() in {"warning", "critical", "degraded"},
                    "metrics_json": {"health": health, "event": payload},
                }
            )


def run_event_subscriber(start_from: str = "0-0") -> None:
    bus = get_event_bus()
    cursor = start_from
    logger.info("Starting event subscriber worker from %s", cursor)
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
