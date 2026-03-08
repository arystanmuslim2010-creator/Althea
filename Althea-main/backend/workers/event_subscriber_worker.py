from __future__ import annotations

import json
import logging
import time
from typing import Any

import pandas as pd

from core.dependencies import get_event_bus, get_ml_service, get_pipeline_service, get_repository
from core.observability import record_worker_task

logger = logging.getLogger("althea.event_worker")

SUBSCRIBED_EVENTS = {
    "alert_ingested",
    "features_generated",
    "alert_scored",
    "alert_governed",
    "case_created",
}


def _handle_event(envelope: dict[str, Any]) -> None:
    event_name = str(envelope.get("event_name", ""))
    tenant_id = str(envelope.get("tenant_id", ""))
    payload = envelope.get("payload", {}) or {}
    if event_name not in SUBSCRIBED_EVENTS:
        return

    logger.info("event_received name=%s tenant=%s payload=%s", event_name, tenant_id, payload)

    # Lightweight projection hooks for future async stage workers.
    if event_name == "features_generated":
        run_id = payload.get("run_id")
        if tenant_id and run_id:
            repository = get_repository()
            ml_service = get_ml_service()
            feature_rows = repository.list_feature_rows(tenant_id=tenant_id, run_id=run_id, limit=200000)
            if feature_rows:
                frame = pd.DataFrame(feature_rows)
                if "alert_id" in frame.columns and len(frame.columns) > 1:
                    alert_ids = frame["alert_id"].astype(str).tolist()
                    matrix = frame.drop(columns=["alert_id"], errors="ignore").fillna(0)
                    score_by_alert: dict[str, float] = {}
                    explain_by_alert: dict[str, dict[str, Any]] = {}
                    batch_size = 10000
                    for start in range(0, len(matrix), batch_size):
                        sub_matrix = matrix.iloc[start : start + batch_size]
                        sub_ids = alert_ids[start : start + batch_size]
                        inference = ml_service.predict(tenant_id=tenant_id, features=sub_matrix)
                        scores = inference.get("scores", [])
                        explanations = inference.get("explanations", [])
                        for idx, alert_id in enumerate(sub_ids):
                            score_by_alert[alert_id] = float(scores[idx]) if idx < len(scores) else 0.0
                            explain_by_alert[alert_id] = explanations[idx] if idx < len(explanations) else {}

                    alert_payloads = repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=run_id, limit=500000)
                    if alert_payloads:
                        updated: list[dict[str, Any]] = []
                        for row in alert_payloads:
                            item = dict(row)
                            alert_id = str(item.get("alert_id", ""))
                            if not alert_id or alert_id not in score_by_alert:
                                updated.append(item)
                                continue
                            item["ml_service_score"] = float(score_by_alert[alert_id])
                            item["ml_service_explain_json"] = json.dumps(explain_by_alert.get(alert_id, {}))
                            updated.append(item)
                        repository.save_alert_payloads(tenant_id=tenant_id, run_id=run_id, records=updated)

    if event_name == "alert_scored":
        run_id = payload.get("run_id")
        if tenant_id and run_id:
            # Snapshot latest model health at scoring completion.
            health = get_pipeline_service().compute_health(run_id)
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
