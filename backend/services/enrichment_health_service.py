from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from core.observability import set_enrichment_source_health


class EnrichmentHealthService:
    def __init__(self, repository, settings) -> None:
        self._repository = repository
        self._settings = settings

    def rebuild(self, tenant_id: str, source_name: str | None = None) -> list[dict[str, Any]]:
        sources = [source_name] if source_name else self._repository.list_registered_sources()
        snapshots: list[dict[str, Any]] = []
        stale_seconds = int(getattr(self._settings, "enrichment_health_stale_seconds", 3600))
        now = datetime.now(timezone.utc)
        active_run = next(iter(self._repository._repository.list_pipeline_runs(tenant_id, limit=1) or []), {})
        active_run_id = str(active_run.get("run_id") or "").strip()
        active_alerts = []
        if active_run_id:
            active_alerts = list(self._repository._repository.list_alert_payloads_by_run(tenant_id=tenant_id, run_id=active_run_id, limit=500000) or [])
        total_alerts = max(1, len(active_alerts)) if active_alerts else 0

        for current_source in sources:
            state = self._repository.get_sync_state(tenant_id, current_source) or {}
            last_success_at = pd.to_datetime(state.get("last_success_at"), utc=True, errors="coerce")
            last_event_time = pd.to_datetime(state.get("last_event_time"), utc=True, errors="coerce")
            freshness_seconds = float((now - last_success_at.to_pydatetime()).total_seconds()) if pd.notna(last_success_at) else float(stale_seconds * 10)
            lag_seconds = float((now - last_event_time.to_pydatetime()).total_seconds()) if pd.notna(last_event_time) else freshness_seconds
            records_read = max(0, int(state.get("records_read") or 0))
            records_failed = max(0, int(state.get("records_failed") or 0))
            error_rate = float(records_failed / records_read) if records_read else 0.0
            matched_alerts = 0
            if active_alerts:
                for alert in active_alerts:
                    snapshot = self._repository.get_enrichment_context_snapshot(
                        tenant_id=tenant_id,
                        alert_id=str(alert.get("alert_id") or ""),
                        as_of_timestamp=alert.get("timestamp") or alert.get("created_at"),
                    )
                    if current_source == "internal_case" and snapshot.get("case_actions"):
                        matched_alerts += 1
                    elif current_source == "internal_outcome" and snapshot.get("alert_outcomes"):
                        matched_alerts += 1
                    elif current_source not in {"internal_case", "internal_outcome"} and snapshot.get("account_events"):
                        matched_alerts += 1
            coverage_ratio = float(matched_alerts / total_alerts) if total_alerts else 0.0
            status = "healthy"
            if freshness_seconds > stale_seconds or error_rate > 0.25:
                status = "degraded"
            snapshot = self._repository.write_source_health(
                tenant_id=tenant_id,
                source_name=current_source,
                measured_at=now,
                freshness_seconds=freshness_seconds,
                lag_seconds=lag_seconds,
                coverage_ratio=coverage_ratio,
                error_rate=error_rate,
                status=status,
                details_json={
                    "records_read": records_read,
                    "records_failed": records_failed,
                    "matched_alerts": matched_alerts,
                    "total_alerts": total_alerts,
                },
            )
            if total_alerts:
                self._repository.write_coverage_snapshot(
                    tenant_id=tenant_id,
                    payload={
                        "source_name": current_source,
                        "alert_type": "all",
                        "coverage_ratio": coverage_ratio,
                        "matched_alerts": matched_alerts,
                        "total_alerts": total_alerts,
                        "details_json": {"active_run_id": active_run_id},
                        "measured_at": now,
                    },
                )
            set_enrichment_source_health(
                current_source,
                freshness_seconds=freshness_seconds,
                coverage_ratio=coverage_ratio,
            )
            snapshots.append(snapshot)
        return snapshots

    def status(self, tenant_id: str) -> dict[str, Any]:
        states = self._repository.list_sync_states(tenant_id)
        health = self._repository.list_latest_source_health(tenant_id)
        latest_health_by_source = {str(item.get("source_name") or ""): item for item in health}
        items = []
        for source_name in self._repository.list_registered_sources():
            item = {
                "source_name": source_name,
                "sync_state": next((state for state in states if state.get("source_name") == source_name), None),
                "health": latest_health_by_source.get(source_name),
            }
            items.append(item)
        overall = "healthy"
        if any(str((item.get("health") or {}).get("status") or "").lower() == "degraded" for item in items):
            overall = "degraded"
        return {"status": overall, "sources": items}
