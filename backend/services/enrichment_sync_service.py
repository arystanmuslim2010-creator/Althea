from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.enrichment_connectors.base import BaseEnrichmentConnector


class EnrichmentSyncService:
    def __init__(
        self,
        *,
        repository,
        master_data_service,
        entity_resolution_service,
        audit_service,
        dead_letter_service,
        schema_drift_service,
        health_service,
        connectors: dict[str, BaseEnrichmentConnector] | None = None,
        settings=None,
    ) -> None:
        self._repository = repository
        self._master_data_service = master_data_service
        self._entity_resolution_service = entity_resolution_service
        self._audit_service = audit_service
        self._dead_letter_service = dead_letter_service
        self._schema_drift_service = schema_drift_service
        self._health_service = health_service
        self._connectors = connectors or {}
        self._settings = settings

    def list_sources(self) -> list[dict[str, Any]]:
        enabled_sources = set(getattr(self._settings, "enrichment_sources_enabled", []) or [])
        items = []
        for source_name in self._repository.list_registered_sources():
            connector = self._connectors.get(source_name)
            items.append(
                {
                    "source_name": source_name,
                    "enabled": source_name in enabled_sources and (connector.enabled if connector is not None else True),
                    "kind": "internal" if source_name.startswith("internal_") else "connector",
                }
            )
        return items

    def sync_source(self, tenant_id: str, source_name: str, *, full_backfill: bool = False, actor_id: str | None = None) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        state = self._repository.get_sync_state(tenant_id, source_name) or {}
        cursor = state.get("cursor")
        self._repository.upsert_sync_state(
            tenant_id,
            source_name,
            status="running",
            last_attempt_at=now,
            last_error=None,
        )
        self._audit_service.record(
            tenant_id=tenant_id,
            source_name=source_name,
            action="sync_started",
            actor_id=actor_id,
            status="running",
            details={"full_backfill": bool(full_backfill), "cursor": cursor},
        )
        try:
            if source_name == "internal_case":
                payload = {
                    "case_actions": self._repository.extract_internal_case_actions(tenant_id),
                    "schema_preview": {"keys": ["case_actions"]},
                    "next_cursor": cursor,
                }
            elif source_name == "internal_outcome":
                payload = {
                    "alert_outcomes": self._repository.extract_internal_alert_outcomes(tenant_id),
                    "schema_preview": {"keys": ["alert_outcomes"]},
                    "next_cursor": cursor,
                }
            else:
                connector = self._connectors.get(source_name)
                if connector is None:
                    raise ValueError(f"Unknown enrichment source '{source_name}'")
                payload = connector.fetch_records(
                    cursor=str(cursor or ""),
                    batch_size=int(getattr(self._settings, "enrichment_sync_batch_size", 500)),
                    full_backfill=full_backfill,
                )
            self._schema_drift_service.observe(source_name, dict(payload.get("schema_preview") or payload.get("raw_payload") or payload))
            master_counts = self._master_data_service.upsert_payloads(
                tenant_id=tenant_id,
                customers=list(payload.get("customers") or []),
                accounts=list(payload.get("accounts") or []),
                counterparties=list(payload.get("counterparties") or []),
            )
            event_count = self._repository.append_account_events(tenant_id, list(payload.get("account_events") or []))
            outcome_count = self._repository.append_alert_outcomes(tenant_id, list(payload.get("alert_outcomes") or []))
            case_action_count = self._repository.append_case_actions(tenant_id, list(payload.get("case_actions") or []))
            if any(master_counts.values()):
                self._entity_resolution_service.rebuild_links(tenant_id)
            next_cursor = payload.get("next_cursor") or cursor
            records_read = sum(
                len(list(payload.get(key) or []))
                for key in ("customers", "accounts", "counterparties", "account_events", "alert_outcomes", "case_actions")
            )
            records_written = int(sum(master_counts.values()) + event_count + outcome_count + case_action_count)
            sync_state = self._repository.upsert_sync_state(
                tenant_id,
                source_name,
                cursor=str(next_cursor) if next_cursor is not None else None,
                last_event_time=now,
                last_success_at=now,
                status="completed",
                records_read=records_read,
                records_written=records_written,
                records_failed=0,
            )
            self._audit_service.record(
                tenant_id=tenant_id,
                source_name=source_name,
                action="sync_completed",
                actor_id=actor_id,
                status="completed",
                details={
                    "records_read": records_read,
                    "records_written": records_written,
                    "master_counts": master_counts,
                    "account_events": event_count,
                    "alert_outcomes": outcome_count,
                    "case_actions": case_action_count,
                },
            )
            self._health_service.rebuild(tenant_id=tenant_id, source_name=source_name)
            return {
                "status": "completed",
                "source_name": source_name,
                "sync_state": sync_state,
                "records_read": records_read,
                "records_written": records_written,
                "master_counts": master_counts,
            }
        except Exception as exc:
            self._repository.upsert_sync_state(
                tenant_id,
                source_name,
                status="failed",
                last_attempt_at=now,
                last_error=str(exc),
                records_failed=1,
            )
            self._dead_letter_service.capture(
                tenant_id=tenant_id,
                source_name=source_name,
                error_code="sync_failed",
                error_message=str(exc),
                payload={"cursor": cursor, "full_backfill": bool(full_backfill)},
            )
            self._audit_service.record(
                tenant_id=tenant_id,
                source_name=source_name,
                action="sync_failed",
                actor_id=actor_id,
                status="failed",
                details={"error": str(exc)},
            )
            self._health_service.rebuild(tenant_id=tenant_id, source_name=source_name)
            raise

    def backfill_internal_targets(self, tenant_id: str, targets: list[str], actor_id: str | None = None) -> dict[str, Any]:
        results = {}
        mapping = {
            "case_actions": "internal_case",
            "alert_outcomes": "internal_outcome",
        }
        for target in targets:
            source_name = mapping.get(str(target or "").strip())
            if source_name is None:
                continue
            results[target] = self.sync_source(tenant_id=tenant_id, source_name=source_name, full_backfill=True, actor_id=actor_id)
        return {"status": "completed", "targets": results}
