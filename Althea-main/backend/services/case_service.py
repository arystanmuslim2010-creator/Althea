from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from storage.postgres_repository import EnterpriseRepository
from src.services.case_service import CaseService as LegacyCaseService
from src.storage import Storage as LegacyStorage


class CaseWorkflowService:
    def __init__(self, repository: EnterpriseRepository, legacy_sqlite_path: Path) -> None:
        self._repository = repository
        self._legacy_storage = LegacyStorage(db_path=str(legacy_sqlite_path))
        self._legacy_case_service = LegacyCaseService(storage=self._legacy_storage)

    def get_actor(self, tenant_id: str, user_scope: str) -> str:
        context = self._repository.get_runtime_context(tenant_id, user_scope)
        return context.get("actor") or "Analyst_1"

    def set_actor(self, tenant_id: str, user_scope: str, actor: str) -> dict[str, Any]:
        return self._repository.upsert_runtime_context(tenant_id, user_scope, actor=actor)

    def _load_state(self, actor: str) -> dict[str, Any]:
        cases_dict, case_counter, audit_log = self._legacy_storage.load_state_from_db()
        return {
            "cases": cases_dict or {},
            "case_counter": case_counter or 1,
            "audit_log": audit_log or [],
            "actor": actor,
        }

    def list_cases(self) -> dict[str, Any]:
        state = self._load_state("Analyst_1")
        return state["cases"]

    def create_case(
        self,
        tenant_id: str,
        user_scope: str,
        alert_ids: list[str],
        run_id: str,
        actor: str,
    ) -> dict[str, Any]:
        state = self._load_state(actor)
        alerts_df = self._legacy_storage.load_alerts_by_run(run_id)
        case_id = self._legacy_case_service.create_case(state, alerts_df, alert_ids)
        case = state["cases"][case_id]
        self._repository.save_case(
            {
                "case_id": case_id,
                "tenant_id": tenant_id,
                "status": case.get("status", "OPEN"),
                "created_by": actor,
                "assigned_to": case.get("assigned_to"),
                "alert_id": (alert_ids or [None])[0],
                "payload_json": case,
                "immutable_timeline_json": self._legacy_storage.get_audit_log_for_case(case_id),
            }
        )
        self._repository.append_investigation_log(
            {
                "id": uuid.uuid4().hex,
                "tenant_id": tenant_id,
                "case_id": case_id,
                "alert_id": (alert_ids or [None])[0],
                "action": "case_created",
                "performed_by": actor,
                "details_json": {"alert_ids": alert_ids},
                "timestamp": datetime.now(timezone.utc),
            }
        )
        self.set_actor(tenant_id, user_scope, actor)
        return case

    def update_case(
        self,
        tenant_id: str,
        user_scope: str,
        case_id: str,
        run_id: str,
        actor: str,
        status: str | None = None,
        assigned_to: str | None = None,
        notes: str | None = None,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        state = self._load_state(actor)
        alerts_df = self._legacy_storage.load_alerts_by_run(run_id)
        ok, message = self._legacy_case_service.update_case(
            state,
            case_id=case_id,
            df=alerts_df,
            status=status,
            assigned_to=assigned_to,
            notes=notes,
            action="CASE_UPDATED",
        )
        if not ok:
            return False, message, None
        case = state["cases"][case_id]
        self._repository.save_case(
            {
                "case_id": case_id,
                "tenant_id": tenant_id,
                "status": case.get("status", "OPEN"),
                "created_by": actor,
                "assigned_to": case.get("assigned_to"),
                "alert_id": (case.get("alert_ids") or [None])[0],
                "payload_json": case,
                "immutable_timeline_json": self._legacy_storage.get_audit_log_for_case(case_id),
            }
        )
        self._repository.append_investigation_log(
            {
                "id": uuid.uuid4().hex,
                "tenant_id": tenant_id,
                "case_id": case_id,
                "alert_id": (case.get("alert_ids") or [None])[0],
                "action": "case_updated",
                "performed_by": actor,
                "details_json": {"status": status, "assigned_to": assigned_to, "notes": notes},
                "timestamp": datetime.now(timezone.utc),
            }
        )
        self.set_actor(tenant_id, user_scope, actor)
        return True, message, case

    def delete_case(self, tenant_id: str, case_id: str) -> bool:
        self._legacy_storage.delete_case(case_id)
        self._repository.delete_case(tenant_id, case_id)
        return True

    def get_case_audit(self, case_id: str) -> list[dict[str, Any]]:
        return self._legacy_storage.get_audit_log_for_case(case_id)
