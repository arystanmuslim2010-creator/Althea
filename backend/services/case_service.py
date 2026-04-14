from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from storage.postgres_repository import EnterpriseRepository
from workflows.state_model import ALLOWED_CASE_TRANSITIONS, normalize_case_state


class CaseWorkflowService:
    """
    Enterprise case lifecycle with backward-compatible payload shape.

    Existing `/api/cases*` endpoints keep working while case state is persisted in
    the enterprise repository.
    """

    def __init__(self, repository: EnterpriseRepository) -> None:
        self._repository = repository

    def get_actor(self, tenant_id: str, user_scope: str) -> str:
        context = self._repository.get_runtime_context(tenant_id, user_scope)
        return context.get("actor") or "Analyst_1"

    def set_actor(self, tenant_id: str, user_scope: str, actor: str) -> dict[str, Any]:
        return self._repository.upsert_runtime_context(tenant_id, user_scope, actor=actor)

    def _next_case_id(self, tenant_id: str) -> str:
        existing = self._repository.list_cases(tenant_id)
        max_n = 0
        for row in existing:
            case_id = str(row.get("case_id", ""))
            m = re.match(r"^CASE_(\d{5})$", case_id)
            if m:
                max_n = max(max_n, int(m.group(1)))
        return f"CASE_{max_n + 1:05d}"

    def _default_case_payload(self, case_id: str, alert_ids: list[str], actor: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        sla_due_at = now + timedelta(hours=24)
        return {
            "case_id": case_id,
            "status": "OPEN",
            "state": "OPEN",
            "created_by": actor,
            "assigned_to": actor,
            "owner": actor,
            "alert_ids": list(alert_ids),
            "notes": "",
            "version": 1,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "sla_due_at": sla_due_at.isoformat(),
            "approval_chain": [
                {
                    "step": "analyst_review",
                    "status": "completed",
                    "actor": actor,
                    "timestamp": now.isoformat(),
                },
                {"step": "manager_approval", "status": "pending", "actor": None, "timestamp": None},
            ],
        }

    def list_cases(self, tenant_id: str = "default-bank") -> dict[str, Any]:
        enterprise_cases = self._repository.list_cases(tenant_id)
        out: dict[str, Any] = {}
        for row in enterprise_cases:
            payload = row.get("payload_json") or {}
            out[row["case_id"]] = payload
        return out

    def create_case(
        self,
        tenant_id: str,
        user_scope: str,
        alert_ids: list[str],
        run_id: str,
        actor: str,
    ) -> dict[str, Any]:
        case_id = self._next_case_id(tenant_id)
        payload = self._default_case_payload(case_id=case_id, alert_ids=alert_ids, actor=actor)
        now = datetime.now(timezone.utc)
        self._repository.save_case(
            {
                "case_id": case_id,
                "tenant_id": tenant_id,
                "status": "open",
                "created_by": actor,
                "assigned_to": actor,
                "alert_id": (alert_ids or [None])[0],
                "payload_json": payload,
                "immutable_timeline_json": [
                    {
                        "id": uuid.uuid4().hex,
                        "action": "case_created",
                        "performed_by": actor,
                        "timestamp": now.isoformat(),
                        "details": {"alert_ids": alert_ids, "run_id": run_id},
                    }
                ],
                "created_at": now,
                "updated_at": now,
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
                "details_json": {"alert_ids": alert_ids, "run_id": run_id},
                "timestamp": now,
            }
        )
        self.set_actor(tenant_id, user_scope, actor)
        return payload

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
        existing = self._repository.get_case(tenant_id, case_id)
        if not existing:
            return False, "Case not found", None

        payload = dict(existing.get("payload_json") or {})
        current_state = normalize_case_state(payload.get("status")) or "open"
        requested_state = normalize_case_state(status) if status is not None else current_state
        new_state = requested_state or current_state
        if new_state not in ALLOWED_CASE_TRANSITIONS:
            return False, f"Invalid status: {new_state}", None
        if new_state != current_state and new_state not in ALLOWED_CASE_TRANSITIONS.get(current_state, set()):
            return False, f"Invalid transition: {current_state} -> {new_state}", None

        if assigned_to is not None:
            payload["assigned_to"] = assigned_to
            payload["owner"] = assigned_to
        if notes is not None:
            payload["notes"] = notes

        payload["status"] = new_state.upper()
        payload["state"] = new_state.upper()
        payload["version"] = int(payload.get("version", 1)) + 1
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()

        if new_state in {"sar_filed", "closed"}:
            chain = list(payload.get("approval_chain") or [])
            for step in chain:
                if step.get("step") == "manager_approval":
                    step["status"] = "completed"
                    step["actor"] = actor
                    step["timestamp"] = datetime.now(timezone.utc).isoformat()
            payload["approval_chain"] = chain

        timeline = list(existing.get("immutable_timeline_json") or [])
        timeline.append(
            {
                "id": uuid.uuid4().hex,
                "action": "case_updated",
                "performed_by": actor,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": {"status": new_state, "assigned_to": assigned_to, "notes": notes, "run_id": run_id},
            }
        )

        self._repository.save_case(
            {
                "case_id": case_id,
                "tenant_id": tenant_id,
                "status": new_state,
                "created_by": existing.get("created_by"),
                "assigned_to": payload.get("assigned_to"),
                "alert_id": existing.get("alert_id"),
                "payload_json": payload,
                "immutable_timeline_json": timeline,
                "created_at": datetime.fromisoformat(existing["created_at"]),
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self._repository.append_investigation_log(
            {
                "id": uuid.uuid4().hex,
                "tenant_id": tenant_id,
                "case_id": case_id,
                "alert_id": existing.get("alert_id"),
                "action": "case_updated",
                "performed_by": actor,
                "details_json": {"status": new_state, "assigned_to": assigned_to, "notes": notes, "run_id": run_id},
                "timestamp": datetime.now(timezone.utc),
            }
        )
        self.set_actor(tenant_id, user_scope, actor)
        return True, "Case updated", payload

    def delete_case(self, tenant_id: str, case_id: str) -> bool:
        existing = self._repository.get_case(tenant_id, case_id)
        if existing:
            self._repository.append_investigation_log(
                {
                    "id": uuid.uuid4().hex,
                    "tenant_id": tenant_id,
                    "case_id": case_id,
                    "alert_id": existing.get("alert_id"),
                    "action": "case_closed",
                    "performed_by": existing.get("assigned_to") or existing.get("created_by") or "system",
                    "details_json": {"reason": "case_deleted"},
                    "timestamp": datetime.now(timezone.utc),
                }
            )
        deleted = self._repository.delete_case(tenant_id, case_id)
        return bool(deleted)

    def get_case_audit(self, case_id: str, tenant_id: str = "default-bank") -> list[dict[str, Any]]:
        timeline = self._repository.list_case_timeline(tenant_id=tenant_id, case_id=case_id, limit=500)
        if timeline:
            return timeline
        case = self._repository.get_case(tenant_id, case_id)
        if not case:
            return []
        events = []
        for item in case.get("immutable_timeline_json", []):
            events.append(
                {
                    "event_id": item.get("id"),
                    "case_id": case_id,
                    "ts": item.get("timestamp"),
                    "actor": item.get("performed_by"),
                    "action": item.get("action"),
                    "payload": item.get("details", {}),
                }
            )
        return events
