from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from workflows.state_model import ALLOWED_CASE_TRANSITIONS, CASE_STATE_TO_WORKFLOW, WORKFLOW_TO_CASE_STATE

WORKFLOW_STATES = set(WORKFLOW_TO_CASE_STATE.keys())
VALID_TRANSITIONS: dict[str, set[str]] = {
    "new": {"assigned", "closed"},
    "assigned": {CASE_STATE_TO_WORKFLOW[s] for s in ALLOWED_CASE_TRANSITIONS["open"]},
    "investigating": {CASE_STATE_TO_WORKFLOW[s] for s in ALLOWED_CASE_TRANSITIONS["under_review"]},
    "escalated": {CASE_STATE_TO_WORKFLOW[s] for s in ALLOWED_CASE_TRANSITIONS["escalated"]},
    "sar_candidate": {CASE_STATE_TO_WORKFLOW[s] for s in ALLOWED_CASE_TRANSITIONS["sar_filed"]},
    "closed": set(),
}

ESCALATION_CHAIN = ["analyst", "manager", "compliance"]


class InvestigationWorkflowEngine:
    def __init__(self, repository, case_service) -> None:
        self._repository = repository
        self._case_service = case_service

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def create_case_from_alert(self, tenant_id: str, alert_id: str, run_id: str, actor: str = "analyst") -> str | None:
        existing = [
            row for row in self._repository.list_cases(tenant_id)
            if str(row.get("alert_id") or "") == str(alert_id) and str(row.get("status") or "") != "closed"
        ]
        if existing:
            return str(existing[0].get("case_id"))

        case = self._case_service.create_case(
            tenant_id=tenant_id,
            user_scope=actor,
            alert_ids=[alert_id],
            run_id=run_id,
            actor=actor,
        )
        case_id = str(case.get("case_id") or "")
        if not case_id:
            return None

        self._record_transition(
            tenant_id=tenant_id,
            case_id=case_id,
            from_state="new",
            to_state="assigned",
            actor=actor,
            reason="case_created",
        )
        return case_id

    def evaluate_rules(self, alert_payload: dict[str, Any]) -> dict[str, Any]:
        risk_score = float(alert_payload.get("risk_score", 0.0) or 0.0)
        if risk_score > 85.0:
            return {"target_state": "escalated", "reason": "risk_score > 85"}
        if risk_score > 70.0:
            return {"target_state": "investigating", "reason": "risk_score > 70"}
        return {"target_state": "assigned", "reason": "default_assignment"}

    def transition_case(
        self,
        tenant_id: str,
        case_id: str,
        to_state: str,
        actor: str,
        reason: str,
        escalation_level: str | None = None,
    ) -> dict[str, Any]:
        case = self._repository.get_case(tenant_id, case_id)
        if not case:
            raise ValueError("Case not found")

        payload = dict(case.get("payload_json") or {})
        from_state = str(payload.get("workflow_state") or "new").lower()
        target = str(to_state).lower().strip()
        if target not in WORKFLOW_STATES:
            raise ValueError(f"Invalid workflow state: {to_state}")
        if target not in VALID_TRANSITIONS.get(from_state, set()):
            raise ValueError(f"Invalid transition: {from_state} -> {target}")

        case_state = WORKFLOW_TO_CASE_STATE.get(target, "open")
        payload["workflow_state"] = target
        payload["status"] = case_state.upper()
        payload["state"] = case_state.upper()
        payload["sla_due_at"] = payload.get("sla_due_at") or (self._now() + timedelta(hours=48)).isoformat()
        payload["escalation_level"] = escalation_level or payload.get("escalation_level") or "analyst"
        payload["updated_at"] = self._now().isoformat()

        self._repository.save_case(
            {
                "case_id": case_id,
                "tenant_id": tenant_id,
                "status": case_state,
                "created_by": case.get("created_by"),
                "assigned_to": case.get("assigned_to"),
                "alert_id": case.get("alert_id"),
                "payload_json": payload,
                "immutable_timeline_json": list(case.get("immutable_timeline_json") or []),
                "created_at": datetime.fromisoformat(case["created_at"]),
                "updated_at": self._now(),
            }
        )
        self._record_transition(
            tenant_id=tenant_id,
            case_id=case_id,
            from_state=from_state,
            to_state=target,
            actor=actor,
            reason=reason,
        )
        return {"case_id": case_id, "from_state": from_state, "to_state": target, "reason": reason}

    def monitor_sla(self, tenant_id: str, now: datetime | None = None) -> list[dict[str, Any]]:
        now = now or self._now()
        breached: list[dict[str, Any]] = []
        for row in self._repository.list_cases(tenant_id):
            payload = dict(row.get("payload_json") or {})
            if str(row.get("status") or "").lower() == "closed":
                continue
            due_at = payload.get("sla_due_at")
            if not due_at:
                continue
            try:
                due = datetime.fromisoformat(str(due_at))
            except Exception:
                continue
            if due.tzinfo is None:
                due = due.replace(tzinfo=timezone.utc)
            if now > due:
                breached.append(
                    {
                        "case_id": row.get("case_id"),
                        "alert_id": row.get("alert_id"),
                        "sla_due_at": due.isoformat(),
                        "status": row.get("status"),
                    }
                )
        return breached

    def escalate_case(self, tenant_id: str, case_id: str, actor: str) -> dict[str, Any]:
        case = self._repository.get_case(tenant_id, case_id)
        if not case:
            raise ValueError("Case not found")
        payload = dict(case.get("payload_json") or {})
        current_level = str(payload.get("escalation_level") or "analyst").lower()
        if current_level not in ESCALATION_CHAIN:
            current_level = "analyst"
        idx = ESCALATION_CHAIN.index(current_level)
        next_level = ESCALATION_CHAIN[min(idx + 1, len(ESCALATION_CHAIN) - 1)]
        return self.transition_case(
            tenant_id=tenant_id,
            case_id=case_id,
            to_state="escalated",
            actor=actor,
            reason=f"escalated_to_{next_level}",
            escalation_level=next_level,
        )

    def _record_transition(
        self,
        tenant_id: str,
        case_id: str,
        from_state: str,
        to_state: str,
        actor: str,
        reason: str,
    ) -> None:
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO workflow_state_transitions (
                        id, tenant_id, case_id, from_state, to_state, actor_id, reason, created_at
                    ) VALUES (
                        :id, :tenant_id, :case_id, :from_state, :to_state, :actor_id, :reason, :created_at
                    )
                    """
                ),
                {
                    "id": uuid.uuid4().hex,
                    "tenant_id": tenant_id,
                    "case_id": case_id,
                    "from_state": from_state,
                    "to_state": to_state,
                    "actor_id": actor,
                    "reason": reason,
                    "created_at": self._now(),
                },
            )
