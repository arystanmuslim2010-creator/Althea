from __future__ import annotations

import enum
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text


class ModelLifecycleState(str, enum.Enum):
    DRAFT = "draft"
    VALIDATED = "validated"
    APPROVED = "approved"
    DEPLOYED = "deployed"
    RETIRED = "retired"


APPROVAL_CHAIN = ["data_scientist", "risk_validation", "compliance_approval"]


class ModelGovernanceLifecycle:
    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def transition_state(
        self,
        tenant_id: str,
        model_version: str,
        target_state: ModelLifecycleState,
        actor_role: str,
        actor_id: str,
        notes: str | None = None,
    ) -> dict[str, Any]:
        state = str(target_state.value)
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO model_governance_lifecycle (
                        id, tenant_id, model_version, lifecycle_state, actor_role, actor_id, notes, created_at
                    ) VALUES (
                        :id, :tenant_id, :model_version, :lifecycle_state, :actor_role, :actor_id, :notes, :created_at
                    )
                    """
                ),
                {
                    "id": uuid.uuid4().hex,
                    "tenant_id": tenant_id,
                    "model_version": model_version,
                    "lifecycle_state": state,
                    "actor_role": actor_role,
                    "actor_id": actor_id,
                    "notes": notes,
                    "created_at": self._now(),
                },
            )
            session.execute(
                text(
                    """
                    UPDATE model_versions
                    SET approval_status = :approval_status,
                        approved_by = CASE WHEN :approval_status IN ('approved', 'deployed') THEN :actor_id ELSE approved_by END,
                        approved_at = CASE WHEN :approval_status IN ('approved', 'deployed') THEN :approved_at ELSE approved_at END
                    WHERE tenant_id = :tenant_id
                      AND model_version = :model_version
                    """
                ),
                {
                    "tenant_id": tenant_id,
                    "model_version": model_version,
                    "approval_status": state,
                    "actor_id": actor_id,
                    "approved_at": self._now(),
                },
            )
        return {
            "tenant_id": tenant_id,
            "model_version": model_version,
            "state": state,
            "actor_role": actor_role,
            "actor_id": actor_id,
        }

    def submit_approval(self, tenant_id: str, model_version: str, stage: str, actor_id: str, decision: str, notes: str = "") -> dict[str, Any]:
        normalized_stage = str(stage).strip().lower()
        if normalized_stage not in APPROVAL_CHAIN:
            raise ValueError(f"Invalid approval stage: {stage}")
        normalized_decision = str(decision).strip().lower()
        if normalized_decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")

        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO model_governance_approvals (
                        id, tenant_id, model_version, stage, actor_id, decision, notes, created_at
                    ) VALUES (
                        :id, :tenant_id, :model_version, :stage, :actor_id, :decision, :notes, :created_at
                    )
                    """
                ),
                {
                    "id": uuid.uuid4().hex,
                    "tenant_id": tenant_id,
                    "model_version": model_version,
                    "stage": normalized_stage,
                    "actor_id": actor_id,
                    "decision": normalized_decision,
                    "notes": notes,
                    "created_at": self._now(),
                },
            )

            decisions = session.execute(
                text(
                    """
                    SELECT stage, decision
                    FROM model_governance_approvals
                    WHERE tenant_id = :tenant_id
                      AND model_version = :model_version
                    ORDER BY created_at
                    """
                ),
                {"tenant_id": tenant_id, "model_version": model_version},
            ).mappings().all()

        by_stage = {str(row["stage"]): str(row["decision"]) for row in decisions}
        if any(by_stage.get(stage_name) == "rejected" for stage_name in APPROVAL_CHAIN):
            next_state = ModelLifecycleState.DRAFT
        elif all(by_stage.get(stage_name) == "approved" for stage_name in APPROVAL_CHAIN):
            next_state = ModelLifecycleState.APPROVED
        elif by_stage.get("data_scientist") == "approved":
            next_state = ModelLifecycleState.VALIDATED
        else:
            next_state = ModelLifecycleState.DRAFT

        self.transition_state(
            tenant_id=tenant_id,
            model_version=model_version,
            target_state=next_state,
            actor_role=normalized_stage,
            actor_id=actor_id,
            notes=notes,
        )
        return {
            "tenant_id": tenant_id,
            "model_version": model_version,
            "stage": normalized_stage,
            "decision": normalized_decision,
            "state": next_state.value,
        }

    def list_monitoring(self, tenant_id: str, model_version: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT id, tenant_id, model_version, drift_metric, score_shift_metric, feedback_outcome_rate, metadata_json, created_at
                    FROM model_governance_monitoring
                    WHERE tenant_id = :tenant_id
                      AND model_version = :model_version
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                {"tenant_id": tenant_id, "model_version": model_version, "limit": int(limit)},
            ).mappings().all()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            metadata = item.get("metadata_json")
            if isinstance(metadata, str):
                try:
                    item["metadata_json"] = json.loads(metadata)
                except Exception:
                    item["metadata_json"] = {}
            elif metadata is None:
                item["metadata_json"] = {}
            out.append(item)
        return out

    def record_monitoring(
        self,
        tenant_id: str,
        model_version: str,
        model_drift: float,
        score_distribution_shift: float,
        alert_outcome_feedback: float,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        row_id = uuid.uuid4().hex
        with self._repository.session(tenant_id=tenant_id) as session:
            session.execute(
                text(
                    """
                    INSERT INTO model_governance_monitoring (
                        id, tenant_id, model_version, drift_metric, score_shift_metric, feedback_outcome_rate, metadata_json, created_at
                    ) VALUES (
                        :id, :tenant_id, :model_version, :drift_metric, :score_shift_metric, :feedback_outcome_rate, :metadata_json, :created_at
                    )
                    """
                ),
                {
                    "id": row_id,
                    "tenant_id": tenant_id,
                    "model_version": model_version,
                    "drift_metric": float(model_drift),
                    "score_shift_metric": float(score_distribution_shift),
                    "feedback_outcome_rate": float(alert_outcome_feedback),
                    "metadata_json": json.dumps(dict(metadata or {}), ensure_ascii=True),
                    "created_at": self._now(),
                },
            )
        return row_id

