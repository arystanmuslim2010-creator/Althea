"""Domain models for AML alerts, cases, and audit events."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Alert:
    """Wrapper for alert dictionaries while preserving field names/values."""

    data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Alert":
        return cls(data=dict(payload))

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.data)


@dataclass
class Case:
    """Case entity for investigation lifecycle."""

    case_id: str
    alert_ids: List[str]
    status: str
    assigned_to: str
    notes: str
    created_at: str
    updated_at: str

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Case":
        return cls(
            case_id=payload["case_id"],
            alert_ids=list(payload["alert_ids"]),
            status=payload["status"],
            assigned_to=payload["assigned_to"],
            notes=payload["notes"],
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_id": self.case_id,
            "alert_ids": list(self.alert_ids),
            "status": self.status,
            "assigned_to": self.assigned_to,
            "notes": self.notes,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class AuditEvent:
    """Audit log entry for case actions."""

    ts: str
    case_id: str
    action: str
    actor: str
    details: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "AuditEvent":
        return cls(
            ts=payload["ts"],
            case_id=payload["case_id"],
            action=payload["action"],
            actor=payload["actor"],
            details=dict(payload.get("details", {})),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "case_id": self.case_id,
            "action": self.action,
            "actor": self.actor,
            "details": dict(self.details),
        }
