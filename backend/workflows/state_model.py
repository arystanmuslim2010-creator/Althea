from __future__ import annotations

from typing import Final

# Canonical case states exposed by investigation and case APIs.
CANONICAL_CASE_STATES: Final[set[str]] = {
    "open",
    "under_review",
    "escalated",
    "sar_filed",
    "closed",
}

# Legacy and cross-module aliases normalized into canonical case states.
CASE_STATE_ALIASES: Final[dict[str, str]] = {
    "assigned": "open",
    "in_progress": "under_review",
    "in_review": "under_review",
    "investigating": "under_review",
    "manager_review": "escalated",
    "sar_candidate": "sar_filed",
    "closed_tp": "closed",
    "closed_fp": "closed",
}

# Canonical assignment statuses (queue-level) used by alert assignment records.
ASSIGNMENT_STATUSES: Final[set[str]] = {
    "open",
    "in_review",
    "escalated",
    "closed",
}

ASSIGNMENT_TO_CASE_STATE: Final[dict[str, str]] = {
    "open": "open",
    "in_review": "under_review",
    "escalated": "escalated",
    "closed": "closed",
}

CASE_STATE_TO_ASSIGNMENT: Final[dict[str, str]] = {
    "open": "open",
    "under_review": "in_review",
    "escalated": "escalated",
    "sar_filed": "escalated",
    "closed": "closed",
}

# Workflow engine states mapped to canonical case states.
CASE_STATE_TO_WORKFLOW: Final[dict[str, str]] = {
    "open": "assigned",
    "under_review": "investigating",
    "escalated": "escalated",
    "sar_filed": "sar_candidate",
    "closed": "closed",
}

WORKFLOW_TO_CASE_STATE: Final[dict[str, str]] = {
    "new": "open",
    "assigned": "open",
    "investigating": "under_review",
    "escalated": "escalated",
    "sar_candidate": "sar_filed",
    "closed": "closed",
}

ALLOWED_CASE_TRANSITIONS: Final[dict[str, set[str]]] = {
    "open": {"under_review", "escalated", "closed"},
    "under_review": {"escalated", "sar_filed", "closed"},
    "escalated": {"under_review", "sar_filed", "closed"},
    "sar_filed": {"closed"},
    "closed": set(),
}


def normalize_case_state(status: str | None) -> str | None:
    if status is None:
        return None
    raw = str(status).strip().lower()
    if not raw:
        return None
    mapped = CASE_STATE_ALIASES.get(raw, raw)
    if mapped in CANONICAL_CASE_STATES:
        return mapped
    return None


def normalize_assignment_status(status: str | None) -> str | None:
    case_state = normalize_case_state(status)
    if case_state:
        return CASE_STATE_TO_ASSIGNMENT.get(case_state, "open")
    raw = str(status or "").strip().lower()
    return raw if raw in ASSIGNMENT_STATUSES else None


def case_state_from_assignment(status: str | None) -> str | None:
    normalized = normalize_assignment_status(status)
    if not normalized:
        return None
    return ASSIGNMENT_TO_CASE_STATE.get(normalized)


def workflow_state_from_case(status: str | None) -> str | None:
    case_state = normalize_case_state(status)
    if not case_state:
        return None
    return CASE_STATE_TO_WORKFLOW.get(case_state)


def case_state_from_workflow(status: str | None) -> str | None:
    if status is None:
        return None
    raw = str(status).strip().lower()
    if not raw:
        return None
    return WORKFLOW_TO_CASE_STATE.get(raw)
