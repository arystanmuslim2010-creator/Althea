from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from fastapi import HTTPException, Request

from core.security import ROLE_PERMISSIONS, normalize_role

_SAFE_USER_FIELDS = {
    "id",
    "user_id",
    "email",
    "role",
    "roles",
    "permissions",
    "team",
    "is_active",
    "created_at",
    "last_login_at",
    "tenant_id",
}
_SENSITIVE_KEY_RE = re.compile(
    r"(password|hash|token|secret|credential|private|stack|trace|artifact_uri|artifact_path|file_path|path|dataset_hash)",
    re.IGNORECASE,
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _redact_sensitive_mapping(raw: dict[str, Any], *, full: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in dict(raw or {}).items():
        clean_key = str(key)
        if _SENSITIVE_KEY_RE.search(clean_key):
            if full and clean_key in {"error_message", "error", "detail"}:
                out[clean_key] = "Redacted internal detail"
            continue
        if isinstance(value, dict):
            out[clean_key] = _redact_sensitive_mapping(value, full=full)
        elif isinstance(value, list):
            out[clean_key] = [
                _redact_sensitive_mapping(item, full=full) if isinstance(item, dict) else _jsonable(item)
                for item in value
            ]
        else:
            out[clean_key] = _jsonable(value)
    return out


def sanitize_user_dto(user: dict) -> dict:
    safe = {}
    for key in _SAFE_USER_FIELDS:
        if key in (user or {}):
            safe[key] = _jsonable(user.get(key))
    if "user_id" not in safe and safe.get("id"):
        safe["user_id"] = safe["id"]
    return safe


def sanitize_training_run_dto(run: dict, *, full: bool = False) -> dict:
    raw = dict(run or {})
    metrics = {
        key: raw.get(key)
        for key in (
            "pr_auc",
            "roc_auc",
            "precision_at_k",
            "recall_at_k",
            "suspicious_capture_top_20pct",
            "ece_after_calibration",
        )
        if raw.get(key) is not None
    }
    safe = {
        "run_id": raw.get("training_run_id") or raw.get("run_id") or raw.get("id"),
        "model_version": raw.get("model_version")
        or raw.get("escalation_model_version")
        or raw.get("time_model_version"),
        "status": raw.get("status"),
        "started_at": _jsonable(raw.get("started_at")),
        "completed_at": _jsonable(raw.get("completed_at")),
        "metrics": _redact_sensitive_mapping(metrics, full=full),
        "created_by": raw.get("created_by") or raw.get("initiated_by"),
        "approval_status": raw.get("approval_status"),
    }
    if full:
        for key in (
            "id",
            "snapshot_id",
            "row_count",
            "feature_schema_version",
            "cutoff_timestamp",
            "escalation_model_version",
            "time_model_version",
            "metadata",
        ):
            if key in raw:
                safe[key] = _redact_sensitive_mapping(raw[key], full=True) if isinstance(raw[key], dict) else _jsonable(raw[key])
    return {k: v for k, v in safe.items() if v is not None}


def sanitize_model_monitoring_dto(record: dict, *, full: bool = False) -> dict:
    raw = dict(record or {})
    metrics = raw.get("metrics_json") or raw.get("metrics") or {}
    safe = {
        "id": raw.get("id"),
        "run_id": raw.get("run_id"),
        "model_version": raw.get("model_version"),
        "created_at": _jsonable(raw.get("created_at")),
        "psi_score": raw.get("psi_score"),
        "drift_score": raw.get("drift_score"),
        "degradation_flag": raw.get("degradation_flag"),
        "metrics": _redact_sensitive_mapping(metrics if isinstance(metrics, dict) else {}, full=full),
    }
    if full:
        for key, value in raw.items():
            if key not in safe and not _SENSITIVE_KEY_RE.search(str(key)):
                safe[key] = _redact_sensitive_mapping(value, full=True) if isinstance(value, dict) else _jsonable(value)
    return {k: v for k, v in safe.items() if v is not None}


def user_has_permission(user: dict, permission: str) -> bool:
    role = normalize_role((user or {}).get("role"))
    roles = {normalize_role(item) for item in ((user or {}).get("roles") or [])}
    roles.add(role)
    if "admin" in roles:
        return True
    granted = {str(item) for item in ((user or {}).get("permissions") or [])}
    for candidate_role in roles:
        granted.update(ROLE_PERMISSIONS.get(candidate_role, set()))
    return str(permission) in granted


def user_has_any_permission(user: dict, permissions: list[str]) -> bool:
    return any(user_has_permission(user, permission) for permission in permissions or [])


def _is_demo_mode(request: Request) -> bool:
    settings = getattr(request.app.state, "settings", None)
    if hasattr(settings, "demo_features_enabled"):
        return bool(settings.demo_features_enabled())
    return str(getattr(settings, "runtime_mode", "demo") or "demo").lower() == "demo"


def _user_id(user: dict) -> str:
    return str((user or {}).get("user_id") or (user or {}).get("id") or "").strip()


def _case_alert_ids(case: dict) -> set[str]:
    ids = {str(case.get("alert_id") or "").strip()}
    payload = case.get("payload_json") if isinstance(case.get("payload_json"), dict) else {}
    for key in ("alert_ids", "alerts"):
        for item in payload.get(key) or []:
            if isinstance(item, dict):
                ids.add(str(item.get("alert_id") or item.get("id") or "").strip())
            else:
                ids.add(str(item or "").strip())
    return {item for item in ids if item}


def _case_accessible_to_user(user: dict, case: dict) -> bool:
    if not case:
        return False
    if user_has_any_permission(user, ["view_all_alerts", "manager_approval"]):
        return True
    if normalize_role(user.get("role")) in {"manager", "admin", "governance"}:
        return True
    uid = _user_id(user)
    if not uid:
        return False
    payload = case.get("payload_json") if isinstance(case.get("payload_json"), dict) else {}
    owners = {
        str(case.get("assigned_to") or "").strip(),
        str(case.get("created_by") or "").strip(),
        str(payload.get("assigned_to") or "").strip(),
        str(payload.get("owner") or "").strip(),
        str(payload.get("created_by") or "").strip(),
    }
    return uid in owners


def _alert_team(alert: dict, assignment: dict | None = None) -> str:
    assignment = assignment or {}
    payload = alert.get("payload_json") if isinstance(alert.get("payload_json"), dict) else {}
    metadata = alert.get("metadata") if isinstance(alert.get("metadata"), dict) else {}
    for source in (assignment, alert, payload, metadata):
        for key in ("team", "assigned_team", "queue_team", "analyst_team", "owner_team"):
            value = str((source or {}).get(key) or "").strip()
            if value:
                return value
    return ""


def _alert_assigned_to_user(user: dict, alert: dict, assignment: dict | None = None) -> bool:
    uid = _user_id(user)
    if not uid:
        return False
    candidates = {
        str((assignment or {}).get("assigned_to") or "").strip(),
        str(alert.get("assigned_to") or "").strip(),
        str(alert.get("owner") or "").strip(),
        str(alert.get("analyst_id") or "").strip(),
    }
    return uid in candidates


def _alert_has_missing_visibility_fields(alert: dict, assignment: dict | None, cases: list[dict]) -> bool:
    return not assignment and not cases and not _alert_team(alert)


def require_alert_access(
    request,
    tenant_id: str,
    user: dict,
    alert_id: str,
    run_id: str | None = None,
) -> dict:
    repository = request.app.state.repository
    alert = repository.get_alert_payload(tenant_id=tenant_id, alert_id=str(alert_id), run_id=run_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Resource not found")

    role = normalize_role((user or {}).get("role"))
    if role in {"admin", "manager", "governance"} or user_has_permission(user, "view_all_alerts"):
        return alert

    assignment = None
    try:
        assignment = repository.get_latest_assignment(tenant_id, str(alert_id))
    except Exception:
        assignment = None
    if _alert_assigned_to_user(user, alert, assignment):
        return alert

    user_team = str((user or {}).get("team") or "").strip()
    alert_team = _alert_team(alert, assignment)
    if user_team and alert_team and user_team == alert_team and user_has_permission(user, "view_team_queue"):
        return alert

    linked_cases = []
    try:
        linked_cases = [
            case for case in repository.list_cases(tenant_id)
            if str(alert_id) in _case_alert_ids(case)
        ]
    except Exception:
        linked_cases = []
    if any(_case_accessible_to_user(user, case) for case in linked_cases):
        return alert

    if _is_demo_mode(request) and _alert_has_missing_visibility_fields(alert, assignment, linked_cases):
        return alert

    raise HTTPException(status_code=403, detail="Not authorized")


def filter_visible_alerts(request, tenant_id: str, user: dict, alerts: list[dict]) -> list[dict]:
    role = normalize_role((user or {}).get("role"))
    if role in {"admin", "manager", "governance"} or user_has_permission(user, "view_all_alerts"):
        return list(alerts or [])

    repository = request.app.state.repository
    cases = []
    try:
        cases = repository.list_cases(tenant_id)
    except Exception:
        cases = []
    accessible_case_alert_ids = {
        alert_id
        for case in cases
        if _case_accessible_to_user(user, case)
        for alert_id in _case_alert_ids(case)
    }

    visible: list[dict] = []
    for alert in alerts or []:
        alert_id = str(alert.get("alert_id") or "").strip()
        assignment = None
        try:
            assignment = repository.get_latest_assignment(tenant_id, alert_id) if alert_id else None
        except Exception:
            assignment = None
        if _alert_assigned_to_user(user, alert, assignment):
            visible.append(alert)
            continue
        user_team = str((user or {}).get("team") or "").strip()
        alert_team = _alert_team(alert, assignment)
        if user_team and alert_team and user_team == alert_team and user_has_permission(user, "view_team_queue"):
            visible.append(alert)
            continue
        if alert_id and alert_id in accessible_case_alert_ids:
            visible.append(alert)
            continue
        if _is_demo_mode(request) and _alert_has_missing_visibility_fields(alert, assignment, []):
            visible.append(alert)
    return visible


def require_governance_access(user: dict, *, write: bool = False) -> None:
    role = normalize_role((user or {}).get("role"))
    if role == "admin":
        return
    if write:
        if role == "governance" or user_has_permission(user, "manage_model_governance"):
            return
        raise HTTPException(status_code=403, detail="Not authorized")
    if role in {"manager", "governance"} or user_has_any_permission(
        user,
        ["view_model_governance", "manage_model_governance", "manager_approval"],
    ):
        return
    raise HTTPException(status_code=403, detail="Not authorized")
