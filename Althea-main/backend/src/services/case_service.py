"""Service facade for case management, audit logging, and AI summaries."""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple

import numpy as np
import pandas as pd

from .. import ai_summary, config, utils
from ..models import Alert
from ..storage import CaseVersionConflictError, Storage


class CaseService:
    """Facade over case lifecycle and audit logging."""

    def __init__(self, logger: Optional[logging.Logger] = None, storage: Optional[Storage] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)
        self._storage = storage

    def log_event(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        action: str,
        actor: str = config.CASE_ACTOR_DEFAULT,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log audit event. Never clears audit log on rerun - only appends."""
        try:
            # Ensure audit_log exists and is never cleared
            if "audit_log" not in session_state:
                session_state["audit_log"] = []
            
            event = {
                "case_id": case_id,
                "action": action,
                "actor": actor,
                "timestamp": time.time(),
                "details": details or {},
            }
            session_state["audit_log"].append(event)
        except Exception:
            self._logger.exception("Failed to log audit event")
            raise

    def create_case(
        self,
        session_state: MutableMapping[str, Any],
        df: pd.DataFrame,
        alert_ids: Iterable[str],
    ) -> str:
        try:
            case_id = f"{config.CASE_ID_PREFIX}{session_state['case_counter']:0{config.CASE_ID_PAD}d}"
            session_state["case_counter"] += 1
            alert_ids = [str(a) for a in alert_ids]
            df["alert_id"] = df["alert_id"].astype(str)
            alerts_df = df[df["alert_id"].isin(alert_ids)]
            avg_risk = float(pd.to_numeric(alerts_df.get("risk_score", 0.0), errors="coerce").mean())
            if not np.isfinite(avg_risk):
                avg_risk = 0.0
            if avg_risk > 85:
                priority = "HIGH"
            elif avg_risk > 60:
                priority = "MEDIUM"
            else:
                priority = "LOW"

            now_ts = time.time()
            case = {
                "case_id": case_id,
                "alert_ids": alert_ids,
                "status": "OPEN",
                "state": "OPEN",
                "assigned_to": None,
                "owner": None,
                "version": 0,
                "created_at": now_ts,
                "updated_at": now_ts,
                "notes": "",
                "decision": None,
                "priority": priority,
                "risk_snapshot": avg_risk,
                "history_size": int(len(alert_ids)),
            }
            session_state["cases"][case_id] = case

            df.loc[df["alert_id"].isin(alert_ids), "case_id"] = case_id
            df.loc[df["alert_id"].isin(alert_ids), "case_status"] = config.CASE_STATUS_IN_CASE

            if "alert_eligible" in df.columns:
                suppressed_alerts = df[df["alert_id"].isin(alert_ids) & (df["alert_eligible"] == False)]
                if len(suppressed_alerts) > 0:
                    df.loc[df["alert_id"].isin(alert_ids), "alert_eligible"] = True
                    df.loc[df["alert_id"].isin(alert_ids), "suppression_code"] = ""
                    df.loc[df["alert_id"].isin(alert_ids), "suppression_reason"] = ""
                    self.log_event(
                        session_state=session_state,
                        case_id=case_id,
                        action="UNSUPPRESSED_FOR_CASE",
                        details={"alert_ids": list(alert_ids), "n_unsuppressed": len(suppressed_alerts)},
                    )

            actor = session_state.get("actor", config.CASE_ACTOR_DEFAULT)
            self._append_audit("case", case_id, actor, "CREATE_CASE", {"alert_ids": list(alert_ids)})

            if self._storage:
                self._storage.save_case_to_db(case)
            
            return case_id
        except Exception:
            self._logger.exception("Failed to create case")
            raise

    def check_lock(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        actor: str,
    ) -> Tuple[bool, Optional[str]]:
        """Check if case is locked by someone else. Returns (is_locked, lock_owner)."""
        if self._storage:
            lock_info = self._storage.get_lock("case", case_id)
            if lock_info:
                owner = lock_info.get("owner")
                if owner and owner != actor:
                    return True, owner
        return False, None

    def acquire_lock(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        actor: str,
    ) -> bool:
        """Acquire lock for case. Returns True if acquired, False if held by another."""
        if self._storage:
            ok = self._storage.acquire_lock("case", case_id, actor, config.CASE_LOCK_TTL_SECONDS)
            if ok:
                self._append_audit("case", case_id, actor, "LOCK_ACQUIRED", {})
            return ok
        return True

    def release_lock(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        actor: str,
    ) -> None:
        """Release lock for case."""
        if self._storage:
            self._storage.release_lock("case", case_id, actor)
            self._append_audit("case", case_id, actor, "LOCK_RELEASED", {})

    def _append_audit(self, entity_type: str, entity_id: str, actor: str, action: str, payload: Dict) -> None:
        """Append immutable audit entry (hash-chained)."""
        if self._storage:
            self._storage.append_audit(entity_type, entity_id, actor, action, payload)
    
    def _validate_transition(self, old_state: str, new_state: str) -> None:
        """Validate state transition. Raises ValueError if invalid."""
        if old_state in ("NEW",):
            old_state = "OPEN"
        allowed = config.ALLOWED_CASE_TRANSITIONS.get(old_state, [])
        if new_state not in allowed:
            raise ValueError(
                f"Invalid transition: {old_state} -> {new_state}. Allowed: {allowed}"
            )

    def update_case(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        df: Optional[pd.DataFrame] = None,
        status: Optional[str] = None,
        assigned_to: Optional[str] = None,
        notes: Optional[str] = None,
        action: str = "CASE_UPDATED",
    ) -> Tuple[bool, str]:
        """
        Update case. Returns (success_bool, message).
        Requires lock for mutations. Uses optimistic concurrency (version).
        """
        try:
            case = session_state.get("cases", {}).get(case_id)
            if not case:
                return False, "Case not found"
            actor = session_state.get("actor", config.CASE_ACTOR_DEFAULT)
            current_state = case.get("state", case.get("status", "OPEN"))
            if current_state == "NEW":
                current_state = "OPEN"
            expected_version = int(case.get("version", 0))

            # Require lock for any mutation
            is_locked, lock_owner = self.check_lock(session_state, case_id, actor)
            if is_locked:
                return False, f"Case is locked by {lock_owner}. Cannot modify."
            if not self.acquire_lock(session_state, case_id, actor):
                return False, "Failed to acquire lock"

            new_state = current_state
            new_owner = case.get("owner", case.get("assigned_to", ""))

            if status is not None:
                self._validate_transition(current_state, status)
                new_state = status
            if assigned_to is not None:
                if current_state == "OPEN":
                    self._validate_transition("OPEN", "ASSIGNED")
                    new_state = "ASSIGNED"
                new_owner = assigned_to

            if self._storage:
                self._storage.update_case_state(
                    case_id, new_state, actor, expected_version,
                    new_owner=new_owner if assigned_to is not None else None,
                    notes=notes,
                )
            case["status"] = new_state
            case["state"] = new_state
            case["assigned_to"] = new_owner
            case["owner"] = new_owner
            case["version"] = expected_version + 1
            case["notes"] = notes if notes is not None else case.get("notes", "")
            case["updated_at"] = time.time()

            if new_state in config.TERMINAL_STATES and df is not None:
                alert_ids = case.get("alert_ids", [])
                df.loc[df["alert_id"].isin(alert_ids), "case_status"] = new_state

            payload = {"status": new_state, "assigned_to": new_owner}
            if notes is not None:
                payload["notes"] = notes
            self._append_audit("case", case_id, actor, action, payload)

            messages = []
            if status:
                messages.append(f"Status updated to {status}")
            if assigned_to:
                messages.append(f"Assigned to {assigned_to}")
            if notes is not None:
                messages.append("Notes updated")
            return True, "; ".join(messages) if messages else "Case updated"

        except ValueError as e:
            return False, str(e)
        except CaseVersionConflictError as e:
            return False, f"Version conflict. {e}. Reload and retry."
        except PermissionError as e:
            return False, str(e)
        except Exception as e:
            self._logger.exception("Failed to update case")
            return False, f"Error updating case: {str(e)}"

    def get_gemini_status(self) -> Tuple[bool, Optional[str]]:
        try:
            return ai_summary.get_gemini_status()
        except Exception:
            self._logger.exception("Failed to fetch Gemini status")
            raise

    def get_ai_summary(self, row_dict: Dict[str, Any]) -> str:
        try:
            return ai_summary.generate_case_summary(row_dict)
        except Exception:
            self._logger.exception("Failed to generate AI summary")
            raise

    def add_alerts_to_case(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        alert_ids: Iterable[str],
    ) -> Tuple[bool, str]:
        """Add alerts to case. Blocked if case in terminal state (must REOPEN first)."""
        case = session_state.get("cases", {}).get(case_id)
        if not case:
            return False, "Case not found"
        state = case.get("state", case.get("status", "OPEN"))
        if state in config.TERMINAL_STATES:
            return False, f"Cannot add alerts: case is {state}. Reopen first."
        is_locked, lock_owner = self.check_lock(session_state, case_id, session_state.get("actor", config.CASE_ACTOR_DEFAULT))
        if is_locked:
            return False, f"Case locked by {lock_owner}"
        if not self.acquire_lock(session_state, case_id, session_state.get("actor", config.CASE_ACTOR_DEFAULT)):
            return False, "Failed to acquire lock"
        alert_ids = list(alert_ids)
        existing = set(case.get("alert_ids", []))
        to_add = [a for a in alert_ids if a not in existing]
        if not to_add:
            return True, "No new alerts to add"
        case["alert_ids"] = list(existing) + to_add
        if self._storage:
            self._storage.add_alerts_to_case(case_id, to_add)
        actor = session_state.get("actor", config.CASE_ACTOR_DEFAULT)
        self._append_audit("case", case_id, actor, "ADD_ALERTS", {"alert_ids": to_add})
        return True, f"Added {len(to_add)} alert(s)"

    def remove_alert_from_case(
        self,
        session_state: MutableMapping[str, Any],
        case_id: str,
        alert_id: str,
    ) -> Tuple[bool, str]:
        """Remove alert from case. Blocked if case in terminal state (must REOPEN first)."""
        case = session_state.get("cases", {}).get(case_id)
        if not case:
            return False, "Case not found"
        state = case.get("state", case.get("status", "OPEN"))
        if state in config.TERMINAL_STATES:
            return False, f"Cannot remove alerts: case is {state}. Reopen first."
        is_locked, lock_owner = self.check_lock(session_state, case_id, session_state.get("actor", config.CASE_ACTOR_DEFAULT))
        if is_locked:
            return False, f"Case locked by {lock_owner}"
        if not self.acquire_lock(session_state, case_id, session_state.get("actor", config.CASE_ACTOR_DEFAULT)):
            return False, "Failed to acquire lock"
        alert_ids = case.get("alert_ids", [])
        if alert_id not in alert_ids:
            return True, "Alert not in case"
        case["alert_ids"] = [a for a in alert_ids if a != alert_id]
        if self._storage:
            self._storage.remove_alert_from_case(case_id, alert_id)
        actor = session_state.get("actor", config.CASE_ACTOR_DEFAULT)
        self._append_audit("case", case_id, actor, "REMOVE_ALERT", {"alert_id": alert_id})
        return True, "Alert removed"

    def get_case_audit_events(
        self, session_state: MutableMapping[str, Any], case_id: str
    ) -> List[Dict[str, Any]]:
        """Get audit events for case. Prefer DB (hash-chained) over session_state."""
        if self._storage:
            events = self._storage.get_audit_log_for_case(case_id)
            if events:
                return events
        try:
            events = [e for e in session_state.get("audit_log", []) if e.get("case_id") == case_id]
            return sorted(events, key=lambda e: e.get("ts", e.get("timestamp", 0)))
        except Exception:
            self._logger.exception("Failed to fetch case audit events")
            raise

    def build_export_payload(
        self,
        session_state: MutableMapping[str, Any],
        df: pd.DataFrame,
        case_id: str,
    ) -> Dict[str, Any]:
        try:
            case = session_state["cases"][case_id]
            alert_dicts = df[df["alert_id"].isin(case["alert_ids"])].to_dict(orient="records")
            alerts = [Alert.from_dict(alert).to_dict() for alert in alert_dicts]
            audit_log = self.get_case_audit_events(session_state, case_id)
            alerts_df = df[df["alert_id"].isin(case["alert_ids"])]
            avg_risk = float(pd.to_numeric(alerts_df.get("risk_score", 0.0), errors="coerce").mean())
            reason_codes = []
            for codes in alerts_df.get("reason_codes", pd.Series([], dtype=str)).fillna("[]"):
                try:
                    reason_codes.extend(json.loads(codes))
                except Exception:
                    pass
            top_reason_codes = (
                pd.Series(reason_codes).value_counts().head(5).index.tolist() if reason_codes else []
            )
            baseline_levels = (
                alerts_df.get("baseline_level", pd.Series([], dtype=str)).astype(str).value_counts().to_dict()
            )
            dominant_components = (
                alerts_df.get("risk_reason_code", pd.Series([], dtype=str)).astype(str).value_counts().to_dict()
            )
            case_summary = {
                "case_id": case_id,
                "n_alerts": int(len(case["alert_ids"])),
                "avg_risk": avg_risk if np.isfinite(avg_risk) else 0.0,
                "top_reason_codes": top_reason_codes,
                "baseline_levels": baseline_levels,
                "dominant_components": dominant_components,
            }
            # Include audit log from database if available
            if self._storage:
                db_audit_log = self._storage.get_audit_log_for_case(case_id)
                # Merge with session_state audit log (prefer DB)
                audit_log = db_audit_log if db_audit_log else audit_log
            
            # Ensure export includes all required case fields
            export_case = {
                "case_id": case.get("case_id", case_id),
                "status": case.get("status", config.CASE_STATUS_NEW),
                "assigned_to": case.get("assigned_to", None),
                "created_at": case.get("created_at", 0),
                "notes": case.get("notes", ""),
                "alert_ids": case.get("alert_ids", []),
                "priority": case.get("priority", "LOW"),
                "risk_snapshot": case.get("risk_snapshot", 0.0),
            }
            
            return {"case": export_case, "alerts": alerts, "audit_log": audit_log, "case_summary": case_summary}
        except Exception:
            self._logger.exception("Failed to build export payload")
            raise
    
    def apply_case_overlay(self, df: pd.DataFrame, session_state: MutableMapping[str, Any]) -> pd.DataFrame:
        """
        Apply case overlay to dataframe (set case_id and case_status from session_state).
        Does NOT reset existing values - only updates alerts that are in cases.
        
        Args:
            df: DataFrame with alert_id column
            session_state: Streamlit session_state with cases dict
            
        Returns:
            DataFrame with case_id and case_status updated (preserving existing values)
        """
        df = df.copy()
        
        # Ensure columns exist (only if missing, don't overwrite)
        if "case_id" not in df.columns:
            df["case_id"] = ""
        if "case_status" not in df.columns:
            df["case_status"] = config.CASE_STATUS_NEW
        
        # DO NOT reset existing values - only update alerts that are in cases
        # Apply overlay from session_state cases
        cases = session_state.get("cases", {})
        for case_id, case_data in cases.items():
            alert_ids = case_data.get("alert_ids", [])
            case_status = case_data.get("status", config.CASE_STATUS_NEW)
            
            # Map case status to alert case_status
            if case_status in config.TERMINAL_STATES:
                alert_case_status = case_status
            elif case_status == config.CASE_STATUS_ESCALATED:
                alert_case_status = config.CASE_STATUS_ESCALATED
            else:
                alert_case_status = config.CASE_STATUS_IN_CASE
            
            # Update df for alerts in this case (only these alerts, preserve others)
            mask = df["alert_id"].isin(alert_ids)
            df.loc[mask, "case_id"] = case_id
            df.loc[mask, "case_status"] = alert_case_status
        
        return df