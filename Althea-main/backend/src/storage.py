"""SQLite storage for case management persistence."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd


class CaseVersionConflictError(Exception):
    """Raised when optimistic concurrency check fails (stale write)."""
    pass


class Storage:
    """SQLite-based storage for cases, alerts, audit log, and locks."""
    
    def __init__(self, db_path: str = "data/app.db"):
        """
        Initialize storage.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Cases table (Phase E: state, owner, version)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                case_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                owner TEXT,
                version INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Migration: add Phase E columns to cases if missing
        try:
            cursor.execute("PRAGMA table_info(cases)")
            cols = {row[1] for row in cursor.fetchall()}
            if "state" not in cols:
                cursor.execute("ALTER TABLE cases ADD COLUMN state TEXT")
                if "status" in cols:
                    cursor.execute("UPDATE cases SET state = COALESCE(status, 'OPEN') WHERE state IS NULL")
                cursor.execute("UPDATE cases SET state = 'OPEN' WHERE state IS NULL OR state = ''")
            if "owner" not in cols:
                cursor.execute("ALTER TABLE cases ADD COLUMN owner TEXT")
                if "assigned_to" in cols:
                    cursor.execute("UPDATE cases SET owner = assigned_to WHERE owner IS NULL")
            if "version" not in cols:
                cursor.execute("ALTER TABLE cases ADD COLUMN version INTEGER NOT NULL DEFAULT 0")
            if "created_at" not in cols:
                cursor.execute("ALTER TABLE cases ADD COLUMN created_at TEXT")
                cursor.execute("UPDATE cases SET created_at = datetime('now') WHERE created_at IS NULL")
            if "updated_at" not in cols:
                cursor.execute("ALTER TABLE cases ADD COLUMN updated_at TEXT")
                cursor.execute("UPDATE cases SET updated_at = datetime('now') WHERE updated_at IS NULL")
        except Exception:
            pass

        # Case alerts junction table (Phase E: added_at)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS case_alerts (
                case_id TEXT NOT NULL,
                alert_id TEXT NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (case_id, alert_id),
                FOREIGN KEY (case_id) REFERENCES cases(case_id)
            )
        """)
        try:
            cursor.execute("PRAGMA table_info(case_alerts)")
            ca_cols = {row[1] for row in cursor.fetchall()}
            if "added_at" not in ca_cols:
                cursor.execute("ALTER TABLE case_alerts ADD COLUMN added_at TEXT")
                cursor.execute("UPDATE case_alerts SET added_at = datetime('now') WHERE added_at IS NULL OR added_at = ''")
        except Exception:
            pass

        # Audit log table (Phase E: immutable, hash-chained)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                prev_hash TEXT,
                entry_hash TEXT NOT NULL
            )
        """)
        # Migration: if old audit_log has event_id, rename and use new
        try:
            cursor.execute("PRAGMA table_info(audit_log)")
            al_cols = [row[1] for row in cursor.fetchall()]
            if "event_id" in al_cols and "audit_id" not in al_cols:
                cursor.execute("ALTER TABLE audit_log RENAME TO audit_log_legacy")
                cursor.execute("""
                    CREATE TABLE audit_log (
                        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        action TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        prev_hash TEXT,
                        entry_hash TEXT NOT NULL
                    )
                """)
        except Exception:
            pass

        # Locks table (Phase E: resource_type, resource_id)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locks (
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                PRIMARY KEY (resource_type, resource_id)
            )
        """)
        # Migration: if old locks has case_id, migrate to new schema
        try:
            cursor.execute("PRAGMA table_info(locks)")
            lock_cols = [row[1] for row in cursor.fetchall()]
            if "case_id" in lock_cols and "resource_type" not in lock_cols:
                cursor.execute("DROP TABLE IF EXISTS locks")
                cursor.execute("""
                    CREATE TABLE locks (
                        resource_type TEXT NOT NULL,
                        resource_id TEXT NOT NULL,
                        owner TEXT NOT NULL,
                        acquired_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        PRIMARY KEY (resource_type, resource_id)
                    )
                """)
        except Exception:
            pass
        
        # Alerts table (single source of truth)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                user_id TEXT,
                tx_ref TEXT,
                created_at TEXT,
                segment TEXT,
                typology TEXT,
                risk_score_raw REAL,
                risk_prob REAL,
                risk_score REAL,
                risk_band TEXT,
                priority TEXT,
                model_version TEXT,
                top_features_json TEXT,
                top_feature_contributions_json TEXT,
                risk_explain_json TEXT,
                governance_status TEXT,
                suppression_code TEXT,
                suppression_reason TEXT,
                in_queue INTEGER,
                policy_version TEXT,
                features_json TEXT,
                ml_signals_json TEXT,
                rules_json TEXT,
                rule_evidence_json TEXT,
                external_versions_json TEXT,
                decision_trace_json TEXT,
                schema_version TEXT,
                run_id TEXT,
                updated_at TEXT
            )
        """)

        # Governance policy table (versioned params)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS governance_policy (
                policy_version TEXT PRIMARY KEY,
                params_json TEXT,
                created_at TEXT,
                author TEXT
            )
        """)

        # Phase G: Alert daily stats table for health monitoring
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_daily_stats (
                date TEXT PRIMARY KEY,
                total_alerts INTEGER NOT NULL,
                in_queue INTEGER NOT NULL,
                mandatory_review INTEGER NOT NULL,
                suppressed INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        # Phase G: Rule hit stats table for health monitoring
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rule_hit_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                hit_rate REAL NOT NULL,
                n_alerts INTEGER NOT NULL,
                n_hits INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rule_hit_stats_date
            ON rule_hit_stats(run_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rule_hit_stats_rule
            ON rule_hit_stats(rule_id)
        """)

        # Migration: add Phase D risk columns and Phase F external_versions_json to alerts if missing (existing DBs)
        try:
            cursor.execute("PRAGMA table_info(alerts)")
            cols = [row[1] for row in cursor.fetchall()]
            if "risk_prob" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN risk_prob REAL")
            if "risk_explain_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN risk_explain_json TEXT")
            if "external_versions_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN external_versions_json TEXT")
            if "run_id" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN run_id TEXT")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_run_id ON alerts(run_id)")
            if "decision_trace_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN decision_trace_json TEXT")
            if "schema_version" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN schema_version TEXT")
            if "context_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN context_json TEXT")
            if "priority" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN priority TEXT")
            if "model_version" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN model_version TEXT")
            if "top_features_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN top_features_json TEXT")
            if "top_feature_contributions_json" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN top_feature_contributions_json TEXT")
            if "hard_constraint" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN hard_constraint INTEGER DEFAULT 0")
            if "hard_constraint_reason" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN hard_constraint_reason TEXT")
            if "hard_constraint_code" not in cols:
                cursor.execute("ALTER TABLE alerts ADD COLUMN hard_constraint_code TEXT")
        except Exception:
            pass

        # Runs table: tracks each pipeline execution with source, hash, row count
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                dataset_hash TEXT,
                row_count INTEGER,
                created_at TEXT NOT NULL,
                notes TEXT
            )
        """)

        # Run artifacts: determinism hashes per run (config_hash, model_hash, rules_hash, etc.)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_artifacts (
                run_id TEXT PRIMARY KEY,
                policy_version TEXT,
                schema_version TEXT,
                model_hash TEXT,
                rules_hash TEXT,
                external_versions_json TEXT,
                config_hash TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        """)

        # AI Summaries table – stores generated AI narrative summaries
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_summaries (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                ts TEXT NOT NULL,
                model TEXT,
                prompt_hash TEXT,
                run_id TEXT,
                policy_version TEXT,
                actor TEXT,
                PRIMARY KEY (entity_type, entity_id)
            )
        """)

        conn.commit()
        conn.close()
    
    def load_state_from_db(self) -> Tuple[Dict, int, List]:
        """
        Load state from database.
        
        Returns:
            Tuple of (cases_dict, case_counter, audit_log_list)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Load cases (Phase E: state, owner, version; fallback to status/assigned_to for old DBs)
        cases = {}
        case_counter = 1
        try:
            cursor.execute("PRAGMA table_info(cases)")
            case_cols = {r[1] for r in cursor.fetchall()}
        except Exception:
            case_cols = set()
        if "state" in case_cols:
            cursor.execute("""
                SELECT case_id, state, owner, version, notes, created_at, updated_at
                FROM cases
            """)
        else:
            cursor.execute("""
                SELECT case_id, status, assigned_to, notes, created_at, updated_at
                FROM cases
            """)

        for row in cursor.fetchall():
            if "state" in case_cols:
                case_id, state, owner, version, notes, created_at, updated_at = row
            else:
                case_id, state, owner, version, notes, created_at, updated_at = (
                    row[0], row[1], row[2], 0, row[3], row[4], row[5]
                )
            if state is None or state == "":
                state = "OPEN"
            if state == "NEW":
                state = "OPEN"
            # Load alert_ids for this case
            cursor.execute("SELECT alert_id FROM case_alerts WHERE case_id = ?", (case_id,))
            alert_ids = [r[0] for r in cursor.fetchall()]

            cases[case_id] = {
                "status": state,
                "state": state,
                "assigned_to": owner or "",
                "owner": owner or "",
                "version": version if version is not None else 0,
                "notes": notes or "",
                "alert_ids": alert_ids,
                "created_at": created_at,
                "updated_at": updated_at or created_at,
            }
            
            # Extract counter from case_id if possible (CASE_001 -> 1)
            try:
                if case_id.startswith("CASE_"):
                    num = int(case_id.split("_")[1])
                    case_counter = max(case_counter, num + 1)
            except (ValueError, IndexError):
                pass
        
        # Load audit log (Phase E: from new schema if available)
        audit_log = []
        try:
            cursor.execute("PRAGMA table_info(audit_log)")
            al_cols = [r[1] for r in cursor.fetchall()]
            if "audit_id" in al_cols:
                cursor.execute("""
                    SELECT audit_id, ts, actor, action, entity_type, entity_id, payload_json
                    FROM audit_log ORDER BY audit_id DESC
                """)
                for row in cursor.fetchall():
                    audit_id, ts, actor, action, entity_type, entity_id, payload_json = row
                    payload = json.loads(payload_json) if payload_json else {}
                    payload["case_id"] = entity_id if entity_type == "case" else ""
                    audit_log.append({
                        "event_id": str(audit_id),
                        "case_id": entity_id if entity_type == "case" else "",
                        "ts": ts,
                        "actor": actor,
                        "action": action,
                        "payload": payload,
                    })
            else:
                cursor.execute("""
                    SELECT event_id, case_id, ts, actor, action, payload_json
                    FROM audit_log ORDER BY ts DESC
                """)
                for row in cursor.fetchall():
                    event_id, case_id, ts, actor, action, payload_json = row
                    payload = json.loads(payload_json) if payload_json else {}
                    audit_log.append({
                        "event_id": event_id,
                        "case_id": case_id,
                        "ts": ts,
                        "actor": actor,
                        "action": action,
                        "payload": payload,
                    })
        except Exception:
            pass

        conn.close()
        return cases, case_counter, audit_log
    
    def save_case_to_db(self, case_dict: Dict):
        """
        Save case to database (create or full replace). For state updates use update_case_state.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        case_id = case_dict["case_id"]
        state = case_dict.get("state", case_dict.get("status", "OPEN"))
        if state == "NEW":
            state = "OPEN"
        owner = case_dict.get("owner", case_dict.get("assigned_to", ""))
        version = int(case_dict.get("version", 0))
        notes = case_dict.get("notes", "")
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        created_at = case_dict.get("created_at", now_iso)
        if isinstance(created_at, (int, float)):
            created_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(created_at))
        updated_at = now_iso

        cursor.execute("""
            INSERT OR REPLACE INTO cases (case_id, state, owner, version, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (case_id, state, owner or None, version, notes, str(created_at), updated_at))

        alert_ids = case_dict.get("alert_ids", [])
        cursor.execute("DELETE FROM case_alerts WHERE case_id = ?", (case_id,))
        for alert_id in alert_ids:
            cursor.execute("""
                INSERT OR REPLACE INTO case_alerts (case_id, alert_id, added_at)
                VALUES (?, ?, ?)
            """, (case_id, alert_id, now_iso))
        conn.commit()
        conn.close()

    def delete_case(self, case_id: str) -> bool:
        """Delete a case and its case_alerts. Returns True if deleted."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM case_alerts WHERE case_id = ?", (case_id,))
        cursor.execute("DELETE FROM cases WHERE case_id = ?", (case_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def update_case_state(
        self,
        case_id: str,
        new_state: str,
        actor: str,
        expected_version: int,
        new_owner: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """
        Update case state with optimistic concurrency. Raises CaseVersionConflictError on mismatch.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT version, state, owner FROM cases WHERE case_id = ?",
            (case_id,),
        )
        row = cursor.fetchone()
        if not row:
            conn.close()
            raise ValueError(f"Case {case_id} not found")
        cur_version, cur_state, cur_owner = row
        if int(cur_version) != int(expected_version):
            conn.close()
            raise CaseVersionConflictError(
                f"Version conflict: expected {expected_version}, current {cur_version}. Reload and retry."
            )
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        next_version = int(cur_version) + 1
        owner = new_owner if new_owner is not None else cur_owner
        notes_val = notes if notes is not None else ""
        cursor.execute(
            "SELECT notes FROM cases WHERE case_id = ?", (case_id,)
        )
        r = cursor.fetchone()
        if r and notes is None:
            notes_val = r[0] or ""
        cursor.execute("""
            UPDATE cases SET state = ?, owner = ?, version = ?, notes = ?, updated_at = ?
            WHERE case_id = ? AND version = ?
        """, (new_state, owner or None, next_version, notes_val, now_iso, case_id, cur_version))
        if cursor.rowcount == 0:
            conn.close()
            raise CaseVersionConflictError("Version conflict during update")
        conn.commit()
        conn.close()
    
    def append_audit(
        self,
        entity_type: str,
        entity_id: str,
        actor: str,
        action: str,
        payload_dict: Dict,
    ) -> None:
        """
        Append immutable audit entry with hash chaining. No updates/deletes.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        payload_json = json.dumps(payload_dict, sort_keys=True)
        cursor.execute("""
            SELECT entry_hash FROM audit_log
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY audit_id DESC LIMIT 1
        """, (entity_type, entity_id))
        row = cursor.fetchone()
        prev_hash = row[0] if row else None
        to_hash = f"{prev_hash or ''}{ts}{actor}{action}{entity_type}{entity_id}{payload_json}"
        entry_hash = hashlib.sha256(to_hash.encode("utf-8")).hexdigest()
        cursor.execute("""
            INSERT INTO audit_log (ts, actor, action, entity_type, entity_id, payload_json, prev_hash, entry_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts, actor, action, entity_type, entity_id, payload_json, prev_hash, entry_hash))
        conn.commit()
        conn.close()

    def verify_audit_chain(self, entity_type: str, entity_id: str) -> Tuple[bool, Optional[str]]:
        """
        Verify hash chain for entity. Returns (pass: bool, error_message: Optional[str]).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT audit_id, prev_hash, entry_hash, ts, actor, action, entity_type, entity_id, payload_json
            FROM audit_log
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY audit_id ASC
        """, (entity_type, entity_id))
        rows = cursor.fetchall()
        conn.close()
        prev_hash = None
        for row in rows:
            aid, p_h, e_h, ts, actor, action, et, eid, payload_json = row
            if p_h != prev_hash:
                return False, f"Chain broken at audit_id={aid}: prev_hash mismatch"
            to_hash = f"{p_h or ''}{ts}{actor}{action}{et}{eid}{payload_json}"
            expected = hashlib.sha256(to_hash.encode("utf-8")).hexdigest()
            if expected != e_h:
                return False, f"Chain broken at audit_id={aid}: hash mismatch"
            prev_hash = e_h
        return True, None
    
    def link_alerts_to_case(self, case_id: str, alert_ids: List[str]) -> None:
        """Link alerts to a case. Uses added_at for audit."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute("DELETE FROM case_alerts WHERE case_id = ?", (case_id,))
        for alert_id in alert_ids:
            cursor.execute("""
                INSERT OR REPLACE INTO case_alerts (case_id, alert_id, added_at)
                VALUES (?, ?, ?)
            """, (case_id, alert_id, now_iso))
        conn.commit()
        conn.close()

    def add_alerts_to_case(self, case_id: str, alert_ids: List[str]) -> None:
        """Add alerts to case without removing existing. Uses added_at."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        for alert_id in alert_ids:
            cursor.execute("""
                INSERT OR REPLACE INTO case_alerts (case_id, alert_id, added_at)
                VALUES (?, ?, ?)
            """, (case_id, alert_id, now_iso))
        conn.commit()
        conn.close()

    def remove_alert_from_case(self, case_id: str, alert_id: str) -> None:
        """Remove one alert from case."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM case_alerts WHERE case_id = ? AND alert_id = ?", (case_id, alert_id))
        conn.commit()
        conn.close()
    
    def acquire_lock(
        self,
        resource_type: str,
        resource_id: str,
        owner: str,
        ttl_seconds: int = 120,
    ) -> bool:
        """Acquire lock. Returns True if acquired, False if held by another."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        now_ts = time.time()
        expires_ts = now_ts + ttl_seconds
        expires_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(expires_ts))
        cursor.execute(
            "SELECT owner, expires_at FROM locks WHERE resource_type = ? AND resource_id = ?",
            (resource_type, resource_id),
        )
        row = cursor.fetchone()
        if row:
            cur_owner, exp_str = row
            try:
                exp_ts = time.mktime(time.strptime(exp_str, "%Y-%m-%d %H:%M:%S"))
            except Exception:
                exp_ts = 0
            if exp_ts > now_ts:
                if cur_owner == owner:
                    cursor.execute(
                        "UPDATE locks SET acquired_at = ?, expires_at = ? WHERE resource_type = ? AND resource_id = ?",
                        (now_iso, expires_iso, resource_type, resource_id),
                    )
                    conn.commit()
                    conn.close()
                    return True
                conn.close()
                return False
        cursor.execute("""
            INSERT OR REPLACE INTO locks (resource_type, resource_id, owner, acquired_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
        """, (resource_type, resource_id, owner, now_iso, expires_iso))
        conn.commit()
        conn.close()
        return True

    def renew_lock(
        self,
        resource_type: str,
        resource_id: str,
        owner: str,
        ttl_seconds: int = 120,
    ) -> bool:
        """Renew lock if held by owner. Returns True if renewed."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        expires_ts = time.time() + ttl_seconds
        expires_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(expires_ts))
        cursor.execute("""
            UPDATE locks SET acquired_at = ?, expires_at = ?
            WHERE resource_type = ? AND resource_id = ? AND owner = ?
        """, (now_iso, expires_iso, resource_type, resource_id, owner))
        ok = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return ok

    def release_lock(
        self,
        resource_type_or_case_id: str,
        resource_id_or_actor: str,
        owner: Optional[str] = None,
    ) -> None:
        """Release lock. (resource_type, resource_id, owner) or legacy (case_id, actor)."""
        if owner is not None:
            rt, rid, own = resource_type_or_case_id, resource_id_or_actor, owner
        else:
            rt, rid, own = "case", resource_type_or_case_id, resource_id_or_actor
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM locks WHERE resource_type = ? AND resource_id = ? AND owner = ?",
            (rt, rid, own),
        )
        conn.commit()
        conn.close()

    def get_lock(
        self,
        resource_type: str,
        resource_id: str,
    ) -> Optional[Dict]:
        """Get current lock info. Returns None if expired or not locked."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT owner, acquired_at, expires_at FROM locks WHERE resource_type = ? AND resource_id = ?",
            (resource_type, resource_id),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        owner, acquired_at, expires_at = row
        try:
            exp_ts = time.mktime(time.strptime(expires_at, "%Y-%m-%d %H:%M:%S"))
        except Exception:
            return None
        if exp_ts <= time.time():
            return None
        return {"owner": owner, "acquired_at": acquired_at, "expires_at": expires_at}

    def get_lock_info(self, case_id: str) -> Optional[Dict]:
        """Backward-compat: get lock for case. Returns dict with locked_by, locked_at, expires_at."""
        info = self.get_lock("case", case_id)
        if not info:
            return None
        return {
            "locked_by": info["owner"],
            "locked_at": info["acquired_at"],
            "expires_at": info["expires_at"],
        }

    def refresh_lock(self, case_id: str, actor: str, lock_duration: int = 120) -> None:
        """Backward-compat: renew lock for case."""
        self.renew_lock("case", case_id, actor, lock_duration)
    
    def get_audit_log_for_case(self, case_id: str) -> List[Dict]:
        """Get audit log events for a case. Supports new (entity_type/entity_id) and legacy schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA table_info(audit_log)")
            cols = [r[1] for r in cursor.fetchall()]
        except Exception:
            cols = []
        events = []
        if "entity_type" in cols:
            cursor.execute("""
                SELECT audit_id, ts, actor, action, payload_json
                FROM audit_log
                WHERE entity_type = 'case' AND entity_id = ?
                ORDER BY audit_id ASC
            """, (case_id,))
            for row in cursor.fetchall():
                audit_id, ts, actor, action, payload_json = row
                payload = json.loads(payload_json) if payload_json else {}
                events.append({
                    "event_id": str(audit_id),
                    "case_id": case_id,
                    "ts": ts,
                    "actor": actor,
                    "action": action,
                    "payload": payload,
                })
        else:
            cursor.execute("""
                SELECT event_id, case_id, ts, actor, action, payload_json
                FROM audit_log WHERE case_id = ? ORDER BY ts DESC
            """, (case_id,))
            for row in cursor.fetchall():
                event_id, cid, ts, actor, action, payload_json = row
                payload = json.loads(payload_json) if payload_json else {}
                events.append({
                    "event_id": event_id,
                    "case_id": cid,
                    "ts": ts,
                    "actor": actor,
                    "action": action,
                    "payload": payload,
                })
        conn.close()
        return list(reversed(events))

    def get_governance_policy(self, policy_version: str) -> Optional[Dict[str, Any]]:
        """
        Load governance policy params by version.

        Returns:
            Dict of params (e.g. min_risk, daily_budget, max_share_per_segment)
            or None if version not found.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT params_json FROM governance_policy WHERE policy_version = ?",
            (policy_version,),
        )
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            try:
                return json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                return None
        return None

    def save_governance_policy(
        self,
        policy_version: str,
        params: Dict[str, Any],
        author: str = "",
    ) -> None:
        """Save or update a governance policy version."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        params_json = json.dumps(params)
        cursor.execute("""
            INSERT OR REPLACE INTO governance_policy (policy_version, params_json, created_at, author)
            VALUES (?, ?, ?, ?)
        """, (policy_version, params_json, now_iso, author))
        conn.commit()
        conn.close()

    def upsert_alerts(self, alerts: List[Dict[str, Any]], run_id: Optional[str] = None):
        """
        Upsert alerts to database (INSERT OR REPLACE).
        
        Args:
            alerts: List of alert dictionaries with required fields
            run_id: Optional run_id to tag every alert with
        """
        if not alerts:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        
        for alert in alerts:
            alert_id = alert.get("alert_id", "")
            if not alert_id:
                continue
            
            # Extract fields with defaults
            user_id = str(alert.get("user_id", ""))
            tx_ref = str(alert.get("tx_ref", alert.get("tx_id", "")))
            created_at = str(alert.get("created_at", now_iso))
            segment = str(alert.get("segment", ""))
            typology = str(alert.get("typology", ""))
            risk_score_raw = float(alert.get("risk_score_raw", 0.0))
            risk_prob = float(alert.get("risk_prob", 0.0))
            risk_score = float(alert.get("risk_score", 0.0))
            risk_band = str(alert.get("risk_band", ""))
            priority = str(alert.get("priority", risk_band.lower() if risk_band else ""))
            model_version = str(alert.get("model_version", ""))
            top_features_raw = alert.get("top_features_json", alert.get("top_features", []))
            top_feature_contrib_raw = alert.get(
                "top_feature_contributions_json",
                alert.get("top_feature_contributions", []),
            )
            top_features_json = (
                json.dumps(top_features_raw) if not isinstance(top_features_raw, str) else (top_features_raw or "[]")
            )
            top_feature_contributions_json = (
                json.dumps(top_feature_contrib_raw)
                if not isinstance(top_feature_contrib_raw, str)
                else (top_feature_contrib_raw or "[]")
            )
            risk_explain_json = alert.get("risk_explain_json", "")
            if not isinstance(risk_explain_json, str) and risk_explain_json is not None:
                risk_explain_json = json.dumps(risk_explain_json) if risk_explain_json != "" else "{}"
            risk_explain_json = str(risk_explain_json or "{}")
            governance_status = str(alert.get("governance_status", ""))
            suppression_code = str(alert.get("suppression_code", ""))
            suppression_reason = str(alert.get("suppression_reason", ""))
            in_queue = 1 if alert.get("in_queue", False) else 0
            policy_version = str(alert.get("policy_version", "1.0"))
            external_versions_json = alert.get("external_versions_json", "{}")
            if not isinstance(external_versions_json, str):
                external_versions_json = json.dumps(external_versions_json) if external_versions_json else "{}"
            else:
                external_versions_json = external_versions_json or "{}"
            
            # JSON fields - support both names (pipeline uses rules_json, queue uses rules)
            features_raw = alert.get("features_json", alert.get("features", {}))
            ml_raw = alert.get("ml_signals_json", alert.get("ml_signals", {}))
            rules_raw = alert.get("rules_json", alert.get("rules", []))
            rule_ev_raw = alert.get("rule_evidence_json", alert.get("rule_evidence", {}))
            features_json = json.dumps(features_raw) if not isinstance(features_raw, str) else (features_raw or "{}")
            ml_signals_json = json.dumps(ml_raw) if not isinstance(ml_raw, str) else (ml_raw or "{}")
            rules_json = json.dumps(rules_raw) if not isinstance(rules_raw, str) else (rules_raw or "[]")
            rule_evidence_json = json.dumps(rule_ev_raw) if not isinstance(rule_ev_raw, str) else (rule_ev_raw or "{}")

            # run_id: prefer explicit param, then per-alert value
            rid = run_id or alert.get("run_id") or None
            decision_trace_json_val = alert.get("decision_trace_json", "")
            if not isinstance(decision_trace_json_val, str) and decision_trace_json_val is not None:
                decision_trace_json_val = json.dumps(decision_trace_json_val) if decision_trace_json_val != "" else "{}"
            decision_trace_json_val = str(decision_trace_json_val or "{}")
            schema_version_val = str(alert.get("schema_version", "1.0"))
            context_json_val = alert.get("context_json", "{}")
            if not isinstance(context_json_val, str) and context_json_val is not None:
                context_json_val = json.dumps(context_json_val) if context_json_val != "" else "{}"
            context_json_val = str(context_json_val or "{}")
            hard_constraint = 1 if alert.get("hard_constraint", 0) else 0
            hard_constraint_reason = str(alert.get("hard_constraint_reason", "") or "")
            hard_constraint_code = str(alert.get("hard_constraint_code", "") or "")

            cursor.execute("""
                INSERT OR REPLACE INTO alerts (
                    alert_id, user_id, tx_ref, created_at, segment, typology,
                    risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                    governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                    features_json, ml_signals_json, rules_json, rule_evidence_json,
                    external_versions_json,
                    decision_trace_json, schema_version, context_json,
                    hard_constraint, hard_constraint_reason, hard_constraint_code,
                    run_id,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert_id, user_id, tx_ref, created_at, segment, typology,
                risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                features_json, ml_signals_json, rules_json, rule_evidence_json,
                external_versions_json,
                decision_trace_json_val, schema_version_val, context_json_val,
                hard_constraint, hard_constraint_reason, hard_constraint_code,
                rid,
                now_iso
            ))
        
        conn.commit()
        conn.close()
    
    def load_alerts_df(self) -> pd.DataFrame:
        """
        Load all alerts from database as DataFrame.
        
        Returns:
            DataFrame with all alert columns
        """
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql_query("""
            SELECT 
                alert_id, user_id, tx_ref, created_at, segment, typology,
                risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                features_json, ml_signals_json, rules_json, rule_evidence_json,
                external_versions_json,
                updated_at
            FROM alerts
        """, conn)
        
        conn.close()
        
        # Convert in_queue to boolean
        if "in_queue" in df.columns:
            df["in_queue"] = df["in_queue"].astype(bool)
        
        return df

    def load_queue_df(self) -> pd.DataFrame:
        """Load queue alerts from database (in_queue=1) ordered by risk_score desc."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT 
                alert_id, user_id, tx_ref, created_at, segment, typology,
                risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                features_json, ml_signals_json, rules_json, rule_evidence_json,
                external_versions_json, updated_at
            FROM alerts WHERE in_queue = 1
            ORDER BY risk_score DESC, created_at ASC
        """, conn)
        conn.close()
        if "in_queue" in df.columns:
            df["in_queue"] = df["in_queue"].astype(bool)
        if len(df) > 0:
            df["queue_rank"] = range(1, len(df) + 1)
        return df

    # =========================================================================
    # Run Management Methods
    # =========================================================================

    def save_run(self, run_id: str, source: str, dataset_hash: str,
                 row_count: int, notes: str = "") -> None:
        """Persist a pipeline run record."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute("""
            INSERT OR REPLACE INTO runs (run_id, source, dataset_hash, row_count, created_at, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (run_id, source, dataset_hash, row_count, now_iso, notes))
        conn.commit()
        conn.close()

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Load run metadata by run_id."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT run_id, source, dataset_hash, row_count, created_at, notes FROM runs WHERE run_id = ?", (run_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "run_id": row[0], "source": row[1], "dataset_hash": row[2],
            "row_count": row[3], "created_at": row[4], "notes": row[5],
        }

    def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List recent runs (for UI run selector)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, source, dataset_hash, row_count, created_at, notes
            FROM runs ORDER BY created_at DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [
            {"run_id": r[0], "source": r[1], "dataset_hash": r[2], "row_count": r[3], "created_at": r[4], "notes": r[5] or ""}
            for r in rows
        ]

    def save_run_artifacts(
        self,
        run_id: str,
        policy_version: str = "",
        schema_version: str = "",
        model_hash: str = "",
        rules_hash: str = "",
        external_versions_json: str = "{}",
        config_hash: str = "",
    ) -> None:
        """Persist run artifacts for determinism (config_hash, model_hash, rules_hash, etc.)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute("""
            INSERT OR REPLACE INTO run_artifacts
            (run_id, policy_version, schema_version, model_hash, rules_hash, external_versions_json, config_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, policy_version, schema_version, model_hash, rules_hash, external_versions_json, config_hash, now_iso))
        conn.commit()
        conn.close()

    def get_run_artifacts(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Load run artifacts by run_id."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, policy_version, schema_version, model_hash, rules_hash, external_versions_json, config_hash, created_at
            FROM run_artifacts WHERE run_id = ?
        """, (run_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "run_id": row[0], "policy_version": row[1], "schema_version": row[2],
            "model_hash": row[3], "rules_hash": row[4], "external_versions_json": row[5],
            "config_hash": row[6], "created_at": row[7],
        }

    def load_alerts_by_run(self, run_id: str) -> pd.DataFrame:
        """Load alerts for a specific run_id. Returns empty DataFrame if table missing or error."""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("""
                SELECT 
                    alert_id, user_id, tx_ref, created_at, segment, typology,
                    risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                    governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                    features_json, ml_signals_json, rules_json, rule_evidence_json,
                    external_versions_json, decision_trace_json, schema_version, context_json,
                    hard_constraint, hard_constraint_reason, hard_constraint_code,
                    run_id, updated_at
                FROM alerts WHERE run_id = ?
            """, conn, params=(run_id,))
            conn.close()
            if "in_queue" in df.columns:
                df["in_queue"] = df["in_queue"].astype(bool)
            return df
        except Exception:
            return pd.DataFrame()

    def load_queue_by_run(self, run_id: str) -> pd.DataFrame:
        """Load queue alerts (in_queue=1) for a specific run_id."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT 
                alert_id, user_id, tx_ref, created_at, segment, typology,
                risk_score_raw, risk_prob, risk_score, risk_band, priority, model_version, top_features_json, top_feature_contributions_json, risk_explain_json,
                governance_status, suppression_code, suppression_reason, in_queue, policy_version,
                features_json, ml_signals_json, rules_json, rule_evidence_json,
                external_versions_json, run_id, updated_at
            FROM alerts WHERE in_queue = 1 AND run_id = ?
            ORDER BY risk_score DESC, created_at ASC
        """, conn, params=(run_id,))
        conn.close()
        if "in_queue" in df.columns:
            df["in_queue"] = df["in_queue"].astype(bool)
        if len(df) > 0:
            df["queue_rank"] = range(1, len(df) + 1)
        return df

    def delete_alerts_by_run(self, run_id: str) -> int:
        """Delete all alerts for a given run_id. Returns count deleted."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM alerts WHERE run_id = ?", (run_id,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted

    # =========================================================================
    # Phase G: Health Monitoring Storage Methods
    # =========================================================================

    def upsert_daily_stats(self, stats: Dict[str, Any]) -> None:
        """Upsert daily stats for health monitoring."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute("""
            INSERT OR REPLACE INTO alert_daily_stats
            (date, total_alerts, in_queue, mandatory_review, suppressed, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            stats.get("date", time.strftime("%Y-%m-%d")),
            int(stats.get("total_alerts", 0)),
            int(stats.get("in_queue", 0)),
            int(stats.get("mandatory_review", 0)),
            int(stats.get("suppressed", 0)),
            now_iso,
        ))
        conn.commit()
        conn.close()

    def load_daily_stats(self, limit: int = 7) -> List[Dict[str, Any]]:
        """Load recent daily stats for baseline computation."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT date, total_alerts, in_queue, mandatory_review, suppressed, created_at
            FROM alert_daily_stats ORDER BY date DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [
            {"date": r[0], "total_alerts": r[1], "in_queue": r[2],
             "mandatory_review": r[3], "suppressed": r[4], "created_at": r[5]}
            for r in rows
        ]

    def upsert_rule_hit_stats(self, stats_list: List[Dict[str, Any]]) -> None:
        """Upsert rule hit stats for health monitoring."""
        if not stats_list:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        today = stats_list[0].get("run_date", time.strftime("%Y-%m-%d"))
        cursor.execute("DELETE FROM rule_hit_stats WHERE run_date = ?", (today,))
        for stats in stats_list:
            cursor.execute("""
                INSERT INTO rule_hit_stats
                (run_date, rule_id, hit_rate, n_alerts, n_hits, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                stats.get("run_date", today), str(stats.get("rule_id", "")),
                float(stats.get("hit_rate", 0.0)), int(stats.get("n_alerts", 0)),
                int(stats.get("n_hits", 0)), now_iso,
            ))
        conn.commit()
        conn.close()

    def load_rule_hit_stats(self, limit_days: int = 7) -> pd.DataFrame:
        """Load recent rule hit stats for baseline computation."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT run_date, rule_id, hit_rate, n_alerts, n_hits, created_at
            FROM rule_hit_stats
            WHERE run_date IN (
                SELECT DISTINCT run_date FROM rule_hit_stats
                ORDER BY run_date DESC LIMIT ?
            ) ORDER BY run_date DESC, rule_id
        """, conn, params=(limit_days,))
        conn.close()
        return df

    def get_baseline_rule_rates(self, exclude_today: bool = True) -> pd.DataFrame:
        """Get aggregated baseline rule hit rates from historical data."""
        conn = sqlite3.connect(self.db_path)
        today = time.strftime("%Y-%m-%d")
        if exclude_today:
            df = pd.read_sql_query("""
                SELECT rule_id, AVG(hit_rate) as hit_rate,
                       SUM(n_alerts) as n_alerts, SUM(n_hits) as n_hits
                FROM rule_hit_stats WHERE run_date < ? GROUP BY rule_id
            """, conn, params=(today,))
        else:
            df = pd.read_sql_query("""
                SELECT rule_id, AVG(hit_rate) as hit_rate,
                       SUM(n_alerts) as n_alerts, SUM(n_hits) as n_hits
                FROM rule_hit_stats GROUP BY rule_id
            """, conn)
        conn.close()
        return df

    # =========================================================================
    # AI Summaries Storage Methods
    # =========================================================================

    def save_ai_summary(
        self, entity_type: str, entity_id: str, summary: str,
        model: str = "", prompt_hash: str = "", run_id: str = "",
        policy_version: str = "", actor: str = "",
    ) -> None:
        """Persist an AI-generated summary (upsert by entity_type + entity_id)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        cursor.execute("""
            INSERT OR REPLACE INTO ai_summaries
                (entity_type, entity_id, summary, ts, model, prompt_hash,
                 run_id, policy_version, actor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entity_type, entity_id, summary, ts, model, prompt_hash,
              run_id, policy_version, actor))
        conn.commit()
        conn.close()

    def get_ai_summary(self, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """Load a stored AI summary. Returns dict or None."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT summary, ts, model, prompt_hash, run_id, policy_version, actor
            FROM ai_summaries WHERE entity_type = ? AND entity_id = ?
        """, (entity_type, entity_id))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "summary": row[0], "ts": row[1], "model": row[2] or "",
            "prompt_hash": row[3] or "", "run_id": row[4] or "",
            "policy_version": row[5] or "", "actor": row[6] or "",
        }

    def delete_ai_summary(self, entity_type: str, entity_id: str) -> None:
        """Remove a stored AI summary."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM ai_summaries WHERE entity_type = ? AND entity_id = ?",
            (entity_type, entity_id),
        )
        conn.commit()
        conn.close()


def get_storage(db_path: Optional[str] = None) -> Storage:
    """Return default SQLite storage (Postgres-ready abstraction: use this for local run)."""
    return Storage(db_path or "data/app.db")
