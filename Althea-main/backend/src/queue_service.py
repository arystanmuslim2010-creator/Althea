"""Bank-Grade Queue Control: Deduplication, Throttling, SLA, and Daily Budget."""
from __future__ import annotations

import json
from typing import Dict, Optional

import numpy as np
import pandas as pd

from . import config
from .hard_constraints import evaluate_hard_constraints


def _finalize_governance_and_rank(queue_df: pd.DataFrame, policy_version: str) -> None:
    """Set governance_status for suppressible rows, policy_version, queue_rank; drop temp cols."""
    mask_suppressible = queue_df["suppression_allowed"]
    queue_df.loc[mask_suppressible, "governance_status"] = queue_df.loc[
        mask_suppressible, "in_queue"
    ].apply(lambda x: "eligible" if x else "suppressed")
    queue_df["policy_version"] = policy_version
    final_queue = queue_df[queue_df["in_queue"]].copy()
    if len(final_queue) > 0:
        final_queue = final_queue.sort_values("risk_score", ascending=False)
        final_queue["queue_rank"] = range(1, len(final_queue) + 1)
        queue_df.loc[final_queue.index, "queue_rank"] = final_queue["queue_rank"]
    if "_dedupe_key" in queue_df.columns:
        queue_df.drop(columns=["_dedupe_key"], inplace=True)


class QueueService:
    """
    Service for building and managing alert queue with bank-grade controls.
    
    Features:
    - Deduplication (same user + typology within time window)
    - Per-user throttling (cap alerts per customer)
    - Daily budget enforcement (capacity limit)
    - SLA tracking (age buckets: fresh/aging/breached)
    - Suppression reason codes
    - Persistence to database
    """
    
    def __init__(self, storage: Optional[object] = None):
        """
        Initialize QueueService.
        
        Args:
            storage: Optional Storage instance for persistence
        """
        self.storage = storage

    def build_alert_queue(
        self,
        df: pd.DataFrame,
        min_risk: float,
        daily_budget: int,
        per_user_cap: int,
        dedupe_window_seconds: int,
        policy_version: Optional[str] = None,
        persist: bool = True,
        external_flags_per_alert: Optional[Dict[str, Dict[str, bool]]] = None,
        external_versions_snapshot: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> pd.DataFrame:
        """
        Build alert queue with suppression logic applied.
        Hard-constraint alerts are never suppressed (MANDATORY_REVIEW, in_queue=1).

        Args:
            df: DataFrame with alert_id, user_id, risk_score, typology, case_status, segment
            min_risk: Minimum risk score threshold (overridden by policy if loaded)
            daily_budget: Maximum alerts to show per day (overridden by policy if loaded)
            per_user_cap: Maximum alerts per user_id (overridden by policy if loaded)
            dedupe_window_seconds: Time window for deduplication
            policy_version: Governance policy version (loads params from DB if storage set)
            persist: Whether to persist queue state to DB
            external_flags_per_alert: Optional dict alert_id -> {sanctions_hit, mandatory_rule_hit, high_risk_country_critical}
            external_versions_snapshot: Optional dict source_name -> {version, hash} for hard constraint recording and persistence

        Returns:
            DataFrame with columns:
            - in_queue (bool), governance_status, suppression_reason, suppression_code,
            - policy_version, suppression_allowed (bool), sla_age_minutes, sla_bucket, queue_rank
        """
        if len(df) == 0:
            return pd.DataFrame()

        # Load policy params from DB if available (no hardcoded thresholds in queue logic)
        policy_version = policy_version or getattr(config, "CURRENT_POLICY_VERSION", "1.0")
        params = {}
        if self.storage is not None:
            params = self.storage.get_governance_policy(policy_version) or {}
        min_risk = params.get("min_risk", min_risk)
        daily_budget = int(params.get("daily_budget", daily_budget))
        per_user_cap = int(params.get("per_user_cap", per_user_cap))
        dedupe_window_seconds = int(params.get("dedupe_window_seconds", dedupe_window_seconds))
        max_share_per_segment = float(params.get("max_share_per_segment", getattr(config, "MAX_SHARE_PER_SEGMENT", 0.4)))

        # Create working copy
        queue_df = df.copy()

        # Ensure required columns exist
        required_cols = ["alert_id", "user_id", "risk_score"]
        missing = [col for col in required_cols if col not in queue_df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Initialize columns
        queue_df["in_queue"] = True
        queue_df["suppression_reason"] = ""
        queue_df["suppression_code"] = ""
        queue_df["sla_age_minutes"] = 0.0
        queue_df["sla_bucket"] = "fresh"
        queue_df["queue_rank"] = 0
        queue_df["suppression_allowed"] = True
        queue_df["governance_status"] = "eligible"
        queue_df["policy_version"] = policy_version
        queue_df["segment_throttle_flag"] = 0
        
        # Generate created_at if missing
        if "created_at" not in queue_df.columns:
            now = pd.Timestamp.utcnow()
            rng = np.random.default_rng(42)  # Fixed seed for reproducibility
            hours_ago = rng.uniform(0, 72, size=len(queue_df))
            queue_df["created_at"] = now - pd.to_timedelta(hours_ago, unit="h")
        
        # Ensure created_at is datetime
        queue_df["created_at"] = pd.to_datetime(queue_df["created_at"], errors="coerce")
        now = pd.Timestamp.utcnow()
        
        # Compute SLA age
        queue_df["sla_age_minutes"] = (now - queue_df["created_at"]).dt.total_seconds() / 60.0
        queue_df["sla_age_minutes"] = queue_df["sla_age_minutes"].fillna(0.0).clip(lower=0.0)
        
        # Assign SLA buckets
        queue_df["sla_bucket"] = pd.cut(
            queue_df["sla_age_minutes"],
            bins=[-np.inf, 60, 240, np.inf],
            labels=["fresh", "aging", "breached"],
        ).astype(str)
        queue_df["sla_bucket"] = queue_df["sla_bucket"].fillna("fresh")
        
        # Ensure case_status exists
        if "case_status" not in queue_df.columns:
            queue_df["case_status"] = config.CASE_STATUS_NEW
        
        # Ensure typology exists
        if "typology" not in queue_df.columns:
            queue_df["typology"] = "none"
        if "segment" not in queue_df.columns:
            queue_df["segment"] = "unknown"

        # --- Hard constraints (B1): non-suppressible alerts ---
        external_flags_per_alert = external_flags_per_alert or {}
        external_versions_snapshot = external_versions_snapshot or {}
        for idx, row in queue_df.iterrows():
            ext = external_flags_per_alert.get(str(row.get("alert_id", "")), {})
            result = evaluate_hard_constraints(row, ext, external_versions=external_versions_snapshot)
            if result["hard_hit"]:
                queue_df.at[idx, "in_queue"] = True
                queue_df.at[idx, "governance_status"] = "MANDATORY_REVIEW"
                queue_df.at[idx, "suppression_reason"] = result["hard_reason"]
                queue_df.at[idx, "suppression_code"] = result["hard_code"] or ""
                queue_df.at[idx, "suppression_allowed"] = False

        # Rule A: Below threshold (only for suppression_allowed)
        below_threshold = (queue_df["risk_score"] < min_risk) & queue_df["suppression_allowed"]
        queue_df.loc[below_threshold, "in_queue"] = False
        queue_df.loc[below_threshold, "suppression_reason"] = "below_threshold"

        # Rule B: Already in case / closed (only for suppression_allowed)
        not_new = (queue_df["case_status"].astype(str) != config.CASE_STATUS_NEW) & queue_df["suppression_allowed"]
        queue_df.loc[not_new, "in_queue"] = False
        queue_df.loc[not_new, "suppression_reason"] = "already_in_case_or_closed"

        # Work with eligible alerts only from now on
        eligible = queue_df[queue_df["in_queue"]].copy()
        
        if len(eligible) == 0:
            return queue_df
        
        # Rule C: Dedupe (same user + typology within time window)
        eligible = eligible.sort_values("created_at")
        eligible["_dedupe_key"] = (
            eligible["user_id"].astype(str) + "|" + eligible["typology"].astype(str)
        )
        
        # Group by dedupe key and apply time window deduplication
        deduped_to_suppress = []
        
        for key, group in eligible.groupby("_dedupe_key", sort=False):
            if len(group) <= 1:
                continue
            
            # Sort by risk_score desc, then created_at asc (keep highest risk, oldest first)
            group = group.sort_values(["risk_score", "created_at"], ascending=[False, True])
            
            # Keep first (highest risk) alert as reference
            keep_indices = [group.index[0]]
            keep_times = [group.loc[group.index[0], "created_at"]]
            
            # Check others for deduplication
            for idx in group.index[1:]:
                alert_time = group.loc[idx, "created_at"]
                is_duplicate = False
                
                # Check against all kept alerts in this group
                for keep_time in keep_times:
                    time_diff = abs((alert_time - keep_time).total_seconds())
                    if time_diff <= dedupe_window_seconds:
                        # Within window - suppress as duplicate
                        deduped_to_suppress.append(idx)
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    # Outside window - keep (new alert)
                    keep_indices.append(idx)
                    keep_times.append(alert_time)
        
        # Apply dedupe suppression (only for suppression_allowed)
        suppressible_deduped = [i for i in deduped_to_suppress if queue_df.loc[i, "suppression_allowed"]]
        queue_df.loc[suppressible_deduped, "in_queue"] = False
        queue_df.loc[suppressible_deduped, "suppression_reason"] = "deduped"

        # Update eligible after dedupe
        eligible = queue_df[queue_df["in_queue"]].copy()

        if len(eligible) == 0:
            _finalize_governance_and_rank(queue_df, policy_version)
            if persist and self.storage is not None:
                self._persist_queue_state(queue_df, policy_version)
            return queue_df

        # Rule D: Per-user cap (only suppress suppression_allowed)
        eligible = eligible.sort_values("risk_score", ascending=False)
        per_user_kept = []
        per_user_suppressed = []

        for user_id, user_group in eligible.groupby("user_id", sort=False):
            user_group = user_group.sort_values("risk_score", ascending=False)
            keep_count = min(per_user_cap, len(user_group))

            kept = user_group.head(keep_count)
            suppressed = user_group.tail(len(user_group) - keep_count)

            per_user_kept.extend(kept.index)
            if len(suppressed) > 0:
                per_user_suppressed.extend(suppressed.index)

        suppressible_per_user = [i for i in per_user_suppressed if queue_df.loc[i, "suppression_allowed"]]
        queue_df.loc[suppressible_per_user, "in_queue"] = False
        queue_df.loc[suppressible_per_user, "suppression_reason"] = "per_user_cap"

        # Update eligible after per-user cap
        eligible = queue_df[queue_df["in_queue"]].copy()

        if len(eligible) == 0:
            _finalize_governance_and_rank(queue_df, policy_version)
            if persist and self.storage is not None:
                self._persist_queue_state(queue_df, policy_version)
            return queue_df

        # Rule E: Segment-level throttling (B3) then daily budget
        eligible = eligible.sort_values("risk_score", ascending=False)
        segment_cap = max(1, int(daily_budget * max_share_per_segment))
        kept_indices = set()
        # Per-segment cap: no segment exceeds segment_cap
        for _seg, group in eligible.groupby("segment", sort=False):
            seg_top = group.head(segment_cap)
            kept_indices.update(seg_top.index.tolist())
        # Reallocate leftover capacity to next-highest from any segment
        if len(kept_indices) < daily_budget:
            rest = eligible[~eligible.index.isin(kept_indices)].sort_values("risk_score", ascending=False)
            need = daily_budget - len(kept_indices)
            kept_indices.update(rest.head(need).index.tolist())
        # Suppress those not in kept_indices (only suppression_allowed)
        budget_suppressed = eligible.index.difference(kept_indices)
        suppressible_budget = [i for i in budget_suppressed if queue_df.loc[i, "suppression_allowed"]]
        queue_df.loc[suppressible_budget, "in_queue"] = False
        queue_df.loc[suppressible_budget, "suppression_reason"] = "daily_budget_exceeded"
        queue_df.loc[suppressible_budget, "segment_throttle_flag"] = 1

        _finalize_governance_and_rank(queue_df, policy_version)

        # Attach external_versions_json to each row for persistence (Phase F)
        if external_versions_snapshot:
            queue_df["external_versions_json"] = json.dumps(external_versions_snapshot)
        else:
            queue_df["external_versions_json"] = "{}"

        # Persist to database if storage is available and persist=True
        if persist and self.storage is not None:
            self._persist_queue_state(queue_df, policy_version)
        
        return queue_df
    
    def _persist_queue_state(self, queue_df: pd.DataFrame, policy_version: str):
        """
        Persist queue state to database.
        
        Args:
            queue_df: DataFrame with queue state
            policy_version: Policy version string
        """
        if len(queue_df) == 0:
            return
        
        alerts = []
        for _, row in queue_df.iterrows():
            _explain = row.get("risk_explain_json", "")
            if _explain is not None and not isinstance(_explain, str):
                _explain = json.dumps(_explain) if _explain != "" else "{}"
            _ext_ver = row.get("external_versions_json", "{}")
            if not isinstance(_ext_ver, str):
                _ext_ver = json.dumps(_ext_ver) if _ext_ver else "{}"
            alert_dict = {
                "alert_id": str(row.get("alert_id", "")),
                "user_id": str(row.get("user_id", "")),
                "tx_ref": str(row.get("tx_ref", row.get("tx_id", ""))),
                "created_at": str(row.get("created_at", "")),
                "segment": str(row.get("segment", "")),
                "typology": str(row.get("typology", "")),
                "risk_score_raw": float(row.get("risk_score_raw", 0.0)),
                "risk_prob": float(row.get("risk_prob", 0.0)),
                "risk_score": float(row.get("risk_score", 0.0)),
                "risk_band": str(row.get("risk_band", "")),
                "risk_explain_json": _explain if isinstance(_explain, str) else "{}",
                "governance_status": str(row.get("governance_status", "")),
                "suppression_code": str(row.get("suppression_code", "")),
                "suppression_reason": str(row.get("suppression_reason", "")),
                "in_queue": bool(row.get("in_queue", False)),
                "policy_version": policy_version,
                "features": {},
                "ml_signals": {},
                "rules": row.get("rules_json", row.get("rules", [])),
                "rule_evidence": row.get("rule_evidence_json", row.get("rule_evidence", {})),
                "external_versions_json": _ext_ver,
            }
            alerts.append(alert_dict)
        
        self.storage.upsert_alerts(alerts)

    def compute_queue_stats(self, queue_df: pd.DataFrame) -> Dict[str, any]:
        """
        Compute queue statistics.
        
        Args:
            queue_df: DataFrame from build_alert_queue
            
        Returns:
            Dictionary with stats:
            - total_alerts
            - eligible_alerts
            - suppressed_count
            - suppressed_by_reason (dict)
            - avg_sla_age_minutes
            - sla_bucket_counts (dict)
        """
        if len(queue_df) == 0:
            return {
                "total_alerts": 0,
                "eligible_alerts": 0,
                "suppressed_count": 0,
                "suppressed_by_reason": {},
                "avg_sla_age_minutes": 0.0,
                "sla_bucket_counts": {},
            }
        
        total_alerts = len(queue_df)
        
        if "in_queue" not in queue_df.columns:
            eligible_alerts = total_alerts
            suppressed_count = 0
        else:
            eligible_alerts = queue_df["in_queue"].sum()
            suppressed_count = total_alerts - eligible_alerts
        
        # Suppression by reason
        suppressed_by_reason = {}
        if "suppression_reason" in queue_df.columns:
            suppressed = queue_df[queue_df["in_queue"] == False] if "in_queue" in queue_df.columns else pd.DataFrame()
            if len(suppressed) > 0:
                reason_counts = suppressed["suppression_reason"].value_counts().to_dict()
                suppressed_by_reason = {k: int(v) for k, v in reason_counts.items() if k and k != ""}
        
        # Average SLA age
        avg_sla_age_minutes = 0.0
        if "sla_age_minutes" in queue_df.columns:
            eligible = queue_df[queue_df["in_queue"]] if "in_queue" in queue_df.columns else queue_df
            if len(eligible) > 0:
                avg_sla_age_minutes = float(eligible["sla_age_minutes"].mean())
        
        # SLA bucket counts
        sla_bucket_counts = {}
        if "sla_bucket" in queue_df.columns:
            eligible = queue_df[queue_df["in_queue"]] if "in_queue" in queue_df.columns else queue_df
            if len(eligible) > 0:
                bucket_counts = eligible["sla_bucket"].value_counts().to_dict()
                sla_bucket_counts = {k: int(v) for k, v in bucket_counts.items()}
        
        return {
            "total_alerts": int(total_alerts),
            "eligible_alerts": int(eligible_alerts),
            "suppressed_count": int(suppressed_count),
            "suppressed_by_reason": suppressed_by_reason,
            "avg_sla_age_minutes": round(avg_sla_age_minutes, 2),
            "sla_bucket_counts": sla_bucket_counts,
        }
