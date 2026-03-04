"""Alert reduction/suppression layer for bank-grade AML triage."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from . import config


def apply_suppression(df: pd.DataFrame, cfg: Any) -> pd.DataFrame:
    """
    Apply suppression logic to dataframe, adding suppression columns.
    
    Does NOT delete rows - adds columns: alert_eligible, suppression_reason, 
    suppression_code, alert_priority, risk_bucket, risk_score_rank.
    
    Args:
        df: DataFrame with risk_score, baseline_level, baseline_confidence, 
            Top_Driver, typology, segment, user_id, reason_codes, case_status
        cfg: Config object with suppression parameters
        
    Returns:
        DataFrame with suppression columns added
    """
    df = df.copy()
    
    # Initialize suppression columns
    df["alert_eligible"] = True
    df["suppression_reason"] = ""
    df["suppression_code"] = ""
    df["alert_priority"] = "P2"
    df["risk_bucket"] = "LOW"
    df["risk_score_rank"] = 0
    
    # Ensure required columns exist with defaults
    if "baseline_level" not in df.columns:
        df["baseline_level"] = "global"
    if "baseline_confidence" not in df.columns:
        df["baseline_confidence"] = 0.3
    if "risk_score" not in df.columns:
        df["risk_score"] = 0.0
    if "Top_Driver" not in df.columns:
        df["Top_Driver"] = ""
    if "typology" not in df.columns:
        df["typology"] = "none"
    if "segment" not in df.columns:
        df["segment"] = "unknown"
    if "user_id" not in df.columns:
        df["user_id"] = range(len(df))
    if "reason_codes" not in df.columns:
        df["reason_codes"] = "[]"
    if "case_status" not in df.columns:
        df["case_status"] = config.CASE_STATUS_NEW
    
    # Convert to numeric where needed
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    df["baseline_confidence"] = pd.to_numeric(df["baseline_confidence"], errors="coerce").fillna(0.3)
    
    # Compute risk_score_rank (dense rank, descending) - ALL rows, even suppressed
    df["risk_score_rank"] = df["risk_score"].rank(method="dense", ascending=False).astype(int)
    
    # SUPPRESSION LOGIC (in order - first match suppresses)
    
    # 1. BASELINE_CONFIDENCE_GUARD
    baseline_mask = (
        (df["baseline_level"].astype(str).str.lower() != "user") &
        (df["baseline_confidence"] < 0.5) &
        (df["risk_score"] < 85)
    )
    df.loc[baseline_mask, "alert_eligible"] = False
    df.loc[baseline_mask, "suppression_code"] = "BASELINE_WEAK"
    df.loc[baseline_mask, "suppression_reason"] = "Baseline not reliable (segment/global) and risk below escalation threshold"
    
    # 2. LOW_RISK_BUCKET
    low_risk_mask = df["risk_score"] < getattr(cfg, "RISK_QUEUE_MIN_SCORE", 70)
    # Only suppress if not already suppressed
    low_risk_mask = low_risk_mask & df["alert_eligible"]
    df.loc[low_risk_mask, "alert_eligible"] = False
    df.loc[low_risk_mask, "suppression_code"] = "LOW_RISK"
    df.loc[low_risk_mask, "suppression_reason"] = "Below minimum triage threshold"
    
    # 3. REPEAT_ALERT_SAME_USER_COOLDOWN
    # Define signature = (Top_Driver, typology, segment)
    df["_signature"] = (
        df["Top_Driver"].astype(str) + "|" +
        df["typology"].astype(str) + "|" +
        df["segment"].astype(str)
    )

    max_per_signature = getattr(cfg, "MAX_ALERTS_PER_USER_PER_SIGNATURE", 2)

    # Vectorized: rank alerts by risk_score (desc) within each (user_id, _signature) group
    # among currently eligible alerts only.
    eligible_mask = df["alert_eligible"]
    rank_sig = (
        df.loc[eligible_mask, "risk_score"]
        .groupby([df.loc[eligible_mask, "user_id"], df.loc[eligible_mask, "_signature"]])
        .rank(method="first", ascending=False)
    )
    over_sig_cap = eligible_mask & rank_sig.reindex(df.index).gt(max_per_signature)
    df.loc[over_sig_cap, "alert_eligible"] = False
    df.loc[over_sig_cap, "suppression_code"] = "DUPLICATE_SIGNATURE"
    df.loc[over_sig_cap, "suppression_reason"] = (
        "Duplicate alert signature for user within batch"
    )

    # 4. PER_USER_DAILY_CAP (vectorized)
    max_per_user = getattr(cfg, "MAX_ALERTS_PER_USER", 5)

    eligible_mask = df["alert_eligible"]
    rank_user = (
        df.loc[eligible_mask, "risk_score"]
        .groupby(df.loc[eligible_mask, "user_id"])
        .rank(method="first", ascending=False)
    )
    over_user_cap = eligible_mask & rank_user.reindex(df.index).gt(max_per_user)
    df.loc[over_user_cap, "alert_eligible"] = False
    df.loc[over_user_cap, "suppression_code"] = "USER_CAP"
    df.loc[over_user_cap, "suppression_reason"] = (
        f"User alert cap exceeded: kept top {max_per_user}"
    )
    
    # 5. SEGMENT_THROTTLE (optional)
    segment_max_share = getattr(cfg, "SEGMENT_MAX_SHARE", 0.55)
    segment_keep_pct = getattr(cfg, "SEGMENT_KEEP_PERCENT", 0.35)
    
    # Compute share of alerts per segment among non-suppressed
    eligible_df = df[df["alert_eligible"]].copy()
    if len(eligible_df) > 0:
        segment_counts = eligible_df["segment"].value_counts()
        total_eligible = len(eligible_df)
        
        for segment, count in segment_counts.items():
            segment_share = count / total_eligible if total_eligible > 0 else 0
            
            if segment_share > segment_max_share:
                # For this segment, keep only top X% by risk_score
                seg_mask = (df["segment"] == segment) & (df["alert_eligible"] == True)
                seg_alerts = df[seg_mask].copy()
                
                if len(seg_alerts) > 0:
                    seg_alerts = seg_alerts.sort_values("risk_score", ascending=False)
                    keep_count = max(1, int(len(seg_alerts) * segment_keep_pct))
                    keep_indices = seg_alerts.head(keep_count).index
                    suppress_indices = seg_alerts.iloc[keep_count:].index
                    
                    df.loc[suppress_indices, "alert_eligible"] = False
                    df.loc[suppress_indices, "suppression_code"] = "SEGMENT_THROTTLE"
                    df.loc[suppress_indices, "suppression_reason"] = "Segment alert flood throttle applied"
    
    # Clean up temporary column
    df = df.drop(columns=["_signature"], errors="ignore")
    
    # PRIORITY ASSIGNMENT (after suppression)
    # P0 if risk_score >= 90 OR (risk_bucket=="HIGH" and "TYP_STRUCTURING" in reason_codes) OR case_status=="IN_CASE"
    # P1 if risk_score >= 75
    # P2 otherwise
    
    # First assign risk_bucket
    df.loc[df["risk_score"] >= 85, "risk_bucket"] = "HIGH"
    df.loc[(df["risk_score"] >= 60) & (df["risk_score"] < 85), "risk_bucket"] = "MEDIUM"
    df.loc[df["risk_score"] < 60, "risk_bucket"] = "LOW"
    
    # Check for TYP_STRUCTURING in reason_codes
    has_structuring = df["reason_codes"].astype(str).str.contains("TYP_STRUCTURING", na=False, regex=False)
    in_case = df["case_status"].astype(str) == config.CASE_STATUS_IN_CASE
    
    # Assign priority
    p0_mask = (df["risk_score"] >= 90) | ((df["risk_bucket"] == "HIGH") & has_structuring) | in_case
    p1_mask = (df["risk_score"] >= 75) & ~p0_mask
    
    df.loc[p0_mask, "alert_priority"] = "P0"
    df.loc[p1_mask, "alert_priority"] = "P1"
    df.loc[~p0_mask & ~p1_mask, "alert_priority"] = "P2"
    
    return df


def compute_queue_view(df: pd.DataFrame, cfg: Any) -> pd.DataFrame:
    """
    Compute filtered queue view showing only unsuppressed alerts.
    
    Args:
        df: DataFrame with suppression columns (from apply_suppression)
        cfg: Config object (unused but kept for API consistency)
        
    Returns:
        Filtered DataFrame with only alert_eligible==True rows
    """
    if "alert_eligible" not in df.columns:
        # If suppression hasn't been applied, return all rows
        return df.copy()
    
    return df[df["alert_eligible"] == True].copy()


def validate_suppression(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate suppression results and return summary statistics.
    
    Args:
        df: DataFrame with suppression columns
        
    Returns:
        Dictionary with validation metrics
    """
    if "alert_eligible" not in df.columns:
        return {
            "error": "Suppression columns not found. Call apply_suppression first.",
            "total": len(df),
            "eligible": len(df),
            "suppressed": 0,
        }
    
    total = len(df)
    eligible = df["alert_eligible"].sum()
    suppressed = total - eligible
    
    # Top suppression codes
    suppression_code_counts = df[df["suppression_code"] != ""]["suppression_code"].value_counts().to_dict()
    
    # Per-segment eligible counts
    if "segment" in df.columns:
        segment_eligible = df[df["alert_eligible"]]["segment"].value_counts().to_dict()
    else:
        segment_eligible = {}
    
    return {
        "total": int(total),
        "eligible": int(eligible),
        "suppressed": int(suppressed),
        "suppression_rate": float(suppressed / total) if total > 0 else 0.0,
        "top_suppression_codes": suppression_code_counts,
        "per_segment_eligible": segment_eligible,
    }
