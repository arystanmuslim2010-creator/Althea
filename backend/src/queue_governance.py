"""Alert Governance Layer: Daily capacity, per-user caps, deduplication, and prioritization."""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from . import config


def apply_alert_governance(
    df: pd.DataFrame,
    governance_config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Apply alert governance rules to control which alerts reach analysts.
    Hard-constraint rows (hard_constraint==1) are NEVER suppressed: they keep
    governance_status MANDATORY_REVIEW and in_queue=1.

    Rules applied in order (only to non-hard-constraint rows):
    1. Remove low-risk alerts (below MIN_RISK_FOR_QUEUE)
    2. Deduplication (same user + same rule within DEDUP_WINDOW_DAYS)
    3. Per-user cap (PER_USER_DAILY_CAP alerts per customer)
    4. Daily budget cap (DAILY_ALERT_BUDGET total alerts; None = uncapped for evaluation)

    Args:
        df: DataFrame with alert_id, user_id, risk_score, rule_top_hit, risk_score_rank.
            If column "hard_constraint" exists and is 1/True for some rows, those rows
            are left unchanged and restored to MANDATORY_REVIEW/in_queue=1 at the end.
        governance_config: Optional per-run overrides: daily_budget (int or None), per_user_cap,
            min_risk_for_queue, segment_caps (dict segment -> cap).

    Returns:
        DataFrame with governance_status, suppression_reason; hard-constraint rows
        remain MANDATORY_REVIEW and in_queue=1.
    """
    df = df.copy()
    gov = governance_config or {}

    # Identify hard-constraint rows: they must never be suppressed
    if "hard_constraint" in df.columns:
        hard_mask = (df["hard_constraint"].fillna(0).astype(int) != 0)
        hard_alert_ids_set = set(df.loc[hard_mask, "alert_id"].astype(str).tolist())
    else:
        hard_mask = pd.Series(False, index=df.index)
        hard_alert_ids_set = set()

    # Initialize governance columns: preserve existing status for hard-constraint rows only
    if "governance_status" not in df.columns:
        df["governance_status"] = "eligible"
    if "suppression_reason" not in df.columns:
        df["suppression_reason"] = ""
    # Non-hard rows start as eligible (do not overwrite hard-constraint rows)
    df.loc[~hard_mask, "governance_status"] = "eligible"
    df.loc[~hard_mask, "suppression_reason"] = ""

    # Ensure required columns exist
    if "risk_score" not in df.columns:
        raise ValueError("DataFrame must contain 'risk_score' column")
    
    if "user_id" not in df.columns:
        raise ValueError("DataFrame must contain 'user_id' column")
    
    if "alert_id" not in df.columns:
        raise ValueError("DataFrame must contain 'alert_id' column")
    
    # Ensure risk_score_rank exists (used for prioritization)
    # Lower rank number = higher priority (rank 1 is best)
    if "risk_score_rank" not in df.columns:
        # Rank by risk_score descending (higher score = lower rank number = better)
        df["risk_score_rank"] = df["risk_score"].rank(method="dense", ascending=False).astype(int)
    
    # Ensure rule_top_hit exists (used for deduplication)
    if "rule_top_hit" not in df.columns:
        df["rule_top_hit"] = "none"
    
    # Get config values: per-run overrides take precedence
    min_risk = gov.get("min_risk_for_queue") if gov.get("min_risk_for_queue") is not None else getattr(config, "MIN_RISK_FOR_QUEUE", 60)
    daily_budget = gov.get("daily_budget") if "daily_budget" in gov else getattr(config, "DAILY_ALERT_BUDGET", 400)
    per_user_cap = gov.get("per_user_cap") if gov.get("per_user_cap") is not None else getattr(config, "PER_USER_DAILY_CAP", 3)
    dedup_window_days = gov.get("dedup_window_days") or getattr(config, "DEDUP_WINDOW_DAYS", 7)
    segment_caps = gov.get("segment_caps") or {}
    
    # Rule 1: Remove low-risk alerts (only for non-hard-constraint rows)
    low_risk_mask = (df["risk_score"] < min_risk) & (~hard_mask)
    df.loc[low_risk_mask, "governance_status"] = "suppressed"
    df.loc[low_risk_mask, "suppression_reason"] = "below_threshold"

    # Work with eligible alerts only from now on (exclude hard-constraint rows from suppression logic)
    eligible = df[(df["governance_status"] == "eligible") & (~hard_mask)].copy()

    if len(eligible) == 0:
        # Restore hard-constraint rows to MANDATORY_REVIEW and in_queue=1
        if hard_alert_ids_set:
            h_mask = df["alert_id"].astype(str).isin(hard_alert_ids_set)
            df.loc[h_mask, "governance_status"] = "MANDATORY_REVIEW"
            df.loc[h_mask, "in_queue"] = True
        return df
    
    # Rule 2: Deduplication (same user + same rule)
    # Create deduplication key
    eligible["dup_key"] = (
        eligible["user_id"].astype(str) + "_" + eligible["rule_top_hit"].astype(str)
    )
    
    # Rank alerts within each dup_key group by risk_score_rank (ascending = better rank)
    eligible["dup_rank"] = eligible.groupby("dup_key")["risk_score_rank"].rank(
        method="first", ascending=True
    )
    
    # Suppress duplicates (keep rank 1, suppress rank > 1)
    dup_mask = eligible["dup_rank"] > 1
    df.loc[eligible[dup_mask].index, "governance_status"] = "suppressed"
    df.loc[eligible[dup_mask].index, "suppression_reason"] = "deduplicated"
    
    # Update eligible after deduplication
    eligible = df[df["governance_status"] == "eligible"].copy()
    
    if len(eligible) == 0:
        return df
    
    # Rule 3: Per-user cap
    # Rank alerts within each user by risk_score_rank (ascending = better rank)
    eligible["user_rank"] = eligible.groupby("user_id")["risk_score_rank"].rank(
        method="first", ascending=True
    )
    
    # Suppress alerts beyond per_user_cap
    user_cap_mask = eligible["user_rank"] > per_user_cap
    df.loc[eligible[user_cap_mask].index, "governance_status"] = "suppressed"
    df.loc[eligible[user_cap_mask].index, "suppression_reason"] = "user_cap"
    
    # Update eligible after per-user cap
    eligible = df[df["governance_status"] == "eligible"].copy()
    
    if len(eligible) == 0:
        return df
    
    # Optional: segment caps (per-segment budget)
    if segment_caps and "segment" in df.columns:
        for seg, cap in segment_caps.items():
            if cap is None:
                continue
            seg_eligible = eligible[eligible["segment"].astype(str) == str(seg)]
            if len(seg_eligible) > cap:
                seg_eligible = seg_eligible.sort_values("risk_score_rank", ascending=True)
                cutoff_ids = seg_eligible.iloc[int(cap):]["alert_id"]
                seg_mask = df["alert_id"].isin(cutoff_ids)
                df.loc[seg_mask, "governance_status"] = "suppressed"
                df.loc[seg_mask, "suppression_reason"] = "segment_cap"
        eligible = df[df["governance_status"] == "eligible"].copy()
        if len(eligible) == 0:
            return df

    # Rule 4: Daily budget cap (None = uncapped for evaluation runs)
    eligible = eligible.sort_values("risk_score_rank", ascending=True)
    if daily_budget is not None and len(eligible) > daily_budget:
        # Keep top daily_budget alerts, suppress the rest
        cutoff_ids = eligible.iloc[daily_budget:]["alert_id"]
        budget_mask = df["alert_id"].isin(cutoff_ids)
        df.loc[budget_mask, "governance_status"] = "suppressed"
        df.loc[budget_mask, "suppression_reason"] = "budget_exceeded"

    # Clean up temporary columns
    temp_cols = ["dup_key", "dup_rank", "user_rank"]
    for col in temp_cols:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Restore hard-constraint rows: MUST remain MANDATORY_REVIEW and in_queue=1 (never suppressed)
    if hard_alert_ids_set:
        h_mask = df["alert_id"].astype(str).isin(hard_alert_ids_set)
        df.loc[h_mask, "governance_status"] = "MANDATORY_REVIEW"
        df.loc[h_mask, "in_queue"] = True
        df.loc[h_mask, "suppression_reason"] = ""

    return df
