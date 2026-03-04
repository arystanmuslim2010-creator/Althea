"""Bank-grade Alert Suppression Engine for operational AML governance."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from . import config, utils

_logger = utils.get_logger(__name__)


def apply_alert_suppression(
    df: pd.DataFrame,
    case_history: Optional[Dict[str, Dict]] = None,
    cfg: Optional[object] = None,
) -> pd.DataFrame:
    """
    Adds suppression intelligence to alerts.
    
    Returns df with NEW columns:
    - suppressed (bool)
    - suppression_reason (str)
    - suppression_score (float 0–1, where 1 = strong reason to suppress)
    
    Args:
        df: DataFrame with risk_score, amount_dev, velocity_dev, activity_dev, 
            segment, typology, user_id columns
        case_history: Optional dict mapping user_id to case history
        cfg: Config object with suppression parameters
        
    Returns:
        DataFrame with suppression columns added
    """
    df = df.copy()
    
    # Initialize suppression columns
    df["suppressed"] = False
    df["suppression_reason"] = ""
    df["suppression_score"] = 0.0
    
    # Get config values with defaults
    if cfg is None:
        cfg = config
    
    # Safe column access with defaults
    amount_dev = pd.to_numeric(df.get("amount_dev", 0.0), errors="coerce").fillna(0.0)
    velocity_dev = pd.to_numeric(df.get("velocity_dev", 1.0), errors="coerce").fillna(1.0)
    activity_dev = pd.to_numeric(df.get("activity_dev", 1.0), errors="coerce").fillna(1.0)
    risk_score = pd.to_numeric(df.get("risk_score", 0.0), errors="coerce").fillna(0.0)
    segment = df.get("segment", "").astype(str)
    typology = df.get("typology", "").astype(str).str.lower()
    user_id = df.get("user_id", "")
    
    # Get config thresholds
    suppress_amount_threshold = getattr(cfg, "SUPPRESS_AMOUNT_DEV_THRESHOLD", 1.5)
    suppress_velocity_threshold = getattr(cfg, "SUPPRESS_VELOCITY_DEV_THRESHOLD", 1.5)
    suppress_activity_threshold = getattr(cfg, "SUPPRESS_ACTIVITY_DEV_THRESHOLD", 1.5)
    suppress_risk_segment_cap = getattr(cfg, "SUPPRESS_RISK_SEGMENT_CAP", 75)
    suppress_risk_hard_cap = getattr(cfg, "SUPPRESS_RISK_HARD_CAP", 85)
    suppression_decision_threshold = getattr(cfg, "SUPPRESSION_DECISION_THRESHOLD", 0.5)
    fp_suppression_ratio = getattr(cfg, "FP_SUPPRESSION_RATIO", 0.7)
    
    high_volume_segments = getattr(cfg, "HIGH_VOLUME_SEGMENTS", ["retail_low", "retail_high"])
    benign_typologies = getattr(cfg, "BENIGN_TYPOLOGIES", ["salary_pattern", "merchant_settlement", "recurring_subscription"])
    
    # 1️⃣ Behavioral Norm Suppression
    behavioral_norm_mask = (
        (amount_dev < suppress_amount_threshold) &
        (velocity_dev < suppress_velocity_threshold) &
        (activity_dev < suppress_activity_threshold)
    )
    df.loc[behavioral_norm_mask, "suppression_score"] += 0.6
    df.loc[behavioral_norm_mask, "suppression_reason"] = df.loc[behavioral_norm_mask, "suppression_reason"].apply(
        lambda x: "within_behavioral_norm" if not x else x + "; within_behavioral_norm"
    )
    
    # 2️⃣ Segment-Aware Suppression
    segment_mask = (
        segment.isin(high_volume_segments) &
        (amount_dev < 2.5) &
        (risk_score < suppress_risk_segment_cap)
    )
    df.loc[segment_mask, "suppression_score"] += 0.3
    df.loc[segment_mask, "suppression_reason"] = df.loc[segment_mask, "suppression_reason"].apply(
        lambda x: "segment_expected_pattern" if not x else x + "; segment_expected_pattern"
    )
    
    # 3️⃣ Known Benign Pattern Suppression
    benign_mask = typology.isin([t.lower() for t in benign_typologies])
    df.loc[benign_mask, "suppression_score"] += 0.4
    df.loc[benign_mask, "suppression_reason"] = df.loc[benign_mask, "suppression_reason"].apply(
        lambda x: "known_benign_pattern" if not x else x + "; known_benign_pattern"
    )
    
    # 4️⃣ Historical FP Suppression
    if case_history is not None:
        user_fp_ratios = {}
        for uid, history in case_history.items():
            total_cases = history.get("total_cases", 0)
            fp_cases = history.get("fp_cases", 0)
            if total_cases > 0:
                user_fp_ratios[uid] = fp_cases / total_cases
            else:
                user_fp_ratios[uid] = 0.0
        
        # Map user_id to FP ratio
        user_fp_series = user_id.map(user_fp_ratios).fillna(0.0)
        fp_mask = user_fp_series > fp_suppression_ratio
        
        df.loc[fp_mask, "suppression_score"] += 0.5
        df.loc[fp_mask, "suppression_reason"] = df.loc[fp_mask, "suppression_reason"].apply(
            lambda x: "historical_false_positive_pattern" if not x else x + "; historical_false_positive_pattern"
        )
    
    # 5️⃣ Risk Floor Rule (CRITICAL) - Never suppress truly high-risk alerts
    high_risk_mask = risk_score > suppress_risk_hard_cap
    df.loc[high_risk_mask, "suppressed"] = False
    df.loc[high_risk_mask, "suppression_score"] = 0.0
    df.loc[high_risk_mask, "suppression_reason"] = "high_risk_override"
    
    # Clip suppression_score to [0, 1]
    df["suppression_score"] = df["suppression_score"].clip(0.0, 1.0)
    
    # FINAL DECISION: suppressed = suppression_score > threshold
    # But respect high_risk_override
    final_suppress_mask = (
        (df["suppression_score"] > suppression_decision_threshold) &
        ~high_risk_mask
    )
    df.loc[final_suppress_mask, "suppressed"] = True
    
    # Clean up suppression_reason for non-suppressed alerts
    df.loc[~df["suppressed"], "suppression_reason"] = ""
    
    # Log suppression counts
    suppressed_count = df["suppressed"].sum()
    total_count = len(df)
    suppression_rate = suppressed_count / total_count if total_count > 0 else 0.0
    
    _logger.info(
        f"Alert suppression applied: {suppressed_count}/{total_count} suppressed "
        f"({suppression_rate:.1%})"
    )
    
    # Log top suppression reasons
    if suppressed_count > 0:
        reasons = df[df["suppressed"]]["suppression_reason"].value_counts().head(5)
        _logger.info(f"Top suppression reasons: {reasons.to_dict()}")
    
    return df
