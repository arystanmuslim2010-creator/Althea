"""Risk Orchestration Layer for production-controlled AML risk scoring."""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import config


def apply_distribution_control(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply distribution control to prevent score collapse at 0 or 100.
    When scores are too concentrated (e.g. all CRITICAL), spreads them
    realistically: ~45% LOW, ~30% MEDIUM, ~15% HIGH, ~10% CRITICAL.
    
    Args:
        df: DataFrame with risk_score column
        
    Returns:
        DataFrame with risk_score_dist_adj column added
    """
    df = df.copy()
    
    # Use risk_score_original if available (original before governance), otherwise risk_score
    if "risk_score_original" in df.columns:
        risk_scores = pd.to_numeric(df["risk_score_original"], errors="coerce").fillna(0.0)
    elif "risk_score" in df.columns:
        risk_scores = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    else:
        risk_scores = pd.Series(0.0, index=df.index)
    
    n = len(risk_scores)
    if n == 0:
        df["risk_score_dist_adj"] = risk_scores
        return df
    
    # Check if scores are too concentrated (low variance)
    std_val = risk_scores.std()
    mean_val = risk_scores.mean()
    cv = std_val / (mean_val + 1e-9) if mean_val > 0 else 1.0
    pct_above_90 = (risk_scores >= 90).mean()
    
    # Realistic target percentiles: LOW <40, MEDIUM <70, HIGH <90, CRITICAL >=90
    # Target: ~45% LOW (0-40), ~25% MEDIUM (40-70), ~20% HIGH (70-90), ~10% CRITICAL (90-100)
    t1, t2, t3 = 40, 70, 90
    target_pct_low = 0.45
    target_pct_medium = 0.25
    target_pct_high = 0.20
    # CRITICAL = rest
    
    if pct_above_90 > 0.7 or cv < 0.05:
        # Scores too concentrated — apply realistic spread based on rank
        df["_used_realistic_spread"] = True
        rank_pct = risk_scores.rank(pct=True, method="average")
        # Map rank to score: preserve relative order but spread across bands
        adj = np.where(
            rank_pct < target_pct_low,
            rank_pct / target_pct_low * t1,
            np.where(
                rank_pct < target_pct_low + target_pct_medium,
                t1 + (rank_pct - target_pct_low) / target_pct_medium * (t2 - t1),
                np.where(
                    rank_pct < target_pct_low + target_pct_medium + target_pct_high,
                    t2 + (rank_pct - target_pct_low - target_pct_medium) / target_pct_high * (t3 - t2),
                    t3 + (rank_pct - target_pct_low - target_pct_medium - target_pct_high) / (1.0 - target_pct_low - target_pct_medium - target_pct_high) * (100 - t3),
                ),
            ),
        )
        df["risk_score_dist_adj"] = np.clip(adj, 0.0, 100.0)
    else:
        df["_used_realistic_spread"] = False
        # Normal spread — blend percentile rank with original (softer sigmoid)
        percentile_rank = risk_scores.rank(pct=True, method="average")
        sigmoid_input = -4 * (percentile_rank - 0.5)  # Softer: 4 instead of 8
        sigmoid_output = 1.0 / (1.0 + np.exp(sigmoid_input))
        blended = 0.4 * risk_scores + 0.6 * (sigmoid_output * 100.0)
        df["risk_score_dist_adj"] = np.clip(blended, 0.0, 100.0)
    
    return df


def apply_uncertainty_penalty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply uncertainty penalty to risk score.
    
    Uses risk_uncertainty column if available.
    
    Args:
        df: DataFrame with risk_score_dist_adj and risk_uncertainty columns
        
    Returns:
        DataFrame with risk_score_uncertainty_adj column added
    """
    df = df.copy()
    
    # Ensure required columns exist
    if "risk_score_dist_adj" not in df.columns:
        # Fallback to risk_score if distribution control wasn't applied
        if "risk_score" in df.columns:
            df["risk_score_dist_adj"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
        else:
            df["risk_score_dist_adj"] = 0.0
    
    if "risk_uncertainty" not in df.columns:
        # If uncertainty doesn't exist, use 0 (no penalty)
        df["risk_uncertainty"] = 0.0
    
    risk_uncertainty = pd.to_numeric(df["risk_uncertainty"], errors="coerce").fillna(0.0)
    risk_score_dist_adj = pd.to_numeric(df["risk_score_dist_adj"], errors="coerce").fillna(0.0)
    
    # Penalty logic: penalty = min(0.25, risk_uncertainty * 0.6)
    penalty = np.minimum(0.25, risk_uncertainty * 0.6)
    
    # Apply penalty: risk_score_uncertainty_adj = risk_score_dist_adj * (1 - penalty)
    df["risk_score_uncertainty_adj"] = risk_score_dist_adj * (1.0 - penalty)
    
    return df


def apply_baseline_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply baseline confidence integration to risk score.
    
    If baseline_level != "user", apply confidence penalty.
    
    Args:
        df: DataFrame with risk_score_uncertainty_adj, baseline_level, baseline_confidence columns
        
    Returns:
        DataFrame with risk_score_baseline_adj column added
    """
    df = df.copy()
    
    # Ensure required columns exist
    if "risk_score_uncertainty_adj" not in df.columns:
        if "risk_score_dist_adj" in df.columns:
            df["risk_score_uncertainty_adj"] = pd.to_numeric(df["risk_score_dist_adj"], errors="coerce").fillna(0.0)
        elif "risk_score" in df.columns:
            df["risk_score_uncertainty_adj"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
        else:
            df["risk_score_uncertainty_adj"] = 0.0
    
    if "baseline_level" not in df.columns:
        df["baseline_level"] = "global"
    
    if "baseline_confidence" not in df.columns:
        df["baseline_confidence"] = 0.3
    
    risk_score_uncertainty_adj = pd.to_numeric(df["risk_score_uncertainty_adj"], errors="coerce").fillna(0.0)
    baseline_level = df["baseline_level"].astype(str).str.lower()
    baseline_confidence = pd.to_numeric(df["baseline_confidence"], errors="coerce").fillna(0.3)
    
    # If baseline_level != "user": confidence_penalty = (1 - baseline_confidence) * 0.3
    # Else keep unchanged
    is_user_baseline = baseline_level == "user"
    confidence_penalty = np.where(
        is_user_baseline,
        0.0,  # No penalty for user baseline
        (1.0 - baseline_confidence) * 0.3
    )
    
    # Apply penalty: risk_score_baseline_adj = risk_score_uncertainty_adj * (1 - confidence_penalty)
    df["risk_score_baseline_adj"] = risk_score_uncertainty_adj * (1.0 - confidence_penalty)
    
    return df


def detect_data_drift(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect data drift signal based on mean z-scores.
    
    Computes mean of amount_z, velocity_z, activity_z.
    If abs(mean) > 1.5, sets data_drift_flag = True.
    
    Args:
        df: DataFrame with amount_z, velocity_z, activity_z columns
        
    Returns:
        DataFrame with data_drift_flag column added
    """
    df = df.copy()
    
    # Ensure required columns exist
    for col in ["amount_z", "velocity_z", "activity_z"]:
        if col not in df.columns:
            df[col] = 0.0
    
    amount_z = pd.to_numeric(df["amount_z"], errors="coerce").fillna(0.0)
    velocity_z = pd.to_numeric(df["velocity_z"], errors="coerce").fillna(0.0)
    activity_z = pd.to_numeric(df["activity_z"], errors="coerce").fillna(0.0)
    
    # Compute mean z-scores
    mean_amount_z = amount_z.mean()
    mean_velocity_z = velocity_z.mean()
    mean_activity_z = activity_z.mean()
    
    # Overall mean (absolute value)
    mean_abs = np.abs(np.mean([mean_amount_z, mean_velocity_z, mean_activity_z]))
    
    # If abs(mean) > 1.5: data_drift_flag = True
    df["data_drift_flag"] = mean_abs > 1.5
    
    return df


def finalize_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finalize governed score and assign risk buckets.
    
    Also creates risk component breakdown columns.
    
    Args:
        df: DataFrame with risk_score_baseline_adj and other risk components
        
    Returns:
        DataFrame with risk_score_governed, risk_bucket, and component columns
    """
    df = df.copy()
    
    # Ensure required column exists
    if "risk_score_baseline_adj" not in df.columns:
        if "risk_score_uncertainty_adj" in df.columns:
            df["risk_score_baseline_adj"] = pd.to_numeric(df["risk_score_uncertainty_adj"], errors="coerce").fillna(0.0)
        elif "risk_score_dist_adj" in df.columns:
            df["risk_score_baseline_adj"] = pd.to_numeric(df["risk_score_dist_adj"], errors="coerce").fillna(0.0)
        elif "risk_score" in df.columns:
            df["risk_score_baseline_adj"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
        else:
            df["risk_score_baseline_adj"] = 0.0
    
    risk_score_baseline_adj = pd.to_numeric(df["risk_score_baseline_adj"], errors="coerce").fillna(0.0)
    
    # Final governed score: clip to 0-100
    df["risk_score_governed"] = np.clip(risk_score_baseline_adj, 0.0, 100.0)
    
    # Assign risk buckets
    df["risk_bucket"] = np.where(
        df["risk_score_governed"] >= 85, "HIGH",
        np.where(df["risk_score_governed"] >= 60, "MEDIUM", "LOW")
    )
    
    # Risk component breakdown
    # All components must sum to risk_score_governed
    
    # Get base risk score (original before governance)
    if "risk_score_original" in df.columns:
        risk_score_original = pd.to_numeric(df["risk_score_original"], errors="coerce").fillna(0.0)
    elif "risk_score" in df.columns:
        risk_score_original = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    else:
        risk_score_original = df["risk_score_governed"].copy()
    
    # Model component: base contribution from ML model
    # This is the primary component
    model_weight = 0.7  # 70% from model
    df["risk_model_component"] = risk_score_original * model_weight
    
    # Data quality component: based on uncertainty penalty
    if "risk_uncertainty" in df.columns:
        risk_uncertainty = pd.to_numeric(df["risk_uncertainty"], errors="coerce").fillna(0.0)
        uncertainty_penalty = np.minimum(0.25, risk_uncertainty * 0.6)
        # Negative component (reduces score)
        df["risk_uncertainty_component"] = -risk_score_original * uncertainty_penalty * 0.3
    else:
        df["risk_uncertainty_component"] = 0.0
    
    # Baseline component: based on baseline confidence
    if "baseline_level" in df.columns and "baseline_confidence" in df.columns:
        baseline_level = df["baseline_level"].astype(str).str.lower()
        baseline_confidence = pd.to_numeric(df["baseline_confidence"], errors="coerce").fillna(0.3)
        is_user_baseline = baseline_level == "user"
        confidence_penalty = np.where(
            is_user_baseline,
            0.0,
            (1.0 - baseline_confidence) * 0.3
        )
        # Negative component (reduces score)
        df["risk_baseline_component"] = -risk_score_original * confidence_penalty * 0.2
    else:
        df["risk_baseline_component"] = 0.0
    
    # Distribution control component: difference from original
    if "risk_score_dist_adj" in df.columns:
        risk_score_dist_adj = pd.to_numeric(df["risk_score_dist_adj"], errors="coerce").fillna(0.0)
        df["risk_data_quality_component"] = (risk_score_dist_adj - risk_score_original) * 0.1
    else:
        df["risk_data_quality_component"] = 0.0
    
    # Normalize components to sum to risk_score_governed
    component_sum = (
        df["risk_model_component"] +
        df["risk_uncertainty_component"] +
        df["risk_baseline_component"] +
        df["risk_data_quality_component"]
    )
    
    # Avoid division by zero
    scale_factor = np.where(
        np.abs(component_sum) > 1e-6,
        df["risk_score_governed"] / component_sum,
        1.0
    )
    
    df["risk_model_component"] = df["risk_model_component"] * scale_factor
    df["risk_uncertainty_component"] = df["risk_uncertainty_component"] * scale_factor
    df["risk_baseline_component"] = df["risk_baseline_component"] * scale_factor
    df["risk_data_quality_component"] = df["risk_data_quality_component"] * scale_factor
    
    # Ensure components sum exactly to risk_score_governed
    component_sum_final = (
        df["risk_model_component"] +
        df["risk_uncertainty_component"] +
        df["risk_baseline_component"] +
        df["risk_data_quality_component"]
    )
    
    # Adjust model component to account for any rounding differences
    df["risk_model_component"] = df["risk_model_component"] + (df["risk_score_governed"] - component_sum_final)
    
    return df


def stabilize_risk_scores(df: pd.DataFrame, cfg: object) -> pd.DataFrame:
    """
    Adds stabilization layer to risk scores.
    
    Adds columns:
    - risk_consensus
    - risk_volatility_penalty
    - risk_smoothed
    - risk_score_final
    
    Args:
        df: DataFrame with risk_behavioral, risk_structural, risk_temporal, 
            risk_meta, risk_score_raw, risk_score columns
        cfg: Config object with stabilization parameters
        
    Returns:
        DataFrame with stabilization columns added
    """
    df = df.copy()
    
    # Safe column access with defaults
    risk_behavioral = pd.to_numeric(df.get("risk_behavioral", 0.0), errors="coerce").fillna(0.0)
    risk_structural = pd.to_numeric(df.get("risk_structural", 0.0), errors="coerce").fillna(0.0)
    risk_temporal = pd.to_numeric(df.get("risk_temporal", 0.0), errors="coerce").fillna(0.0)
    risk_meta = pd.to_numeric(df.get("risk_meta", 0.0), errors="coerce").fillna(0.0)
    risk_score_raw = pd.to_numeric(df.get("risk_score_raw", 0.0), errors="coerce").fillna(0.0)
    # For high-risk override, use governed score (after distribution control)
    # so that realistic spread is preserved; avoid restoring collapsed original
    risk_score_for_override = pd.to_numeric(
        df.get("risk_score_governed", df.get("risk_score", 0.0)),
        errors="coerce"
    ).fillna(0.0)
    
    # Get config values with defaults
    volatility_weight = getattr(cfg, "VOLATILITY_WEIGHT", 0.2)
    dampening_exponent = getattr(cfg, "DAMPENING_EXPONENT", 0.85)
    weight_dampened = getattr(cfg, "WEIGHT_DAMPENED", 0.5)
    weight_behavioral = getattr(cfg, "WEIGHT_BEHAVIORAL", 0.3)
    weight_consensus = getattr(cfg, "WEIGHT_CONSENSUS", 0.2)
    high_risk_override = getattr(cfg, "HIGH_RISK_OVERRIDE", 90)
    
    # 1️⃣ Risk Consensus Score
    # Measure agreement between risk components
    # consensus = std([behavioral, structural, temporal, meta])
    # Low std = strong agreement → risk reliable
    # High std = disagreement → risk unstable
    risk_components = np.vstack([
        risk_behavioral.to_numpy(),
        risk_structural.to_numpy(),
        risk_temporal.to_numpy(),
        risk_meta.to_numpy()
    ]).T
    
    consensus_std = np.std(risk_components, axis=1)
    # Normalize std to [0, 1] range (assuming max std is 0.5 for 0-1 scores)
    consensus_std_normalized = np.clip(consensus_std / 0.5, 0.0, 1.0)
    risk_consensus = 1.0 - consensus_std_normalized
    df["risk_consensus"] = risk_consensus
    
    # 2️⃣ Volatility Penalty
    # If temporal risk fluctuates strongly relative to behavioral baseline
    volatility = np.abs(risk_temporal.to_numpy() - risk_behavioral.to_numpy())
    risk_volatility_penalty = volatility * volatility_weight
    df["risk_volatility_penalty"] = risk_volatility_penalty
    
    # 3️⃣ Score Dampening (Prevents 100 spikes)
    # risk_dampened = risk_score_raw ** config.DAMPENING_EXPONENT
    risk_dampened = np.power(
        np.clip(risk_score_raw.to_numpy(), 0.0, 1.0),
        dampening_exponent
    )
    
    # 4️⃣ Smoothed Risk
    # risk_smoothed = (
    #     risk_dampened * config.WEIGHT_DAMPENED +
    #     risk_behavioral * config.WEIGHT_BEHAVIORAL +
    #     risk_consensus * config.WEIGHT_CONSENSUS
    # ) - risk_volatility_penalty
    risk_smoothed = (
        risk_dampened * weight_dampened +
        risk_behavioral.to_numpy() * weight_behavioral +
        risk_consensus * weight_consensus
    ) - risk_volatility_penalty
    
    # Clip to [0, 1]
    risk_smoothed = np.clip(risk_smoothed, 0.0, 1.0)
    df["risk_smoothed"] = risk_smoothed
    
    # 5️⃣ Final Score
    risk_score_final = risk_smoothed * 100.0
    df["risk_score_final"] = risk_score_final
    
    # 6️⃣ When realistic spread was applied, preserve risk_score_governed
    used_spread = "_used_realistic_spread" in df.columns and df["_used_realistic_spread"].any()
    if used_spread and "risk_score_governed" in df.columns:
        df["risk_score_final"] = pd.to_numeric(df["risk_score_governed"], errors="coerce").fillna(0.0)
    else:
        high_risk_mask = risk_score_for_override > high_risk_override
        df.loc[high_risk_mask, "risk_score_final"] = risk_score_for_override[high_risk_mask]
    
    # Log statistics
    dampened_count = (risk_dampened < risk_score_raw.to_numpy()).sum()
    dampened_pct = dampened_count / len(df) if len(df) > 0 else 0.0
    
    volatility_penalty_applied = (risk_volatility_penalty > 0.01).sum()
    volatility_pct = volatility_penalty_applied / len(df) if len(df) > 0 else 0.0
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(
        f"Risk stabilization: {dampened_pct:.1%} alerts dampened, "
        f"{volatility_pct:.1%} with volatility penalty"
    )
    
    return df


def apply_risk_governance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master function to apply all risk governance transformations.
    
    Applies in order:
    1. Distribution control
    2. Uncertainty penalty
    3. Baseline confidence integration
    4. Data drift detection
    5. Finalize score
    
    Args:
        df: DataFrame with risk_score and related columns
        
    Returns:
        DataFrame with all governance columns added
    """
    df = apply_distribution_control(df)
    df = apply_uncertainty_penalty(df)
    df = apply_baseline_confidence(df)
    df = detect_data_drift(df)
    df = finalize_score(df)
    
    return df
