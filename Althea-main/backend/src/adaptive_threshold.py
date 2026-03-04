"""Adaptive Alert Thresholding Engine for operational AML risk governance."""
from __future__ import annotations

import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from . import config, utils

_logger = utils.get_logger(__name__)


def compute_adaptive_threshold(
    df: pd.DataFrame,
    cfg: object,
    analyst_capacity: int,
) -> Tuple[float, pd.DataFrame]:
    """
    Returns threshold that keeps alert queue aligned with analyst workload.
    
    Adds columns:
    - risk_score_rank
    - alert_priority_bucket
    
    Args:
        df: DataFrame with risk_score column
        cfg: Config object with configuration parameters
        analyst_capacity: Target number of alerts per day
        
    Returns:
        Tuple of (threshold_value, df_with_columns_added)
    """
    df = df.copy()
    
    # Ensure risk_score exists
    if "risk_score" not in df.columns:
        _logger.warning("risk_score column not found, using default threshold")
        return getattr(cfg, "RISK_SCORE_THRESHOLD", 70), df
    
    risk_score = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    
    # 1️⃣ Rank alerts
    # df["risk_score_rank"] = df["risk_score"].rank(ascending=False, method="first")
    df["risk_score_rank"] = risk_score.rank(ascending=False, method="first").astype(int)
    
    # 2️⃣ Compute threshold based on capacity
    # target_rank = analyst_capacity
    target_rank = analyst_capacity
    
    # If not enough alerts → use default threshold
    if len(df) < target_rank:
        default_threshold = getattr(cfg, "RISK_SCORE_THRESHOLD", 70)
        _logger.info(
            f"Not enough alerts ({len(df)}) for capacity ({target_rank}), "
            f"using default threshold: {default_threshold}"
        )
    else:
        # Get threshold from target rank
        # Sort by risk_score descending to get top N alerts
        ranked_df = df.sort_values("risk_score", ascending=False)
        if target_rank <= len(ranked_df):
            # Get the risk_score at the target_rank position (0-indexed, so target_rank - 1)
            threshold = ranked_df.iloc[target_rank - 1]["risk_score"]
        else:
            # If capacity exceeds available alerts, use lowest score
            threshold = ranked_df.iloc[-1]["risk_score"]
        
        # Ensure threshold is not too low (safety floor)
        min_threshold = getattr(cfg, "RISK_SCORE_THRESHOLD", 70)
        threshold = max(threshold, min_threshold)
    
    # 3️⃣ Add alert priority buckets
    # Top 10% → CRITICAL
    # Next 20% → HIGH
    # Next 30% → MEDIUM
    # Rest → LOW
    try:
        df["alert_priority_bucket"] = pd.qcut(
            df["risk_score_rank"],
            q=[0, 0.1, 0.3, 0.6, 1.0],
            labels=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            duplicates="drop"
        )
    except ValueError:
        # Fallback if not enough unique values for qcut
        # Use percentile-based assignment
        risk_percentiles = df["risk_score"].rank(pct=True)
        df["alert_priority_bucket"] = pd.cut(
            risk_percentiles,
            bins=[0, 0.1, 0.3, 0.6, 1.0],
            labels=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            include_lowest=True
        )
    
    # Fill any NaN values with "LOW"
    df["alert_priority_bucket"] = df["alert_priority_bucket"].fillna("LOW")
    
    # 4️⃣ Logging
    queue_size = (df["risk_score"] >= threshold).sum()
    
    # Compute precision@capacity using real labels when available
    precision_at_capacity = None
    from .evaluation_service import detect_outcome_source, build_binary_labels, OutcomeLabelSource
    source, col, _ = detect_outcome_source(df)
    if source != OutcomeLabelSource.NONE:
        top_alerts = df[df["risk_score"] >= threshold].nlargest(analyst_capacity, "risk_score")
        if len(top_alerts) > 0:
            labels, mask = build_binary_labels(top_alerts, source, col)
            resolved = labels[mask]
            denom = max(len(resolved), 1)
            tp_count = int(resolved.sum())
            precision_at_capacity = tp_count / denom

    _logger.info(
        f"Adaptive threshold: {threshold:.1f}, "
        f"Queue size: {queue_size}, "
        f"Analyst capacity: {analyst_capacity}, "
        + (f"Precision@{analyst_capacity}: {precision_at_capacity:.2%}" if precision_at_capacity is not None else "")
    )
    
    return threshold, df
