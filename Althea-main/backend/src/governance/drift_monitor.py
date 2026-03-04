"""Score Distribution and Feature Drift Monitoring."""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from .. import config


def score_distribution_monitor(df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
    """
    Monitor risk score distribution drift over time.
    
    Args:
        df: DataFrame with risk_score and optional timestamp
        time_col: Column name for timestamp (default: "timestamp")
        
    Returns:
        DataFrame with daily stats:
        - date: date bucket
        - mean_risk_score: average risk score for the day
        - share_high_risk: % of alerts with risk_score >= threshold
        - share_suppressed: % of alerts with suppression_flag == True
        - drift_flag: True if drift detected
    """
    if len(df) == 0:
        return pd.DataFrame()
    
    df_work = df.copy()
    
    # Generate timestamp if not present
    if time_col not in df_work.columns or df_work[time_col].isna().all():
        # Simulate daily buckets: assign each row to a day
        now = pd.Timestamp.utcnow()
        n_days = 30  # Last 30 days
        rng = np.random.default_rng(42)  # Fixed seed for reproducibility
        days_ago = rng.integers(0, n_days, size=len(df_work))
        df_work[time_col] = now - pd.to_timedelta(days_ago, unit="d")
    
    # Ensure timestamp is datetime
    df_work[time_col] = pd.to_datetime(df_work[time_col], errors="coerce")
    df_work = df_work[df_work[time_col].notna()].copy()
    
    if len(df_work) == 0:
        return pd.DataFrame()
    
    # Extract date (day bucket)
    df_work["_date"] = df_work[time_col].dt.date
    
    # Ensure risk_score exists
    if "risk_score" not in df_work.columns:
        df_work["risk_score"] = 0.0
    
    risk_score = pd.to_numeric(df_work["risk_score"], errors="coerce").fillna(0.0)
    threshold = getattr(config, "RISK_SCORE_THRESHOLD", 70)
    
    # Detect suppression flag
    suppression_cols = ["governance_status", "alert_eligible", "in_queue", "suppressed"]
    suppression_flag = None
    for col in suppression_cols:
        if col in df_work.columns:
            if col == "governance_status":
                suppression_flag = df_work[col] == "suppressed"
            elif col == "alert_eligible":
                suppression_flag = df_work[col] == False
            elif col == "in_queue":
                suppression_flag = df_work[col] == False
            elif col == "suppressed":
                suppression_flag = df_work[col] == True
            break
    
    if suppression_flag is None:
        suppression_flag = pd.Series(False, index=df_work.index)
    
    # Compute daily stats
    daily_stats = []
    for date, group in df_work.groupby("_date", sort=True):
        group_risk = risk_score.loc[group.index]
        mean_risk = float(group_risk.mean())
        share_high_risk = float((group_risk >= threshold).sum() / len(group) * 100)
        share_suppressed = float(suppression_flag.loc[group.index].sum() / len(group) * 100)
        
        daily_stats.append({
            "date": date,
            "mean_risk_score": mean_risk,
            "share_high_risk": share_high_risk,
            "share_suppressed": share_suppressed,
        })
    
    stats_df = pd.DataFrame(daily_stats)
    
    if len(stats_df) == 0:
        stats_df["drift_flag"] = False
        return stats_df
    
    # Sort by date
    stats_df = stats_df.sort_values("date").reset_index(drop=True)
    
    # Anomaly detection: flag drift
    stats_df["drift_flag"] = False
    
    if len(stats_df) >= 8:  # Need at least 8 days for 7-day rolling mean
        # Rolling 7-day mean
        stats_df["rolling_mean_risk"] = stats_df["mean_risk_score"].rolling(window=7, min_periods=1).mean()
        stats_df["rolling_mean_high_risk"] = stats_df["share_high_risk"].rolling(window=7, min_periods=1).mean()
        
        # Drift detection
        for i in range(7, len(stats_df)):
            current_mean = stats_df.loc[i, "mean_risk_score"]
            rolling_mean = stats_df.loc[i, "rolling_mean_risk"]
            
            current_high_risk = stats_df.loc[i, "share_high_risk"]
            rolling_high_risk = stats_df.loc[i, "rolling_mean_high_risk"]
            
            # Drift if mean score change > 20% vs rolling 7-day mean
            if rolling_mean > 0:
                mean_change_pct = abs(current_mean - rolling_mean) / rolling_mean
                if mean_change_pct > 0.20:
                    stats_df.loc[i, "drift_flag"] = True
            
            # OR share_high_risk change > 30%
            if rolling_high_risk > 0:
                high_risk_change_pct = abs(current_high_risk - rolling_high_risk) / rolling_high_risk
                if high_risk_change_pct > 0.30:
                    stats_df.loc[i, "drift_flag"] = True
    
    return stats_df


def feature_drift_monitor(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    """
    Monitor feature distribution drift.
    
    Args:
        df: DataFrame with features
        feature_cols: List of feature column names to monitor
        
    Returns:
        DataFrame with drift analysis:
        - feature_name: name of the feature
        - historical_mean: mean of first 30% of data
        - current_mean: mean of last 30% of data
        - drift_ratio: (current - historical) / (historical + eps)
        - drift_flag: True if drift_ratio > 0.25
    """
    if len(df) == 0:
        return pd.DataFrame()
    
    df_work = df.copy()
    
    # Sort by timestamp if available, otherwise by index
    time_cols = ["timestamp", "ts", "created_at"]
    sort_col = None
    for col in time_cols:
        if col in df_work.columns:
            sort_col = col
            break
    
    if sort_col:
        df_work = df_work.sort_values(sort_col).reset_index(drop=True)
    else:
        df_work = df_work.reset_index(drop=True)
    
    n_total = len(df_work)
    n_historical = max(1, int(n_total * 0.30))
    n_current = max(1, int(n_total * 0.30))
    
    historical_slice = df_work.iloc[:n_historical]
    current_slice = df_work.iloc[-n_current:]
    
    drift_results = []
    
    for col in feature_cols:
        if col not in df_work.columns:
            continue
        
        feature_values = pd.to_numeric(df_work[col], errors="coerce")
        if feature_values.isna().all():
            continue
        
        historical_values = pd.to_numeric(historical_slice[col], errors="coerce").fillna(0.0)
        current_values = pd.to_numeric(current_slice[col], errors="coerce").fillna(0.0)
        
        historical_mean = float(historical_values.mean())
        current_mean = float(current_values.mean())
        
        eps = 1e-6
        drift_ratio = abs(current_mean - historical_mean) / (abs(historical_mean) + eps)
        drift_flag = drift_ratio > 0.25
        
        drift_results.append({
            "feature_name": col,
            "historical_mean": historical_mean,
            "current_mean": current_mean,
            "drift_ratio": drift_ratio,
            "drift_flag": drift_flag,
        })
    
    return pd.DataFrame(drift_results)
