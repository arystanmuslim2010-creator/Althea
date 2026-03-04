"""Bank-grade temporal aggregates for AML transaction monitoring.

This module adds rolling-window behavioral aggregates (7-day and 30-day windows)
per user_id to enable detection of structuring, dormancy, bursts, and regime change.
"""
from __future__ import annotations

from typing import List, Tuple

import numpy as np
import pandas as pd

from . import config


def ensure_timestamp(df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
    """Ensure timestamp column exists and is pandas datetime.
    
    If time_col exists, ensure it is pandas datetime.
    If missing, create it deterministically:
    - For each user_id, sort by original row order
    - Assign start date fixed (e.g., 2025-01-01) + t days increment
    - Convert to datetime
    
    Args:
        df: DataFrame with transaction data
        time_col: Name of timestamp column to ensure/create
        
    Returns:
        DataFrame with time_col present as datetime
    """
    df = df.copy()
    
    # Check if time_col exists and is valid datetime
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        # If all NaNs, treat as missing
        if df[time_col].isna().all():
            df = df.drop(columns=[time_col])
        else:
            # Fill any remaining NaNs with deterministic values
            if df[time_col].isna().any():
                if "user_id" in df.columns:
                    for user_id, group in df.groupby("user_id", sort=False):
                        group_indices = group.index
                        group_times = df.loc[group_indices, time_col]
                        if group_times.isna().any():
                            # Use existing valid times or create from scratch
                            valid_times = group_times.dropna()
                            if len(valid_times) > 0:
                                base_time = valid_times.min()
                            else:
                                base_time = pd.Timestamp("2025-01-01")
                            # Fill NaNs with monotonic sequence
                            nan_mask = group_times.isna()
                            nan_count = nan_mask.sum()
                            if nan_count > 0:
                                nan_indices = group_indices[nan_mask]
                                start_idx = len(valid_times)
                                for i, idx in enumerate(nan_indices):
                                    df.loc[idx, time_col] = base_time + pd.Timedelta(days=start_idx + i)
            return df
    
    # Try alternative column names
    time_candidates = getattr(config, "TIME_COL_CANDIDATES", ["timestamp", "event_time", "tx_time", "datetime", "created_at", "ts"])
    for candidate in time_candidates:
        if candidate in df.columns and candidate != time_col:
            df[time_col] = pd.to_datetime(df[candidate], errors="coerce")
            if not df[time_col].isna().all():
                return df
    
    # Synthesize timestamp deterministically
    if "user_id" in df.columns:
        # For each user_id, order rows by existing index, assign timestamps as monotonic sequence
        df_sorted = df.sort_values("user_id")
        base_time = pd.Timestamp("2025-01-01")
        
        timestamps = []
        last_user = None
        day_counter = 0
        
        for idx, row in df_sorted.iterrows():
            user_id = row["user_id"]
            if user_id != last_user:
                day_counter = 0
                last_user = user_id
            timestamps.append(base_time + pd.Timedelta(days=day_counter))
            day_counter += 1
        
        # Reindex to original order
        df[time_col] = pd.Series(timestamps, index=df_sorted.index).reindex(df.index, fill_value=base_time)
    else:
        # No user_id: use index as days since base
        base_time = pd.Timestamp("2025-01-01")
        df[time_col] = pd.to_datetime(
            pd.Series(range(len(df)), index=df.index), unit="D", origin=base_time
        )
    
    return df


def add_temporal_aggregates(df: pd.DataFrame, time_col: str = "timestamp") -> Tuple[pd.DataFrame, List[str]]:
    """Compute per-user rolling-window behavioral aggregates.
    
    Windows: 7D and 30D.
    
    For each window compute:
    - tx_count_w (rolling count of rows)
    - amt_sum_w (rolling sum of amount)
    - amt_mean_w (rolling mean amount)
    - amt_max_w (rolling max amount)
    - gap_mean_w (rolling mean time_gap)
    - velocity_mean_w (rolling mean of velocity = 1/time_gap)
    - high_amount_share_w (share of amount > user baseline mean * 2 within window)
    - burst_indicator_w (tx_count_w / max(1, user_baseline_count_mean)) (a ratio feature)
    
    Args:
        df: DataFrame with user_id, timestamp, amount, time_gap columns
        time_col: Name of timestamp column
        
    Returns:
        Tuple of (DataFrame with temporal columns added, list of temporal column names)
    """
    original_len = len(df)
    df = df.copy()
    
    # Ensure timestamp exists
    df = ensure_timestamp(df, time_col)
    
    # Required columns
    required_cols = ["user_id", time_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for temporal aggregates: {missing}")
    
    # Ensure amount and time_gap exist (with defaults if missing)
    if "amount" not in df.columns:
        df["amount"] = 0.0
    if "time_gap" not in df.columns:
        df["time_gap"] = 1.0
    
    amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    time_gap = pd.to_numeric(df["time_gap"], errors="coerce").fillna(1.0)
    
    # Compute velocity if not exists
    if "velocity" not in df.columns:
        eps = getattr(config, "EPS", 1e-9)
        safe_time_gap = time_gap.clip(lower=eps)
        df["velocity"] = 1.0 / safe_time_gap
    else:
        df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0.0)
    
    velocity = df["velocity"]
    
    # Ensure timestamp is datetime
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    
    # Sort by user_id and timestamp for rolling operations
    # Keep original index for later mapping
    df_sorted = df.sort_values(["user_id", time_col], kind="mergesort").copy()
    df_sorted["_orig_index"] = df_sorted.index
    
    temporal_cols = []
    
    # Compute user baseline mean for high_amount_share calculation
    # Use first 30% of transactions per user as baseline
    user_baseline_amount_mean = {}
    user_baseline_count_mean = {}
    
    for user_id, group in df_sorted.groupby("user_id", sort=False):
        n_hist = max(1, int(len(group) * 0.3))
        hist_group = group.iloc[:n_hist]
        if len(hist_group) > 0:
            user_baseline_amount_mean[user_id] = float(hist_group["amount"].mean())
            # Count per day in baseline period
            if len(hist_group) > 1:
                hist_min = pd.to_datetime(hist_group[time_col], errors="coerce").min()
                hist_max = pd.to_datetime(hist_group[time_col], errors="coerce").max()
                if pd.isna(hist_min) or pd.isna(hist_max):
                    hist_days = 1.0
                else:
                    hist_days = max(1.0, ((hist_max - hist_min).total_seconds() / 86400.0) + 1.0)
                user_baseline_count_mean[user_id] = len(hist_group) / hist_days
            else:
                user_baseline_count_mean[user_id] = 1.0
        else:
            user_baseline_amount_mean[user_id] = float(amount.mean()) if len(amount) > 0 else 1.0
            user_baseline_count_mean[user_id] = 1.0
    
    # Process each window size
    for window_days in [7, 30]:
        window_str = f"{window_days}D"
        suffix = f"_{window_days}d"
        
        # Initialize columns
        tx_count_col = f"tx_count{suffix}"
        amt_sum_col = f"amt_sum{suffix}"
        amt_mean_col = f"amt_mean{suffix}"
        amt_max_col = f"amt_max{suffix}"
        gap_mean_col = f"gap_mean{suffix}"
        velocity_mean_col = f"velocity_mean{suffix}"
        high_amount_share_col = f"high_amount_share{suffix}"
        burst_ratio_col = f"burst_ratio{suffix}"
        
        df[tx_count_col] = 0.0
        df[amt_sum_col] = 0.0
        df[amt_mean_col] = 0.0
        df[amt_max_col] = 0.0
        df[gap_mean_col] = 0.0
        df[velocity_mean_col] = 0.0
        df[high_amount_share_col] = 0.0
        df[burst_ratio_col] = 0.0
        
        # Process per user
        for user_id, group in df_sorted.groupby("user_id", sort=False):
            if len(group) == 0:
                continue
            
            # Set timestamp as index for time-based rolling
            group_timed = group.set_index(time_col)
            
            # Try time-based rolling first
            try:
                rolling = group_timed.rolling(window_str, closed="left", min_periods=1)
                
                # Rolling count
                tx_count = rolling["amount"].count()
                
                # Rolling sum, mean, max of amount
                amt_sum = rolling["amount"].sum()
                amt_mean = rolling["amount"].mean()
                amt_max = rolling["amount"].max()
                
                # Rolling mean of time_gap
                gap_mean = rolling["time_gap"].mean()
                
                # Rolling mean of velocity
                velocity_mean = rolling["velocity"].mean()
                
                # High amount share: share of transactions with amount > user_baseline_mean * 2
                baseline_mean = user_baseline_amount_mean.get(user_id, 1.0)
                threshold = baseline_mean * 2.0
                high_amount_mask = group_timed["amount"] > threshold
                high_amount_count = high_amount_mask.rolling(window_str, closed="left", min_periods=1).sum()
                high_amount_share = high_amount_count / tx_count.clip(lower=1.0)
                
                # Burst ratio: tx_count / user_baseline_count_mean
                baseline_count_mean = user_baseline_count_mean.get(user_id, 1.0)
                # Convert tx_count to transactions per day
                # For each timestamp, calculate days in window
                window_start = group_timed.index - pd.Timedelta(days=window_days)
                window_days_actual = pd.Series(
                    (group_timed.index - window_start).total_seconds() / 86400.0,
                    index=group_timed.index
                ).clip(lower=1.0)
                tx_count_per_day = tx_count / window_days_actual
                burst_ratio = tx_count_per_day / max(baseline_count_mean, 1.0)
                
            except Exception:
                # Fallback to row-based approximation
                # Use last N rows where N approximates window_days
                # Assume roughly 1 transaction per day on average
                window_rows = window_days
                rolling = group_timed.rolling(window=window_rows, min_periods=1)
                
                tx_count = rolling["amount"].count()
                amt_sum = rolling["amount"].sum()
                amt_mean = rolling["amount"].mean()
                amt_max = rolling["amount"].max()
                gap_mean = rolling["time_gap"].mean()
                velocity_mean = rolling["velocity"].mean()
                
                baseline_mean = user_baseline_amount_mean.get(user_id, 1.0)
                threshold = baseline_mean * 2.0
                high_amount_mask = group_timed["amount"] > threshold
                high_amount_count = high_amount_mask.rolling(window=window_rows, min_periods=1).sum()
                high_amount_share = high_amount_count / tx_count.clip(lower=1.0)
                
                baseline_count_mean = user_baseline_count_mean.get(user_id, 1.0)
                burst_ratio = tx_count / max(baseline_count_mean * window_days, 1.0)
            
            # Map back to original indices
            orig_indices = group["_orig_index"].values
            df.loc[orig_indices, tx_count_col] = tx_count.fillna(0.0).values
            df.loc[orig_indices, amt_sum_col] = amt_sum.fillna(0.0).values
            df.loc[orig_indices, amt_mean_col] = amt_mean.fillna(0.0).values
            df.loc[orig_indices, amt_max_col] = amt_max.fillna(0.0).values
            df.loc[orig_indices, gap_mean_col] = gap_mean.fillna(0.0).values
            df.loc[orig_indices, velocity_mean_col] = velocity_mean.fillna(0.0).values
            df.loc[orig_indices, high_amount_share_col] = high_amount_share.fillna(0.0).values
            df.loc[orig_indices, burst_ratio_col] = burst_ratio.fillna(0.0).values
        
        # Add to temporal_cols list
        temporal_cols.extend([
            tx_count_col, amt_sum_col, amt_mean_col, amt_max_col,
            gap_mean_col, velocity_mean_col, high_amount_share_col, burst_ratio_col
        ])
    
    # Fill NaNs with 0.0
    for col in temporal_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            # Replace inf with 0.0
            df[col] = df[col].replace([np.inf, -np.inf], 0.0)
    
    # PART 6: Validation checks
    # Check temporal_cols exist
    missing_cols = [col for col in temporal_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Temporal columns missing after computation: {missing_cols}")
    
    # Check no inf in temporal_cols
    for col in temporal_cols:
        if col in df.columns:
            if np.isinf(df[col]).any():
                raise ValueError(f"Temporal column {col} contains infinite values")
    
    # Ensure df length unchanged
    if len(df) != original_len:
        raise ValueError(f"DataFrame length changed during temporal aggregate computation: {original_len} -> {len(df)}")
    
    # Debug output (if enabled)
    show_debug = getattr(config, "SHOW_FEATURE_DEBUG", False)
    if show_debug:
        print(f"Temporal columns created: {temporal_cols}")
        print(f"\nHead of temporal columns:")
        print(df[temporal_cols].head())
        print(f"\nDescribe of tx_count_7d:")
        if "tx_count_7d" in df.columns:
            print(df["tx_count_7d"].describe())
        print(f"\nDescribe of burst_ratio_7d:")
        if "burst_ratio_7d" in df.columns:
            print(df["burst_ratio_7d"].describe())
    
    return df, temporal_cols


def compute_temporal_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Compute temporal behavior modeling features for sequence dynamics.
    
    Captures how behavior changes over time: bursts, regime shifts, volatility.
    
    Args:
        df: DataFrame with user_id, timestamp, amount, time_gap, and optionally risk_score_ml_calibrated
        
    Returns:
        Tuple of (DataFrame with temporal features added, list of temporal feature column names)
    """
    # #region agent log
    import os, json, time
    _log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.cursor')
    os.makedirs(_log_dir, exist_ok=True)
    _log_path = os.path.join(_log_dir, 'debug.log')
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:333","message":"compute_temporal_features entry","data":{"df_len":len(df),"df_cols":list(df.columns)[:10]},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    original_len = len(df)
    df = df.copy()
    
    # Ensure timestamp exists
    # #region agent log
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:348","message":"Before ensure_timestamp","data":{},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    df = ensure_timestamp(df, time_col="timestamp")
    # #region agent log
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:350","message":"After ensure_timestamp","data":{"has_timestamp":"timestamp" in df.columns},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    # Required columns
    if "user_id" not in df.columns:
        raise ValueError("Missing required column: user_id")
    
    # Ensure required columns exist with defaults
    if "amount" not in df.columns:
        df["amount"] = 0.0
    if "time_gap" not in df.columns:
        df["time_gap"] = 1.0
    
    amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    time_gap = pd.to_numeric(df["time_gap"], errors="coerce").fillna(1.0)
    
    # Compute velocity if not exists
    if "velocity" not in df.columns:
        eps = getattr(config, "EPS", 1e-9)
        safe_time_gap = time_gap.clip(lower=eps)
        df["velocity"] = 1.0 / safe_time_gap
    else:
        df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0.0)
    
    velocity = df["velocity"]
    
    # Get ML risk score for regime shift (use risk_score_ml_calibrated if available, else risk_score)
    if "risk_score_ml_calibrated" in df.columns:
        ml_risk = pd.to_numeric(df["risk_score_ml_calibrated"], errors="coerce").fillna(0.0) / 100.0
    elif "risk_score" in df.columns:
        ml_risk = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0) / 100.0
    elif "risk_prob" in df.columns:
        ml_risk = pd.to_numeric(df["risk_prob"], errors="coerce").fillna(0.0)
    else:
        ml_risk = pd.Series(0.0, index=df.index)
    
    # Sort by user_id and timestamp, keep original index
    df_sorted = df.sort_values(["user_id", "timestamp"], kind="mergesort").copy()
    df_sorted["_orig_index"] = df_sorted.index
    
    temporal_feature_cols = []
    eps = getattr(config, "EPS", 1e-9)
    
    # Process per user (row-based rolling, not time-based for simplicity)
    # #region agent log
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:411","message":"Before user loop","data":{"df_sorted_len":len(df_sorted),"unique_users":df_sorted["user_id"].nunique()},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    user_count = 0
    for user_id, group in df_sorted.groupby("user_id", sort=False):
        if len(group) == 0:
            continue
        
        user_count += 1
        if user_count == 1:
            # #region agent log
            try:
                with open(_log_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:424","message":"First user processing","data":{"user_id":str(user_id),"group_len":len(group)},"timestamp":int(time.time()*1000)}) + "\n")
            except: pass
            # #endregion
        
        group = group.sort_values("timestamp").copy()
        orig_indices = group["_orig_index"].values  # Get original DataFrame indices
        
        # 1. Rolling Behavior Drift
        rolling_mean_10 = group["amount"].rolling(window=10, min_periods=1).mean()
        rolling_mean_30 = group["amount"].rolling(window=30, min_periods=1).mean()
        drift_amount = rolling_mean_10 / (rolling_mean_30 + eps)
        drift_amount = np.clip(drift_amount, 0.0, 10.0).fillna(1.0)
        
        df.loc[orig_indices, "rolling_mean_amount_10"] = rolling_mean_10.fillna(0.0).values
        df.loc[orig_indices, "rolling_mean_amount_30"] = rolling_mean_30.fillna(0.0).values
        df.loc[orig_indices, "drift_amount"] = drift_amount.values
        
        # 2. Activity Burst Intensity
        tx_count_10 = group["amount"].rolling(window=10, min_periods=1).count()
        tx_count_50 = group["amount"].rolling(window=50, min_periods=1).count()
        burst_ratio = tx_count_10 / (tx_count_50 + eps)
        burst_ratio = np.clip(burst_ratio, 0.0, 10.0).fillna(1.0)
        
        df.loc[orig_indices, "tx_count_last_10"] = tx_count_10.fillna(0.0).values
        df.loc[orig_indices, "tx_count_last_50"] = tx_count_50.fillna(0.0).values
        df.loc[orig_indices, "burst_ratio"] = burst_ratio.values
        
        # 3. Velocity Acceleration
        velocity_group = velocity.loc[orig_indices]
        rolling_velocity_5 = pd.Series(velocity_group.values).rolling(window=5, min_periods=1).mean()
        rolling_velocity_20 = pd.Series(velocity_group.values).rolling(window=20, min_periods=1).mean()
        velocity_acceleration = rolling_velocity_5 / (rolling_velocity_20 + eps)
        velocity_acceleration = np.clip(velocity_acceleration, 0.0, 10.0).fillna(1.0)
        
        df.loc[orig_indices, "rolling_velocity_5"] = rolling_velocity_5.fillna(0.0).values
        df.loc[orig_indices, "rolling_velocity_20"] = rolling_velocity_20.fillna(0.0).values
        df.loc[orig_indices, "velocity_acceleration"] = velocity_acceleration.values
        
        # 4. Volatility Spike
        rolling_std_10 = group["amount"].rolling(window=10, min_periods=1).std()
        rolling_std_30 = group["amount"].rolling(window=30, min_periods=1).std()
        volatility_ratio = rolling_std_10 / (rolling_std_30 + eps)
        volatility_ratio = np.clip(volatility_ratio, 0.0, 10.0).fillna(1.0)
        
        df.loc[orig_indices, "rolling_std_amount_10"] = rolling_std_10.fillna(0.0).values
        df.loc[orig_indices, "rolling_std_amount_30"] = rolling_std_30.fillna(0.0).values
        df.loc[orig_indices, "volatility_ratio"] = volatility_ratio.values
        
        # 5. Risk Regime Shift
        ml_risk_group_vals = ml_risk.loc[orig_indices].values
        rolling_risk_5 = pd.Series(ml_risk_group_vals).rolling(window=5, min_periods=1).mean()
        rolling_risk_30 = pd.Series(ml_risk_group_vals).rolling(window=30, min_periods=1).mean()
        risk_regime_shift = rolling_risk_5 / (rolling_risk_30 + eps)
        risk_regime_shift = np.clip(risk_regime_shift, 0.0, 10.0).fillna(1.0)
        
        df.loc[orig_indices, "rolling_risk_5"] = rolling_risk_5.fillna(0.0).values
        df.loc[orig_indices, "rolling_risk_30"] = rolling_risk_30.fillna(0.0).values
        df.loc[orig_indices, "risk_regime_shift"] = risk_regime_shift.values
        
        # 6. Dormancy Break
        # Calculate days since previous transaction
        if len(group) > 1:
            time_diffs = pd.to_datetime(group["timestamp"]).diff().dt.total_seconds() / 86400.0
            time_diffs = time_diffs.fillna(0.0)
            df.loc[orig_indices, "days_since_prev_tx"] = time_diffs.values
            
            # User median gap
            user_median_gap = time_diffs.median()
            if not np.isfinite(user_median_gap) or user_median_gap <= 0:
                user_median_gap = 1.0
            
            dormancy_flag = (time_diffs > user_median_gap * 5.0).astype(int)
            df.loc[orig_indices, "dormancy_flag"] = dormancy_flag.values
        else:
            df.loc[orig_indices, "days_since_prev_tx"] = 0.0
            df.loc[orig_indices, "dormancy_flag"] = 0
    
    # Define temporal feature columns
    temporal_feature_cols = [
        "rolling_mean_amount_10",
        "rolling_mean_amount_30",
        "drift_amount",
        "tx_count_last_10",
        "tx_count_last_50",
        "burst_ratio",
        "rolling_velocity_5",
        "rolling_velocity_20",
        "velocity_acceleration",
        "rolling_std_amount_10",
        "rolling_std_amount_30",
        "volatility_ratio",
        "rolling_risk_5",
        "rolling_risk_30",
        "risk_regime_shift",
        "days_since_prev_tx",
        "dormancy_flag",
    ]
    
    # Fill NaNs and ensure no inf
    for col in temporal_feature_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            df[col] = df[col].replace([np.inf, -np.inf], 0.0)
            # Clip ratios to [0, 10]
            if "ratio" in col or "drift" in col or "acceleration" in col or "shift" in col:
                df[col] = np.clip(df[col], 0.0, 10.0)
    
    # Validation
    # #region agent log
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:497","message":"Before validation","data":{"original_len":original_len,"current_len":len(df),"cols_created":sum(1 for c in temporal_feature_cols if c in df.columns)},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    if len(df) != original_len:
        raise ValueError(f"DataFrame length changed: {original_len} -> {len(df)}")
    
    missing_cols = [col for col in temporal_feature_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Temporal feature columns missing: {missing_cols}")
    
    # #region agent log
    try:
        with open(_log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"init","hypothesisId":"B","location":"temporal_features.py:505","message":"compute_temporal_features exit","data":{"temporal_cols_count":len(temporal_feature_cols)},"timestamp":int(time.time()*1000)}) + "\n")
    except: pass
    # #endregion
    
    return df, temporal_feature_cols
