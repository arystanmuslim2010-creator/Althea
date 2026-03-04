"""Behavioral Baseline Engine for AML anomaly detection."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import config


class BaselineEngine:
    """Computes behavioral baselines with fallback chain (user -> segment -> global)."""
    
    def __init__(self):
        """Initialize Baseline Engine."""
        pass
    
    def compute_baselines(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute behavioral baselines for each transaction.
        
        Fallback chain:
        1. User baseline if user history >= 20 rows
        2. Segment baseline if segment count >= 100
        3. Global baseline otherwise
        
        Adds columns:
        - baseline_level: "user" / "segment" / "global"
        - baseline_window: 30 (days)
        - n_hist: number of historical points used
        
        Args:
            df: DataFrame with user_id, segment, amount columns
            
        Returns:
            DataFrame with baseline columns added
        """
        df = df.copy()
        
        # Initialize baseline columns
        df["baseline_level"] = "global"
        df["baseline_window"] = 30
        df["baseline_window_days"] = 30  # Also add baseline_window_days for compatibility
        df["n_hist"] = 0
        
        # Ensure required columns exist
        if "user_id" not in df.columns:
            df["user_id"] = "unknown"
        if "segment" not in df.columns:
            df["segment"] = "unknown"
        
        amount = pd.to_numeric(df.get("amount", 0.0), errors="coerce").fillna(0.0)
        
        # Compute user-level statistics
        user_stats = df.groupby("user_id")["amount"].agg(["count", "mean", "std"]).reset_index()
        user_stats.columns = ["user_id", "user_count", "user_mean", "user_std"]
        user_stats["user_std"] = user_stats["user_std"].fillna(0.0)
        
        # Merge user stats back
        df = df.merge(user_stats, on="user_id", how="left")
        
        # Compute segment-level statistics
        segment_stats = df.groupby("segment")["amount"].agg(["count", "mean", "std"]).reset_index()
        segment_stats.columns = ["segment", "segment_count", "segment_mean", "segment_std"]
        segment_stats["segment_std"] = segment_stats["segment_std"].fillna(0.0)
        
        # Merge segment stats back
        df = df.merge(segment_stats, on="segment", how="left")
        
        # Compute global statistics
        global_mean = float(amount.mean())
        global_std = float(amount.std()) if amount.std() > 0 else 1.0
        
        # Apply fallback chain logic
        # 1. User baseline if user history >= 20 rows
        user_baseline_mask = df["user_count"] >= 20
        df.loc[user_baseline_mask, "baseline_level"] = "user"
        df.loc[user_baseline_mask, "n_hist"] = df.loc[user_baseline_mask, "user_count"]
        
        # 2. Segment baseline if segment count >= 100 (and not user baseline)
        segment_baseline_mask = (~user_baseline_mask) & (df["segment_count"] >= 100)
        df.loc[segment_baseline_mask, "baseline_level"] = "segment"
        df.loc[segment_baseline_mask, "n_hist"] = df.loc[segment_baseline_mask, "segment_count"]
        
        # 3. Global baseline otherwise
        global_baseline_mask = (~user_baseline_mask) & (~segment_baseline_mask)
        df.loc[global_baseline_mask, "baseline_level"] = "global"
        df.loc[global_baseline_mask, "n_hist"] = len(df)
        
        # Calculate z-scores vs chosen baseline
        user_z = np.where(
            df["baseline_level"] == "user",
            (amount - df["user_mean"]) / np.maximum(df["user_std"], 1e-6),
            0.0
        )
        
        segment_z = np.where(
            df["baseline_level"] == "segment",
            (amount - df["segment_mean"]) / np.maximum(df["segment_std"], 1e-6),
            0.0
        )
        
        global_z = np.where(
            df["baseline_level"] == "global",
            (amount - global_mean) / max(global_std, 1e-6),
            0.0
        )
        
        df["amount_z"] = user_z + segment_z + global_z
        
        # Clean up temporary columns
        df = df.drop(columns=[
            "user_count", "user_mean", "user_std",
            "segment_count", "segment_mean", "segment_std"
        ], errors="ignore")
        
        return df
