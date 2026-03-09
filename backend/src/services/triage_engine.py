"""Bank-grade Triage Engine for AML alert prioritization."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import config


class TriageEngine:
    """Adds triage fields for operational alert prioritization."""
    
    def __init__(self, cfg):
        """
        Initialize Triage Engine.
        
        Args:
            cfg: Config object
        """
        self.cfg = cfg
    
    def add_triage_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add triage fields to dataframe.
        
        Adds columns:
        - risk_score_rank: Percentile rank (0-100)
        - risk_band: critical/high/medium/low
        - sla_hours: SLA hours based on band
        - queue_bucket: Combination of typology + band
        
        Args:
            df: DataFrame with risk_score and typology columns
            
        Returns:
            DataFrame with triage columns added
        """
        df = df.copy()
        
        # Ensure risk_score exists
        risk_score = pd.to_numeric(df.get("risk_score", 0.0), errors="coerce").fillna(0.0)
        
        # Calculate risk_score_rank: percentile-like ranking (0-100)
        # Use robust rank with method="average" for ties
        df["risk_score_rank"] = risk_score.rank(pct=True, method="average") * 100.0
        df["risk_score_rank"] = df["risk_score_rank"].clip(0.0, 100.0).astype(float)
        
        # Add risk_band categories based on rank
        # critical: rank >= 99
        # high: 95-99
        # medium: 80-95
        # low: <80
        df["risk_band"] = pd.cut(
            df["risk_score_rank"],
            bins=[0, 80, 95, 99, 100],
            labels=["low", "medium", "high", "critical"],
            include_lowest=True,
            right=True
        ).astype(str)
        
        # Fill any NaN values with "low"
        df["risk_band"] = df["risk_band"].fillna("low")
        
        # Add sla_hours based on risk_band
        # critical: 4, high: 24, medium: 72, low: 168
        df["sla_hours"] = df["risk_band"].map({
            "critical": 4,
            "high": 24,
            "medium": 72,
            "low": 168
        }).fillna(168).astype(int)
        
        # Add queue_bucket: combining typology + band
        # Example: "critical_smurfing", "high_rapid_velocity", etc.
        typology = df.get("typology", "behavioral_anomaly").astype(str).str.lower()
        typology = typology.fillna("behavioral_anomaly")
        risk_band = df["risk_band"].astype(str)
        
        df["queue_bucket"] = risk_band + "_" + typology
        
        return df
