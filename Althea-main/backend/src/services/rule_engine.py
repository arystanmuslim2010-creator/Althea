"""AML Typology Rule Engine for deterministic risk detection."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import config


class RuleEngine:
    """Applies AML typology rules to detect suspicious patterns."""
    
    def __init__(self, cfg):
        """
        Initialize Rule Engine.
        
        Args:
            cfg: Config object with rule parameters
        """
        self.cfg = cfg
    
    def apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply AML typology rules to dataframe.
        
        Adds columns:
        - typology: Detected typology name
        - rule_score: Rule-based risk score (0-1)
        - rule_flags: List of triggered rule flags (semicolon-separated)
        
        Args:
            df: DataFrame with amount, num_transactions, time_gap columns
            
        Returns:
            DataFrame with rule columns added
        """
        df = df.copy()
        
        # Ensure required columns exist with safe defaults
        amount = pd.to_numeric(df.get("amount", 0.0), errors="coerce").fillna(0.0)
        num_transactions = pd.to_numeric(df.get("num_transactions", 1.0), errors="coerce").fillna(1.0)
        time_gap = pd.to_numeric(df.get("time_gap", 60.0), errors="coerce").fillna(60.0)
        
        # Initialize rule columns
        df["typology"] = "behavioral_anomaly"
        df["rule_score"] = 0.4
        df["rule_flags"] = ""
        
        # 1️⃣ STRUCTURING / SMURFING
        # Trigger if: many transactions, medium size, short time gap
        cond_smurf = (
            (num_transactions > num_transactions.quantile(0.85)) &
            (amount < amount.quantile(0.7)) &
            (time_gap < time_gap.quantile(0.3))
        )
        df.loc[cond_smurf, "typology"] = "smurfing"
        df.loc[cond_smurf, "rule_score"] = 0.8
        df.loc[cond_smurf, "rule_flags"] = "smurfing"
        
        # 2️⃣ RAPID VELOCITY
        cond_velocity = time_gap < time_gap.quantile(0.1)
        # Only apply if not already smurfing (priority order)
        cond_velocity = cond_velocity & ~cond_smurf
        df.loc[cond_velocity, "typology"] = "rapid_velocity"
        df.loc[cond_velocity, "rule_score"] = 0.7
        df.loc[cond_velocity, "rule_flags"] = df.loc[cond_velocity, "rule_flags"].apply(
            lambda x: "rapid_velocity" if not x else x + "; rapid_velocity"
        )
        
        # 3️⃣ HIGH AMOUNT OUTLIER
        cond_amount = amount > amount.quantile(0.98)
        # Only apply if not already smurfing or rapid_velocity
        cond_amount = cond_amount & ~cond_smurf & ~cond_velocity
        df.loc[cond_amount, "typology"] = "high_amount_outlier"
        df.loc[cond_amount, "rule_score"] = 0.75
        df.loc[cond_amount, "rule_flags"] = df.loc[cond_amount, "rule_flags"].apply(
            lambda x: "high_amount_outlier" if not x else x + "; high_amount_outlier"
        )
        
        # 4️⃣ BURST ACTIVITY
        cond_burst = num_transactions > num_transactions.quantile(0.95)
        # Only apply if not already matched by higher priority rules
        cond_burst = cond_burst & ~cond_smurf & ~cond_velocity & ~cond_amount
        df.loc[cond_burst, "typology"] = "burst_activity"
        df.loc[cond_burst, "rule_score"] = 0.65
        df.loc[cond_burst, "rule_flags"] = df.loc[cond_burst, "rule_flags"].apply(
            lambda x: "burst_activity" if not x else x + "; burst_activity"
        )
        
        # Ensure rule_score is in [0, 1] range
        df["rule_score"] = df["rule_score"].clip(0.0, 1.0)
        
        return df
