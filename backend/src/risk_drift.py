"""Model Risk Drift Monitoring for AML behavioral risk engine health checks."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from . import config, utils

_logger = utils.get_logger(__name__)


def compute_drift_metrics(
    df: pd.DataFrame,
    feature_cols: Optional[List[str]] = None,
) -> Dict[str, any]:
    """
    Analyze health of the risk model and return drift diagnostics.
    
    Detects:
    - Risk score collapse/saturation
    - Risk distribution flatness
    - Feature drift (collapse/explosion)
    - Model instability
    - Alert rate anomalies
    
    Args:
        df: DataFrame with risk_score, risk_uncertainty, Suspicious, and feature columns
        feature_cols: Optional list of feature column names to check for drift
        
    Returns:
        Dictionary with metrics and drift flags
    """
    df = df.copy()
    
    drift_flags: List[str] = []
    
    # Ensure required columns exist
    if "risk_score" not in df.columns:
        _logger.warning("risk_score column not found in drift monitoring")
        return {
            "risk_mean": 0.0,
            "risk_std": 0.0,
            "pct_high": 0.0,
            "pct_low": 0.0,
            "alert_ratio": 0.0,
            "uncertainty_mean": 0.0,
            "drift_flags": ["missing_risk_score"],
        }
    
    risk_score = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    
    # Risk distribution health checks
    mean_risk = float(risk_score.mean())
    std_risk = float(risk_score.std())
    pct_high = float((risk_score >= 90).mean())
    pct_low = float((risk_score <= 10).mean())
    
    # Add drift flags for risk distribution
    if pct_high > 0.8:
        drift_flags.append("risk_saturation")
        _logger.warning(f"Risk saturation detected: {pct_high:.1%} of alerts have risk_score >= 90")
    
    if pct_low > 0.8:
        drift_flags.append("risk_collapse")
        _logger.warning(f"Risk collapse detected: {pct_low:.1%} of alerts have risk_score <= 10")
    
    if std_risk < 5.0:
        drift_flags.append("risk_distribution_flat")
        _logger.warning(f"Risk distribution too flat: std={std_risk:.2f} (< 5.0)")
    
    # Feature drift detection
    if feature_cols:
        available_features = [col for col in feature_cols if col in df.columns]
        
        if available_features:
            feature_df = df[available_features]
            feature_means = feature_df.mean()
            feature_stds = feature_df.std()
            
            # Check for feature collapse (std extremely small)
            for col in available_features:
                feat_std = feature_stds[col]
                if feat_std < 1e-6 and abs(feat_std) > 0:  # Near-zero but not exactly zero
                    drift_flags.append(f"feature_collapse_{col}")
                    _logger.warning(f"Feature collapse detected: {col} has std={feat_std:.2e}")
            
            # Check for feature explosion (mean extremely large)
            # Use z-score approach: if mean is > 10 standard deviations from 0
            for col in available_features:
                feat_mean = abs(feature_means[col])
                feat_std = feature_stds[col]
                if feat_std > 0 and feat_mean / feat_std > 10:
                    drift_flags.append(f"feature_explosion_{col}")
                    _logger.warning(f"Feature explosion detected: {col} mean={feat_mean:.2f}, std={feat_std:.2f}")
    
    # Risk uncertainty stability
    uncertainty_mean = 0.0
    if "risk_uncertainty" in df.columns:
        risk_uncertainty = pd.to_numeric(df["risk_uncertainty"], errors="coerce").fillna(0.0)
        uncertainty_mean = float(risk_uncertainty.mean())
        
        # If uncertainty is unusually high (> 0.5 for normalized uncertainty)
        if uncertainty_mean > 0.5:
            drift_flags.append("model_instability")
            _logger.warning(f"Model instability detected: uncertainty_mean={uncertainty_mean:.3f} (> 0.5)")
    
    # Alert rate sanity
    alert_ratio = 0.0
    if "Suspicious" in df.columns:
        suspicious_col = df["Suspicious"].astype(str)
        alert_ratio = float((suspicious_col == config.RISK_LABEL_YES).mean())
        
        if alert_ratio > 0.5:
            drift_flags.append("too_many_alerts")
            _logger.warning(f"Too many alerts: {alert_ratio:.1%} of transactions flagged (> 50%)")
        
        if alert_ratio < 0.01:
            drift_flags.append("too_few_alerts")
            _logger.warning(f"Too few alerts: {alert_ratio:.1%} of transactions flagged (< 1%)")
    
    # Additional checks: Check for NaN/inf in risk_score
    if risk_score.isna().any() or np.isinf(risk_score).any():
        drift_flags.append("risk_score_corruption")
        _logger.warning("Risk score contains NaN or Inf values")
    
    # Check for extreme values
    if (risk_score > 100).any() or (risk_score < 0).any():
        drift_flags.append("risk_score_out_of_bounds")
        _logger.warning("Risk score contains values outside [0, 100] range")
    
    return {
        "risk_mean": mean_risk,
        "risk_std": std_risk,
        "pct_high": pct_high,
        "pct_low": pct_low,
        "alert_ratio": alert_ratio,
        "uncertainty_mean": uncertainty_mean,
        "drift_flags": drift_flags,
    }
