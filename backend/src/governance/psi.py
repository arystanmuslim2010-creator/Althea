"""Population Stability Index (PSI) Calculation for Model Monitoring."""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd


def calculate_psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    """
    Calculate Population Stability Index (PSI) between expected and actual distributions.
    
    PSI measures how much the distribution of a variable has shifted.
    
    PSI interpretation:
    - < 0.1: Stable (no significant change)
    - 0.1 - 0.25: Warning (minor shift)
    - >= 0.25: Unstable (significant shift)
    
    Args:
        expected: Expected/reference distribution (e.g., training data)
        actual: Actual/current distribution (e.g., production data)
        bins: Number of bins for discretization
        
    Returns:
        PSI value (float)
    """
    if len(expected) == 0 or len(actual) == 0:
        return 0.0
    
    # Convert to numeric, handle NaN
    expected_clean = pd.to_numeric(expected, errors="coerce").dropna()
    actual_clean = pd.to_numeric(actual, errors="coerce").dropna()
    
    if len(expected_clean) == 0 or len(actual_clean) == 0:
        return 0.0
    
    # Create bins based on expected distribution
    try:
        # Use quantile-based binning for robustness
        bin_edges = np.quantile(expected_clean, np.linspace(0, 1, bins + 1))
        bin_edges = np.unique(bin_edges)  # Remove duplicates
        
        if len(bin_edges) < 2:
            # Fallback: use min/max
            bin_edges = np.linspace(expected_clean.min(), expected_clean.max(), bins + 1)
    except Exception:
        # Fallback: use min/max
        bin_edges = np.linspace(expected_clean.min(), expected_clean.max(), bins + 1)
    
    # Compute histograms
    expected_hist, _ = np.histogram(expected_clean, bins=bin_edges)
    actual_hist, _ = np.histogram(actual_clean, bins=bin_edges)
    
    # Normalize to probabilities
    expected_probs = expected_hist / (len(expected_clean) + 1e-10)
    actual_probs = actual_hist / (len(actual_clean) + 1e-10)
    
    # Add small epsilon to avoid log(0)
    eps = 1e-10
    expected_probs = expected_probs + eps
    actual_probs = actual_probs + eps
    
    # Normalize again after adding epsilon
    expected_probs = expected_probs / expected_probs.sum()
    actual_probs = actual_probs / actual_probs.sum()
    
    # Calculate PSI
    psi = 0.0
    for i in range(len(expected_probs)):
        if expected_probs[i] > 0 and actual_probs[i] > 0:
            psi += (actual_probs[i] - expected_probs[i]) * np.log(actual_probs[i] / expected_probs[i])
    
    return float(psi)


def psi_for_feature(df: pd.DataFrame, col: str, reference_slice: Optional[pd.Series] = None) -> dict:
    """
    Calculate PSI for a specific feature column.
    
    Args:
        df: DataFrame with the feature column
        col: Column name to calculate PSI for
        reference_slice: Optional reference distribution (if None, uses first 30% of data)
        
    Returns:
        Dictionary with:
        - feature_name: column name
        - psi_value: PSI score
        - psi_status: "stable", "warning", or "unstable"
    """
    if col not in df.columns:
        return {
            "feature_name": col,
            "psi_value": 0.0,
            "psi_status": "missing",
        }
    
    feature_values = pd.to_numeric(df[col], errors="coerce").dropna()
    
    if len(feature_values) == 0:
        return {
            "feature_name": col,
            "psi_value": 0.0,
            "psi_status": "no_data",
        }
    
    # Use reference slice if provided, otherwise use first 30% as reference
    if reference_slice is not None:
        expected = reference_slice
    else:
        n_ref = max(1, int(len(feature_values) * 0.30))
        expected = feature_values.iloc[:n_ref]
    
    # Current distribution: last 30%
    n_current = max(1, int(len(feature_values) * 0.30))
    actual = feature_values.iloc[-n_current:]
    
    psi_value = calculate_psi(expected, actual)
    
    # Determine status
    if psi_value < 0.1:
        psi_status = "stable"
    elif psi_value < 0.25:
        psi_status = "warning"
    else:
        psi_status = "unstable"
    
    return {
        "feature_name": col,
        "psi_value": psi_value,
        "psi_status": psi_status,
    }


def compute_psi_table(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    """
    Compute PSI for multiple features.
    
    Args:
        df: DataFrame with features
        feature_cols: List of feature column names
        
    Returns:
        DataFrame with PSI results for each feature
    """
    psi_results = []
    
    for col in feature_cols:
        result = psi_for_feature(df, col)
        psi_results.append(result)
    
    return pd.DataFrame(psi_results)
