"""Probability calibration and score mapping for AML risk scoring.

This module provides calibration methods (isotonic, platt) and score mapping
functions to reduce saturation and produce stable, interpretable risk scores.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from . import config


def fit_calibrator(
    y_true: np.ndarray,
    p_pred: np.ndarray,
    method: str = "isotonic",
) -> Optional[object]:
    """Fit a probability calibrator.
    
    Args:
        y_true: True binary labels (0/1)
        p_pred: Predicted probabilities in [0,1]
        method: "isotonic" or "platt"
        
    Returns:
        Fitted calibrator object, or None if calibration cannot be fitted
    """
    if len(y_true) == 0 or len(p_pred) == 0:
        return None
    
    # Check if we have both classes
    if len(np.unique(y_true)) < 2:
        return None
    
    # Check if predictions have variance
    if np.unique(p_pred).size < 2:
        return None
    
    try:
        if method == "isotonic":
            calibrator = IsotonicRegression(out_of_bounds="clip")
            calibrator.fit(p_pred, y_true)
            return calibrator
        elif method == "platt":
            calibrator = LogisticRegression(solver="lbfgs", max_iter=1000)
            # Platt scaling expects 2D input
            p_pred_2d = p_pred.reshape(-1, 1)
            calibrator.fit(p_pred_2d, y_true)
            return calibrator
        else:
            raise ValueError(f"Unknown calibration method: {method}")
    except Exception:
        # If calibration fails, return None (will use raw probabilities)
        return None


def apply_calibrator(calibrator: Optional[object], p_pred: np.ndarray) -> np.ndarray:
    """Apply a fitted calibrator to predicted probabilities.
    
    Args:
        calibrator: Fitted calibrator object (from fit_calibrator)
        p_pred: Predicted probabilities in [0,1]
        
    Returns:
        Calibrated probabilities in [0,1]
    """
    if calibrator is None:
        return p_pred
    
    try:
        if isinstance(calibrator, IsotonicRegression):
            p_cal = calibrator.predict(p_pred)
        elif isinstance(calibrator, LogisticRegression):
            p_pred_2d = p_pred.reshape(-1, 1)
            p_cal = calibrator.predict_proba(p_pred_2d)[:, 1]
        else:
            # Unknown calibrator type, return raw
            return p_pred
        
        # Clip to [0, 1]
        p_cal = np.clip(p_cal, 0.0, 1.0)
        return p_cal
    except Exception:
        # If application fails, return raw probabilities
        return p_pred


def score_mapping(p: np.ndarray, kind: str = "logit_stretch") -> np.ndarray:
    """Map probabilities to 0-100 scores with saturation reduction.
    
    Args:
        p: Probabilities in [0,1]
        kind: "logit_stretch" or "rank_percentile"
        
    Returns:
        Scores in [0,100]
    """
    p = np.asarray(p, dtype=float)
    
    if kind == "logit_stretch":
        # Option A: logit stretch
        eps = 1e-6
        p_clipped = np.clip(p, eps, 1.0 - eps)
        
        # z = log(p/(1-p))
        z = np.log(p_clipped / (1.0 - p_clipped))
        
        # z_scaled = z / temperature
        temperature = getattr(config, "SCORE_TEMPERATURE", 1.8)
        z_scaled = z / max(temperature, 0.1)
        
        # p2 = sigmoid(z_scaled)
        p2 = 1.0 / (1.0 + np.exp(-z_scaled))
        
        # Return p2 * 100
        score = p2 * 100.0
        return np.clip(score, 0.0, 100.0)
    
    elif kind == "rank_percentile":
        # Option B: rank percentile
        if len(p) == 0:
            return np.array([])
        
        # Compute percentile rank
        p_series = pd.Series(p)
        rank_pct = p_series.rank(pct=True, method="average")
        score = rank_pct.values * 100.0
        return np.clip(score, 0.0, 100.0)
    
    else:
        # Fallback: direct mapping
        return np.clip(p * 100.0, 0.0, 100.0)


def compute_calibration_metrics(
    y_true: np.ndarray,
    p_pred: np.ndarray,
    n_bins: int = 10,
) -> Dict[str, Any]:
    """
    Compute calibration quality metrics: Brier score, reliability curve bins, and AUC.

    Args:
        y_true: True binary labels (0/1)
        p_pred: Predicted probabilities in [0,1]
        n_bins: Number of bins for reliability curve

    Returns:
        Dict with brier, auc, reliability_bins (list of {bin_low, bin_high, mean_pred, mean_true, count})
    """
    from sklearn.metrics import brier_score_loss, roc_auc_score

    y_true = np.asarray(y_true, dtype=float).ravel()
    p_pred = np.asarray(p_pred, dtype=float).ravel()
    p_pred = np.clip(p_pred, 1e-6, 1.0 - 1e-6)

    brier = float(brier_score_loss(y_true, p_pred))
    try:
        auc = float(roc_auc_score(y_true, p_pred))
    except Exception:
        auc = 0.5

    # Reliability curve: bin predictions and compare mean pred vs mean true
    reliability_bins: List[Dict[str, Any]] = []
    if len(p_pred) > 0 and n_bins >= 1:
        bin_edges = np.linspace(0, 1, n_bins + 1)
        for i in range(n_bins):
            lo, hi = bin_edges[i], bin_edges[i + 1]
            mask = (p_pred >= lo) & (p_pred < hi) if i < n_bins - 1 else (p_pred >= lo) & (p_pred <= hi)
            if mask.sum() > 0:
                reliability_bins.append({
                    "bin_low": float(lo),
                    "bin_high": float(hi),
                    "mean_pred": float(np.mean(p_pred[mask])),
                    "mean_true": float(np.mean(y_true[mask])),
                    "count": int(mask.sum()),
                })
            else:
                reliability_bins.append({
                    "bin_low": float(lo),
                    "bin_high": float(hi),
                    "mean_pred": float((lo + hi) / 2),
                    "mean_true": 0.0,
                    "count": 0,
                })

    return {
        "brier": brier,
        "auc": auc,
        "reliability_bins": reliability_bins,
        "n_bins": n_bins,
    }
