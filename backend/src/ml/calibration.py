"""
Calibration fit on validation set only: train base model on train, fit calibrator on val, evaluate on test.
"""
from __future__ import annotations

from typing import Any, Optional, Union

import numpy as np


def fit_calibrator(
    val_scores: Union[list, np.ndarray],
    y_val: Union[list, np.ndarray],
    method: str = "isotonic",
) -> Optional[Any]:
    """
    Fit calibrator on validation scores and labels.

    Args:
        val_scores: Predicted probabilities on validation set.
        y_val: True labels (0/1) on validation set.
        method: "isotonic" or "platt" (sigmoid).

    Returns:
        Fitted calibrator object (IsotonicRegression or Platt scaler) or None.
    """
    val_scores = np.asarray(val_scores, dtype=float).ravel()
    y_val = np.asarray(y_val, dtype=float).ravel()
    if val_scores.size == 0 or y_val.size == 0 or np.unique(y_val).size < 2:
        return None
    val_scores = np.clip(val_scores, 1e-6, 1.0 - 1e-6)

    if method == "isotonic":
        from sklearn.isotonic import IsotonicRegression
        cal = IsotonicRegression(out_of_bounds="clip")
        cal.fit(val_scores, y_val)
        return cal
    if method == "platt":
        from sklearn.linear_model import LogisticRegression
        lr = LogisticRegression(C=1e10, max_iter=1000, random_state=42)
        lr.fit(val_scores.reshape(-1, 1), y_val)
        return lr
    return None


def apply_calibrator(calibrator: Any, scores: Union[list, np.ndarray]) -> np.ndarray:
    """Apply fitted calibrator to scores. Returns calibrated probabilities."""
    scores = np.asarray(scores, dtype=float).ravel()
    if calibrator is None:
        return np.clip(scores, 0.0, 1.0)
    # IsotonicRegression accepts (n,) or (n,1); LogisticRegression expects (n, 1)
    X = scores.reshape(-1, 1)
    out = calibrator.predict(X)
    out = np.asarray(out, dtype=float).ravel()
    return np.clip(out, 0.0, 1.0)
