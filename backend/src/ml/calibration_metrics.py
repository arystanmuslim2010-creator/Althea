"""
Calibration metrics: Brier score and Expected Calibration Error (bin-based ECE).
"""
from __future__ import annotations

from typing import Union

import numpy as np


def brier_score(y_true: Union[list, np.ndarray], y_prob: Union[list, np.ndarray]) -> float:
    """Brier score: mean squared error between predicted prob and 0/1 outcome. Lower is better."""
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_prob = np.asarray(y_prob, dtype=float).ravel()
    if y_true.size == 0:
        return 0.0
    return float(np.mean((y_prob - y_true) ** 2))


def ece(
    y_true: Union[list, np.ndarray],
    y_prob: Union[list, np.ndarray],
    n_bins: int = 10,
) -> float:
    """
    Expected Calibration Error (bin-based): weighted average of |acc(b) - conf(b)|.

    Bins are equal-width on [0,1] by predicted probability.
    """
    y_true = np.asarray(y_true, dtype=float).ravel()
    y_prob = np.asarray(y_prob, dtype=float).ravel()
    if y_true.size == 0 or n_bins < 1:
        return 0.0
    bin_edges = np.linspace(0, 1, n_bins + 1)
    ece_val = 0.0
    n = len(y_true)
    for i in range(n_bins):
        low, high = bin_edges[i], bin_edges[i + 1]
        mask = (y_prob >= low) & (y_prob < high) if i < n_bins - 1 else (y_prob >= low) & (y_prob <= high)
        if mask.sum() == 0:
            continue
        acc = y_true[mask].mean()
        conf = y_prob[mask].mean()
        weight = mask.sum() / n
        ece_val += weight * abs(acc - conf)
    return float(ece_val)
