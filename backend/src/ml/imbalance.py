"""
Class imbalance handling for binary targets (scale_pos_weight for LightGBM/XGBoost).
"""
from __future__ import annotations

from typing import Optional, Union

import numpy as np


def compute_scale_pos_weight(y: Union[list, np.ndarray]) -> float:
    """
    Compute scale_pos_weight = (count_neg / count_pos) for binary y.

    Use in LightGBM: scale_pos_weight=compute_scale_pos_weight(y_train).
    If no positives, return 1.0 (no weighting).
    """
    y = np.asarray(y, dtype=float)
    if y.size == 0:
        return 1.0
    pos = (y >= 0.5).sum()
    neg = (y < 0.5).sum()
    if pos == 0:
        return 1.0
    return float(neg) / float(pos)


def get_class_weight_dict(y: Union[list, np.ndarray]) -> Optional[dict]:
    """
    Return {0: weight_0, 1: weight_1} for sklearn-style class_weight.
    Balanced: n_samples / (2 * n_class_i). Return None if not applicable.
    """
    y = np.asarray(y, dtype=float)
    if y.size == 0:
        return None
    n = len(y)
    n1 = (y >= 0.5).sum()
    n0 = n - n1
    if n0 == 0 or n1 == 0:
        return None
    w0 = n / (2.0 * n0)
    w1 = n / (2.0 * n1)
    return {0: w0, 1: w1}
