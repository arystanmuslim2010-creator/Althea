"""
Product-aligned evaluation metrics for AML alert governance (not accuracy).

- PR-AUC (primary)
- TP retention at suppression rate
- Review reduction at TP retention target
- Precision uplift in top quantile
- ROC-AUC optional
"""
from __future__ import annotations

from typing import Optional, Union

import numpy as np


def pr_auc(y_true: Union[list, np.ndarray], y_score: Union[list, np.ndarray]) -> float:
    """Precision-Recall AUC. Primary metric for imbalanced binary classification."""
    from sklearn.metrics import average_precision_score
    y_true = np.asarray(y_true, dtype=float)
    y_score = np.asarray(y_score, dtype=float)
    if y_true.size == 0 or np.unique(y_true).size < 2:
        return 0.0
    return float(average_precision_score(y_true, y_score, zero_division=0))


def tp_retention_at_suppression(
    y_true: Union[list, np.ndarray],
    y_score: Union[list, np.ndarray],
    suppression_rate: float,
) -> float:
    """
    If we suppress the bottom suppression_rate (e.g. 0.2 = 20%) of alerts by predicted score,
    what fraction of true positives remain in the kept set?

    suppression_rate in [0, 1]; 0.2 = drop bottom 20% by score.
    Returns: (TP in kept set) / (total TP), or 0 if no TPs.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_score = np.asarray(y_score, dtype=float)
    if y_true.size == 0:
        return 0.0
    total_tp = (y_true >= 0.5).sum()
    if total_tp == 0:
        return 0.0
    n = len(y_true)
    k = max(0, int(np.round(n * (1.0 - suppression_rate))))
    if k == 0:
        return 0.0
    # Keep top k by score
    order = np.argsort(-y_score)
    kept = order[:k]
    tp_kept = (y_true[kept] >= 0.5).sum()
    return float(tp_kept) / float(total_tp)


def suppression_at_tp_retention(
    y_true: Union[list, np.ndarray],
    y_score: Union[list, np.ndarray],
    retention_target: float = 0.98,
) -> float:
    """
    What fraction of alerts can we suppress while keeping >= retention_target of TPs?

    Returns suppression rate in [0, 1] (e.g. 0.3 = can suppress 30% of alerts).
    """
    y_true = np.asarray(y_true, dtype=float)
    y_score = np.asarray(y_score, dtype=float)
    if y_true.size == 0:
        return 0.0
    total_tp = (y_true >= 0.5).sum()
    if total_tp == 0:
        return 1.0
    n = len(y_true)
    # Binary search over suppression rate
    low, high = 0.0, 1.0
    for _ in range(50):
        mid = (low + high) / 2
        ret = tp_retention_at_suppression(y_true, y_score, mid)
        if ret >= retention_target:
            low = mid
        else:
            high = mid
    return (low + high) / 2


def precision_at_k_percent(
    y_true: Union[list, np.ndarray],
    y_score: Union[list, np.ndarray],
    k: float = 0.1,
) -> float:
    """
    Precision in top k (as fraction, e.g. 0.1 = top 10%) of alerts by score.

    Precision uplift: compare this to baseline (e.g. overall positive rate).
    """
    y_true = np.asarray(y_true, dtype=float)
    y_score = np.asarray(y_score, dtype=float)
    if y_true.size == 0:
        return 0.0
    n = len(y_true)
    top_k = max(1, int(np.round(n * k)))
    order = np.argsort(-y_score)
    top_idx = order[:top_k]
    tp = (y_true[top_idx] >= 0.5).sum()
    return float(tp) / float(top_k)


def roc_auc_optional(y_true: Union[list, np.ndarray], y_score: Union[list, np.ndarray]) -> Optional[float]:
    """ROC-AUC when both classes present; None otherwise."""
    from sklearn.metrics import roc_auc_score
    y_true = np.asarray(y_true, dtype=float)
    y_score = np.asarray(y_score, dtype=float)
    if y_true.size == 0 or np.unique(y_true).size < 2:
        return None
    return float(roc_auc_score(y_true, y_score))
