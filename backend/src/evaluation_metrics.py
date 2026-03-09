"""Standalone AML evaluation metrics library.

No ALTHEA-specific dependencies — importable independently for unit testing.
All functions accept numpy arrays and return plain Python floats or DataFrames.
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd


def precision_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    """
    Precision@K: fraction of true positives in the top-K scored alerts.

    AML meaning: of the K alerts analysts review today, how many are genuine
    suspicious cases? Industry baseline without ranking is ~5-8%. ALTHEA target: >25%.
    """
    if k <= 0 or len(y_true) == 0:
        return 0.0
    k = min(k, len(y_true))
    order = np.argsort(y_score)[::-1]
    topk = y_true[order[:k]]
    return float(topk.sum()) / k


def recall_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    """
    Recall@K: fraction of all true positives captured in the top-K alerts.

    AML meaning (SAR Capture Rate): of all alerts warranting a SAR, what
    percentage appear in the priority queue that analysts review?
    Regulators expect this to be high — missing SARs is a compliance violation.
    """
    if k <= 0 or len(y_true) == 0:
        return 0.0
    total_tp = float(y_true.sum())
    if total_tp == 0:
        return 0.0
    k = min(k, len(y_true))
    order = np.argsort(y_score)[::-1]
    topk = y_true[order[:k]]
    return float(topk.sum()) / total_tp


def average_precision(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """
    Average Precision (AP): area under the precision-recall curve.

    AML meaning: more informative than AUROC for the 90-95% FP rate typical
    in AML. AUROC is dominated by true-negative rate; AP focuses on the rare
    positive class. Higher AP = model maintains precision across all recall levels.
    """
    if len(y_true) == 0 or y_true.sum() == 0:
        return 0.0
    try:
        from sklearn.metrics import average_precision_score
        return float(average_precision_score(y_true, y_score))
    except Exception:
        return 0.0


def auroc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """
    Area under ROC curve.

    AML meaning: probability that the model ranks a random true positive higher
    than a random false positive. Returns 0.5 (random) if only one class present.
    Note: for heavily imbalanced AML datasets, prefer average_precision over AUROC.
    """
    if len(y_true) == 0:
        return 0.5
    if len(np.unique(y_true)) < 2:
        return 0.5  # Only one class — not meaningful
    try:
        from sklearn.metrics import roc_auc_score
        return float(roc_auc_score(y_true, y_score))
    except Exception:
        return 0.5


def lift_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    """
    Lift@K = Precision@K / base_rate.

    AML meaning: how much better than random selection is the model?
    Lift of 3.5 means the model finds 3.5x more true positives per alert
    reviewed compared to random selection. Primary "sales metric" for AML triage.
    """
    if len(y_true) == 0:
        return 0.0
    base_rate = float(y_true.sum()) / len(y_true)
    if base_rate == 0.0:
        return 0.0
    p_at_k = precision_at_k(y_true, y_score, k)
    return p_at_k / base_rate


def ndcg_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    """
    Normalized Discounted Cumulative Gain@K.

    AML meaning: rewards models that rank the most severe true positives highest
    within the top-K. Critical for AML because the highest-risk cases (sanctions
    evasion, terrorism financing) must appear first, before structuring or smurfing.
    NDCG = 1.0 means all true positives are at the top in perfect severity order.
    """
    if k <= 0 or len(y_true) == 0:
        return 0.0
    k = min(k, len(y_true))
    order = np.argsort(y_score)[::-1]
    topk_labels = y_true[order[:k]]

    gains = topk_labels / np.log2(np.arange(2, k + 2))
    dcg = float(gains.sum())

    ideal_topk = np.sort(y_true)[::-1][:k]
    ideal_gains = ideal_topk / np.log2(np.arange(2, k + 2))
    idcg = float(ideal_gains.sum())

    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def false_positive_reduction(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    """
    FP reduction = 1 - (FP_rate_in_topk / FP_rate_overall).

    AML meaning: primary operational metric for triage systems.
    How much does the model reduce the false positive burden on analysts?
    A reduction of 0.60 means analysts encounter 60% fewer FPs than without ranking.
    """
    if len(y_true) == 0 or k <= 0:
        return 0.0
    k = min(k, len(y_true))

    total_fp = float((y_true == 0).sum())
    fpr_overall = total_fp / len(y_true) if len(y_true) > 0 else 0.0
    if fpr_overall == 0.0:
        return 0.0

    order = np.argsort(y_score)[::-1]
    topk = y_true[order[:k]]
    fp_in_topk = float((topk == 0).sum())
    fpr_topk = fp_in_topk / k if k > 0 else 0.0

    return 1.0 - (fpr_topk / fpr_overall)


def precision_recall_table(
    y_true: np.ndarray,
    y_score: np.ndarray,
    k_values: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    Returns a DataFrame of precision and recall at multiple K values.

    AML meaning: used to find the optimal operating point — the K at which
    the tradeoff between how many alerts are reviewed (workload) and how many
    true cases are caught (effectiveness) is most favorable for the compliance team.

    Columns: k, precision, recall, f1, threshold.
    """
    n = len(y_true)
    if n == 0:
        return pd.DataFrame(columns=["k", "precision", "recall", "f1", "threshold"])

    if k_values is None:
        k_values = [kv for kv in [10, 25, 50, 100, 200, 500, n] if kv <= n]
        if not k_values:
            k_values = [n]

    order = np.argsort(y_score)[::-1]
    sorted_labels = y_true[order]
    sorted_scores = y_score[order]
    total_tp = float(y_true.sum())

    rows = []
    for k in sorted(set(k_values)):
        k = min(k, n)
        topk = sorted_labels[:k]
        tp_count = float(topk.sum())
        p = tp_count / k if k > 0 else 0.0
        r = tp_count / total_tp if total_tp > 0 else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
        threshold = float(sorted_scores[k - 1]) if k <= len(sorted_scores) else 0.0
        rows.append({"k": k, "precision": p, "recall": r, "f1": f1, "threshold": threshold})

    return pd.DataFrame(rows)
