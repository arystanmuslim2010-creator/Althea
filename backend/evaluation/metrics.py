from __future__ import annotations

import math
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        cast = float(value)
    except Exception:
        return default
    return cast if math.isfinite(cast) else default


def coerce_binary_label(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        if float(value) == 1.0:
            return 1
        if float(value) == 0.0:
            return 0
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "sar", "str", "positive", "suspicious", "escalated"}:
        return 1
    if raw in {"0", "false", "no", "n", "negative", "benign"}:
        return 0
    return None


def validate_binary_labels(records: list[dict[str, Any]], label_field: str) -> dict[str, Any]:
    labels = [coerce_binary_label(row.get(label_field)) for row in records]
    present = [label for label in labels if label is not None]
    positives = sum(1 for label in present if label == 1)
    negatives = sum(1 for label in present if label == 0)
    if not present or positives == 0 or negatives == 0:
        return {
            "is_valid": False,
            "warning": "Evaluation requires both positive and negative labeled alerts.",
            "labeled_alerts": len(present),
            "positive_alerts": positives,
            "negative_alerts": negatives,
        }
    return {
        "is_valid": True,
        "warning": None,
        "labeled_alerts": len(present),
        "positive_alerts": positives,
        "negative_alerts": negatives,
    }


def _top_n(total: int, percent: float) -> int:
    if total <= 0:
        return 0
    clipped = max(0.0, min(float(percent), 100.0))
    return max(1, int(math.ceil(total * clipped / 100.0)))


def _precision_at_k(labels: list[int], top_n: int) -> float | None:
    if top_n <= 0 or not labels:
        return None
    ranked = labels[:top_n]
    return sum(ranked) / float(len(ranked))


def _recall_at_k(labels: list[int], top_n: int, positive_total: int) -> float | None:
    if positive_total <= 0 or top_n <= 0 or not labels:
        return None
    return sum(labels[:top_n]) / float(positive_total)


def _review_fraction_for_target_recall(labels: list[int], target_recall: float) -> float | None:
    positive_total = sum(labels)
    if positive_total <= 0:
        return None
    target_hits = positive_total * max(0.0, min(float(target_recall), 1.0))
    hits = 0
    for index, label in enumerate(labels, start=1):
        hits += label
        if hits >= target_hits:
            return index / float(len(labels))
    return 1.0


def _pr_auc(labels: list[int], scores: list[float]) -> float | None:
    positive_total = sum(labels)
    negative_total = len(labels) - positive_total
    if positive_total <= 0 or negative_total <= 0 or not labels:
        return None

    paired = sorted(zip(scores, labels), key=lambda item: item[0], reverse=True)
    tp = 0
    fp = 0
    prev_recall = 0.0
    area = 0.0
    for _, label in paired:
        if label:
            tp += 1
        else:
            fp += 1
        recall = tp / float(positive_total)
        precision = tp / float(tp + fp) if (tp + fp) else 0.0
        area += (recall - prev_recall) * precision
        prev_recall = recall
    return round(area, 4)


def compute_ranking_metrics(
    ranked_records: list[dict[str, Any]],
    *,
    label_field: str,
    score_field: str = "ranking_score",
    target_recall: float = 0.8,
) -> dict[str, Any]:
    validation = validate_binary_labels(ranked_records, label_field)
    total_alerts = len(ranked_records)
    positives = int(validation.get("positive_alerts") or 0)
    if not validation["is_valid"]:
        return {
            "is_valid": False,
            "warning": validation["warning"],
            "total_alerts": total_alerts,
            "positive_alerts": positives,
            "recall_at_top_10_pct": None,
            "recall_at_top_20_pct": None,
            "recall_at_top_30_pct": None,
            "precision_at_top_10_pct": None,
            "precision_at_top_20_pct": None,
            "sar_capture_at_top_10_pct": None,
            "sar_capture_at_top_20_pct": None,
            "sar_capture_at_top_30_pct": None,
            "workload_reduction_at_target_recall": None,
            "review_fraction_at_target_recall": None,
            "pr_auc": None,
        }

    labels = [int(coerce_binary_label(row.get(label_field)) or 0) for row in ranked_records]
    scores = [_safe_float(row.get(score_field), default=0.0) for row in ranked_records]
    top_10 = _top_n(total_alerts, 10.0)
    top_20 = _top_n(total_alerts, 20.0)
    top_30 = _top_n(total_alerts, 30.0)
    review_fraction = _review_fraction_for_target_recall(labels, target_recall)
    workload_reduction = None if review_fraction is None else round(1.0 - review_fraction, 4)

    recall_10 = _recall_at_k(labels, top_10, positives)
    recall_20 = _recall_at_k(labels, top_20, positives)
    recall_30 = _recall_at_k(labels, top_30, positives)
    precision_10 = _precision_at_k(labels, top_10)
    precision_20 = _precision_at_k(labels, top_20)

    return {
        "is_valid": True,
        "warning": None,
        "total_alerts": total_alerts,
        "positive_alerts": positives,
        "recall_at_top_10_pct": round(recall_10, 4) if recall_10 is not None else None,
        "recall_at_top_20_pct": round(recall_20, 4) if recall_20 is not None else None,
        "recall_at_top_30_pct": round(recall_30, 4) if recall_30 is not None else None,
        "precision_at_top_10_pct": round(precision_10, 4) if precision_10 is not None else None,
        "precision_at_top_20_pct": round(precision_20, 4) if precision_20 is not None else None,
        "sar_capture_at_top_10_pct": round(recall_10, 4) if recall_10 is not None else None,
        "sar_capture_at_top_20_pct": round(recall_20, 4) if recall_20 is not None else None,
        "sar_capture_at_top_30_pct": round(recall_30, 4) if recall_30 is not None else None,
        "review_fraction_at_target_recall": round(review_fraction, 4) if review_fraction is not None else None,
        "workload_reduction_at_target_recall": workload_reduction,
        "pr_auc": _pr_auc(labels, scores),
    }
