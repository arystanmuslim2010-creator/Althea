from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Any


def _rows(records: list[dict] | tuple[dict, ...]) -> list[dict[str, Any]]:
    return [dict(item or {}) for item in (records or [])]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "sar", "str", "escalated", "positive", "suspicious"}


def _top_k(records: list[dict], score_field: str, k_percent: float) -> list[dict]:
    rows = _rows(records)
    if not rows:
        return []
    k = max(1, int(round(len(rows) * max(0.0, min(float(k_percent), 100.0)) / 100.0)))
    return sorted(rows, key=lambda row: float(row.get(score_field) or 0.0), reverse=True)[:k]


def recall_at_top_k_percent(records, score_field, label_field, k_percent):
    rows = _rows(records)
    positives = [row for row in rows if _truthy(row.get(label_field))]
    if not positives:
        return {"benchmark_recall_at_top_percent": 0.0, "positive_count": 0, "top_count": 0}
    top = _top_k(rows, score_field, k_percent)
    captured = sum(1 for row in top if _truthy(row.get(label_field)))
    return {
        "benchmark_recall_at_top_percent": captured / len(positives),
        "positive_count": len(positives),
        "top_count": len(top),
        "captured_positive_count": captured,
    }


def precision_at_top_k_percent(records, score_field, label_field, k_percent):
    top = _top_k(_rows(records), score_field, k_percent)
    if not top:
        return {"precision_at_top_percent": 0.0, "top_count": 0}
    positives = sum(1 for row in top if _truthy(row.get(label_field)))
    return {"precision_at_top_percent": positives / len(top), "top_count": len(top), "captured_positive_count": positives}


def workload_reduction_at_threshold(records, score_field, threshold):
    rows = _rows(records)
    if not rows:
        return {"modeled_workload_reduction": 0.0, "review_count": 0, "total_count": 0}
    review_count = sum(1 for row in rows if float(row.get(score_field) or 0.0) >= float(threshold))
    return {
        "modeled_workload_reduction": 1.0 - (review_count / len(rows)),
        "review_count": review_count,
        "total_count": len(rows),
    }


def escalation_capture_rate(records, score_field, label_field, threshold):
    rows = _rows(records)
    positives = [row for row in rows if _truthy(row.get(label_field))]
    if not positives:
        return {"observed_capture_rate": 0.0, "positive_count": 0}
    captured = sum(1 for row in positives if float(row.get(score_field) or 0.0) >= float(threshold))
    return {"observed_capture_rate": captured / len(positives), "positive_count": len(positives), "captured_positive_count": captured}


def false_positive_reduction_estimate(records, score_field, label_field, threshold):
    rows = _rows(records)
    false_positives = [row for row in rows if not _truthy(row.get(label_field))]
    if not false_positives:
        return {"modeled_false_positive_reduction": 0.0, "false_positive_count": 0}
    suppressed = sum(1 for row in false_positives if float(row.get(score_field) or 0.0) < float(threshold))
    return {
        "modeled_false_positive_reduction": suppressed / len(false_positives),
        "false_positive_count": len(false_positives),
        "suppressed_false_positive_count": suppressed,
    }


def analyst_override_rate(records, override_field="analyst_override"):
    rows = _rows(records)
    if not rows:
        return {"analyst_override_rate": 0.0, "total_count": 0}
    overrides = sum(1 for row in rows if _truthy(row.get(override_field)))
    return {"analyst_override_rate": overrides / len(rows), "override_count": overrides, "total_count": len(rows)}


def median_time_to_escalation(records, start_field="investigation_start_time", end_field="escalation_time"):
    durations: list[float] = []
    for row in _rows(records):
        try:
            start = row.get(start_field)
            end = row.get(end_field)
            if isinstance(start, str):
                start = datetime.fromisoformat(start.replace("Z", "+00:00"))
            if isinstance(end, str):
                end = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if isinstance(start, datetime) and isinstance(end, datetime):
                durations.append(max(0.0, (end - start).total_seconds() / 3600.0))
        except Exception:
            continue
    return {"median_time_to_escalation_hours": median(durations) if durations else None, "sample_count": len(durations)}


def explanation_usefulness_summary(records, score_field="explanation_usefulness_score"):
    values = []
    for row in _rows(records):
        value = row.get(score_field)
        if value is not None:
            try:
                values.append(float(value))
            except Exception:
                pass
    if not values:
        return {"explanation_usefulness_average": None, "sample_count": 0}
    return {"explanation_usefulness_average": sum(values) / len(values), "sample_count": len(values)}
