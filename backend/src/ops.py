"""Operational metrics and case statistics for the AML dashboard."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from . import config


def compute_ops_metrics(df: pd.DataFrame, analyst_capacity: int, minutes_per_case: int) -> Dict[str, Any]:
    """Compute ops metrics used in the dashboard."""

    # Use eligible alerts only for precision@k
    if "alert_eligible" in df.columns:
        eligible_df = df[df["alert_eligible"] == True].copy()
    else:
        eligible_df = df.copy()
    
    ranked = eligible_df.sort_values("risk_score", ascending=False)
    topk = ranked.head(analyst_capacity)

    from .evaluation_service import detect_outcome_source, build_binary_labels, OutcomeLabelSource
    source, col, warning = detect_outcome_source(eligible_df)
    if source != OutcomeLabelSource.NONE:
        labels, mask = build_binary_labels(topk, source, col)
        tp = int(labels[mask].sum())
        using_synthetic = (source == OutcomeLabelSource.SYNTHETIC)
    else:
        tp = 0
        using_synthetic = False
    precision_k = tp / analyst_capacity if analyst_capacity else 0
    alerts_per_case = analyst_capacity / tp if tp else None
    total_minutes = analyst_capacity * minutes_per_case
    
    # Suppression metrics
    if "alert_eligible" in df.columns:
        total_count = len(df)
        eligible_count = df["alert_eligible"].sum()
        suppressed_count = total_count - eligible_count
        suppression_rate = suppressed_count / total_count if total_count > 0 else 0.0
    else:
        total_count = len(df)
        eligible_count = len(df)
        suppressed_count = 0
        suppression_rate = 0.0

    return {
        "precision_k": precision_k,
        "alerts_per_case": alerts_per_case,
        "total_minutes": total_minutes,
        "tp": tp,
        "suppression_rate": suppression_rate,
        "eligible_alerts_count": int(eligible_count),
        "suppressed_alerts_count": int(suppressed_count),
        "label_source": source.value if source != OutcomeLabelSource.NONE else "none",
        "using_synthetic_labels": using_synthetic,
        "synthetic_label_warning": warning if using_synthetic else "",
    }


def compute_case_status_counts(cases: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Aggregate case counts by status."""

    status_counts: Dict[str, int] = {}
    for _, case in cases.items():
        status_counts[case["status"]] = status_counts.get(case["status"], 0) + 1
    return status_counts


def get_calibration_metadata(data_signature: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return calibration metrics from model cache for ops panel (no retraining)."""
    from .scoring import get_calibration_metadata_from_cache
    cache_dir = Path(getattr(config, "MODEL_CACHE_DIR", "data/model_cache"))
    return get_calibration_metadata_from_cache(cache_dir=cache_dir, data_signature=data_signature)
