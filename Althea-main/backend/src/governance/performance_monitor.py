"""Performance Trends Monitoring for AML Risk System."""
from __future__ import annotations

import numpy as np
from typing import Optional

import pandas as pd

from .. import config


def performance_trends(df: pd.DataFrame, k: int = 50, time_col: str = "timestamp") -> pd.DataFrame:
    """
    Monitor performance trends over time.

    Computes daily:
    - Precision@K: TP / K in top-K alerts (using real analyst labels when available)
    - TP rate: True positives / total alerts
    - Suppression rate: Suppressed alerts / total alerts

    Args:
        df: DataFrame with risk_score and optional timestamp / label columns
        k: Top-K alerts to consider for Precision@K (default: 50)
        time_col: Column name for timestamp (default: "timestamp")

    Returns:
        DataFrame with daily performance metrics:
        - date, precision_at_k, tp_rate, suppression_rate, label_source
    """
    if len(df) == 0:
        return pd.DataFrame()

    from ..evaluation_service import detect_outcome_source, build_binary_labels, OutcomeLabelSource

    df_work = df.copy()

    # Generate timestamp if not present
    if time_col not in df_work.columns or df_work[time_col].isna().all():
        now = pd.Timestamp.utcnow()
        rng = np.random.default_rng(42)
        days_ago = rng.integers(0, 30, size=len(df_work))
        df_work[time_col] = now - pd.to_timedelta(days_ago, unit="d")

    df_work[time_col] = pd.to_datetime(df_work[time_col], errors="coerce")
    df_work = df_work[df_work[time_col].notna()].copy()

    if len(df_work) == 0:
        return pd.DataFrame()

    df_work["_date"] = df_work[time_col].dt.date

    if "risk_score" not in df_work.columns:
        df_work["risk_score"] = 0.0

    # Detect outcome labels
    source, col, _ = detect_outcome_source(df_work)
    if source != OutcomeLabelSource.NONE:
        labels, mask = build_binary_labels(df_work, source, col)
        df_work["_is_tp"]   = labels
        df_work["_lbl_mask"] = mask
    else:
        df_work["_is_tp"]    = 0
        df_work["_lbl_mask"] = False

    # Detect suppression flag
    suppression_flag = pd.Series(False, index=df_work.index)
    for scol, cond in [
        ("governance_status", lambda c: df_work[c] == "suppressed"),
        ("alert_eligible",    lambda c: df_work[c] == False),
        ("in_queue",          lambda c: df_work[c] == False),
        ("suppressed",        lambda c: df_work[c] == True),
    ]:
        if scol in df_work.columns:
            suppression_flag = cond(scol)
            break

    daily_stats = []

    for date, group in df_work.groupby("_date", sort=True):
        n_total = len(group)
        if n_total == 0:
            continue

        group_sorted = group.sort_values("risk_score", ascending=False)
        topk_size    = min(k, n_total)
        topk         = group_sorted.head(topk_size)

        # Precision@K
        if source != OutcomeLabelSource.NONE:
            topk_resolved = topk[topk["_lbl_mask"]]
            topk_tp       = int(topk_resolved["_is_tp"].sum())
            denom         = len(topk_resolved) if len(topk_resolved) > 0 else topk_size
        else:
            topk_tp = 0
            denom   = topk_size
        precision_at_k_val = topk_tp / denom if denom > 0 else 0.0

        # TP rate for the day
        resolved_group = group[group["_lbl_mask"]] if source != OutcomeLabelSource.NONE else group.iloc[0:0]
        tp_rate = (
            float(resolved_group["_is_tp"].sum()) / len(resolved_group)
            if len(resolved_group) > 0 else 0.0
        )

        # Suppression rate
        group_suppressed = suppression_flag.loc[group.index]
        suppression_rate = float(group_suppressed.sum()) / n_total if n_total > 0 else 0.0

        daily_stats.append({
            "date":           date,
            "precision_at_k": precision_at_k_val,
            "tp_rate":        tp_rate,
            "suppression_rate": suppression_rate,
            "label_source":   source.value,
        })

    return pd.DataFrame(daily_stats).sort_values("date").reset_index(drop=True)
