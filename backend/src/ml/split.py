"""
Time-based train/validation/test split for AML alerts (no random split).

- train: all data before validation month range
- validation: next month
- test: last month

If timestamp is missing, fail with a clear error.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

import pandas as pd


def _find_time_column(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    candidates: Optional[List[str]] = None,
) -> str:
    if time_col and time_col in df.columns:
        return time_col
    for c in (candidates or ["alert_created_at", "alert_date", "created_at", "timestamp", "event_time"]):
        if c in df.columns:
            return c
    raise ValueError(
        "Time column for split not found. Required one of: "
        "alert_created_at, alert_date, created_at, timestamp, event_time. "
        "Set time_column in config/ml.yaml or pass time_col=..."
    )


def time_split(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    val_window: int = 1,
    test_window: int = 1,
    time_column_candidates: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame by time: train (before val window), validation (next month), test (last month).

    Args:
        df: DataFrame with a timestamp column.
        time_col: Explicit timestamp column name. If None, use first of candidates.
        val_window: Number of months for validation window.
        test_window: Number of months for test window.
        time_column_candidates: List of column names to try if time_col not set.

    Returns:
        (train_df, val_df, test_df) with non-overlapping rows; train < val < test by time.

    Raises:
        ValueError: If timestamp column is missing or has all NaT.
    """
    col = _find_time_column(df, time_col, time_column_candidates)
    series = pd.to_datetime(df[col], errors="coerce")
    if series.isna().all():
        raise ValueError(
            f"Timestamp column '{col}' has no valid datetime values. "
            "Provide a column with alert_created_at / alert_date / created_at (or similar)."
        )
    df = df.copy()
    df["_split_ts"] = series
    df = df.dropna(subset=["_split_ts"]).sort_values("_split_ts").reset_index(drop=True)
    if len(df) == 0:
        raise ValueError("No rows left after dropping rows with missing timestamp.")

    # Use month boundaries
    min_ts = df["_split_ts"].min()
    max_ts = df["_split_ts"].max()
    # End of test = max date; start of test = max_ts - test_window months; start of val = test_start - val_window
    test_end = max_ts
    test_start = test_end - pd.DateOffset(months=test_window)
    val_end = test_start
    val_start = val_end - pd.DateOffset(months=val_window)
    train_end = val_start

    train_df = df[df["_split_ts"] < train_end].drop(columns=["_split_ts"])
    val_df = df[(df["_split_ts"] >= val_start) & (df["_split_ts"] < val_end)].drop(columns=["_split_ts"])
    test_df = df[(df["_split_ts"] >= test_start) & (df["_split_ts"] <= test_end)].drop(columns=["_split_ts"])

    return train_df, val_df, test_df


def time_split_indices(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    val_window: int = 1,
    test_window: int = 1,
    time_column_candidates: Optional[List[str]] = None,
) -> Tuple[pd.Index, pd.Index, pd.Index]:
    """
    Return (train_index, val_index, test_index) for use with df.loc.
    Same logic as time_split but returns indices only.
    """
    col = _find_time_column(df, time_col, time_column_candidates)
    series = pd.to_datetime(df[col], errors="coerce")
    if series.isna().all():
        raise ValueError(
            f"Timestamp column '{col}' has no valid datetime values. "
            "Provide a column with alert_created_at / alert_date / created_at (or similar)."
        )
    order = series.dropna().sort_values()
    if len(order) == 0:
        raise ValueError("No rows with valid timestamp.")
    min_ts = order.min()
    max_ts = order.max()
    test_end = max_ts
    test_start = test_end - pd.DateOffset(months=test_window)
    val_end = test_start
    val_start = val_end - pd.DateOffset(months=val_window)
    train_end = val_start

    train_idx = df.index[series < train_end]
    val_idx = df.index[(series >= val_start) & (series < val_end)]
    test_idx = df.index[(series >= test_start) & (series <= test_end)]
    return train_idx, val_idx, test_idx
