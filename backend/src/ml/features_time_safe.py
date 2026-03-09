"""
Time-safe feature generation for AML alerts: all features use only "as-of time" (no future leakage).

Features:
- rule_fatigue: alerts per TP by rule_id historically (as-of time)
- entity_alert_velocity: rolling alert count windows per entity (as-of time)
- recency_weighted_outcomes: exponentially decayed history for entity/rule (as-of time)
- peer_deviation: entity vs cohort baseline (as-of time)

feature_version is set for artifact metadata.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

FEATURE_VERSION = "v1_time_safe"


def _ensure_time_column(
    df: pd.DataFrame,
    time_col: str,
    candidates: Optional[List[str]] = None,
) -> str:
    if time_col and time_col in df.columns:
        return time_col
    for c in candidates or ["alert_created_at", "alert_date", "created_at", "timestamp"]:
        if c in df.columns:
            return c
    raise ValueError(
        "Time column required for time-safe features. Provide one of: "
        "alert_created_at, alert_date, created_at, timestamp."
    )


def add_rule_fatigue(
    df: pd.DataFrame,
    time_col: str,
    rule_col: str = "rule_id",
    outcome_col: str = "y_sar",
    entity_col: Optional[str] = "entity_id",
) -> pd.DataFrame:
    """
    rule_fatigue: for each row (as-of its timestamp), alerts_per_tp by rule_id = count of alerts / max(1, TPs) for that rule in the past.
    No future data: only rows with timestamp < current row's timestamp are used.
    """
    df = df.copy()
    ts = pd.to_datetime(df[time_col], errors="coerce")
    df["_ts"] = ts
    df = df.sort_values("_ts", kind="mergesort").reset_index(drop=True)
    rule_vals = df[rule_col].astype(str) if rule_col in df.columns else pd.Series(["__default__"] * len(df), index=df.index)
    y = df[outcome_col] if outcome_col in df.columns else pd.Series(0, index=df.index)

    # Rolling counts per rule (strictly before current row in time)
    rule_alert_count: Dict[str, List[float]] = {}
    rule_tp_count: Dict[str, List[float]] = {}
    out_fatigue = np.full(len(df), np.nan, dtype=float)

    for i in range(len(df)):
        r = rule_vals.iloc[i]
        t = df["_ts"].iloc[i]
        # Rows strictly before t (same rule)
        past_mask = (df["_ts"] < t) & (rule_vals == r)
        alerts_past = past_mask.sum()
        tps_past = (past_mask & (y >= 0.5)).sum()
        denom = max(1.0, float(tps_past))
        out_fatigue[i] = alerts_past / denom

    df["rule_fatigue"] = out_fatigue
    df["rule_fatigue"] = df["rule_fatigue"].fillna(0.0)
    df = df.drop(columns=["_ts"])
    return df


def add_entity_alert_velocity(
    df: pd.DataFrame,
    time_col: str,
    entity_col: str = "entity_id",
    windows_days: Optional[List[int]] = None,
) -> pd.DataFrame:
    """
    entity_alert_velocity: rolling count of alerts per entity in 7d/30d windows (as-of time).
    """
    df = df.copy()
    ts = pd.to_datetime(df[time_col], errors="coerce")
    entity = df[entity_col].astype(str) if entity_col in df.columns else pd.Series(["__default__"] * len(df), index=df.index)
    df["_ts"] = ts
    sort_cols = [entity_col, "_ts"] if entity_col in df.columns else ["_ts"]
    df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    windows_days = windows_days or [7, 30]
    for w in windows_days:
        col = f"entity_alert_velocity_{w}d"
        out = np.zeros(len(df), dtype=float)
        for idx in range(len(df)):
            t = df["_ts"].iloc[idx]
            e = entity.iloc[idx]
            start = t - pd.Timedelta(days=w)
            # Same entity, timestamp in (start, t]
            mask = (entity == e) & (df["_ts"] > start) & (df["_ts"] <= t)
            out[idx] = mask.sum()
        df[col] = out
    df = df.drop(columns=["_ts"])
    return df


def add_recency_weighted_outcomes(
    df: pd.DataFrame,
    time_col: str,
    outcome_col: str = "y_sar",
    entity_col: Optional[str] = "entity_id",
    rule_col: Optional[str] = "rule_id",
    decay_days: float = 90.0,
) -> pd.DataFrame:
    """
    Exponentially decayed sum of outcomes in the past (as-of time). weight = exp(-(t - t_i) / decay_days).
    """
    df = df.copy()
    ts = pd.to_datetime(df[time_col], errors="coerce")
    y = df[outcome_col] if outcome_col in df.columns else pd.Series(0, index=df.index)
    df["_ts"] = ts
    df = df.sort_values("_ts", kind="mergesort").reset_index(drop=True)
    out_entity = np.zeros(len(df), dtype=float)
    out_global = np.zeros(len(df), dtype=float)
    entity = df[entity_col].astype(str) if entity_col and entity_col in df.columns else None

    for i in range(len(df)):
        t = df["_ts"].iloc[i]
        yi = y.iloc[i]
        # Past only
        past = df["_ts"] < t
        dt = (t - df.loc[past, "_ts"]).dt.total_seconds() / 86400.0
        w = np.exp(-dt / decay_days)
        out_global[i] = (w * y.loc[past]).sum()
        if entity is not None:
            e = entity.iloc[i]
            mask = past & (entity == e)
            dt_e = (t - df.loc[mask, "_ts"]).dt.total_seconds() / 86400.0
            w_e = np.exp(-dt_e / decay_days)
            out_entity[i] = (w_e * y.loc[mask]).sum()
        else:
            out_entity[i] = out_global[i]

    df["recency_weighted_outcome_global"] = out_global
    df["recency_weighted_outcome_entity"] = out_entity
    df = df.drop(columns=["_ts"])
    return df


def add_peer_deviation(
    df: pd.DataFrame,
    time_col: str,
    value_col: str,
    entity_col: str = "entity_id",
    cohort_col: Optional[str] = "segment",
) -> pd.DataFrame:
    """
    peer_deviation: entity's value_col vs cohort (e.g. segment) baseline computed only from past data (as-of time).
    """
    df = df.copy()
    ts = pd.to_datetime(df[time_col], errors="coerce")
    vals = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)
    cohort = df[cohort_col].astype(str) if cohort_col and cohort_col in df.columns else pd.Series(["__all__"] * len(df), index=df.index)
    df["_ts"] = ts
    df = df.sort_values("_ts", kind="mergesort").reset_index(drop=True)
    out = np.zeros(len(df), dtype=float)

    for i in range(len(df)):
        t = df["_ts"].iloc[i]
        c = cohort.iloc[i]
        past = (df["_ts"] < t) & (cohort == c)
        if past.sum() == 0:
            out[i] = 0.0
            continue
        baseline_mean = vals.loc[past].mean()
        baseline_std = vals.loc[past].std()
        if baseline_std == 0 or not np.isfinite(baseline_std):
            out[i] = 0.0
        else:
            out[i] = (vals.iloc[i] - baseline_mean) / baseline_std
    df["peer_deviation_" + value_col] = out
    df = df.drop(columns=["_ts"])
    return df


def build_time_safe_features(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    time_column_candidates: Optional[List[str]] = None,
    rule_col: str = "rule_id",
    entity_col: str = "entity_id",
    outcome_col: str = "y_sar",
    cohort_col: Optional[str] = "segment",
    value_col_for_peer: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[str], str]:
    """
    Add all time-safe features. Returns (df, list of feature names, feature_version).

    Requires: time column, and optionally rule_id, entity_id, y_sar for full set.
    """
    col = _ensure_time_column(df, time_col or "", time_column_candidates)
    feature_list: List[str] = []

    if rule_col in df.columns and outcome_col in df.columns:
        df = add_rule_fatigue(df, col, rule_col=rule_col, outcome_col=outcome_col, entity_col=entity_col if entity_col in df.columns else None)
        feature_list.append("rule_fatigue")

    if entity_col in df.columns:
        df = add_entity_alert_velocity(df, col, entity_col=entity_col)
        feature_list.extend([c for c in df.columns if c.startswith("entity_alert_velocity_")])

    if outcome_col in df.columns:
        df = add_recency_weighted_outcomes(df, col, outcome_col=outcome_col, entity_col=entity_col if entity_col in df.columns else None, rule_col=rule_col if rule_col in df.columns else None)
        feature_list.append("recency_weighted_outcome_global")
        feature_list.append("recency_weighted_outcome_entity")

    if value_col_for_peer and value_col_for_peer in df.columns and cohort_col and cohort_col in df.columns:
        df = add_peer_deviation(df, col, value_col_for_peer, entity_col=entity_col, cohort_col=cohort_col)
        feature_list.append("peer_deviation_" + value_col_for_peer)

    return df, feature_list, FEATURE_VERSION
