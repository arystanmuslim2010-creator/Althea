"""Feature engineering and scoring logic for AML alerts."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.metrics import precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

from . import config
from .risk_engine import compute_risk
from .utils import build_alert_id

logger = logging.getLogger(__name__)


class MissingColumnsError(ValueError):
    """Raised when required CSV columns are missing."""

    def __init__(self, missing: List[str]) -> None:
        super().__init__(f"CSV file is missing required columns: {', '.join(missing)}")
        self.missing = missing


def load_transactions_csv(uploaded_file: object) -> pd.DataFrame:
    """Load and normalize uploaded CSV transactions to expected schema."""
    if getattr(config, "OVERLAY_MODE", False):
        from .domain.schemas import OverlayInputError
        raise OverlayInputError("Overlay requires alert-level input from AML monitoring systems.")

    df = pd.read_csv(uploaded_file)
    required_cols = config.REQUIRED_CSV_COLUMNS

    if all(col in df.columns for col in required_cols):
        # If user_id is missing, assign each row to a unique user
        if "user_id" not in df.columns:
            df["user_id"] = range(len(df))

        # Add missing columns with default values
        if "segment" not in df.columns:
            df["segment"] = config.DEFAULT_SEGMENT
        if "typology" not in df.columns:
            df["typology"] = config.DEFAULT_TYPOLOGY
        if "synthetic_true_suspicious" not in df.columns:
            df["synthetic_true_suspicious"] = config.DEFAULT_SYNTHETIC_SUSPICIOUS
    else:
        missing = [col for col in required_cols if col not in df.columns]
        raise MissingColumnsError(missing)

    return df


def _build_cfg_with_callbacks(
    base_cfg: object,
    status_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> SimpleNamespace:
    cfg_values = {name: getattr(base_cfg, name) for name in dir(base_cfg) if name.isupper()}
    cfg_values["status_cb"] = status_cb
    cfg_values["progress_cb"] = progress_cb
    return SimpleNamespace(**cfg_values)


def _safe_divide(numer: pd.Series, denom: pd.Series, eps: float) -> pd.Series:
    denom_series = denom if isinstance(denom, pd.Series) else pd.Series(denom, index=numer.index)
    denom_series = denom_series.replace(0, np.nan)
    return numer / (denom_series + eps)


def _mad(series: pd.Series) -> float:
    values = series.to_numpy(dtype=float)
    median = np.nanmedian(values)
    if np.isnan(median):
        return np.nan
    return np.nanmedian(np.abs(values - median))


def _robust_sigma_series(mad: pd.Series) -> pd.Series:
    sigma = 1.4826 * mad
    return sigma.mask(~np.isfinite(sigma) | (sigma <= 0), 1.0)


def _robust_sigma_scalar(mad: float) -> float:
    sigma = 1.4826 * mad
    if not np.isfinite(sigma) or sigma <= 0:
        return 1.0
    return float(sigma)


def _safe_median(series: pd.Series) -> float:
    median = np.nanmedian(series.to_numpy(dtype=float))
    if not np.isfinite(median):
        return 0.0
    return float(median)


def _winsorize_by_segment(
    values: pd.Series,
    segment: pd.Series,
    p_low: float,
    p_high: float,
) -> pd.Series:
    quantiles = values.groupby(segment).quantile([p_low, p_high]).unstack()
    low = segment.map(quantiles[p_low])
    high = segment.map(quantiles[p_high])
    return values.clip(lower=low, upper=high)


def _select_baseline(
    use_user: pd.Series,
    user_values: pd.Series,
    seg_values: pd.Series,
) -> pd.Series:
    return pd.Series(np.where(use_user, user_values, seg_values), index=user_values.index)


def _find_timestamp_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def compute_behavioral_features(
    df: pd.DataFrame,
    cfg: object,
) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """Compute compliance-grade behavioral features with robust baselines."""

    status_cb = getattr(cfg, "status_cb", None)
    progress_cb = getattr(cfg, "progress_cb", None)
    
    # PART 1: Ensure timestamp exists for temporal aggregates
    from . import temporal_features
    df = temporal_features.ensure_timestamp(df, time_col="timestamp")

    eps = float(getattr(cfg, "EPS", 1e-9))
    robust_eps = float(getattr(cfg, "BASELINE_ROBUST_EPS", eps))
    min_user_history = int(
        getattr(cfg, "BASELINE_MIN_USER_HIST", getattr(cfg, "MIN_USER_HISTORY", getattr(cfg, "MIN_HISTORY", 12)))
    )
    min_seg_history = int(getattr(cfg, "BASELINE_MIN_SEGMENT_HIST", getattr(cfg, "MIN_SEG_HISTORY", 50)))
    window_days = int(getattr(cfg, "BASELINE_WINDOW_DAYS", 30))
    time_candidates = list(
        getattr(cfg, "TIME_COL_CANDIDATES", ["timestamp", "event_time", "tx_time", "datetime", "created_at"])
    )
    winsor_p = getattr(cfg, "WINSOR_P", (0.005, 0.995))
    roll_window = int(getattr(cfg, "ROLL_WINDOW", 5))

    required_cols = ["user_id", "segment", "amount", "time_gap", "num_transactions"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise MissingColumnsError(missing)

    amount = pd.to_numeric(df["amount"], errors="coerce")
    time_gap = pd.to_numeric(df["time_gap"], errors="coerce")
    activity = pd.to_numeric(df["num_transactions"], errors="coerce")
    user_id = df["user_id"]
    segment = df["segment"]

    df["history_size"] = df.groupby("user_id")["user_id"].transform("size")
    df["is_missing_values"] = amount.isna() | time_gap.isna() | activity.isna() | user_id.isna() | segment.isna()

    timestamp_col = "ts" if "ts" in df.columns else _find_timestamp_column(df, time_candidates)
    tx_time = None
    if timestamp_col:
        tx_time = pd.to_datetime(df[timestamp_col], errors="coerce")
    if tx_time is None or tx_time.isna().all():
        order_series = df.groupby("user_id").cumcount()
        base_time = pd.Timestamp("2020-01-01")
        tx_time = base_time + pd.to_timedelta(order_series, unit="m")
    else:
        order_series = df.groupby("user_id").cumcount()
        base_time = tx_time.min()
        if pd.isna(base_time):
            base_time = pd.Timestamp("2020-01-01")
        synthetic_time = base_time + pd.to_timedelta(order_series, unit="m")
        tx_time = tx_time.fillna(synthetic_time)

    df["ts"] = tx_time
    df["tx_hour"] = tx_time.dt.hour.fillna(0).astype(int)
    df["tx_dow"] = tx_time.dt.dayofweek.fillna(0).astype(int)
    df["is_weekend"] = tx_time.dt.dayofweek.isin([5, 6]).astype(int)
    baseline_window = f"{window_days}d"

    df["amount_log"] = np.log1p(amount.clip(lower=0))
    safe_time_gap = time_gap.clip(lower=eps)
    df["velocity"] = 1.0 / safe_time_gap

    p_low, p_high = winsor_p
    df["amount_w"] = _winsorize_by_segment(amount, segment, p_low, p_high)
    df["amount_log_w"] = _winsorize_by_segment(df["amount_log"], segment, p_low, p_high)
    df["velocity_w"] = _winsorize_by_segment(df["velocity"], segment, p_low, p_high)
    df["activity_w"] = _winsorize_by_segment(activity, segment, p_low, p_high)

    df_work = df.copy()
    # Keep a unique, positional row id so alignment remains stable even if df.index has duplicates.
    df_work["_orig_pos"] = np.arange(len(df_work), dtype=np.int64)
    df_work["amount_num"] = amount.fillna(0.0).astype(float)
    df_work["activity_num"] = activity.fillna(0.0).astype(float)
    time_gap_num = time_gap.copy()
    median_gap = time_gap_num[time_gap_num > 0].median()
    if not np.isfinite(median_gap):
        median_gap = 1.0
    df_work["time_gap_num"] = time_gap_num.fillna(median_gap).clip(lower=eps)
    df_work["velocity"] = 1.0 / df_work["time_gap_num"]
    df["velocity"] = df_work["velocity"]

    df_work = df_work.sort_values("ts", kind="mergesort").reset_index(drop=True)

    def _rolling_group_stats(group_key: str) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
        grouped = df_work.sort_values([group_key, "ts", "_orig_pos"], kind="mergesort")
        roll = grouped.groupby(group_key, sort=False).rolling(
            f"{window_days}D", on="ts", closed="left"
        )
        count = pd.Series(roll["amount_num"].count().to_numpy(), index=grouped.index).sort_index()
        amt_mean = pd.Series(roll["amount_num"].mean().to_numpy(), index=grouped.index).sort_index()
        amt_std = pd.Series(roll["amount_num"].std(ddof=0).to_numpy(), index=grouped.index).sort_index()
        vel_mean = pd.Series(roll["velocity"].mean().to_numpy(), index=grouped.index).sort_index()
        vel_std = pd.Series(roll["velocity"].std(ddof=0).to_numpy(), index=grouped.index).sort_index()
        act_mean = pd.Series(roll["activity_num"].mean().to_numpy(), index=grouped.index).sort_index()
        act_std = pd.Series(roll["activity_num"].std(ddof=0).to_numpy(), index=grouped.index).sort_index()
        return count, amt_mean, amt_std, vel_mean, vel_std, act_mean, act_std

    user_count, user_amt_mean, user_amt_std, user_vel_mean, user_vel_std, user_act_mean, user_act_std = (
        _rolling_group_stats("user_id")
    )
    seg_count, seg_amt_mean, seg_amt_std, seg_vel_mean, seg_vel_std, seg_act_mean, seg_act_std = (
        _rolling_group_stats("segment")
    )
    global_idx = df_work.set_index("ts")
    g_amt_roll = global_idx["amount_num"].rolling(f"{window_days}D", closed="left")
    g_count = pd.Series(g_amt_roll.count().to_numpy(), index=df_work.index)
    g_amt_mean = pd.Series(g_amt_roll.mean().to_numpy(), index=df_work.index)
    g_amt_std = pd.Series(g_amt_roll.std(ddof=0).to_numpy(), index=df_work.index)
    g_vel_roll = global_idx["velocity"].rolling(f"{window_days}D", closed="left")
    g_vel_mean = pd.Series(g_vel_roll.mean().to_numpy(), index=df_work.index)
    g_vel_std = pd.Series(g_vel_roll.std(ddof=0).to_numpy(), index=df_work.index)
    g_act_roll = global_idx["activity_num"].rolling(f"{window_days}D", closed="left")
    g_act_mean = pd.Series(g_act_roll.mean().to_numpy(), index=df_work.index)
    g_act_std = pd.Series(g_act_roll.std(ddof=0).to_numpy(), index=df_work.index)

    segment_valid = df_work["segment"].notna() & df_work["segment"].astype(str).str.strip().ne("")
    baseline_level = pd.Series("global", index=df_work.index)
    seg_mask = (seg_count >= min_seg_history) & segment_valid
    baseline_level.loc[seg_mask] = "segment"
    user_mask = user_count >= min_user_history
    baseline_level.loc[user_mask] = "user"

    def _select_by_level(user_vals: pd.Series, seg_vals: pd.Series, glob_vals: pd.Series) -> pd.Series:
        return pd.Series(
            np.where(baseline_level == "user", user_vals, np.where(baseline_level == "segment", seg_vals, glob_vals)),
            index=df_work.index,
        )

    def _fix_mean(primary: pd.Series, fallback: pd.Series, current: pd.Series) -> pd.Series:
        out = primary.copy()
        mask = ~np.isfinite(out)
        if mask.any():
            out = out.where(~mask, fallback)
        mask = ~np.isfinite(out)
        if mask.any():
            out = out.where(~mask, current)
        return out

    def _fix_std(primary: pd.Series, fallback: pd.Series) -> pd.Series:
        floor = max(robust_eps, 1e-6)
        out = primary.copy()
        mask = ~np.isfinite(out) | (out <= 0)
        if mask.any():
            out = out.where(~mask, fallback)
        mask = ~np.isfinite(out) | (out <= 0)
        if mask.any():
            out = out.where(~mask, floor)
        return out

    fallback_amt_mean = pd.Series(
        np.where(baseline_level == "user", seg_amt_mean, g_amt_mean), index=df_work.index
    )
    fallback_vel_mean = pd.Series(
        np.where(baseline_level == "user", seg_vel_mean, g_vel_mean), index=df_work.index
    )
    fallback_act_mean = pd.Series(
        np.where(baseline_level == "user", seg_act_mean, g_act_mean), index=df_work.index
    )

    fallback_amt_std = pd.Series(
        np.where(baseline_level == "user", seg_amt_std, g_amt_std), index=df_work.index
    )
    fallback_vel_std = pd.Series(
        np.where(baseline_level == "user", seg_vel_std, g_vel_std), index=df_work.index
    )
    fallback_act_std = pd.Series(
        np.where(baseline_level == "user", seg_act_std, g_act_std), index=df_work.index
    )

    baseline_amount_mean = _fix_mean(
        _select_by_level(user_amt_mean, seg_amt_mean, g_amt_mean), fallback_amt_mean, df_work["amount_num"]
    )
    baseline_amount_std = _fix_std(
        _select_by_level(user_amt_std, seg_amt_std, g_amt_std), fallback_amt_std
    )
    baseline_velocity_mean = _fix_mean(
        _select_by_level(user_vel_mean, seg_vel_mean, g_vel_mean), fallback_vel_mean, df_work["velocity"]
    )
    baseline_velocity_std = _fix_std(
        _select_by_level(user_vel_std, seg_vel_std, g_vel_std), fallback_vel_std
    )
    baseline_activity_mean = _fix_mean(
        _select_by_level(user_act_mean, seg_act_mean, g_act_mean), fallback_act_mean, df_work["activity_num"]
    )
    baseline_activity_std = _fix_std(
        _select_by_level(user_act_std, seg_act_std, g_act_std), fallback_act_std
    )

    n_hist_user = user_count.fillna(0).astype(int)
    n_hist_seg = seg_count.fillna(0).astype(int)
    n_hist_global = g_count.fillna(0).astype(int)
    n_hist = np.where(
        baseline_level == "user",
        n_hist_user,
        np.where(baseline_level == "segment", n_hist_seg, n_hist_global),
    ).astype(int)

    df_work["baseline_level"] = baseline_level
    df_work["baseline_window"] = baseline_window
    df_work["baseline_window_days"] = int(window_days)
    df_work["n_hist"] = n_hist
    df_work["user_tx_count"] = n_hist_user
    df_work["segment_tx_count"] = n_hist_seg
    df_work["baseline_amount_mean"] = baseline_amount_mean
    df_work["baseline_amount_std"] = baseline_amount_std
    df_work["baseline_velocity_mean"] = baseline_velocity_mean
    df_work["baseline_velocity_std"] = baseline_velocity_std
    df_work["baseline_activity_mean"] = baseline_activity_mean
    df_work["baseline_activity_std"] = baseline_activity_std

    df_work = df_work.sort_values("_orig_pos", kind="mergesort")
    for col in [
        "baseline_level",
        "baseline_window",
        "baseline_window_days",
        "n_hist",
        "user_tx_count",
        "segment_tx_count",
        "baseline_amount_mean",
        "baseline_amount_std",
        "baseline_velocity_mean",
        "baseline_velocity_std",
        "baseline_activity_mean",
        "baseline_activity_std",
    ]:
        df[col] = df_work[col].to_numpy()

    df["is_low_history"] = (pd.to_numeric(df["user_tx_count"], errors="coerce").fillna(0) < min_user_history).astype(int)

    # One-hot encode segment (avoids false ordinal relationships from integer mapping)
    segment_labels = segment.fillna("unknown").astype(str)
    segment_dummies = pd.get_dummies(segment_labels, prefix="seg", dtype=float)
    for col in segment_dummies.columns:
        df[col] = segment_dummies[col].values
    _segment_onehot_cols = list(segment_dummies.columns)

    level_map = {"global": 0, "segment": 1, "user": 2}
    df["baseline_level_encoded"] = df["baseline_level"].map(level_map).fillna(0).astype(int)
    df["history_log"] = np.log1p(df["n_hist"])

    if status_cb:
        status_cb("Calculating baseline statistics...")
    if progress_cb:
        progress_cb(20)

    df["baseline_median_amount_log"] = np.log1p(df["baseline_amount_mean"].clip(lower=0.0))
    df["baseline_sigma_amount_log"] = df["baseline_amount_std"]
    df["baseline_median_velocity"] = df["baseline_velocity_mean"]
    df["baseline_sigma_velocity"] = df["baseline_velocity_std"]
    df["baseline_median_activity"] = df["baseline_activity_mean"]
    df["baseline_sigma_activity"] = df["baseline_activity_std"]

    df["baseline_median_amount"] = df["baseline_amount_mean"]
    df["baseline_source_amount"] = df["baseline_level"]
    df["baseline_source_velocity"] = df["baseline_level"]
    df["baseline_source_activity"] = df["baseline_level"]

    if status_cb:
        status_cb("Calculating velocity metrics...")
    if progress_cb:
        progress_cb(40)

    if status_cb:
        status_cb("Calculating activity patterns...")
    if progress_cb:
        progress_cb(60)

    if status_cb:
        status_cb("Computing deviations...")
    if progress_cb:
        progress_cb(80)

    df["amount_ratio"] = amount / (df["baseline_amount_mean"] + eps)
    df["velocity_ratio"] = df["velocity"] / (df["baseline_velocity_mean"] + eps)
    df["activity_ratio"] = activity / (df["baseline_activity_mean"] + eps)

    df["amount_z"] = (amount - df["baseline_amount_mean"]) / (df["baseline_amount_std"] + eps)
    df["velocity_z"] = (df["velocity"] - df["baseline_velocity_mean"]) / (df["baseline_velocity_std"] + eps)
    df["activity_z"] = (activity - df["baseline_activity_mean"]) / (df["baseline_activity_std"] + eps)

    df["amount_rz"] = df["amount_z"]
    df["velocity_rz"] = df["velocity_z"]
    df["activity_rz"] = df["activity_z"]

    rolling_base = pd.DataFrame(
        {
            "user_id": df["user_id"].to_numpy(),
            "_order": order_series.to_numpy(),
            "_orig_pos": np.arange(len(df), dtype=np.int64),
            "_amount_num": amount.to_numpy(),
            "_activity_num": activity.to_numpy(),
            "velocity": df["velocity"].to_numpy(),
        },
        index=np.arange(len(df), dtype=np.int64),
    )
    rolling_base = rolling_base.sort_values(["user_id", "_order", "_orig_pos"], kind="mergesort")
    grouped = rolling_base.groupby("user_id", sort=False)
    rolling_base["rolling_mean_amount_5"] = (
        grouped["_amount_num"].rolling(window=roll_window, min_periods=3).mean().reset_index(level=0, drop=True)
    )
    rolling_base["rolling_mean_velocity_5"] = (
        grouped["velocity"].rolling(window=roll_window, min_periods=3).mean().reset_index(level=0, drop=True)
    )
    rolling_base["rolling_max_amount_5"] = (
        grouped["_amount_num"].rolling(window=roll_window, min_periods=3).max().reset_index(level=0, drop=True)
    )
    rolling_base["rolling_sum_numtx_5"] = (
        grouped["_activity_num"].rolling(window=roll_window, min_periods=3).sum().reset_index(level=0, drop=True)
    )
    rolling_base = rolling_base.sort_values("_orig_pos", kind="mergesort")

    df["rolling_mean_amount_5"] = rolling_base["rolling_mean_amount_5"].to_numpy()
    df["rolling_mean_velocity_5"] = rolling_base["rolling_mean_velocity_5"].to_numpy()
    df["rolling_max_amount_5"] = rolling_base["rolling_max_amount_5"].to_numpy()
    df["rolling_sum_numtx_5"] = rolling_base["rolling_sum_numtx_5"].to_numpy()
    df["velocity_change_rate"] = df["rolling_mean_velocity_5"] / (df["baseline_median_velocity"] + eps)

    seg_amount_p25 = amount.groupby(segment).quantile(0.25)
    global_amount_p25 = amount.quantile(0.25)
    if not np.isfinite(global_amount_p25):
        global_amount_p25 = 0.0
    seg_amount_p25_map = segment.map(seg_amount_p25).fillna(global_amount_p25)
    df["micro_amount_ratio"] = amount / (seg_amount_p25_map + eps)

    df["burst_score"] = df["rolling_sum_numtx_5"] / (df["baseline_median_activity"] + eps)
    df["drift_amount"] = (df["rolling_mean_amount_5"] - df["baseline_median_amount"]).abs() / (
        df["baseline_median_amount"] + eps
    )
    df["drift_velocity"] = (df["rolling_mean_velocity_5"] - df["baseline_median_velocity"]).abs() / (
        df["baseline_median_velocity"] + eps
    )

    df["amount_seg_pct"] = amount.groupby(segment).rank(pct=True)
    df["velocity_seg_pct"] = df["velocity"].groupby(segment).rank(pct=True)

    df["history_size_log"] = np.log1p(df["history_size"])
    df["data_quality_flag"] = df["is_missing_values"].astype(int)
    
    # PART 2: Add temporal aggregates (7-day and 30-day rolling windows)
    if status_cb:
        status_cb("Computing temporal aggregates...")
    if progress_cb:
        progress_cb(90)
    
    df, temporal_agg_cols = temporal_features.add_temporal_aggregates(df, time_col="timestamp")
    
    # PART 2C: Add temporal behavior modeling features (sequence dynamics)
    if status_cb:
        status_cb("Computing temporal behavior features...")
    if progress_cb:
        progress_cb(95)
    
    logger.debug("Computing temporal features: df_len=%d, has_user_id=%s", len(df), "user_id" in df.columns)
    df, temporal_behavior_cols = temporal_features.compute_temporal_features(df)
    logger.debug("Temporal features computed: %d columns", len(temporal_behavior_cols))

    derived_cols = [
        "amount_log",
        "velocity",
        "amount_w",
        "amount_log_w",
        "velocity_w",
        "activity_w",
        "segment_encoded",
        "baseline_level_encoded",
        "history_log",
        "is_low_history",
        "baseline_median_amount_log",
        "baseline_sigma_amount_log",
        "baseline_amount_mean",
        "baseline_amount_std",
        "baseline_median_velocity",
        "baseline_sigma_velocity",
        "baseline_velocity_mean",
        "baseline_velocity_std",
        "baseline_median_activity",
        "baseline_sigma_activity",
        "baseline_activity_mean",
        "baseline_activity_std",
        "baseline_median_amount",
        "amount_z",
        "velocity_z",
        "activity_z",
        "amount_rz",
        "velocity_rz",
        "activity_rz",
        "amount_ratio",
        "velocity_ratio",
        "activity_ratio",
        "rolling_mean_amount_5",
        "rolling_mean_velocity_5",
        "rolling_max_amount_5",
        "rolling_sum_numtx_5",
        "velocity_change_rate",
        "burst_score",
        "micro_amount_ratio",
        "drift_amount",
        "drift_velocity",
        "amount_seg_pct",
        "velocity_seg_pct",
        "history_size",
        "history_size_log",
        "data_quality_flag",
        "tx_hour",
        "tx_dow",
        "is_weekend",
    ]

    # Memory-efficient approach: process columns one by one instead of creating large numpy array
    # Filter to only existing columns to avoid KeyError
    derived_cols_existing = [col for col in derived_cols if col in df.columns]
    
    if len(derived_cols_existing) > 0:
        # Check for non-finite values column by column (more memory efficient)
        # Process in chunks to avoid memory issues with very large DataFrames
        chunk_size = 50000  # Process 50k rows at a time
        has_non_finite = pd.Series(False, index=df.index)
        
        for col in derived_cols_existing:
            col_values = pd.to_numeric(df[col], errors="coerce")
            # Check for non-finite: inf, -inf, or nan
            # Use pandas methods instead of numpy.isfinite to avoid large array creation
            col_non_finite = col_values.isin([np.inf, -np.inf]) | col_values.isna()
            has_non_finite = has_non_finite | col_non_finite
        
        df["has_non_finite"] = has_non_finite
        
        # Replace inf and fillna column by column (more memory efficient)
        for col in derived_cols_existing:
            # Convert to numeric first
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Replace inf values with nan, then fillna with 0
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            df[col] = df[col].fillna(0.0)
    else:
        df["has_non_finite"] = False
    df["n_hist"] = pd.to_numeric(df["n_hist"], errors="coerce").fillna(0).astype(int)
    df["user_tx_count"] = pd.to_numeric(df["user_tx_count"], errors="coerce").fillna(0).astype(int)
    df["segment_tx_count"] = pd.to_numeric(df["segment_tx_count"], errors="coerce").fillna(0).astype(int)

    if status_cb:
        status_cb("✅ Behavioral analysis complete!")
    if progress_cb:
        progress_cb(100)

    behavioral_cols = [
        "amount_rz",
        "velocity_rz",
        "activity_rz",
        "amount_ratio",
        "velocity_ratio",
        "activity_ratio",
        "burst_score",
        "drift_amount",
        "drift_velocity",
        "amount_seg_pct",
        "velocity_seg_pct",
        "micro_amount_ratio",
    ]
    structural_cols = (
        _segment_onehot_cols
        + [
            "is_low_history",
            "baseline_level_encoded",
            "history_log",
        ]
    )
    temporal_cols = [
        "rolling_mean_amount_5",
        "rolling_mean_velocity_5",
        "rolling_sum_numtx_5",
        "rolling_max_amount_5",
        "velocity_change_rate",
    ]
    
    # PART 2: Append new temporal aggregate columns
    # Filter to only include columns that actually exist in df
    temporal_agg_cols_existing = [col for col in temporal_agg_cols if col in df.columns]
    temporal_cols.extend(temporal_agg_cols_existing)
    
    # PART 2C: Append temporal behavior modeling features
    temporal_behavior_cols_existing = [col for col in temporal_behavior_cols if col in df.columns]
    temporal_cols.extend(temporal_behavior_cols_existing)
    
    meta_cols = [
        "data_quality_flag",
    ]

    all_feature_cols = behavioral_cols + structural_cols + temporal_cols + meta_cols
    
    # PART 2: Add temporal_feature_cols group (for reference, but also in temporal_cols)
    # PART 2C: Add temporal_behavior_cols group for sequence dynamics features
    feature_groups = {
        "behavioral_cols": behavioral_cols,
        "structural_cols": structural_cols,
        "temporal_cols": temporal_cols,
        "temporal_feature_cols": temporal_agg_cols_existing,  # Bank-grade temporal aggregates (7d/30d)
        "temporal_behavior_cols": temporal_behavior_cols_existing,  # Sequence dynamics features
        "meta_cols": meta_cols,
        "all_feature_cols": all_feature_cols,
    }

    return df, feature_groups


def compute_behavioral_baselines(
    df: pd.DataFrame,
    status_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> pd.DataFrame:
    """Compute compliance-grade baselines and deviation features (legacy wrapper)."""

    cfg = _build_cfg_with_callbacks(config, status_cb=status_cb, progress_cb=progress_cb)
    df, _ = compute_behavioral_features(df, cfg)
    return df


def build_feature_matrix(df: pd.DataFrame, feature_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """Return feature matrix used for anomaly detection and risk scoring."""

    if feature_cols is None:
        feature_cols = config.FEATURE_COLUMNS
    return df[feature_cols]


def train_anomaly_model(X: pd.DataFrame) -> IsolationForest:
    """Train IsolationForest once at train time. Save with joblib alongside other models."""
    model = IsolationForest(
        contamination=config.ANOMALY_CONTAMINATION,
        random_state=config.ANOMALY_RANDOM_STATE,
    )
    model.fit(X)
    logger.info("IsolationForest trained on shape %s", X.shape)
    return model


def run_anomaly_detection(
    df: pd.DataFrame,
    X: pd.DataFrame,
    model: Optional[IsolationForest] = None,
) -> pd.DataFrame:
    """Apply IsolationForest anomaly detection and attach anomaly flag.

    In production / inference, always pass a pre-trained ``model`` so the
    forest is never re-fitted at scoring time.  When ``model`` is None (legacy
    pipeline only), a new model is trained on ``X`` as a one-shot fallback.

    Args:
        df: Alert DataFrame to annotate with the 'anomaly' column.
        X: Feature matrix aligned with df.
        model: Pre-trained IsolationForest (preferred).  Pass
            ``models['behavioral_model']`` from ``train_risk_engine``.
            If None, a model is trained once on X (legacy / standalone use).

    Returns:
        df with 'anomaly' column added (-1 = anomaly, 1 = normal).
    """
    if model is None:
        logger.warning(
            "run_anomaly_detection called without a pre-trained model — "
            "fitting on current batch (legacy path). Pass model= at inference time."
        )
        model = train_anomaly_model(X)
    df["anomaly"] = model.predict(X)
    return df


def train_risk_model(
    df: pd.DataFrame,
    X: pd.DataFrame,
    status_cb: Optional[Callable[[str], None]] = None,
    progress_cb: Optional[Callable[[int], None]] = None,
) -> Tuple[float, float, float]:
    """Train the risk model, score alerts, and compute demo metrics."""

    y = (df["synthetic_true_suspicious"] == config.RISK_LABEL_YES).astype(int)

    # train/test split (demo)
    # Handle case where there might not be enough positive samples for stratification
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=config.TRAIN_TEST_SIZE, random_state=config.TRAIN_TEST_RANDOM_STATE, stratify=y
        )
    except ValueError:
        # If stratification fails (not enough samples), use regular split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=config.TRAIN_TEST_SIZE, random_state=config.TRAIN_TEST_RANDOM_STATE
        )

    if status_cb:
        status_cb("Initializing Random Forest...")
    if progress_cb:
        progress_cb(10)

    clf = RandomForestClassifier(
        n_estimators=config.RF_N_ESTIMATORS,
        max_depth=config.RF_MAX_DEPTH,
        random_state=config.RF_RANDOM_STATE,
        class_weight=config.RF_CLASS_WEIGHT,
    )

    if status_cb:
        status_cb("Training model (this may take a moment)...")
    if progress_cb:
        progress_cb(30)
    clf.fit(X_train, y_train)

    if status_cb:
        status_cb("Computing risk scores...")
    if progress_cb:
        progress_cb(70)
    # probability = risk (canonical pipeline: raw -> calibrated prob -> compute_risk)
    proba = np.clip(clf.predict_proba(X)[:, 1].astype(float), 0.0, 1.0)
    df["ml_proba"] = proba
    df["risk_score_raw"] = proba
    df["risk_prob"] = proba
    df = compute_risk(df, policy_params=None)
    df["risk_score_final"] = df["risk_score"]

    df["Suspicious"] = np.where(df["risk_score"] >= config.RISK_SCORE_THRESHOLD, "Yes", "No")

    # ===== Alert layer (B2B fields) =====
    # Generate deterministic alert_id if missing
    # Use build_alert_id() for canonical, stable IDs
    if "alert_id" not in df.columns or df["alert_id"].isna().all() or (df["alert_id"].astype(str).str.strip() == "").all():
        # Generate alert_id for each row using canonical function
        df["alert_id"] = df.apply(lambda row: build_alert_id(row.to_dict()), axis=1)
    else:
        # Ensure alert_id is string type but preserve existing values
        df["alert_id"] = df["alert_id"].astype(str)
    
    # Only create case_id if missing (preserve on rerun)
    if "case_id" not in df.columns:
        df["case_id"] = ""
    else:
        # Preserve existing case_id values (don't overwrite on rerun)
        df["case_id"] = df["case_id"].astype(str)
    
    # Only create case_status if missing (preserve on rerun)
    if "case_status" not in df.columns:
        df["case_status"] = config.CASE_STATUS_NEW
    else:
        # Preserve existing case_status values (don't overwrite CLOSED, etc. on rerun)
        df["case_status"] = df["case_status"].astype(str)

    if status_cb:
        status_cb("Evaluating model performance...")
    if progress_cb:
        progress_cb(90)
    # demo metrics
    test_proba = clf.predict_proba(X_test)[:, 1]
    test_pred = (test_proba >= config.RISK_PROBA_THRESHOLD).astype(int)

    prec = precision_score(y_test, test_pred, zero_division=0)
    rec = recall_score(y_test, test_pred, zero_division=0)
    auc = roc_auc_score(y_test, test_proba)

    if status_cb:
        status_cb("✅ Model training complete!")
    if progress_cb:
        progress_cb(100)

    return prec, rec, auc


def _self_check_baseline(df: pd.DataFrame) -> None:
    valid_levels = {"user", "segment", "global"}
    if "baseline_level" not in df.columns:
        raise AssertionError("baseline_level missing")
    if not df["baseline_level"].isin(valid_levels).all():
        raise AssertionError("baseline_level has invalid values")
    if "baseline_window_days" not in df.columns:
        raise AssertionError("baseline_window_days missing")
    if pd.to_numeric(df["baseline_window_days"], errors="coerce").isna().any():
        raise AssertionError("baseline_window_days contains NaN")
    if "n_hist" not in df.columns:
        raise AssertionError("n_hist missing")
    n_hist = pd.to_numeric(df["n_hist"], errors="coerce")
    if n_hist.isna().any():
        raise AssertionError("n_hist contains NaN")
    if (n_hist < 0).any():
        raise AssertionError("n_hist contains negative values")
    if not np.all(np.floor(n_hist) == n_hist):
        raise AssertionError("n_hist must be integer-like")
