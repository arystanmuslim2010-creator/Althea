"""Multi-layer risk engine for AML alert prioritization."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, IsolationForest, RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

from . import config
from .services.rule_engine import RuleEngine
from .services.baseline_engine import BaselineEngine
from .utils import build_alert_id

logger = logging.getLogger(__name__)


def _minmax_scale(values: np.ndarray, vmin: float, vmax: float) -> np.ndarray:
    if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
        return np.zeros_like(values, dtype=float)
    return (values - vmin) / (vmax - vmin)


def _validate_feature_groups(feature_groups: Dict[str, List[str]]) -> None:
    required = ["behavioral_cols", "structural_cols", "temporal_cols", "meta_cols", "all_feature_cols"]
    for key in required:
        cols = feature_groups.get(key, [])
        if not cols:
            raise ValueError(f"Feature group '{key}' is empty or missing.")


def _score_classifier(model: Optional[object], X: pd.DataFrame, default_score: float) -> np.ndarray:
    if model is None:
        return np.full(len(X), default_score, dtype=float)
    proba = model.predict_proba(X)[:, 1]
    return proba.astype(float)


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _safe_series(df: pd.DataFrame, col: str, default: object = 0.0) -> pd.Series:
    if col not in df.columns:
        if isinstance(default, pd.Series):
            base = default.reindex(df.index)
            return pd.to_numeric(base, errors="coerce").fillna(0.0)
        return pd.Series(np.full(len(df), default), index=df.index, dtype=float)
    fill_value = 0.0 if isinstance(default, pd.Series) else default
    return pd.to_numeric(df[col], errors="coerce").fillna(fill_value)


def _safe_text_series(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.full(len(df), default), index=df.index, dtype=object)
    return df[col].fillna(default).astype(str)


def _winsorize(values: pd.Series, p: float) -> pd.Series:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return values.fillna(0.0)
    lo = float(clean.quantile(p))
    hi = float(clean.quantile(1.0 - p))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return values.fillna(0.0)
    return values.clip(lower=lo, upper=hi)


def _compute_risk_components(df: pd.DataFrame) -> Dict[str, pd.Series]:
    temp = float(getattr(config, "RISK_SIGMOID_TEMP", 1.6))
    eps = float(getattr(config, "BASELINE_ROBUST_EPS", 1e-9))

    amount_z = _safe_series(df, "amount_z")
    velocity_z = _safe_series(df, "velocity_z")
    activity_z = _safe_series(df, "activity_z")

    amount_dev = _safe_series(df, "amount_dev", 1.0)
    velocity_dev = _safe_series(df, "velocity_dev", 1.0)
    activity_dev = _safe_series(df, "activity_dev", 1.0)

    z_mag = np.maximum.reduce([amount_z.abs(), velocity_z.abs(), activity_z.abs()])
    dev_mag = np.maximum.reduce(
        [
            np.log1p(amount_dev.clip(lower=0.0)),
            np.log1p(velocity_dev.clip(lower=0.0)),
            np.log1p(activity_dev.clip(lower=0.0)),
        ]
    )
    behavioral_raw = 0.6 * z_mag + 0.4 * dev_mag
    n_hist = _safe_series(df, "n_hist", 0.0)
    denom = np.log1p(max(float(getattr(config, "BASELINE_MIN_SEGMENT_HIST", 200)), 1.0))
    history_conf = (np.log1p(n_hist) / max(denom, eps)).clip(0.0, 1.0)
    base = _sigmoid(np.asarray(behavioral_raw) / max(temp, eps))
    history_conf_arr = np.asarray(history_conf)
    behavioral_score = base * history_conf_arr + (1.0 - history_conf_arr) * (0.5 * base)

    seg_map = {
        "retail_low": 0.10,
        "retail_high": 0.20,
        "smb": 0.30,
        "corporate": 0.40,
    }
    segment = _safe_text_series(df, "segment", "").str.lower()
    structural_score = segment.map(seg_map).fillna(0.15)
    typology = _safe_text_series(df, "typology", "").str.lower()
    structural_score = (structural_score + np.where(typology.ne("none"), 0.05, 0.0)).clip(0.0, 1.0)

    vel_excess = np.maximum(0.0, velocity_z.to_numpy() - 1.5)
    temporal_score = _sigmoid(vel_excess / max(temp, eps))

    anomaly_flag = (_safe_series(df, "anomaly", 0.0) == -1).astype(float)
    baseline_level = _safe_text_series(df, "baseline_level", "user").str.lower()
    global_flag = (baseline_level == "global").astype(float)
    meta_score = (0.25 * anomaly_flag + 0.05 * global_flag).clip(0.0, 1.0)

    # Use rule_score from RuleEngine if available, otherwise use rule_score_raw
    rule_score_from_df = _safe_series(df, "rule_score", 0.0)
    rule_score_raw = _safe_series(df, "rule_score_raw", rule_score_from_df).clip(0.0, 1.0)
    # If rule_score exists in df, use it instead of rule_score_raw
    if "rule_score" in df.columns:
        rule_score_raw = rule_score_from_df
    rule_soft = 1.0 - np.exp(-1.2 * np.asarray(rule_score_raw))

    return {
        "behavioral_score": pd.Series(behavioral_score, index=df.index),
        "structural_score": pd.Series(structural_score, index=df.index),
        "temporal_score": pd.Series(temporal_score, index=df.index),
        "meta_score": pd.Series(meta_score, index=df.index),
        "rule_score_raw": rule_score_raw,
        "rule_soft": pd.Series(rule_soft, index=df.index),
    }


def debug_risk_distribution(df: pd.DataFrame) -> Dict[str, object]:
    scores = _safe_series(df, "risk_score", 0.0)
    stats = scores.describe()
    shares = {
        "share_ge_99_9": float((scores >= 99.9).mean()),
        "share_ge_90": float((scores >= 90).mean()),
        "share_ge_70": float((scores >= 70).mean()),
    }
    return {"describe": stats, "shares": shares}


def _compute_data_signature(df: pd.DataFrame, feature_groups: Dict[str, List[str]]) -> str:
    """Compute a collision-resistant SHA-256 signature for the training dataset.

    Hashes:
    1. Sorted feature column names (structure identity).
    2. Full numeric feature matrix serialized to bytes (value identity).
    3. Row count (shape guard).

    Returns:
        First 32 hex chars of SHA-256 (128 bits — negligible collision probability).
    """
    all_feature_cols = feature_groups.get("all_feature_cols", [])
    col_names = sorted([c for c in all_feature_cols if c in df.columns])
    row_count = len(df)

    h = hashlib.sha256()
    # 1. Column name structure
    h.update(",".join(col_names).encode("utf-8"))
    h.update(b"|")
    # 2. Row count
    h.update(str(row_count).encode("utf-8"))
    h.update(b"|")
    # 3. Full feature matrix as contiguous float64 bytes
    if col_names and row_count > 0:
        matrix = df[col_names].to_numpy(dtype=np.float64, na_value=0.0)
        h.update(matrix.tobytes())

    return h.hexdigest()[:32]


def _get_cache_paths(data_signature: str, cache_dir: Path) -> Dict[str, Path]:
    """
    Get cache file paths for model artifacts.
    
    Args:
        data_signature: Data signature string
        cache_dir: Cache directory path
        
    Returns:
        Dictionary with paths for model, calibrator, and metadata
    """
    return {
        "model": cache_dir / f"model_{data_signature}.joblib",
        "calibrator": cache_dir / f"calibrator_{data_signature}.joblib",
        "metadata": cache_dir / f"meta_{data_signature}.json",
    }


def load_cached_risk_engine(data_signature: str, cache_dir: Path) -> Optional[Tuple[Dict[str, object], Optional[object]]]:
    """
    Load cached model artifacts from disk (for use with st.cache_resource in the app).
    Returns (models, calibrator) or None if cache miss or corrupted.
    """
    return _load_cached_models(data_signature, cache_dir)


def _load_cached_models(data_signature: str, cache_dir: Path) -> Optional[Tuple[Dict[str, object], Optional[object]]]:
    """
    Load cached model artifacts if available.
    
    Args:
        data_signature: Data signature string
        cache_dir: Cache directory path
        
    Returns:
        Tuple of (models_dict, calibrator) or None if cache miss
    """
    if not getattr(config, "MODEL_CACHE_ENABLED", True):
        return None
    
    paths = _get_cache_paths(data_signature, cache_dir)
    
    # Check if all required files exist
    if not all(p.exists() for p in [paths["model"], paths["metadata"]]):
        return None
    
    try:
        # Load model
        models = joblib.load(paths["model"])
        
        # Load calibrator (optional)
        calibrator = None
        if paths["calibrator"].exists():
            calibrator = joblib.load(paths["calibrator"])
        
        # Load and validate metadata
        with open(paths["metadata"], "r") as f:
            metadata = json.load(f)
        
        # Verify signature matches
        if metadata.get("data_signature") != data_signature:
            return None
        
        return models, calibrator
    except Exception:
        # Cache corrupted, return None to trigger retrain
        return None


def _save_cached_models(
    models: Dict[str, object],
    calibrator: Optional[object],
    data_signature: str,
    cache_dir: Path,
    feature_groups: Dict[str, List[str]],
    calibration_metrics: Optional[Dict] = None,
):
    """
    Save model artifacts to cache.
    
    Args:
        models: Models dictionary
        calibrator: Calibrator object (optional)
        data_signature: Data signature string
        cache_dir: Cache directory path
        feature_groups: Feature groups dictionary
        calibration_metrics: Optional dict with brier, reliability_bins, calibration_method, trained_at
    """
    if not getattr(config, "MODEL_CACHE_ENABLED", True):
        return
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    paths = _get_cache_paths(data_signature, cache_dir)
    
    try:
        joblib.dump(models, paths["model"])
        if calibrator is not None:
            joblib.dump(calibrator, paths["calibrator"])
        
        metadata = {
            "data_signature": data_signature,
            "feature_groups": feature_groups,
            "timestamp": pd.Timestamp.now().isoformat(),
        }
        if calibration_metrics is not None:
            metadata["calibration_method"] = calibration_metrics.get("calibration_method", "")
            metadata["brier"] = calibration_metrics.get("brier")
            metadata["auc"] = calibration_metrics.get("auc")
            metadata["reliability_bins"] = calibration_metrics.get("reliability_bins", [])
            metadata["trained_at"] = calibration_metrics.get("trained_at", metadata["timestamp"])
            metadata["features_version"] = calibration_metrics.get("features_version", data_signature)
        with open(paths["metadata"], "w") as f:
            json.dump(metadata, f, indent=2)
    except Exception:
        pass


def get_calibration_metadata_from_cache(
    cache_dir: Optional[Path] = None,
    data_signature: Optional[str] = None,
) -> Optional[Dict]:
    """
    Load calibration metrics from model cache metadata (for ops panel).
    If data_signature is None, uses the most recent metadata file in cache_dir.
    """
    if cache_dir is None:
        cache_dir = Path(getattr(config, "MODEL_CACHE_DIR", "data/model_cache"))
    if not cache_dir.exists():
        return None
    try:
        if data_signature:
            paths = _get_cache_paths(data_signature, cache_dir)
            meta_path = paths["metadata"]
            if not meta_path.exists():
                return None
        else:
            meta_files = list(cache_dir.glob("meta_*.json"))
            if not meta_files:
                return None
            meta_path = max(meta_files, key=lambda p: p.stat().st_mtime)
        with open(meta_path, "r") as f:
            metadata = json.load(f)
        return {
            "calibration_method": metadata.get("calibration_method"),
            "brier": metadata.get("brier"),
            "auc": metadata.get("auc"),
            "reliability_bins": metadata.get("reliability_bins", []),
            "trained_at": metadata.get("trained_at", metadata.get("timestamp")),
            "data_signature": metadata.get("data_signature"),
        }
    except Exception:
        return None


def train_risk_engine(
    df: pd.DataFrame,
    feature_groups: Dict[str, List[str]],
    force_retrain: bool = False,
    loader: Optional[Callable[[str], Optional[Tuple[Dict[str, object], Optional[object]]]]] = None,
) -> Tuple[Dict[str, object], Optional[object]]:
    _validate_feature_groups(feature_groups)

    # Compute data signature once (used for cache lookup and save)
    data_signature = _compute_data_signature(df, feature_groups)
    cache_dir = Path("data/model_cache")
    if hasattr(config, "MODEL_CACHE_DIR"):
        cache_dir = Path(config.MODEL_CACHE_DIR)

    cache_enabled = getattr(config, "MODEL_CACHE_ENABLED", True)
    force_retrain_flag = getattr(config, "FORCE_RETRAIN", False) or force_retrain

    if cache_enabled and not force_retrain_flag:
        if loader is not None:
            cached = loader(data_signature)
        else:
            cached = _load_cached_models(data_signature, cache_dir)
        if cached is not None:
            models, calibrator = cached
            logger.info("MODEL CACHE HIT data_signature=%s", data_signature)
            return models, calibrator

    logger.info("MODEL TRAIN START data_signature=%s rows=%d", data_signature, len(df))

    y = (df["synthetic_true_suspicious"] == config.RISK_LABEL_YES).astype(int)
    base_rate = float(y.mean()) if len(y) > 0 else 0.0

    behavioral_cols = feature_groups["behavioral_cols"]
    structural_cols = feature_groups["structural_cols"]
    temporal_cols = feature_groups["temporal_cols"]
    meta_cols = feature_groups["meta_cols"]

    X_behavioral = df[behavioral_cols]
    X_structural = df[structural_cols]
    X_temporal = df[temporal_cols]
    X_meta = df[meta_cols].copy()

    max_train = int(getattr(config, "MAX_TRAIN_ROWS", 50000))
    if len(df) > max_train:
        sample_idx = df.sample(n=max_train, random_state=config.RF_RANDOM_STATE).index
        df_train = df.loc[sample_idx]
        X_behavioral = X_behavioral.loc[sample_idx]
        X_structural = X_structural.loc[sample_idx]
        X_temporal = X_temporal.loc[sample_idx]
        X_meta = X_meta.loc[sample_idx]
        y = y.loc[sample_idx]
    else:
        df_train = df

    behavioral_model = IsolationForest(
        contamination=config.ANOMALY_CONTAMINATION,
        random_state=config.ANOMALY_RANDOM_STATE,
    )
    behavioral_model.fit(X_behavioral)
    behavioral_raw = -behavioral_model.score_samples(X_behavioral)
    behavioral_min = float(np.min(behavioral_raw))
    behavioral_max = float(np.max(behavioral_raw))
    behavioral_score = _minmax_scale(behavioral_raw, behavioral_min, behavioral_max)

    structural_model = None
    temporal_model = None
    temporal_behavior_model = None
    meta_model = None

    if y.nunique() >= 2:
        structural_model = GradientBoostingClassifier(random_state=config.RF_RANDOM_STATE)
        structural_model.fit(X_structural, y)
        temporal_model = RandomForestClassifier(
            n_estimators=config.RF_N_ESTIMATORS,
            max_depth=config.RF_MAX_DEPTH,
            random_state=config.RF_RANDOM_STATE,
            class_weight=config.RF_CLASS_WEIGHT,
        )
        temporal_model.fit(X_temporal, y)
        
        # Train separate temporal behavior model
        temporal_behavior_cols = feature_groups.get("temporal_behavior_cols", [])
        logger.debug("Temporal behavior cols: %d available", len(temporal_behavior_cols))

        if temporal_behavior_cols and all(col in df_train.columns for col in temporal_behavior_cols):
            X_temporal_behavior = df_train[temporal_behavior_cols]
            logger.debug("Fitting temporal behavior model on shape %s", X_temporal_behavior.shape)

            temporal_behavior_model = RandomForestClassifier(
                n_estimators=120,
                max_depth=6,
                random_state=config.RF_RANDOM_STATE,
                class_weight=config.RF_CLASS_WEIGHT,
            )
            temporal_behavior_model.fit(X_temporal_behavior, y)
            logger.debug("Temporal behavior model fit complete")

            # Compute temporal_ml_score for training data to use in component_raw calculation
            temporal_behavior_score_train = _score_classifier(
                temporal_behavior_model,
                X_temporal_behavior,
                base_rate
            )
            df_train["risk_temporal_ml"] = temporal_behavior_score_train * 100.0

    structural_score = _score_classifier(structural_model, X_structural, base_rate)
    temporal_score = _score_classifier(temporal_model, X_temporal, base_rate)

    score_variance = np.var(np.vstack([behavioral_score, structural_score, temporal_score]).T, axis=1)
    X_meta["score_variance"] = score_variance

    if y.nunique() >= 2:
        meta_model = LogisticRegression(max_iter=1000, random_state=config.RF_RANDOM_STATE)
        meta_model.fit(X_meta, y)

    meta_score = _score_classifier(meta_model, X_meta, base_rate)

    components = _compute_risk_components(df_train)
    behavioral_score = components["behavioral_score"]
    structural_score = components["structural_score"]
    temporal_score = components["temporal_score"]
    meta_score = components["meta_score"]
    rule_score_raw = components["rule_score_raw"]
    rule_soft = components["rule_soft"]

    # Hybrid blending weights — all values sourced exclusively from config.py
    temporal_ml_score = _safe_series(df_train, "risk_temporal_ml", 0.0) / 100.0
    if temporal_ml_score.sum() == 0:
        temporal_ml_score = temporal_score

    w_b = float(config.RISK_BEHAVIORAL_WEIGHT)
    w_s = float(config.RISK_STRUCTURAL_WEIGHT)
    w_t_ml = float(config.RISK_TEMPORAL_ML_WEIGHT)
    w_r = float(config.RISK_RULE_WEIGHT)
    w_sum = max(w_b + w_s + w_t_ml + w_r, 1e-9)
    
    ml_weight_sum = w_b + w_s + w_t_ml
    component_raw = (
        (w_b * behavioral_score + w_s * structural_score + w_t_ml * temporal_ml_score) / ml_weight_sum
        if ml_weight_sum > 0
        else np.full(len(y), 0.5, dtype=float)
    )
    component_raw = np.clip(component_raw, 0.0, 1.0)

    # PART 4: Hybrid blending with updated weights (for legacy calibrator only)
    final_raw = np.clip(
        (1.0 - w_r) * component_raw + w_r * rule_soft,
        0.0,
        1.0,
    )

    # Existing calibrator (for backward compatibility)
    calibrator = None
    if y.nunique() >= 2 and np.unique(final_raw).size > 1:
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(final_raw, y)
    
    # PART 2: Fit new ML probability calibrator on raw ML probabilities
    from . import calibration
    ml_calibrator = None
    calibration_metrics = None
    calibration_method = getattr(config, "CALIBRATION_METHOD", "isotonic")
    if y.nunique() >= 2 and np.unique(component_raw).size > 1:
        ml_calibrator = calibration.fit_calibrator(y, component_raw, method=calibration_method)
        if ml_calibrator is not None:
            p_cal = calibration.apply_calibrator(ml_calibrator, component_raw)
            calibration_metrics = calibration.compute_calibration_metrics(y, p_cal, n_bins=10)
            if calibration_metrics is not None:
                calibration_metrics["calibration_method"] = calibration_method
                calibration_metrics["trained_at"] = pd.Timestamp.now().isoformat()
                calibration_metrics["features_version"] = data_signature

    models = {
        "behavioral_model": behavioral_model,
        "behavioral_min": behavioral_min,
        "behavioral_max": behavioral_max,
        "structural_model": structural_model,
        "temporal_model": temporal_model,
        "temporal_behavior_model": temporal_behavior_model,  # PART 3: New temporal behavior model
        "meta_model": meta_model,
        "structural_default": base_rate,
        "temporal_default": base_rate,
        "temporal_behavior_default": base_rate,
        "meta_default": base_rate,
        "feature_groups": feature_groups,
        "ml_calibrator": ml_calibrator,  # New calibrator for ML probabilities
    }
    
    # Save to cache if enabled (includes calibration metrics in metadata)
    if cache_enabled:
        _save_cached_models(models, calibrator, data_signature, cache_dir, feature_groups, calibration_metrics=calibration_metrics)

    return models, calibrator


def score_with_risk_engine(
    df: pd.DataFrame,
    models: Dict[str, object],
    calibrator: Optional[object],
    external_sources: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    feature_groups = models["feature_groups"]
    _validate_feature_groups(feature_groups)

    # Step 1: Rule outputs (rules_json, rule_R001_hit, rule_score_total) come from pipeline;
    # do not recompute rules here (single canonical engine runs only in pipeline).
    # Step 2: Apply typology Rule Engine and Baseline Engine
    rule_engine = RuleEngine(config)
    baseline_engine = BaselineEngine()
    df = rule_engine.apply_rules(df)
    df = baseline_engine.compute_baselines(df)

    behavioral_cols = feature_groups["behavioral_cols"]
    structural_cols = feature_groups["structural_cols"]
    temporal_cols = feature_groups["temporal_cols"]
    meta_cols = feature_groups["meta_cols"]

    X_behavioral = df[behavioral_cols]
    X_structural = df[structural_cols]
    X_temporal = df[temporal_cols]
    X_meta = df[meta_cols].copy()

    # Populate 'anomaly' column using the pre-trained IsolationForest (no re-fitting at inference)
    behavioral_model = models.get("behavioral_model")
    if behavioral_model is not None:
        df["anomaly"] = behavioral_model.predict(X_behavioral)
        logger.debug("Anomaly detection applied via pre-trained IsolationForest")

    components = _compute_risk_components(df)
    behavioral_score = components["behavioral_score"]
    structural_score = components["structural_score"]
    temporal_score = components["temporal_score"]
    meta_score = components["meta_score"]
    
    # Compute temporal behavior ML score
    temporal_behavior_model = models.get("temporal_behavior_model", None)
    temporal_behavior_cols = feature_groups.get("temporal_behavior_cols", [])
    logger.debug(
        "Temporal ML scoring: model_present=%s, cols=%d",
        temporal_behavior_model is not None,
        len(temporal_behavior_cols),
    )

    if temporal_behavior_model is not None and temporal_behavior_cols and all(col in df.columns for col in temporal_behavior_cols):
        X_temporal_behavior = df[temporal_behavior_cols]
        temporal_behavior_score = _score_classifier(
            temporal_behavior_model,
            X_temporal_behavior,
            models.get("temporal_behavior_default", 0.0)
        )
        df["risk_temporal_ml"] = temporal_behavior_score * 100.0
        logger.debug("Temporal ML score mean=%.4f", float(temporal_behavior_score.mean()))
    else:
        # Fallback: use existing temporal_score
        df["risk_temporal_ml"] = temporal_score * 100.0
        logger.debug("Using fallback temporal_score (no temporal behavior model)")
    # Use rule_score from RuleEngine if available, otherwise fallback to computed
    rule_score_from_engine = _safe_series(df, "rule_score", 0.0)
    rule_score_raw = components.get("rule_score_raw", pd.Series(rule_score_from_engine, index=df.index))
    rule_soft = components.get("rule_soft", pd.Series(rule_score_from_engine, index=df.index))
    
    # Hybrid risk blending weights — all values sourced exclusively from config.py
    temporal_ml_score = _safe_series(df, "risk_temporal_ml", 0.0) / 100.0
    if temporal_ml_score.sum() == 0:
        temporal_ml_score = temporal_score

    w_b = float(config.RISK_BEHAVIORAL_WEIGHT)
    w_s = float(config.RISK_STRUCTURAL_WEIGHT)
    w_t_ml = float(config.RISK_TEMPORAL_ML_WEIGHT)
    w_r = float(config.RISK_RULE_WEIGHT)
    w_sum = max(w_b + w_s + w_t_ml + w_r, 1e-9)
    
    # Use rule_score_total from rule engine if available
    rule_score_total = _safe_series(df, "rule_score_total", 0.0) / 100.0
    if rule_score_total.sum() == 0:
        rule_score_series = pd.Series(rule_score_from_engine, index=df.index)
    else:
        rule_score_series = rule_score_total
    
    risk_raw = np.clip(
        (
            w_b * behavioral_score +
            w_s * structural_score +
            w_t_ml * temporal_ml_score +
            w_r * rule_score_series
        ) / w_sum,
        0.0,
        1.0,
    )

    winsor_p = float(getattr(config, "RISK_WINSOR_PCT", 0.005))
    risk_w = _winsorize(pd.Series(risk_raw, index=df.index), winsor_p)
    median = float(risk_w.median())
    q1 = float(risk_w.quantile(0.25))
    q3 = float(risk_w.quantile(0.75))
    iqr = max(q3 - q1, float(getattr(config, "BASELINE_ROBUST_EPS", 1e-9)))
    if not np.isfinite(iqr) or iqr <= 0:
        calibrated = np.full(len(df), 0.5, dtype=float)
    else:
        temp = float(getattr(config, "RISK_SIGMOID_TEMP", 1.6))
        x = (risk_w.to_numpy() - median) / iqr
        calibrated = _sigmoid(x / max(temp, 1e-9))
    calibrated = np.clip(calibrated, 0.0, 1.0)

    df["risk_behavioral"] = behavioral_score
    df["risk_structural"] = structural_score
    df["risk_temporal"] = temporal_score
    df["risk_meta"] = meta_score
    df["risk_rules"] = rule_score_raw

    # Canonical pipeline: raw model score -> calibrated prob -> compute_risk (governed 0-100, band, rank)
    # Raw = ML blend (behavioral + structural + temporal) in [0,1]; rules integrated via meta-risk in compute_risk
    ml_weight_sum = w_b + w_s + w_t_ml
    if ml_weight_sum > 0:
        ml_component_raw = (
            w_b * behavioral_score +
            w_s * structural_score +
            w_t_ml * temporal_ml_score
        ) / ml_weight_sum
    else:
        ml_component_raw = risk_raw
    ml_component_raw = np.clip(ml_component_raw, 0.0, 1.0)

    from . import calibration
    from .risk_engine import compute_risk

    df["risk_score_raw"] = ml_component_raw.astype(float)
    ml_calibrator = models.get("ml_calibrator", None)
    df["risk_prob"] = (
        calibration.apply_calibrator(ml_calibrator, ml_component_raw)
        if ml_calibrator is not None
        else np.clip(ml_component_raw, 0.0, 1.0)
    )
    df["risk_score_ml_raw"] = df["risk_score_raw"] * 100.0

    # Single canonical pipeline: meta-risk (segment/country/rules + external priors) + anti-saturation mapping + bands + rank
    policy_params = None  # use config defaults (RISK_BAND_T1/T2/T3, SCORE_MAPPING_KIND)
    df = compute_risk(df, policy_params=policy_params, external_sources=external_sources)
    df["risk_score_final"] = df["risk_score"]

    # Update explainability: If rule_score > 0.7 → Top_Driver = typology
    rule_score_series = pd.Series(rule_score_from_engine, index=df.index)
    typology = _safe_text_series(df, "typology", "behavioral_anomaly")
    
    # Check if rule_score > 0.7, then use typology as Top_Driver
    high_rule_score_mask = rule_score_series > 0.7
    
    rule_top_hit = _safe_text_series(df, "rule_top_hit", "none").str.upper()
    rule_contrib = w_r * rule_score_series
    comp_weight = 1.0 - w_r
    comp_contribs = {
        "BEHAVIORAL": comp_weight * (w_b / w_sum) * behavioral_score,
        "STRUCTURAL": comp_weight * (w_s / w_sum) * structural_score,
        "TEMPORAL": comp_weight * (w_t_ml / w_sum) * temporal_ml_score,
    }
    comp_stack = np.vstack([v.to_numpy() for v in comp_contribs.values()])
    comp_keys = list(comp_contribs.keys())
    comp_top_idx = comp_stack.argmax(axis=0)
    comp_top = np.array([comp_keys[i] for i in comp_top_idx], dtype=object)
    comp_top_val = comp_stack.max(axis=0)
    
    # If rule_score > 0.7, use typology; otherwise use existing logic
    rule_top = np.where(
        high_rule_score_mask,
        typology.str.upper().to_numpy(),
        np.where(
            (rule_score_raw.to_numpy() > 0.6) & (rule_contrib.to_numpy() >= comp_top_val),
            np.where(rule_top_hit.ne("NONE"), "RULE_" + rule_top_hit.to_numpy(), "RULE"),
            comp_top,
        )
    )
    df["risk_reason_code"] = rule_top
    # Also set Top_Driver for explainability
    df["Top_Driver"] = rule_top

    df["risk_uncertainty"] = np.std(
        np.vstack(
            [
                pd.to_numeric(df["risk_behavioral"], errors="coerce").fillna(0.0),
                pd.to_numeric(df["risk_structural"], errors="coerce").fillna(0.0),
                pd.to_numeric(df["risk_temporal"], errors="coerce").fillna(0.0),
            ]
        ).T,
        axis=1,
    )

    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["risk_score"] = np.clip(df["risk_score"], 0.0, 100.0)

    # PART 6: Temporal feature validation (internal, only if debug enabled)
    show_temporal_debug = getattr(config, "SHOW_TEMPORAL_DEBUG", False)
    if show_temporal_debug:
        try:
            import streamlit as st
            st.write("### Temporal Feature Debug")
            
            temporal_behavior_cols = feature_groups.get("temporal_behavior_cols", [])
            if temporal_behavior_cols:
                for col in temporal_behavior_cols[:5]:  # Show first 5 features
                    if col in df.columns:
                        st.write(f"**{col}**: mean={df[col].mean():.4f}, std={df[col].std():.4f}")
                
                # Correlation with final risk
                if "risk_score_final" in df.columns:
                    corr = df[temporal_behavior_cols].corrwith(df["risk_score_final"]).abs().mean()
                    st.write(f"**Avg correlation with risk_score_final**: {corr:.4f}")
        except ImportError:
            # Streamlit not available, print to console
            print("### Temporal Feature Debug")
            temporal_behavior_cols = feature_groups.get("temporal_behavior_cols", [])
            if temporal_behavior_cols:
                for col in temporal_behavior_cols[:5]:
                    if col in df.columns:
                        print(f"{col}: mean={df[col].mean():.4f}, std={df[col].std():.4f}")
                if "risk_score_final" in df.columns:
                    corr = df[temporal_behavior_cols].corrwith(df["risk_score_final"]).abs().mean()
                    print(f"Avg correlation with risk_score_final: {corr:.4f}")
    
    # PART 4: Distribution sanity checks (internal, only if debug enabled)
    show_debug = getattr(config, "SHOW_SCORE_DEBUG", False)
    if show_debug:
        try:
            import streamlit as st
            st.write("### Score Distribution Debug")
            
            for score_name, score_col in [
                ("ML Raw", "risk_score_ml_raw"),
                ("Canonical", "risk_score"),
                ("Final", "risk_score_final"),
            ]:
                if score_col in df.columns:
                    scores = pd.to_numeric(df[score_col], errors="coerce").fillna(0.0)
                    share_999 = (scores >= 99.9).mean() * 100
                    share_90 = (scores >= 90).mean() * 100
                    share_70 = (scores >= 70).mean() * 100
                    st.write(f"**{score_name}**: >=99.9: {share_999:.2f}%, >=90: {share_90:.2f}%, >=70: {share_70:.2f}%")
        except ImportError:
            # Streamlit not available, print to console instead
            print("### Score Distribution Debug")
            for score_name, score_col in [
                ("ML Raw", "risk_score_ml_raw"),
                ("Canonical", "risk_score"),
                ("Final", "risk_score_final"),
            ]:
                if score_col in df.columns:
                    scores = pd.to_numeric(df[score_col], errors="coerce").fillna(0.0)
                    share_999 = (scores >= 99.9).mean() * 100
                    share_90 = (scores >= 90).mean() * 100
                    share_70 = (scores >= 70).mean() * 100
                    print(f"{score_name}: >=99.9: {share_999:.2f}%, >=90: {share_90:.2f}%, >=70: {share_70:.2f}%")
    
    # PART 4: Ensure all score columns are clean (no inf, no NaN, clipped)
    for col in ["risk_score_ml_raw", "risk_score", "risk_score_final"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).replace([np.inf, -np.inf], 0.0)
            df[col] = np.clip(df[col], 0.0, 100.0)
    if "risk_score_raw" in df.columns:
        df["risk_score_raw"] = pd.to_numeric(df["risk_score_raw"], errors="coerce").fillna(0.0).replace([np.inf, -np.inf], 0.0)
        df["risk_score_raw"] = np.clip(df["risk_score_raw"], 0.0, 1.0)
    if "risk_prob" in df.columns:
        df["risk_prob"] = pd.to_numeric(df["risk_prob"], errors="coerce").fillna(0.0).replace([np.inf, -np.inf], 0.0)
        df["risk_prob"] = np.clip(df["risk_prob"], 0.0, 1.0)
    
    # Update Suspicious flag to use risk_score_final
    df["Suspicious"] = np.where(
        df["risk_score_final"] >= config.RISK_SCORE_THRESHOLD, config.RISK_LABEL_YES, config.RISK_LABEL_NO
    )

    # Generate deterministic alert_id if missing
    # Use build_alert_id() for canonical, stable IDs
    if "alert_id" not in df.columns or df["alert_id"].isna().all() or (df["alert_id"].astype(str).str.strip() == "").all():
        # Generate alert_id for each row using canonical function
        df["alert_id"] = df.apply(lambda row: build_alert_id(row.to_dict()), axis=1)
    else:
        # Ensure alert_id is string type
        df["alert_id"] = df["alert_id"].astype(str)
    
    # PART D: Do NOT overwrite case_id and case_status if they already exist
    # Only create if missing (rerun-safe)
    if "case_id" not in df.columns:
        df["case_id"] = ""
    else:
        # Preserve existing case_id values (don't overwrite on rerun)
        df["case_id"] = df["case_id"].astype(str)
    
    if "case_status" not in df.columns:
        df["case_status"] = config.CASE_STATUS_NEW
    else:
        # Preserve existing case_status values (don't overwrite CLOSED, etc. on rerun)
        df["case_status"] = df["case_status"].astype(str)

    _validate_scores(df)

    return df


def evaluate_risk_engine(
    df: pd.DataFrame,
    analyst_capacity: int = int(getattr(config, "DEFAULT_ANALYST_CAPACITY", 50)),
) -> Dict[str, float]:
    """Evaluate the risk engine using a temporal holdout split (no data leakage).

    Sorts rows by timestamp, trains on the first 80% of time-ordered data, and
    evaluates on the last 20%. Returns precision, recall, AUC-ROC, and Precision@K
    (where K = analyst_capacity).

    Args:
        df: Scored DataFrame containing 'risk_score', 'synthetic_true_suspicious',
            and optionally a timestamp column.
        analyst_capacity: Daily analyst review capacity for Precision@K (default from config).

    Returns:
        Dict with keys: precision, recall, auc, precision_at_k, average_precision.
    """
    from sklearn.metrics import average_precision_score

    y = (df["synthetic_true_suspicious"] == config.RISK_LABEL_YES).astype(int)
    scores = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0) / 100.0

    # --- Temporal split: sort by timestamp, train on first 80%, eval on last 20% ---
    ts_col = None
    for candidate in getattr(config, "TIME_COL_CANDIDATES", ["timestamp", "event_time", "tx_time", "datetime"]):
        if candidate in df.columns:
            ts_col = candidate
            break

    if ts_col is not None:
        sorted_idx = df[ts_col].argsort().values
    else:
        # No timestamp — fall back to row order (still deterministic, no random leakage)
        sorted_idx = np.arange(len(df))
        logger.warning("No timestamp column found; using row order for temporal split")

    split_pos = int(len(sorted_idx) * (1.0 - config.TRAIN_TEST_SIZE))
    idx_test = df.index[sorted_idx[split_pos:]]

    y_test = y.loc[idx_test]
    scores_test = scores.loc[idx_test]

    if y_test.nunique() < 2:
        logger.warning("evaluate_risk_engine: test set has only one class — metrics undefined")
        return {"precision": 0.0, "recall": 0.0, "auc": 0.0, "precision_at_k": 0.0, "average_precision": 0.0}

    pred_test = (scores_test >= (config.RISK_SCORE_THRESHOLD / 100.0)).astype(int)
    prec = float(precision_score(y_test, pred_test, zero_division=0))
    rec = float(recall_score(y_test, pred_test, zero_division=0))
    try:
        auc = float(roc_auc_score(y_test, scores_test))
    except ValueError:
        auc = 0.0

    # --- Precision@K: fraction of top-K scored alerts that are true positives ---
    k = min(analyst_capacity, len(y_test))
    top_k_idx = scores_test.nlargest(k).index
    prec_at_k = float(y_test.loc[top_k_idx].mean()) if k > 0 else 0.0

    # --- Average Precision (area under precision-recall curve) ---
    try:
        avg_prec = float(average_precision_score(y_test, scores_test))
    except ValueError:
        avg_prec = 0.0

    logger.info(
        "Evaluation: prec=%.3f rec=%.3f auc=%.3f p@k(%d)=%.3f avg_prec=%.3f",
        prec, rec, auc, k, prec_at_k, avg_prec,
    )
    return {
        "precision": prec,
        "recall": rec,
        "auc": auc,
        "precision_at_k": prec_at_k,
        "average_precision": avg_prec,
    }


def _validate_scores(df: pd.DataFrame) -> None:
    for col in [
        "risk_behavioral",
        "risk_structural",
        "risk_temporal",
        "risk_meta",
        "risk_rules",
        "risk_score_raw",
    ]:
        if col not in df.columns:
            raise AssertionError(f"Missing risk component: {col}")
        values = pd.to_numeric(df[col], errors="coerce")
        if values.isna().any():
            raise AssertionError(f"{col} contains NaN")
        if ((values < 0) | (values > 1)).any():
            raise AssertionError(f"{col} outside [0,1]")
    if "risk_score" not in df.columns:
        raise AssertionError("risk_score missing")
    if pd.to_numeric(df["risk_score"], errors="coerce").isna().any():
        raise AssertionError("risk_score contains NaN")
