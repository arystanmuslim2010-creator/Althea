"""
LightGBM (or XGBoost) classifier with monotonic constraints and SHAP explainability.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

try:
    import lightgbm as lgb
    LGBM_AVAILABLE = True
except ImportError:
    lgb = None
    LGBM_AVAILABLE = False


def train_lgbm(
    X_train: Union[pd.DataFrame, np.ndarray],
    y_train: Union[pd.Series, np.ndarray],
    X_val: Optional[Union[pd.DataFrame, np.ndarray]] = None,
    y_val: Optional[Union[pd.Series, np.ndarray]] = None,
    feature_names: Optional[List[str]] = None,
    categorical_feature: Optional[List[str]] = None,
    scale_pos_weight: float = 1.0,
    monotonic_constraints: Optional[Dict[str, int]] = None,
    random_state: int = 42,
    num_leaves: int = 31,
    max_depth: int = 8,
    n_estimators: int = 200,
    **kwargs: Any,
) -> Any:
    """
    Train LightGBM binary classifier.

    monotonic_constraints: dict feature_name -> 1 (positive) or -1 (negative) or 0 (none).
    """
    if not LGBM_AVAILABLE:
        raise ImportError("lightgbm is required. Install with: pip install lightgbm")

    X_train = np.asarray(X_train) if not isinstance(X_train, pd.DataFrame) else X_train
    y_train = np.asarray(y_train, dtype=float).ravel()
    if feature_names is None and hasattr(X_train, "columns"):
        feature_names = list(X_train.columns)
    elif feature_names is None:
        feature_names = [f"f{i}" for i in range(X_train.shape[1])]

    # Monotonic constraint: list of +1/-1/0 per feature index
    mon_list = None
    if monotonic_constraints:
        mon_list = [monotonic_constraints.get(name, 0) for name in feature_names]

    params = {
        "objective": "binary",
        "metric": "auc",
        "verbosity": -1,
        "random_state": random_state,
        "num_leaves": num_leaves,
        "max_depth": max_depth,
        "n_estimators": n_estimators,
        "scale_pos_weight": scale_pos_weight,
        **kwargs,
    }
    if mon_list is not None:
        params["monotone_constraints"] = mon_list

    train_set = lgb.Dataset(X_train, label=y_train, feature_name=feature_names, categorical_feature=categorical_feature or "auto")
    valid_sets = [train_set]
    valid_names = ["train"]
    if X_val is not None and y_val is not None:
        X_val = np.asarray(X_val) if not isinstance(X_val, pd.DataFrame) else X_val
        y_val = np.asarray(y_val, dtype=float).ravel()
        valid_sets.append(lgb.Dataset(X_val, label=y_val, feature_name=feature_names, reference=train_set))
        valid_names.append("valid")

    model = lgb.train(
        params,
        train_set,
        num_boost_round=n_estimators,
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=[lgb.early_stopping(50, verbose=False)] if valid_names.__len__() > 1 else None,
    )
    return model


def predict_proba(
    model: Any,
    X: Union[pd.DataFrame, np.ndarray],
) -> np.ndarray:
    """Return P(y=1) for each row."""
    X = np.asarray(X) if not isinstance(X, pd.DataFrame) else X
    p = model.predict(X)
    return np.clip(np.asarray(p).ravel(), 0.0, 1.0)


def get_shap_values(
    model: Any,
    X: Union[pd.DataFrame, np.ndarray],
) -> Tuple[np.ndarray, List[str]]:
    """Return (shap_values array, feature_names).

    Requires shap>=0.43.0 (hard dependency in requirements.txt).
    SHAP TreeExplainer is used for LightGBM models.
    """
    import shap  # Hard dependency - install with: pip install shap>=0.43.0

    X = np.asarray(X) if not isinstance(X, pd.DataFrame) else X
    explainer = shap.TreeExplainer(model)
    sh = explainer.shap_values(X)
    if isinstance(sh, list):
        sh = sh[1]  # positive class
    names = getattr(model, "feature_name_", None) or [f"f{i}" for i in range(X.shape[1])]
    return np.asarray(sh), list(names)


def top_contributing_features(
    shap_values: np.ndarray,
    feature_names: List[str],
    top_k: int = 10,
) -> List[Dict[str, Any]]:
    """Return list of {feature_name, mean_abs_shap, direction} for explain payload."""
    if shap_values.size == 0 or not feature_names:
        return []
    mean_abs = np.abs(shap_values).mean(axis=0)
    order = np.argsort(-mean_abs)[:top_k]
    return [
        {
            "feature_name": feature_names[i],
            "mean_abs_shap": float(mean_abs[i]),
            "direction": "positive" if shap_values[:, i].mean() >= 0 else "negative",
        }
        for i in order
    ]


def save_artifact(
    model: Any,
    path: Union[str, Path],
    feature_names: List[str],
    feature_version: str,
    monotonic_constraints: Optional[Dict[str, int]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Save LightGBM model and metadata (feature_version, constraint map)."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    model_file = path / "model.txt"
    model.save_model(str(model_file))
    meta = {
        "feature_version": feature_version,
        "feature_names": feature_names,
        "monotonic_constraints": monotonic_constraints or {},
        **(metadata or {}),
    }
    with open(path / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def load_artifact(path: Union[str, Path]) -> Tuple[Any, Dict[str, Any]]:
    """Load model and metadata from artifact dir."""
    path = Path(path)
    if not LGBM_AVAILABLE:
        raise ImportError("lightgbm is required to load model")
    model = lgb.Booster(model_file=str(path / "model.txt"))
    with open(path / "metadata.json", "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return model, metadata
