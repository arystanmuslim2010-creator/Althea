"""Investigation time model trainer.

Trains a LightGBM regression model that predicts how long (in hours)
an alert is expected to take to resolve. Supports median (p50) and
optionally p90 quantile outputs.
"""
from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from joblib import dump as joblib_dump

logger = logging.getLogger("althea.training.time_model")

# Feature columns for the time model — overlaps substantially with
# escalation model but adds investigation complexity proxies
TIME_MODEL_FEATURE_COLS = [
    # Alert-native
    "amount",
    "amount_log1p",
    "hour_of_day",
    "day_of_week",
    "typology_code",
    "segment_code",
    "country_risk",
    # Behavioral
    "txn_count_7d",
    "txn_count_30d",
    "new_counterparty_ratio",
    "cross_border_ratio",
    # Historical
    "prior_alert_count",
    "prior_escalation_rate",
    "prior_similar_case_time_mean",
    "prior_similar_case_time_p90",
    "prior_touch_count",
    # Cost / complexity
    "linked_entity_count",
    "graph_complexity_proxy",
    "graph_degree",
    "unique_counterparties",
]

MINIMAL_TIME_FEATURE_COLS = [
    "amount",
    "amount_log1p",
    "typology_code",
    "segment_code",
    "country_risk",
]


@dataclass
class TimeModelTrainingResult:
    model_p50: Any
    model_p90: Any | None
    feature_columns: list[str]
    feature_schema: dict[str, Any]
    artifact_bytes_p50: bytes
    artifact_bytes_p90: bytes | None
    artifact_format: str
    training_metadata: dict[str, Any]
    metrics: dict[str, Any] = field(default_factory=dict)
    val_p50_preds: np.ndarray | None = None
    val_p90_preds: np.ndarray | None = None


class InvestigationTimeTrainer:
    """Train investigation time estimation models.

    Trains two LightGBM quantile regressors:
    - p50 model: predicts median resolution time
    - p90 model: predicts 90th-percentile resolution time (optional)

    Both operate on the log-transformed target (resolution_hours_log)
    to stabilize variance, then predictions are exponentiated at
    inference time.
    """

    _DEFAULT_LGB_PARAMS_P50: dict[str, Any] = {
        "objective": "quantile",
        "alpha": 0.50,
        "metric": "quantile",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "n_estimators": 400,
        "verbose": -1,
        "n_jobs": -1,
    }

    _DEFAULT_LGB_PARAMS_P90: dict[str, Any] = {
        "objective": "quantile",
        "alpha": 0.90,
        "metric": "quantile",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "n_estimators": 400,
        "verbose": -1,
        "n_jobs": -1,
    }

    def __init__(
        self,
        hyperparams_p50: dict[str, Any] | None = None,
        hyperparams_p90: dict[str, Any] | None = None,
        train_p90: bool = True,
    ) -> None:
        self._hyperparams_p50 = hyperparams_p50 or {}
        self._hyperparams_p90 = hyperparams_p90 or {}
        self._train_p90 = train_p90

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        label_col: str = "resolution_hours_log",
    ) -> TimeModelTrainingResult:
        """Train p50 (and optionally p90) quantile regression models.

        Training target is ``resolution_hours_log`` (log1p transform of hours).
        Returns raw models; the inference service un-transforms predictions.
        """
        self._validate_inputs(train_df, val_df, label_col)

        feature_cols = self._resolve_feature_cols(train_df)
        logger.info(
            json.dumps(
                {
                    "event": "time_model_training_start",
                    "feature_count": len(feature_cols),
                    "train_rows": len(train_df),
                    "val_rows": len(val_df),
                    "median_target_train": float(train_df[label_col].median()),
                },
                ensure_ascii=True,
            )
        )

        X_train, y_train = self._prepare_xy(train_df, feature_cols, label_col)
        X_val, y_val = self._prepare_xy(val_df, feature_cols, label_col)

        try:
            import lightgbm  # noqa: F401
            use_lgb = True
        except Exception:
            use_lgb = False

        if use_lgb:
            model_p50, bytes_p50 = self._train_lightgbm(X_train, y_train, X_val, y_val, quantile=0.50)
            if self._train_p90:
                model_p90, bytes_p90 = self._train_lightgbm(X_train, y_train, X_val, y_val, quantile=0.90)
            else:
                model_p90, bytes_p90 = None, None
            artifact_format = "lgb_booster"
        else:
            model_p50, bytes_p50 = self._train_sklearn_fallback(X_train, y_train)
            model_p90, bytes_p90 = None, None
            artifact_format = "joblib"

        val_p50_preds = np.asarray(model_p50.predict(X_val), dtype=float) if hasattr(model_p50, "predict") else None
        val_p90_preds = (
            np.asarray(model_p90.predict(X_val), dtype=float)
            if model_p90 is not None and hasattr(model_p90, "predict")
            else None
        )

        feature_schema = self._build_feature_schema(X_train, feature_cols)
        training_metadata = {
            "artifact_format": artifact_format,
            "model_type": "investigation_time_regression",
            "framework": "lightgbm" if use_lgb else "sklearn",
            "feature_count": len(feature_cols),
            "train_rows": len(X_train),
            "val_rows": len(X_val),
            "label_col": label_col,
            "label_transform": "log1p",
            "is_active": False,
            "bootstrap_model": False,
            "feature_schema_version": "v2",
        }

        logger.info(
            json.dumps(
                {
                    "event": "time_model_training_complete",
                    "artifact_format": artifact_format,
                    "p90_trained": model_p90 is not None,
                },
                ensure_ascii=True,
            )
        )

        return TimeModelTrainingResult(
            model_p50=model_p50,
            model_p90=model_p90,
            feature_columns=feature_cols,
            feature_schema=feature_schema,
            artifact_bytes_p50=bytes_p50,
            artifact_bytes_p90=bytes_p90,
            artifact_format=artifact_format,
            training_metadata=training_metadata,
            val_p50_preds=val_p50_preds,
            val_p90_preds=val_p90_preds,
        )

    # ------------------------------------------------------------------

    def _train_lightgbm(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        quantile: float,
    ):
        import lightgbm as lgb

        base_params = self._DEFAULT_LGB_PARAMS_P50 if quantile <= 0.5 else self._DEFAULT_LGB_PARAMS_P90
        overrides = self._hyperparams_p50 if quantile <= 0.5 else self._hyperparams_p90
        params = {**base_params, **overrides}
        params["alpha"] = quantile

        model = lgb.LGBMRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
        )
        booster_str = model.booster_.model_to_string()
        return model, booster_str.encode("utf-8")

    def _train_sklearn_fallback(
        self, X_train: pd.DataFrame, y_train: pd.Series
    ):
        from sklearn.ensemble import GradientBoostingRegressor
        model = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            random_state=42,
        )
        model.fit(X_train, y_train)
        buf = io.BytesIO()
        joblib_dump(model, buf)
        return model, buf.getvalue()

    def _resolve_feature_cols(self, df: pd.DataFrame) -> list[str]:
        available = set(df.columns)
        full_cols = [c for c in TIME_MODEL_FEATURE_COLS if c in available]
        if full_cols:
            return full_cols
        minimal = [c for c in MINIMAL_TIME_FEATURE_COLS if c in available]
        if minimal:
            return minimal
        return list(df.select_dtypes(include="number").columns[:10])

    def _prepare_xy(
        self, df: pd.DataFrame, feature_cols: list[str], label_col: str
    ) -> tuple[pd.DataFrame, pd.Series]:
        X = df[feature_cols].copy().replace([np.inf, -np.inf], 0.0).fillna(0.0)
        y = pd.to_numeric(df[label_col], errors="coerce").fillna(0.0)
        return X, y

    @staticmethod
    def _build_feature_schema(X: pd.DataFrame, feature_cols: list[str]) -> dict[str, Any]:
        import hashlib
        columns = [{"name": col, "dtype": str(X[col].dtype), "index": i} for i, col in enumerate(feature_cols)]
        schema_hash = hashlib.sha256(
            json.dumps([c["name"] for c in columns], sort_keys=True).encode()
        ).hexdigest()[:16]
        return {"version": "v2", "schema_hash": schema_hash, "columns": columns}

    @staticmethod
    def _validate_inputs(train_df: pd.DataFrame, val_df: pd.DataFrame, label_col: str) -> None:
        if train_df.empty:
            raise ValueError("Training DataFrame is empty.")
        if val_df.empty:
            raise ValueError("Validation DataFrame is empty.")
        if label_col not in train_df.columns:
            raise ValueError(f"Label column '{label_col}' not found in training data.")
