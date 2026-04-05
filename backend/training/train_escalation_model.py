"""Escalation likelihood model trainer.

Trains a binary classifier (LightGBM preferred, XGBoost fallback) that
predicts whether an alert should be escalated for investigation.

Design principles:
- Feature schema is frozen at training time and stored alongside the artifact
- Class imbalance is handled via scale_pos_weight / class_weight
- Returns a TrainingResult with the model, schema, and training metadata
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

logger = logging.getLogger("althea.training.escalation")

# Feature columns consumed by the escalation model.
# These must be present (or derivable) in the feature bundle output.
# The schema is frozen here and validated at inference time.
ESCALATION_FEATURE_COLS = [
    # Alert-native
    "amount",
    "amount_log1p",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "country_risk",
    "typology_code",
    "segment_code",
    "source_system_code",
    # Behavioral windows
    "txn_count_1d",
    "txn_count_7d",
    "txn_count_30d",
    "amount_sum_7d",
    "amount_avg_7d",
    "velocity_delta_7d",
    "dormant_reactivation",
    "new_counterparty_ratio",
    "cross_border_ratio",
    "round_amount_ratio",
    # Historical outcome
    "prior_alert_count",
    "prior_escalation_rate",
    "prior_sar_rate",
    "prior_fp_rate",
    "days_since_last_alert",
    # Graph proxies
    "graph_degree",
    "unique_counterparties",
    "suspicious_neighbor_ratio",
    "graph_component_size",
    # Cost proxies
    "linked_entity_count",
    "graph_complexity_proxy",
]

# Minimal fallback column set — used when full feature pipeline is unavailable
# (e.g. during bootstrap training from legacy data without behavioral features)
MINIMAL_FEATURE_COLS = [
    "amount",
    "amount_log1p",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "country_risk",
    "typology_code",
    "segment_code",
    "source_system_code",
]


@dataclass
class EscalationTrainingResult:
    model: Any
    feature_columns: list[str]
    feature_schema: dict[str, Any]
    artifact_bytes: bytes
    artifact_format: str
    training_metadata: dict[str, Any]
    metrics: dict[str, Any] = field(default_factory=dict)
    val_probs: list[float] | None = None
    val_scores: list[float] | None = None


class EscalationModelTrainer:
    """Train a binary escalation likelihood classifier.

    ``hyperparams`` overrides the default LightGBM / XGBoost parameters.
    Pass ``use_xgboost=True`` to force XGBoost even if LightGBM is available.
    """

    _DEFAULT_LGB_PARAMS: dict[str, Any] = {
        "objective": "binary",
        "metric": "average_precision",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "lambda_l1": 0.1,
        "lambda_l2": 0.1,
        "n_estimators": 500,
        "early_stopping_rounds": 50,
        "verbose": -1,
        "n_jobs": -1,
    }

    _DEFAULT_XGB_PARAMS: dict[str, Any] = {
        "objective": "binary:logistic",
        "eval_metric": "aucpr",
        "learning_rate": 0.05,
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "alpha": 0.1,
        "lambda": 0.1,
        "n_estimators": 500,
        "early_stopping_rounds": 50,
        "verbosity": 0,
        "n_jobs": -1,
    }

    def __init__(
        self,
        hyperparams: dict[str, Any] | None = None,
        use_xgboost: bool = False,
    ) -> None:
        self._hyperparams = hyperparams or {}
        self._force_xgboost = use_xgboost

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        label_col: str = "escalation_label",
        weight_col: str | None = "sample_weight",
    ) -> EscalationTrainingResult:
        """Train the escalation model.

        Parameters
        ----------
        train_df : labeled training partition
        val_df   : labeled validation partition (used for early stopping and calibration)
        label_col : column name for binary label (0/1)
        weight_col : optional per-sample weight column

        Returns
        -------
        EscalationTrainingResult with fitted model and metadata.
        """
        self._validate_inputs(train_df, val_df, label_col)

        feature_cols = self._resolve_feature_cols(train_df)
        logger.info(
            json.dumps(
                {
                    "event": "escalation_training_start",
                    "feature_count": len(feature_cols),
                    "train_rows": len(train_df),
                    "val_rows": len(val_df),
                    "positive_rate_train": float(train_df[label_col].mean()),
                    "feature_mode": "full" if len(feature_cols) == len(ESCALATION_FEATURE_COLS) else "minimal",
                },
                ensure_ascii=True,
            )
        )

        X_train, y_train, w_train = self._prepare_xy(train_df, feature_cols, label_col, weight_col)
        X_val, y_val, _w_val = self._prepare_xy(val_df, feature_cols, label_col, weight_col)

        # Compute class imbalance weight
        pos_rate = float(y_train.mean())
        scale_pos_weight = (1.0 - pos_rate) / pos_rate if pos_rate > 0.0 else 1.0

        if int(y_train.nunique()) < 2:
            model, artifact_bytes, artifact_format = self._train_single_class_fallback(
                X_train=X_train,
                y_train=y_train,
            )
        else:
            try:
                import lightgbm as lgb
                use_lgb = not self._force_xgboost
            except Exception:
                use_lgb = False

            if use_lgb:
                model, artifact_bytes, artifact_format = self._train_lightgbm(
                    X_train, y_train, w_train, X_val, y_val, scale_pos_weight
                )
            else:
                model, artifact_bytes, artifact_format = self._train_xgboost(
                    X_train, y_train, w_train, X_val, y_val, scale_pos_weight
                )

        val_probs = self._predict_probabilities(model=model, X_val=X_val)

        feature_schema = self._build_feature_schema(X_train, feature_cols)

        training_metadata = {
            "artifact_format": artifact_format,
            "model_type": "escalation_classifier",
            "framework": "lightgbm" if artifact_format.startswith("lgb") else "xgboost",
            "feature_count": len(feature_cols),
            "feature_mode": "full" if len(feature_cols) == len(ESCALATION_FEATURE_COLS) else "minimal",
            "train_rows": len(X_train),
            "val_rows": len(X_val),
            "positive_rate_train": float(y_train.mean()),
            "scale_pos_weight": scale_pos_weight,
            "is_active": False,
            "bootstrap_model": False,
            "feature_schema_version": "v2",
        }

        logger.info(
            json.dumps(
                {
                    "event": "escalation_training_complete",
                    "artifact_format": artifact_format,
                    "artifact_bytes": len(artifact_bytes),
                    "feature_count": len(feature_cols),
                },
                ensure_ascii=True,
            )
        )

        return EscalationTrainingResult(
            model=model,
            feature_columns=feature_cols,
            feature_schema=feature_schema,
            artifact_bytes=artifact_bytes,
            artifact_format=artifact_format,
            training_metadata=training_metadata,
            val_probs=val_probs.tolist(),
            val_scores=val_probs.tolist(),
        )

    # ------------------------------------------------------------------
    # Framework-specific trainers
    # ------------------------------------------------------------------

    def _train_lightgbm(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        w_train: pd.Series | None,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        scale_pos_weight: float,
    ):
        import lightgbm as lgb

        params = {**self._DEFAULT_LGB_PARAMS, **self._hyperparams}
        params["scale_pos_weight"] = scale_pos_weight

        # LightGBM sklearn API supports early stopping via callbacks
        early_stop = lgb.early_stopping(
            stopping_rounds=int(params.pop("early_stopping_rounds", 50)),
            verbose=False,
        )
        log_eval = lgb.log_evaluation(period=-1)  # suppress per-iter output

        model = lgb.LGBMClassifier(**{k: v for k, v in params.items() if k not in ("early_stopping_rounds",)})
        model.fit(
            X_train, y_train,
            sample_weight=w_train.to_numpy() if w_train is not None else None,
            eval_set=[(X_val, y_val)],
            callbacks=[early_stop, log_eval],
        )

        booster_str = model.booster_.model_to_string()
        artifact_bytes = booster_str.encode("utf-8")
        return model, artifact_bytes, "lgb_booster"

    def _train_xgboost(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        w_train: pd.Series | None,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        scale_pos_weight: float,
    ):
        import xgboost as xgb

        params = {**self._DEFAULT_XGB_PARAMS, **self._hyperparams}
        params["scale_pos_weight"] = scale_pos_weight

        early_stopping = int(params.pop("early_stopping_rounds", 50))
        model = xgb.XGBClassifier(**params, early_stopping_rounds=early_stopping)
        model.fit(
            X_train, y_train,
            sample_weight=w_train.to_numpy() if w_train is not None else None,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        buf = io.BytesIO()
        joblib_dump(model, buf)
        artifact_bytes = buf.getvalue()
        return model, artifact_bytes, "xgb_sklearn"

    def _train_single_class_fallback(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
    ):
        from sklearn.dummy import DummyClassifier

        constant = int(pd.to_numeric(y_train, errors="coerce").fillna(0).iloc[0])
        model = DummyClassifier(strategy="constant", constant=constant)
        model.fit(X_train, y_train.astype(int))
        buf = io.BytesIO()
        joblib_dump(model, buf)
        return model, buf.getvalue(), "joblib"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_feature_cols(self, train_df: pd.DataFrame) -> list[str]:
        """Return the intersection of desired feature columns and available columns."""
        available = set(train_df.columns)
        full_cols = [c for c in ESCALATION_FEATURE_COLS if c in available]
        if len(full_cols) >= len(MINIMAL_FEATURE_COLS):
            return full_cols
        minimal_cols = [c for c in MINIMAL_FEATURE_COLS if c in available]
        if not minimal_cols:
            # Use any numeric column as last resort
            numeric = list(train_df.select_dtypes(include="number").columns)
            return [c for c in numeric if c not in ("escalation_label", "sar_label", "sample_weight")]
        return minimal_cols

    def _prepare_xy(
        self,
        df: pd.DataFrame,
        feature_cols: list[str],
        label_col: str,
        weight_col: str | None,
    ) -> tuple[pd.DataFrame, pd.Series, pd.Series | None]:
        X = df[feature_cols].copy().replace([np.inf, -np.inf], 0.0).fillna(0.0)
        y = df[label_col].astype(int)
        w: pd.Series | None = None
        if weight_col and weight_col in df.columns:
            w = pd.to_numeric(df[weight_col], errors="coerce").fillna(1.0)
        return X, y, w

    @staticmethod
    def _build_feature_schema(X: pd.DataFrame, feature_cols: list[str]) -> dict[str, Any]:
        columns = [
            {"name": col, "dtype": str(X[col].dtype), "index": i}
            for i, col in enumerate(feature_cols)
        ]
        import hashlib
        schema_hash = hashlib.sha256(
            json.dumps([c["name"] for c in columns], sort_keys=True).encode()
        ).hexdigest()[:16]
        return {
            "version": "v2",
            "schema_hash": schema_hash,
            "columns": columns,
            "feature_count": len(columns),
            "feature_names": [item["name"] for item in columns],
            "features": [item["name"] for item in columns],
        }

    @staticmethod
    def _validate_inputs(
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        label_col: str,
    ) -> None:
        if train_df.empty:
            raise ValueError("Training DataFrame is empty.")
        if val_df.empty:
            raise ValueError("Validation DataFrame is empty.")
        if label_col not in train_df.columns:
            raise ValueError(f"Label column '{label_col}' not found in training data.")
        if label_col not in val_df.columns:
            raise ValueError(f"Label column '{label_col}' not found in validation data.")

    @staticmethod
    def _predict_probabilities(model: Any, X_val: pd.DataFrame) -> np.ndarray:
        if hasattr(model, "predict_proba"):
            pred = np.asarray(model.predict_proba(X_val))
            if pred.ndim == 2 and pred.shape[1] > 1:
                return np.clip(pred[:, 1].astype(float), 0.0, 1.0)
            return np.clip(pred.ravel().astype(float), 0.0, 1.0)
        if hasattr(model, "predict"):
            pred = np.asarray(model.predict(X_val))
            return np.clip(pred.astype(float), 0.0, 1.0)
        return np.zeros(len(X_val), dtype=float)
