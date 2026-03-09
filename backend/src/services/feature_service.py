"""Compatibility facade for feature engineering and data preparation."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import pandas as pd

from models.feature_schema import FeatureSchemaValidator
from services.feature_service import EnterpriseFeatureService

from .. import config, demo_data, utils


class MissingColumnsError(ValueError):
    """Raised when required columns are missing from an input dataset."""


class FeatureService:
    """Legacy-facing facade backed by the canonical enterprise feature service."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)
        self._feature_service = EnterpriseFeatureService(FeatureSchemaValidator())

    def generate_demo_data(
        self,
        n_users: int = config.DEMO_DEFAULT_USERS,
        tx_per_user: int = config.DEMO_DEFAULT_TX_PER_USER,
        seed: int = config.DEMO_SEED,
        suspicious_rate: float = config.DEMO_DEFAULT_SUSPICIOUS_RATE,
    ) -> pd.DataFrame:
        try:
            return demo_data.generate_demo_data(
                n_users=n_users,
                tx_per_user=tx_per_user,
                seed=seed,
                suspicious_rate=suspicious_rate,
            )
        except Exception:
            self._logger.exception("Failed to generate demo data")
            raise

    def load_transactions_csv(self, uploaded_file: Any) -> pd.DataFrame:
        try:
            if hasattr(uploaded_file, "read"):
                payload = uploaded_file.read()
                if isinstance(payload, str):
                    payload = payload.encode("utf-8")
                return self._feature_service.load_transactions_csv(payload)
            if isinstance(uploaded_file, (bytes, bytearray)):
                return self._feature_service.load_transactions_csv(bytes(uploaded_file))
            if isinstance(uploaded_file, str):
                return pd.read_csv(uploaded_file)
            raise MissingColumnsError("Unsupported uploaded file payload")
        except Exception:
            self._logger.exception("Failed to load transactions CSV")
            raise

    def compute_behavioral_baselines(
        self,
        df: pd.DataFrame,
        status_cb: Optional[Callable[[str], None]] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> pd.DataFrame:
        try:
            if status_cb:
                status_cb("Computing behavioral baselines")
            out = df.copy()
            if out.empty:
                return out
            if progress_cb:
                progress_cb(30)

            amount = pd.to_numeric(out.get("amount", 0.0), errors="coerce").fillna(0.0)
            user_key = out.get("user_id", pd.Series(["unknown"] * len(out)))
            grouped = amount.groupby(user_key)
            out["user_amount_mean"] = grouped.transform("mean")
            out["user_amount_std"] = grouped.transform("std").fillna(0.0)
            out["user_tx_count"] = grouped.transform("count").astype(float)

            if progress_cb:
                progress_cb(100)
            return out
        except Exception:
            self._logger.exception("Failed to compute behavioral baselines")
            raise

    def build_feature_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            return self._feature_service.generate_inference_features(df)["feature_matrix"]
        except Exception:
            self._logger.exception("Failed to build feature matrix")
            raise
