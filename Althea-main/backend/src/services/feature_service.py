"""Service facade for feature engineering and data preparation."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import pandas as pd

from .. import config, demo_data, features, utils

MissingColumnsError = features.MissingColumnsError


class FeatureService:
    """Facade over feature engineering and data preparation logic."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)

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
            return features.load_transactions_csv(uploaded_file)
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
            return features.compute_behavioral_baselines(df, status_cb=status_cb, progress_cb=progress_cb)
        except Exception:
            self._logger.exception("Failed to compute behavioral baselines")
            raise

    def build_feature_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            return features.build_feature_matrix(df)
        except Exception:
            self._logger.exception("Failed to build feature matrix")
            raise
