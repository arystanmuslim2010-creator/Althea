"""Service facade for scoring and explainability logic."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional, Tuple

import pandas as pd

from .. import explain, features, utils

if TYPE_CHECKING:
    from sklearn.ensemble import IsolationForest


class ScoringService:
    """Facade over scoring, anomaly detection, and explainability."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)

    def run_anomaly_detection(
        self,
        df: pd.DataFrame,
        X: pd.DataFrame,
        model: "Optional[IsolationForest]" = None,
    ) -> pd.DataFrame:
        """Apply anomaly detection.  Pass model= (pre-trained IsolationForest) at inference time."""
        try:
            return features.run_anomaly_detection(df, X, model=model)
        except Exception:
            self._logger.exception("Failed to run anomaly detection")
            raise

    def train_risk_model(
        self,
        df: pd.DataFrame,
        X: pd.DataFrame,
        status_cb: Optional[Callable[[str], None]] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> Tuple[float, float, float]:
        try:
            return features.train_risk_model(df, X, status_cb=status_cb, progress_cb=progress_cb)
        except Exception:
            self._logger.exception("Failed to train risk model")
            raise

    def generate_explainability_drivers(
        self,
        df: pd.DataFrame,
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> pd.DataFrame:
        try:
            return explain.generate_explainability_drivers(df, progress_cb=progress_cb)
        except Exception:
            self._logger.exception("Failed to generate explainability drivers")
            raise
