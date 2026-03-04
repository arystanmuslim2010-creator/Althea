"""Service facade for operations metrics."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pandas as pd

from .. import ops, utils


class OpsService:
    """Facade over operational metrics and case statistics."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)

    def compute_ops_metrics(self, df: pd.DataFrame, analyst_capacity: int, minutes_per_case: int) -> Dict[str, Any]:
        try:
            return ops.compute_ops_metrics(df, analyst_capacity, minutes_per_case)
        except Exception:
            self._logger.exception("Failed to compute ops metrics")
            raise

    def compute_case_status_counts(self, cases: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
        try:
            return ops.compute_case_status_counts(cases)
        except Exception:
            self._logger.exception("Failed to compute case status counts")
            raise

    def get_calibration_metadata(self, data_signature: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Return calibration metrics from model cache (Brier, AUC, reliability bins)."""
        try:
            return ops.get_calibration_metadata(data_signature=data_signature)
        except Exception:
            self._logger.exception("Failed to load calibration metadata")
            return None
