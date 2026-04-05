"""Probability calibration for escalation model outputs.

Fits isotonic regression (preferred) or Platt scaling (logistic fallback)
on the validation partition only, ensuring no information from training
leaks into the calibration fit.

Returns a calibrated probability array and a serializable calibrator
artifact suitable for storage in the model registry alongside the
raw model artifact.
"""
from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from joblib import dump as joblib_dump
from joblib import load as joblib_load
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

logger = logging.getLogger("althea.training.calibration")


@dataclass
class CalibrationResult:
    calibrator: Any  # fitted sklearn calibrator
    artifact_bytes: bytes
    method: str  # "isotonic" | "platt"
    calibration_error_before: float
    calibration_error_after: float
    metadata: dict[str, Any]

    @property
    def ece_before(self) -> float:
        return float(self.calibration_error_before)

    @property
    def ece_after(self) -> float:
        return float(self.calibration_error_after)


class ProbabilityCalibrator:
    """Fit a probability calibrator on validation predictions.

    The calibrator is fitted ONLY on validation data to avoid
    over-fitting on the training distribution.

    Usage
    -----
    raw_probs = model.predict_proba(X_val)[:, 1]
    result = calibrator.fit(raw_probs, y_val)
    calibrated = calibrator.transform(raw_probs_test, result.calibrator)
    """

    def __init__(self, method: str = "isotonic") -> None:
        if method not in {"isotonic", "platt"}:
            raise ValueError("method must be 'isotonic' or 'platt'")
        self._method = method

    def fit(
        self,
        raw_probs: np.ndarray,
        y_true: np.ndarray | pd.Series,
        n_bins: int = 10,
    ) -> CalibrationResult:
        """Fit calibrator on validation predictions.

        Parameters
        ----------
        raw_probs : uncalibrated probabilities from the model [0, 1]
        y_true    : true binary labels from the validation set
        n_bins    : number of bins for calibration error computation

        Returns
        -------
        CalibrationResult with fitted calibrator and quality metrics.
        """
        raw_probs = np.clip(np.asarray(raw_probs, dtype=float), 0.0, 1.0)
        y = np.asarray(y_true, dtype=int)

        ece_before = self._expected_calibration_error(raw_probs, y, n_bins)

        if self._method == "isotonic":
            calibrator = IsotonicRegression(out_of_bounds="clip")
            calibrator.fit(raw_probs, y)
            calibrated = calibrator.predict(raw_probs)
        else:
            # Platt scaling: fit logistic regression on log-odds
            eps = 1e-7
            log_odds = np.log(np.clip(raw_probs, eps, 1 - eps) / np.clip(1 - raw_probs, eps, 1 - eps))
            calibrator = LogisticRegression(C=1.0, max_iter=1000, solver="lbfgs")
            calibrator.fit(log_odds.reshape(-1, 1), y)
            calibrated = calibrator.predict_proba(log_odds.reshape(-1, 1))[:, 1]

        calibrated = np.clip(calibrated, 0.0, 1.0)
        ece_after = self._expected_calibration_error(calibrated, y, n_bins)

        buf = io.BytesIO()
        joblib_dump(calibrator, buf)
        artifact_bytes = buf.getvalue()

        metadata = {
            "calibration_method": self._method,
            "n_val_samples": int(len(y)),
            "positive_rate_val": float(y.mean()),
            "ece_before": round(ece_before, 6),
            "ece_after": round(ece_after, 6),
            "improvement": round(ece_before - ece_after, 6),
        }

        logger.info(
            json.dumps(
                {
                    "event": "calibration_fit_complete",
                    "method": self._method,
                    "ece_before": ece_before,
                    "ece_after": ece_after,
                },
                ensure_ascii=True,
            )
        )

        return CalibrationResult(
            calibrator=calibrator,
            artifact_bytes=artifact_bytes,
            method=self._method,
            calibration_error_before=ece_before,
            calibration_error_after=ece_after,
            metadata=metadata,
        )

    def transform(
        self,
        raw_probs: np.ndarray,
        calibrator: Any,
    ) -> np.ndarray:
        """Apply a fitted calibrator to new raw probabilities."""
        raw_probs = np.clip(np.asarray(raw_probs, dtype=float), 0.0, 1.0)
        if isinstance(calibrator, IsotonicRegression):
            return np.clip(calibrator.predict(raw_probs), 0.0, 1.0)
        if isinstance(calibrator, LogisticRegression):
            eps = 1e-7
            log_odds = np.log(np.clip(raw_probs, eps, 1 - eps) / np.clip(1 - raw_probs, eps, 1 - eps))
            return np.clip(calibrator.predict_proba(log_odds.reshape(-1, 1))[:, 1], 0.0, 1.0)
        # Generic sklearn calibrator
        return np.clip(np.asarray(calibrator.predict(raw_probs.reshape(-1, 1)), dtype=float), 0.0, 1.0)

    def load_calibrator(self, artifact_bytes: bytes) -> Any:
        """Deserialize a stored calibrator artifact."""
        return joblib_load(io.BytesIO(artifact_bytes))

    # ------------------------------------------------------------------

    @staticmethod
    def _expected_calibration_error(
        probs: np.ndarray,
        y: np.ndarray,
        n_bins: int,
    ) -> float:
        """Compute Expected Calibration Error (ECE) across equal-frequency bins."""
        if len(probs) == 0:
            return 0.0
        probs = np.clip(np.asarray(probs, dtype=float), 0.0, 1.0)
        y = np.asarray(y, dtype=float)
        bins = np.linspace(0.0, 1.0, int(max(2, n_bins)) + 1)
        indices = np.digitize(probs, bins, right=True) - 1
        indices = np.clip(indices, 0, len(bins) - 2)

        ece = 0.0
        total = float(len(probs))
        for b in range(len(bins) - 1):
            mask = indices == b
            if not np.any(mask):
                continue
            confidence = float(probs[mask].mean())
            accuracy = float(y[mask].mean())
            ece += abs(accuracy - confidence) * (float(mask.sum()) / total)
        return float(ece)
