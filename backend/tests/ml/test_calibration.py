"""Tests for ProbabilityCalibrator — fitting and ECE reduction."""
from __future__ import annotations

import numpy as np
import pytest

from training.calibration import ProbabilityCalibrator


def _make_miscalibrated(n=1000, seed=42):
    """Return (raw_probs, y_true) where raw probs are systematically overconfident."""
    rng = np.random.default_rng(seed)
    y_true = rng.integers(0, 2, n)
    # Overconfident: push probabilities away from 0.5
    raw = rng.beta(3, 3, n)
    raw = np.clip(raw * 1.6 - 0.3, 0.01, 0.99)
    return raw.astype(float), y_true.astype(int)


class TestProbabilityCalibrator:
    def setup_method(self):
        self.cal = ProbabilityCalibrator()

    def test_fit_returns_result(self):
        raw, y = _make_miscalibrated()
        result = self.cal.fit(raw_probs=raw, y_true=y)
        assert result is not None

    def test_ece_after_leq_before(self):
        """ECE after calibration must be <= ECE before calibration."""
        raw, y = _make_miscalibrated(2000)
        result = self.cal.fit(raw_probs=raw, y_true=y)
        assert result.ece_after <= result.ece_before + 0.01, (
            f"Calibration made things worse: ECE before={result.ece_before:.4f}, after={result.ece_after:.4f}"
        )

    def test_transform_output_in_0_1(self):
        raw, y = _make_miscalibrated()
        result = self.cal.fit(raw_probs=raw, y_true=y)
        calibrated = self.cal.transform(raw, result.calibrator)
        arr = np.asarray(calibrated)
        assert arr.min() >= 0.0 - 1e-9
        assert arr.max() <= 1.0 + 1e-9

    def test_artifact_bytes_serializable(self):
        raw, y = _make_miscalibrated()
        result = self.cal.fit(raw_probs=raw, y_true=y)
        assert result.artifact_bytes is not None
        assert len(result.artifact_bytes) > 0

    def test_ece_fields_present(self):
        raw, y = _make_miscalibrated()
        result = self.cal.fit(raw_probs=raw, y_true=y)
        assert hasattr(result, "ece_before")
        assert hasattr(result, "ece_after")
        assert 0.0 <= result.ece_before <= 1.0
        assert 0.0 <= result.ece_after <= 1.0

    def test_method_field_set(self):
        raw, y = _make_miscalibrated()
        result = self.cal.fit(raw_probs=raw, y_true=y)
        assert result.method in ("isotonic", "platt", "logistic")

    def test_calibrator_can_be_reloaded(self):
        """Serialized calibrator must produce identical outputs after round-trip."""
        import joblib, io
        raw, y = _make_miscalibrated(500)
        result = self.cal.fit(raw_probs=raw, y_true=y)

        buf = io.BytesIO(result.artifact_bytes)
        reloaded = joblib.load(buf)

        out_original = self.cal.transform(raw[:20], result.calibrator)
        out_reloaded = self.cal.transform(raw[:20], reloaded)
        np.testing.assert_allclose(
            np.asarray(out_original),
            np.asarray(out_reloaded),
            atol=1e-8,
            err_msg="Reloaded calibrator must produce identical outputs",
        )
