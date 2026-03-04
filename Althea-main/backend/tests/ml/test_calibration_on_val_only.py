"""Test that calibration is fit on val only (conceptually: we check fit/apply behavior)."""
import pytest
import numpy as np
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from src.ml.calibration import fit_calibrator, apply_calibrator
from src.ml.calibration_metrics import brier_score, ece


def test_fit_calibrator_on_val_only():
    """Calibrator fit on val_scores/y_val should not use train/test."""
    np.random.seed(42)
    val_scores = np.clip(np.random.rand(200), 0.1, 0.9)
    y_val = (np.random.rand(200) < 0.2).astype(float)
    cal = fit_calibrator(val_scores, y_val, method="isotonic")
    assert cal is not None
    out = apply_calibrator(cal, val_scores)
    assert out.min() >= 0 and out.max() <= 1
    assert len(out) == len(val_scores)


def test_brier_and_ece():
    y = np.array([0, 1, 0, 1])
    p = np.array([0.2, 0.8, 0.3, 0.7])
    b = brier_score(y, p)
    assert 0 <= b <= 1
    e = ece(y, p, n_bins=2)
    assert e >= 0
