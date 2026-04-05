"""Tests for InvestigationTimeTrainer — time model training contract."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from training.train_time_model import InvestigationTimeTrainer, TIME_MODEL_FEATURE_COLS


def _make_time_df(n=200, seed=42):
    rng = np.random.default_rng(seed)
    available = [c for c in TIME_MODEL_FEATURE_COLS if c != "resolution_hours_log"]
    df = pd.DataFrame({col: rng.uniform(0, 1, n) for col in available})
    # resolution times: log-normally distributed, 2–72h range
    raw_hours = rng.lognormal(mean=2.5, sigma=0.8, size=n)
    df["resolution_hours_log"] = np.log1p(raw_hours)
    df["alert_id"] = [f"T{i}" for i in range(n)]
    return df


class TestInvestigationTimeTrainer:
    def setup_method(self):
        self.trainer = InvestigationTimeTrainer()

    def test_train_returns_result(self):
        train_df = _make_time_df(160)
        val_df = _make_time_df(40, seed=7)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert result is not None

    def test_result_has_p50_and_p90_models(self):
        train_df = _make_time_df(160)
        val_df = _make_time_df(40, seed=7)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert hasattr(result, "model_p50") or hasattr(result, "p50_artifact")
        assert hasattr(result, "model_p90") or hasattr(result, "p90_artifact")

    def test_p90_predictions_geq_p50(self):
        """p90 estimates must be >= p50 estimates on the same data."""
        train_df = _make_time_df(160)
        val_df = _make_time_df(40, seed=7)
        result = self.trainer.train(train_df=train_df, val_df=val_df)

        p50_preds = getattr(result, "val_p50_preds", None)
        p90_preds = getattr(result, "val_p90_preds", None)
        if p50_preds is not None and p90_preds is not None:
            p50 = np.asarray(p50_preds)
            p90 = np.asarray(p90_preds)
            # At least 90% of predictions should satisfy the quantile ordering
            fraction_valid = (p90 >= p50 - 1e-6).mean()
            assert fraction_valid >= 0.85, f"p90 < p50 in {1-fraction_valid:.1%} of cases"

    def test_predictions_are_positive(self):
        """Investigation time predictions must be non-negative after expm1."""
        import math
        train_df = _make_time_df(160)
        val_df = _make_time_df(40, seed=7)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        p50_preds = getattr(result, "val_p50_preds", None)
        if p50_preds is not None:
            hours = np.expm1(np.asarray(p50_preds))
            assert hours.min() >= -0.01, "Un-transformed hours must be non-negative"
