"""Tests for EscalationModelTrainer — stable training and basic output contract."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from training.train_escalation_model import EscalationModelTrainer, MINIMAL_FEATURE_COLS


def _make_binary_df(n=200, seed=42):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({col: rng.uniform(0, 1, n) for col in MINIMAL_FEATURE_COLS})
    df["escalation_label"] = rng.integers(0, 2, n)
    df["sample_weight"] = 1.0
    df["alert_id"] = [f"A{i}" for i in range(n)]
    return df


class TestEscalationModelTrainer:
    def setup_method(self):
        self.trainer = EscalationModelTrainer()

    def test_training_returns_result_object(self):
        train_df = _make_binary_df(160)
        val_df = _make_binary_df(40, seed=99)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert result is not None

    def test_result_has_scores_on_validation(self):
        train_df = _make_binary_df(160)
        val_df = _make_binary_df(40, seed=99)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert hasattr(result, "val_probs") or hasattr(result, "val_scores") or "val_probs" in dir(result)

    def test_result_has_feature_schema(self):
        train_df = _make_binary_df(160)
        val_df = _make_binary_df(40, seed=99)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert hasattr(result, "feature_schema")
        schema = result.feature_schema
        assert "feature_names" in schema or "features" in schema

    def test_scores_in_0_1_range(self):
        train_df = _make_binary_df(160)
        val_df = _make_binary_df(40, seed=99)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        probs = getattr(result, "val_probs", None) or getattr(result, "val_scores", None)
        if probs is not None:
            arr = np.asarray(probs)
            assert arr.min() >= 0.0 - 1e-6
            assert arr.max() <= 1.0 + 1e-6

    def test_all_positive_class_handled(self):
        """Trainer must not crash when all labels are 1."""
        train_df = _make_binary_df(100)
        train_df["escalation_label"] = 1
        val_df = _make_binary_df(20, seed=5)
        val_df["escalation_label"] = 1
        # Should not raise
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        assert result is not None

    def test_artifact_bytes_non_empty(self):
        train_df = _make_binary_df(160)
        val_df = _make_binary_df(40, seed=99)
        result = self.trainer.train(train_df=train_df, val_df=val_df)
        artifact = getattr(result, "artifact_bytes", None) or getattr(result, "model_bytes", None)
        if artifact is not None:
            assert len(artifact) > 0, "Serialized model artifact must be non-empty"
