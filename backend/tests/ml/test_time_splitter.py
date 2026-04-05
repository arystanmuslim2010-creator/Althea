"""Tests for TimeBasedSplitter — proper time splits, no entity leakage."""
from __future__ import annotations

import pandas as pd
import pytest

from training.splitter import TimeBasedSplitter


def _make_df(n=200, entity_col=None):
    import numpy as np
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    df = pd.DataFrame({
        "alert_id": [f"A{i}" for i in range(n)],
        "alert_created_at": dates,
        "escalation_label": rng.integers(0, 2, n),
        "risk_score": rng.uniform(0, 100, n),
    })
    if entity_col:
        # 50 unique entities, each appearing ~4 times
        df[entity_col] = [f"E{i % 50}" for i in range(n)]
    return df


class TestPositionalSplit:
    def test_fractions_sum_to_one(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15)
        df = _make_df(200)
        result = splitter.split(df)
        total = len(result.train) + len(result.validation) + len(result.test)
        assert total == len(df)

    def test_train_before_val_before_test(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15)
        df = _make_df(200)
        result = splitter.split(df)
        max_train_time = result.train["alert_created_at"].max()
        min_val_time = result.validation["alert_created_at"].min()
        max_val_time = result.validation["alert_created_at"].max()
        min_test_time = result.test["alert_created_at"].min()
        assert max_train_time <= min_val_time, "Train must end before validation starts"
        assert max_val_time <= min_test_time, "Validation must end before test starts"

    def test_train_is_largest_partition(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15)
        df = _make_df(300)
        result = splitter.split(df)
        assert len(result.train) > len(result.validation)
        assert len(result.train) > len(result.test)

    def test_minimum_rows_enforced(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15)
        with pytest.raises((ValueError, AssertionError)):
            splitter.split(pd.DataFrame())


class TestEntityAwareSplit:
    def test_no_entity_leakage(self):
        """The same entity must not appear in both train and test partitions."""
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15, entity_col="customer_id")
        df = _make_df(200, entity_col="customer_id")
        result = splitter.split(df)

        train_entities = set(result.train["customer_id"])
        test_entities = set(result.test["customer_id"])
        overlap = train_entities & test_entities
        assert not overlap, f"Entity leakage detected: {overlap}"

    def test_entity_split_covers_all_rows(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15, entity_col="customer_id")
        df = _make_df(200, entity_col="customer_id")
        result = splitter.split(df)
        total = len(result.train) + len(result.validation) + len(result.test)
        assert total == len(df)

    def test_metadata_contains_split_info(self):
        splitter = TimeBasedSplitter(train_frac=0.70, val_frac=0.15)
        df = _make_df(120)
        result = splitter.split(df)
        assert "train_size" in result.metadata
        assert "val_size" in result.metadata
        assert "test_size" in result.metadata
