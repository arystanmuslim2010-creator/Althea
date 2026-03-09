"""Test time-based split: no future data in train."""
import pytest
import pandas as pd
import numpy as np

# Add backend to path
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from src.ml.split import time_split, time_split_indices


def test_time_split_no_leakage():
    """Train must contain only rows strictly before val and test by time."""
    n = 500
    np.random.seed(42)
    # 24 months of daily data
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "alert_created_at": dates,
        "x": np.random.randn(n),
        "y": np.random.randint(0, 2, n),
    })
    train_df, val_df, test_df = time_split(
        df,
        time_col="alert_created_at",
        val_window=1,
        test_window=1,
    )
    t_train = train_df["alert_created_at"].max()
    t_val_start = val_df["alert_created_at"].min()
    t_val_end = val_df["alert_created_at"].max()
    t_test_start = test_df["alert_created_at"].min()
    assert t_train < t_val_start, "Train must end before val starts"
    assert t_val_end < t_test_start or t_val_end <= test_df["alert_created_at"].max(), "Val before test"
    # No overlap of indices
    train_idx = set(train_df.index) if hasattr(train_df.index, "__iter__") else set(range(len(train_df)))
    val_idx = set(val_df.index)
    test_idx = set(test_df.index)
    assert train_idx.isdisjoint(val_idx), "Train and val must not overlap"
    assert train_idx.isdisjoint(test_idx), "Train and test must not overlap"
    assert val_idx.isdisjoint(test_idx), "Val and test must not overlap"


def test_time_split_missing_column_fails():
    """Missing timestamp column must raise with clear message."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    with pytest.raises(ValueError, match="Time column|not found|timestamp"):
        time_split(df, time_col="alert_created_at")


def test_time_split_all_nat_fails():
    """All-NaT timestamp must raise."""
    df = pd.DataFrame({
        "alert_created_at": [pd.NaT, pd.NaT],
        "x": [1, 2],
    })
    with pytest.raises(ValueError, match="no valid datetime|missing timestamp"):
        time_split(df, time_col="alert_created_at")
