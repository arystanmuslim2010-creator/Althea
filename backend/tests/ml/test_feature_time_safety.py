"""Test that features at time t do not use t+1 (no leakage)."""
import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path
_backend = Path(__file__).resolve().parent.parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from src.ml.features_time_safe import (
    add_rule_fatigue,
    add_entity_alert_velocity,
    build_time_safe_features,
    FEATURE_VERSION,
)


def test_rule_fatigue_no_future():
    """For row at t, rule_fatigue must use only past rows (ts < t)."""
    df = pd.DataFrame({
        "alert_created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
        "rule_id": ["R1", "R1", "R1", "R1"],
        "y_sar": [0, 1, 0, 0],
    })
    df = add_rule_fatigue(df, "alert_created_at", rule_col="rule_id", outcome_col="y_sar")
    # Row 0: no past -> 0 or 0/1
    assert df["rule_fatigue"].iloc[0] == 0.0
    # Row 1: 1 past alert, 0 TP -> alerts/1 = 1
    assert df["rule_fatigue"].iloc[1] == 1.0
    # Row 2: 2 past, 1 TP -> 2/1 = 2
    assert df["rule_fatigue"].iloc[2] == 2.0
    # Row 3: 3 past, 1 TP -> 3/1 = 3
    assert df["rule_fatigue"].iloc[3] == 3.0


def test_entity_alert_velocity_as_of_time():
    """Entity velocity at t should count only alerts in (t-window, t] for that entity."""
    df = pd.DataFrame({
        "alert_created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-05", "2024-01-10"]),
        "entity_id": ["E1", "E1", "E1", "E2"],
    })
    df = add_entity_alert_velocity(df, "alert_created_at", entity_col="entity_id", windows_days=[7])
    # First row E1: only self in 7d -> 1
    assert df["entity_alert_velocity_7d"].iloc[0] == 1
    # Second row E1: 2 alerts in 7d (day 1 and 2)
    assert df["entity_alert_velocity_7d"].iloc[1] == 2
    # Third row E1: day 5; day 1,2,5 in 7d -> 3
    assert df["entity_alert_velocity_7d"].iloc[2] == 3
    # Fourth row E2: only self
    assert df["entity_alert_velocity_7d"].iloc[3] == 1


def test_build_time_safe_returns_version():
    df = pd.DataFrame({
        "alert_created_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        "rule_id": ["R1", "R1"],
        "y_sar": [0, 1],
        "entity_id": ["E1", "E1"],
    })
    df_out, feats, version = build_time_safe_features(
        df,
        time_col="alert_created_at",
        rule_col="rule_id",
        entity_col="entity_id",
        outcome_col="y_sar",
    )
    assert version == FEATURE_VERSION
    assert "rule_fatigue" in feats or "entity_alert_velocity" in feats or "recency_weighted" in str(feats)
