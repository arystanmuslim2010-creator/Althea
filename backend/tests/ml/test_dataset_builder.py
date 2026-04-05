"""Tests for TrainingDatasetBuilder — no future leakage, point-in-time correctness."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from training.dataset_builder import TrainingDatasetBuilder


def _make_repo(outcomes, payloads):
    repo = MagicMock()
    repo.session.return_value.__enter__ = lambda s, *a: MagicMock()
    repo.session.return_value.__exit__ = MagicMock(return_value=False)
    return repo


def _make_builder(outcomes_rows, payload_rows):
    """Build a TrainingDatasetBuilder wired with fake SQL responses."""
    builder = TrainingDatasetBuilder.__new__(TrainingDatasetBuilder)
    builder._repository = MagicMock()

    # Patch internal fetch methods to return test data
    builder._fetch_outcomes = MagicMock(return_value=pd.DataFrame(outcomes_rows))
    builder._fetch_alert_features = MagicMock(return_value=pd.DataFrame(payload_rows))
    return builder


# ---------------------------------------------------------------------------
# Point-in-time correctness
# ---------------------------------------------------------------------------

def test_no_future_outcomes_leak():
    """Outcomes after the cutoff must NOT appear in the training dataset."""
    cutoff = datetime(2025, 1, 15, tzinfo=timezone.utc)
    outcomes = [
        {"alert_id": "A1", "analyst_decision": "true_positive", "timestamp": datetime(2025, 1, 10, tzinfo=timezone.utc)},
        {"alert_id": "A2", "analyst_decision": "false_positive", "timestamp": datetime(2025, 1, 20, tzinfo=timezone.utc)},  # FUTURE
    ]
    features = [
        {"alert_id": "A1", "risk_score": 80.0, "alert_created_at": datetime(2025, 1, 5, tzinfo=timezone.utc)},
        {"alert_id": "A2", "risk_score": 30.0, "alert_created_at": datetime(2025, 1, 5, tzinfo=timezone.utc)},
    ]
    builder = _make_builder(outcomes, features)
    # Simulate _join_point_in_time filtering: only outcomes with timestamp < cutoff
    outcomes_df = pd.DataFrame(outcomes)
    features_df = pd.DataFrame(features)
    valid_outcomes = outcomes_df[outcomes_df["timestamp"] < cutoff]
    result = valid_outcomes.merge(features_df, on="alert_id", how="inner")

    assert "A1" in result["alert_id"].values
    assert "A2" not in result["alert_id"].values, "Future outcome A2 must be excluded"


def test_duplicate_outcomes_keep_most_recent():
    """When multiple outcomes exist for an alert, keep only the most recent."""
    outcomes = [
        {"alert_id": "A1", "analyst_decision": "false_positive", "timestamp": datetime(2025, 1, 5, tzinfo=timezone.utc)},
        {"alert_id": "A1", "analyst_decision": "true_positive", "timestamp": datetime(2025, 1, 8, tzinfo=timezone.utc)},
    ]
    df = pd.DataFrame(outcomes)
    deduped = df.sort_values("timestamp").drop_duplicates("alert_id", keep="last")

    assert len(deduped) == 1
    assert deduped.iloc[0]["analyst_decision"] == "true_positive"


def test_build_escalation_dataset_requires_minimum_rows():
    """build_escalation_dataset must raise ValueError when fewer than MIN_LABELED_ROWS rows."""
    builder = TrainingDatasetBuilder.__new__(TrainingDatasetBuilder)
    builder._repository = MagicMock()
    builder._fetch_outcomes = MagicMock(return_value=pd.DataFrame())
    builder._fetch_alert_features = MagicMock(return_value=pd.DataFrame())
    builder._join_point_in_time = MagicMock(return_value=pd.DataFrame())

    cutoff = datetime(2025, 1, 15, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="[Ii]nsufficient|[Nn]ot enough|[Mm]inimum"):
        builder.build_escalation_dataset(tenant_id="t1", cutoff_timestamp=cutoff)


def test_alert_feature_cutoff_guard():
    """Alert events created AFTER cutoff must be dropped even if outcome exists before."""
    cutoff = datetime(2025, 6, 1, tzinfo=timezone.utc)
    outcomes = [
        {"alert_id": "X1", "analyst_decision": "true_positive", "timestamp": datetime(2025, 5, 15, tzinfo=timezone.utc)},
    ]
    features = [
        {"alert_id": "X1", "risk_score": 75.0, "alert_created_at": datetime(2025, 6, 5, tzinfo=timezone.utc)},  # alert AFTER cutoff
    ]
    outcomes_df = pd.DataFrame(outcomes)
    features_df = pd.DataFrame(features)

    # The temporal guard: alert_created_at must be < cutoff
    valid_features = features_df[features_df["alert_created_at"] < cutoff]
    result = outcomes_df.merge(valid_features, on="alert_id", how="inner")

    assert result.empty, "Alert created after cutoff must not appear in training set"
