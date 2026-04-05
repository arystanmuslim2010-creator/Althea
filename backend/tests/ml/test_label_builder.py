"""Tests for LabelBuilder — correct label mapping and sample weights."""
from __future__ import annotations

import pandas as pd
import pytest

from training.label_builder import LabelBuilder, POSITIVE_DECISIONS, NEGATIVE_DECISIONS


def _base_df(decisions):
    return pd.DataFrame({
        "alert_id": [f"A{i}" for i in range(len(decisions))],
        "analyst_decision": decisions,
        "timestamp": pd.Timestamp("2025-01-01", tz="UTC"),
        "final_label_status": "final",
    })


class TestEscalationLabels:
    def setup_method(self):
        self.builder = LabelBuilder()

    def test_positive_decisions_get_label_one(self):
        df = _base_df(list(POSITIVE_DECISIONS))
        result = self.builder.build_escalation_labels(df)
        assert (result["escalation_label"] == 1).all(), "All positive decisions must map to label 1"

    def test_negative_decisions_get_label_zero(self):
        df = _base_df(list(NEGATIVE_DECISIONS))
        result = self.builder.build_escalation_labels(df)
        assert (result["escalation_label"] == 0).all(), "All negative decisions must map to label 0"

    def test_sar_filed_sets_sar_label(self):
        df = _base_df(["sar_filed"])
        result = self.builder.build_escalation_labels(df)
        assert result.iloc[0]["sar_label"] == 1

    def test_non_sar_does_not_set_sar_label(self):
        df = _base_df(["true_positive", "false_positive"])
        result = self.builder.build_escalation_labels(df)
        assert result.iloc[0]["sar_label"] == 0
        assert result.iloc[1]["sar_label"] == 0

    def test_sample_weights_sar_highest(self):
        decisions = ["sar_filed", "escalated", "true_positive", "false_positive"]
        df = _base_df(decisions)
        result = self.builder.build_escalation_labels(df)
        weights = result.set_index("analyst_decision")["sample_weight"]
        assert weights["sar_filed"] > weights["escalated"]
        assert weights["escalated"] >= weights["true_positive"]
        assert weights["true_positive"] > weights["false_positive"]

    def test_confirmed_suspicious_is_positive(self):
        df = _base_df(["confirmed_suspicious"])
        result = self.builder.build_escalation_labels(df)
        assert result.iloc[0]["escalation_label"] == 1

    def test_benign_activity_is_negative(self):
        df = _base_df(["benign_activity"])
        result = self.builder.build_escalation_labels(df)
        assert result.iloc[0]["escalation_label"] == 0


class TestTimeLabels:
    def setup_method(self):
        self.builder = LabelBuilder()

    def test_log1p_transform_applied(self):
        import numpy as np
        df = _base_df(["true_positive"])
        df["resolution_hours"] = 10.0
        result = self.builder.build_time_labels(df)
        expected = float(np.log1p(10.0))
        assert abs(result.iloc[0]["resolution_hours_log"] - expected) < 1e-9

    def test_negative_hours_dropped(self):
        df = _base_df(["true_positive", "false_positive"])
        df["resolution_hours"] = [-1.0, 5.0]
        result = self.builder.build_time_labels(df)
        assert len(result) == 1
        assert result.iloc[0]["resolution_hours"] == 5.0

    def test_zero_hours_kept(self):
        df = _base_df(["true_positive"])
        df["resolution_hours"] = [0.0]
        result = self.builder.build_time_labels(df)
        assert len(result) == 1
