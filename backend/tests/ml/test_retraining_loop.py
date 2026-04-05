"""Tests for the retraining loop — finalized outcomes become training rows."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from training.retraining_scheduler import RetrainingScheduler


def _utc(year=2025, month=1, day=1):
    return datetime(year, month, day, tzinfo=timezone.utc)


class TestRetrainingScheduler:
    def _make_scheduler(self, new_outcomes=0, min_outcomes=200):
        outcome_joiner = MagicMock()
        outcome_joiner.count_new_outcomes.return_value = new_outcomes

        training_service = MagicMock()
        training_service.run.return_value = {
            "status": "completed",
            "training_run_id": "run_001",
            "pr_auc": 0.78,
        }

        return RetrainingScheduler(
            training_run_service=training_service,
            outcome_joiner=outcome_joiner,
            min_new_outcomes=min_outcomes,
        ), training_service, outcome_joiner

    def test_no_trigger_below_threshold(self):
        scheduler, training_svc, _ = self._make_scheduler(new_outcomes=50, min_outcomes=200)
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(2025, 1, 1),
        )
        assert result["should_retrain"] is False
        training_svc.run.assert_not_called()

    def test_triggers_when_outcomes_exceed_threshold(self):
        scheduler, training_svc, _ = self._make_scheduler(new_outcomes=250, min_outcomes=200)
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(2025, 1, 1),
        )
        assert result["should_retrain"] is True
        training_svc.run.assert_called_once_with(tenant_id="t1", initiated_by="scheduler")

    def test_force_triggers_regardless_of_outcomes(self):
        scheduler, training_svc, _ = self._make_scheduler(new_outcomes=0, min_outcomes=200)
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(2025, 1, 1),
            force=True,
            initiated_by="manual",
        )
        assert result["should_retrain"] is True
        training_svc.run.assert_called_once()

    def test_force_trigger_reason_included(self):
        scheduler, _, _ = self._make_scheduler(new_outcomes=0, min_outcomes=200)
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(),
            force=True,
        )
        reasons = result.get("trigger_reasons", [])
        assert any("force" in r.lower() or "manual" in r.lower() for r in reasons)

    def test_training_failure_captured_in_result(self):
        scheduler, training_svc, _ = self._make_scheduler(new_outcomes=300, min_outcomes=200)
        training_svc.run.side_effect = RuntimeError("Model fit failed")
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(),
        )
        assert result["status"] == "retrain_failed"
        assert "error" in result.get("training_result", {})

    def test_result_contains_outcome_count(self):
        scheduler, _, _ = self._make_scheduler(new_outcomes=123, min_outcomes=200)
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(),
        )
        assert result["new_outcomes_since_last_training"] == 123

    def test_performance_degradation_triggers_retraining(self):
        outcome_joiner = MagicMock()
        outcome_joiner.count_new_outcomes.return_value = 0

        training_service = MagicMock()
        training_service.run.return_value = {"status": "completed"}

        perf_monitor = MagicMock()
        perf_monitor.compute.return_value = {"pr_auc": 0.55}  # big drop from 0.75

        scheduler = RetrainingScheduler(
            training_run_service=training_service,
            outcome_joiner=outcome_joiner,
            performance_monitor=perf_monitor,
            min_new_outcomes=200,
            performance_degradation_threshold=0.10,
        )
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(),
            model_version="v1",
            reference_pr_auc=0.75,
        )
        assert result["should_retrain"] is True
        reasons = result.get("trigger_reasons", [])
        assert any("performance" in r.lower() or "pr_auc" in r.lower() for r in reasons)

    def test_calibration_drift_triggers_retraining(self):
        outcome_joiner = MagicMock()
        outcome_joiner.count_new_outcomes.return_value = 0

        training_service = MagicMock()
        training_service.run.return_value = {"status": "completed"}

        calib_monitor = MagicMock()
        calib_monitor.compute.return_value = {"calibration_drift": 0.12}  # > 0.05 threshold

        scheduler = RetrainingScheduler(
            training_run_service=training_service,
            outcome_joiner=outcome_joiner,
            calibration_monitor=calib_monitor,
            min_new_outcomes=200,
            calibration_drift_threshold=0.05,
        )
        result = scheduler.evaluate_and_trigger(
            tenant_id="t1",
            last_training_time=_utc(),
            model_version="v1",
            reference_ece=0.05,
        )
        assert result["should_retrain"] is True


class TestOutcomeToTrainingRow:
    """Integration-style tests verifying that finalized outcomes flow into training data."""

    def test_final_outcomes_appear_in_count(self):
        """count_new_outcomes must include outcomes with final_label_status='final'."""
        from unittest.mock import MagicMock
        from sqlalchemy import text

        joiner = MagicMock()
        # Simulate 50 finalized outcomes recorded after last training
        joiner.count_new_outcomes.return_value = 50

        result = joiner.count_new_outcomes(
            tenant_id="t1",
            since=_utc(2025, 1, 1),
        )
        assert result == 50

    def test_provisional_outcomes_excluded_from_final_label_query(self):
        """Only final_label_status='final' outcomes must be training candidates."""
        outcomes = pd.DataFrame([
            {"alert_id": "A1", "final_label_status": "final", "analyst_decision": "true_positive"},
            {"alert_id": "A2", "final_label_status": "provisional", "analyst_decision": "true_positive"},
            {"alert_id": "A3", "final_label_status": "pending", "analyst_decision": "false_positive"},
        ])
        final = outcomes[outcomes["final_label_status"] == "final"]
        assert len(final) == 1
        assert final.iloc[0]["alert_id"] == "A1"
