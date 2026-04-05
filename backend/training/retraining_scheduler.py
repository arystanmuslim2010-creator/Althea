"""Retraining scheduler — decides when to trigger model retraining.

Retraining is triggered when any of these conditions are met:
    1. Enough new labeled outcomes have accumulated (min_new_outcomes)
    2. Performance degradation exceeds a threshold (via PerformanceMonitor)
    3. Calibration error exceeds a threshold (via CalibrationMonitor)
    4. A scheduled wall-clock interval has passed (e.g. weekly)
    5. Manual trigger via API

The scheduler does NOT execute training itself; it calls TrainingRunService.run()
and records the result. This keeps the scheduler logic separate from training
orchestration.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.training.retraining_scheduler")


class RetrainingScheduler:
    """Decide whether to retrain and trigger TrainingRunService.run()."""

    def __init__(
        self,
        training_run_service,
        outcome_joiner,
        performance_monitor=None,
        calibration_monitor=None,
        min_new_outcomes: int = 200,
        performance_degradation_threshold: float = 0.10,
        calibration_drift_threshold: float = 0.05,
    ) -> None:
        self._training_service = training_run_service
        self._outcome_joiner = outcome_joiner
        self._perf_monitor = performance_monitor
        self._calib_monitor = calibration_monitor
        self._min_new_outcomes = min_new_outcomes
        self._perf_threshold = performance_degradation_threshold
        self._calib_threshold = calibration_drift_threshold

    def evaluate_and_trigger(
        self,
        tenant_id: str,
        last_training_time: datetime,
        model_version: str | None = None,
        reference_pr_auc: float | None = None,
        reference_ece: float | None = None,
        force: bool = False,
        initiated_by: str = "scheduler",
    ) -> dict[str, Any]:
        """Evaluate retraining triggers and run if needed.

        Parameters
        ----------
        last_training_time     : timestamp of the last completed training run
        model_version          : current production model version
        reference_pr_auc       : PR-AUC from original model evaluation
        reference_ece          : ECE from original calibration
        force                  : bypass all checks and retrain immediately

        Returns a dict with decision, reasons, and training result if triggered.
        """
        now = datetime.now(timezone.utc)
        reasons: list[str] = []
        should_retrain = force

        if force:
            reasons.append("manual_force_trigger")

        # Check 1: New labeled outcomes
        new_count = self._outcome_joiner.count_new_outcomes(
            tenant_id=tenant_id,
            since=last_training_time,
        )
        if new_count >= self._min_new_outcomes:
            reasons.append(f"new_outcomes_threshold_exceeded: {new_count} >= {self._min_new_outcomes}")
            should_retrain = True

        # Check 2: Performance degradation
        if self._perf_monitor is not None and reference_pr_auc is not None:
            try:
                perf = self._perf_monitor.compute(
                    tenant_id=tenant_id,
                    model_version=model_version,
                    lookback_days=30,
                )
                current_pr_auc = perf.get("pr_auc")
                if current_pr_auc is not None:
                    delta = reference_pr_auc - current_pr_auc
                    if delta > self._perf_threshold:
                        reasons.append(f"performance_degradation: pr_auc dropped {delta:.4f}")
                        should_retrain = True
            except Exception as exc:
                logger.warning("Performance check failed: %s", exc)

        # Check 3: Calibration drift
        if self._calib_monitor is not None and reference_ece is not None:
            try:
                calib = self._calib_monitor.compute(
                    tenant_id=tenant_id,
                    model_version=model_version,
                    lookback_days=30,
                    reference_ece=reference_ece,
                )
                drift = calib.get("calibration_drift")
                if drift is not None and abs(float(drift)) > self._calib_threshold:
                    reasons.append(f"calibration_drift: ECE delta {drift:.4f}")
                    should_retrain = True
            except Exception as exc:
                logger.warning("Calibration check failed: %s", exc)

        result: dict[str, Any] = {
            "tenant_id": tenant_id,
            "evaluated_at": now.isoformat(),
            "should_retrain": should_retrain,
            "trigger_reasons": reasons,
            "new_outcomes_since_last_training": new_count,
        }

        if should_retrain:
            logger.info(
                json.dumps(
                    {
                        "event": "retraining_triggered",
                        "tenant_id": tenant_id,
                        "reasons": reasons,
                        "new_outcomes": new_count,
                    },
                    ensure_ascii=True,
                )
            )
            try:
                training_result = self._training_service.run(
                    tenant_id=tenant_id,
                    initiated_by=initiated_by,
                )
                result["training_result"] = training_result
                result["status"] = "retrained"
            except Exception as exc:
                logger.exception("Retraining failed for tenant %s: %s", tenant_id, exc)
                result["training_result"] = {"error": str(exc)}
                result["status"] = "retrain_failed"
        else:
            result["status"] = "no_action_needed"

        return result
