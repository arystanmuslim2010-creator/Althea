from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RolloutThresholds:
    max_failure_rate_hold: float = 0.01
    max_failure_rate_rollback: float = 0.05
    max_validation_error_rate_hold: float = 0.02
    max_validation_error_rate_rollback: float = 0.08
    max_p95_latency_ms_hold: float = 8000.0
    max_p95_latency_ms_rollback: float = 20000.0
    max_data_quality_issue_rate_hold: float = 0.05
    max_data_quality_issue_rate_rollback: float = 0.15
    repeated_failure_runs_hold: int = 2
    repeated_failure_runs_rollback: int = 3
    critical_issue_runs_hold: int = 1
    critical_issue_runs_rollback: int = 2
    min_runs_for_go: int = 3


class RolloutEvaluator:
    def __init__(self, thresholds: RolloutThresholds | None = None) -> None:
        self._thresholds = thresholds or RolloutThresholds()

    def evaluate(self, metrics_snapshot: dict[str, Any], recent_runs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        recent = list(recent_runs or [])
        reasons: list[str] = []
        rollback_reasons: list[str] = []
        hold_reasons: list[str] = []

        run_count = int(metrics_snapshot.get("run_count") or 0)
        failure_rate = float(metrics_snapshot.get("failure_rate") or 0.0)
        validation_error_rate = float(metrics_snapshot.get("validation_error_rate") or 0.0)
        p95_latency_ms = float(metrics_snapshot.get("p95_latency_ms") or 0.0)
        quality_rate = float(metrics_snapshot.get("data_quality_issue_rate") or 0.0)
        repeated_failures = int(metrics_snapshot.get("repeated_failure_runs") or 0)
        critical_issue_runs = int(metrics_snapshot.get("critical_issue_runs") or 0)

        if run_count <= 0:
            hold_reasons.append("no_ingestion_runs_available_for_evaluation")
        elif run_count < self._thresholds.min_runs_for_go:
            hold_reasons.append("insufficient_observation_sample_size")

        if failure_rate >= self._thresholds.max_failure_rate_rollback:
            rollback_reasons.append("failure_rate_exceeds_rollback_threshold")
        elif failure_rate >= self._thresholds.max_failure_rate_hold:
            hold_reasons.append("failure_rate_exceeds_hold_threshold")

        if validation_error_rate >= self._thresholds.max_validation_error_rate_rollback:
            rollback_reasons.append("validation_error_rate_exceeds_rollback_threshold")
        elif validation_error_rate >= self._thresholds.max_validation_error_rate_hold:
            hold_reasons.append("validation_error_rate_exceeds_hold_threshold")

        if p95_latency_ms >= self._thresholds.max_p95_latency_ms_rollback:
            rollback_reasons.append("p95_latency_exceeds_rollback_threshold")
        elif p95_latency_ms >= self._thresholds.max_p95_latency_ms_hold:
            hold_reasons.append("p95_latency_exceeds_hold_threshold")

        if quality_rate >= self._thresholds.max_data_quality_issue_rate_rollback:
            rollback_reasons.append("data_quality_issue_rate_exceeds_rollback_threshold")
        elif quality_rate >= self._thresholds.max_data_quality_issue_rate_hold:
            hold_reasons.append("data_quality_issue_rate_exceeds_hold_threshold")

        if repeated_failures >= self._thresholds.repeated_failure_runs_rollback:
            rollback_reasons.append("repeated_ingestion_failures_detected")
        elif repeated_failures >= self._thresholds.repeated_failure_runs_hold:
            hold_reasons.append("repeated_ingestion_failures_warning")

        if critical_issue_runs >= self._thresholds.critical_issue_runs_rollback:
            rollback_reasons.append("critical_data_quality_issues_repeated")
        elif critical_issue_runs >= self._thresholds.critical_issue_runs_hold:
            hold_reasons.append("critical_data_quality_issue_detected")

        if recent:
            last_status = str(recent[-1].get("status") or "unknown")
            if last_status == "failed_validation":
                hold_reasons.append("latest_run_failed_validation")

        if rollback_reasons:
            decision = "ROLLBACK"
            reasons = rollback_reasons + hold_reasons
        elif hold_reasons:
            decision = "HOLD"
            reasons = hold_reasons
        else:
            decision = "GO"
            reasons = ["all_rollout_gates_passing"]

        return {
            "decision": decision,
            "reasons": reasons,
            "metrics_snapshot": dict(metrics_snapshot),
            "thresholds": {
                "max_failure_rate_hold": self._thresholds.max_failure_rate_hold,
                "max_failure_rate_rollback": self._thresholds.max_failure_rate_rollback,
                "max_validation_error_rate_hold": self._thresholds.max_validation_error_rate_hold,
                "max_validation_error_rate_rollback": self._thresholds.max_validation_error_rate_rollback,
                "max_p95_latency_ms_hold": self._thresholds.max_p95_latency_ms_hold,
                "max_p95_latency_ms_rollback": self._thresholds.max_p95_latency_ms_rollback,
                "max_data_quality_issue_rate_hold": self._thresholds.max_data_quality_issue_rate_hold,
                "max_data_quality_issue_rate_rollback": self._thresholds.max_data_quality_issue_rate_rollback,
            },
        }
