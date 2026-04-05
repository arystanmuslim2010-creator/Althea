from __future__ import annotations

import uuid

from core.observability import get_recent_ingestion_summaries, get_rollout_metrics_snapshot, record_ingestion_summary
from services.rollout_evaluator import RolloutEvaluator


def test_rollout_evaluator_returns_go_when_metrics_are_healthy() -> None:
    evaluator = RolloutEvaluator()
    metrics = {
        "run_count": 5,
        "failure_rate": 0.0,
        "validation_error_rate": 0.0,
        "p95_latency_ms": 500.0,
        "data_quality_issue_rate": 0.01,
        "repeated_failure_runs": 0,
        "critical_issue_runs": 0,
    }
    decision = evaluator.evaluate(metrics_snapshot=metrics, recent_runs=[])
    assert decision["decision"] == "GO"


def test_rollout_evaluator_returns_hold_when_sample_is_insufficient() -> None:
    evaluator = RolloutEvaluator()
    metrics = {
        "run_count": 1,
        "failure_rate": 0.0,
        "validation_error_rate": 0.0,
        "p95_latency_ms": 200.0,
        "data_quality_issue_rate": 0.0,
        "repeated_failure_runs": 0,
        "critical_issue_runs": 0,
    }
    decision = evaluator.evaluate(metrics_snapshot=metrics, recent_runs=[])
    assert decision["decision"] == "HOLD"
    assert "insufficient_observation_sample_size" in decision["reasons"]


def test_rollout_evaluator_returns_rollback_for_high_failure_signal() -> None:
    evaluator = RolloutEvaluator()
    metrics = {
        "run_count": 6,
        "failure_rate": 0.2,
        "validation_error_rate": 0.15,
        "p95_latency_ms": 40000.0,
        "data_quality_issue_rate": 0.2,
        "repeated_failure_runs": 4,
        "critical_issue_runs": 3,
    }
    decision = evaluator.evaluate(metrics_snapshot=metrics, recent_runs=[])
    assert decision["decision"] == "ROLLBACK"
    assert any("rollback_threshold" in reason or "repeated" in reason for reason in decision["reasons"])


def test_rollout_metrics_snapshot_aggregates_recent_runs() -> None:
    source = f"phase3_test_{uuid.uuid4().hex[:8]}"
    record_ingestion_summary(
        {
            "run_id": "run-a",
            "source_system": source,
            "status": "accepted",
            "failure_reason_category": "none",
            "total_rows": 10,
            "success_count": 10,
            "failed_count": 0,
            "warning_count": 1,
            "elapsed_ms": 100,
            "ingested_alert_count": 10,
            "ingested_transaction_count": 30,
            "data_quality_inconsistency_count": 1,
            "critical_issue_count": 0,
            "critical_data_quality_issues": [],
        }
    )
    record_ingestion_summary(
        {
            "run_id": "run-b",
            "source_system": source,
            "status": "failed_validation",
            "failure_reason_category": "validation_failure",
            "total_rows": 10,
            "success_count": 0,
            "failed_count": 10,
            "warning_count": 0,
            "elapsed_ms": 300,
            "ingested_alert_count": 0,
            "ingested_transaction_count": 0,
            "data_quality_inconsistency_count": 5,
            "critical_issue_count": 2,
            "critical_data_quality_issues": ["empty_alerts"],
        }
    )
    snapshot = get_rollout_metrics_snapshot(window_runs=10, source_system=source)
    assert snapshot["run_count"] == 2
    assert snapshot["total_rows_seen"] == 20
    assert snapshot["total_alerts_ingested"] == 10
    assert snapshot["failure_rate"] == 0.5
    assert snapshot["validation_error_rate"] == 0.5
    recent = get_recent_ingestion_summaries(limit=2, source_system=source)
    assert len(recent) == 2
