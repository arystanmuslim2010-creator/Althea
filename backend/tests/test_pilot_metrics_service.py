from __future__ import annotations

from services.pilot_metrics_service import (
    PilotMetricsService,
    analyst_override_rate,
    escalation_capture_rate,
    false_positive_reduction_estimate,
    precision_at_top_k_percent,
    recall_at_top_k_percent,
    workload_reduction_at_threshold,
)


def _records():
    return [
        {"score": 99, "label": True, "override": False},
        {"score": 80, "label": False, "override": True},
        {"score": 50, "label": True, "override": False},
        {"score": 10, "label": False, "override": False},
    ]


def test_pilot_priority_metrics_use_conservative_names():
    recall = recall_at_top_k_percent(_records(), "score", "label", 50)
    precision = precision_at_top_k_percent(_records(), "score", "label", 50)
    assert recall["benchmark_recall_at_top_percent"] == 0.5
    assert precision["precision_at_top_percent"] == 0.5


def test_pilot_operational_metrics_are_modeled_not_claimed_live():
    workload = workload_reduction_at_threshold(_records(), "score", 75)
    capture = escalation_capture_rate(_records(), "score", "label", 75)
    fp = false_positive_reduction_estimate(_records(), "score", "label", 75)
    overrides = analyst_override_rate(_records(), "override")
    assert workload["modeled_workload_reduction"] == 0.5
    assert capture["observed_capture_rate"] == 0.5
    assert fp["modeled_false_positive_reduction"] == 0.5
    assert overrides["analyst_override_rate"] == 0.25


def test_pilot_summary_uses_evaluation_when_labels_are_valid():
    service = PilotMetricsService()
    summary = service.summarize_records(
        [
            {"alert_id": "A1", "risk_score": 88.0, "risk_band": "High", "risk_explain_json": "{}", "ingestion_metadata_json": {"warnings": []}},
            {"alert_id": "A2", "risk_score": 55.0, "risk_band": "Medium", "ingestion_metadata_json": {"warnings": ["missing_counterparty"]}},
        ],
        evaluation={
            "evaluation_valid": True,
            "lift_over_best_baseline": 1.9,
            "althea_metrics": {
                "sar_capture_at_top_10_pct": 0.5,
                "sar_capture_at_top_20_pct": 0.8,
                "sar_capture_at_top_30_pct": 0.9,
                "workload_reduction_at_target_recall": 0.4,
            },
        },
    )

    assert summary["evaluation_available"] is True
    assert "SAR capture" in str(summary["evaluation_summary"])


def test_pilot_summary_avoids_fake_evaluation_when_labels_are_invalid():
    service = PilotMetricsService()
    summary = service.summarize_records(
        [{"alert_id": "A1", "risk_score": 88.0, "risk_band": "High"}],
        evaluation={
            "evaluation_valid": False,
            "warning": "Evaluation requires both positive and negative labeled alerts.",
        },
    )

    assert summary["evaluation_available"] is False
    assert summary["evaluation"] is None
    assert summary["evaluation_summary"] == "Evaluation labels unavailable or not suitable for ranking validation."
