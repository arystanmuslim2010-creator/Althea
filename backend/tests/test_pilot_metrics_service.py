from __future__ import annotations

from services.pilot_metrics_service import (
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
