from __future__ import annotations

from evaluation.metrics import compute_ranking_metrics
from services.evaluation_service import EvaluationService
from services.scoring_service import build_score_contract, derive_risk_band, derive_score_method, normalize_risk_score


def _records() -> list[dict]:
    return [
        {"alert_id": "A1", "risk_score": 95.0, "amount": 5000.0, "created_at": "2026-05-01T10:00:00Z", "evaluation_label_is_sar": 1},
        {"alert_id": "A2", "risk_score": 80.0, "amount": 9000.0, "created_at": "2026-05-01T09:00:00Z", "evaluation_label_is_sar": 0},
        {"alert_id": "A3", "risk_score": 70.0, "amount": 2000.0, "created_at": "2026-05-01T08:00:00Z", "evaluation_label_is_sar": 1},
        {"alert_id": "A4", "risk_score": 20.0, "amount": 1000.0, "created_at": "2026-05-01T07:00:00Z", "evaluation_label_is_sar": 0},
        {"alert_id": "A5", "risk_score": 10.0, "amount": 500.0, "created_at": "2026-05-01T06:00:00Z", "evaluation_label_is_sar": 0},
    ]


def test_top_k_recall_and_precision_are_computed_for_ranked_alerts() -> None:
    ranked = [dict(row, ranking_score=row["risk_score"]) for row in _records()]
    metrics = compute_ranking_metrics(ranked, label_field="evaluation_label_is_sar")

    assert metrics["is_valid"] is True
    assert metrics["recall_at_top_20_pct"] == 0.5
    assert metrics["precision_at_top_20_pct"] == 1.0
    assert metrics["sar_capture_at_top_30_pct"] == 0.5


def test_all_positive_labels_return_warning_instead_of_fake_metrics() -> None:
    rows = [dict(row, evaluation_label_is_sar=1) for row in _records()]
    result = EvaluationService().evaluate_records(dataset_name="all-positive", records=rows)

    assert result["evaluation_valid"] is False
    assert result["warning"] == "Evaluation requires both positive and negative labeled alerts."
    assert result["althea_metrics"]["recall_at_top_20_pct"] is None
    assert result["summary_text"] == "Evaluation requires both positive and negative labeled alerts."


def test_evaluation_compares_althea_against_baselines() -> None:
    result = EvaluationService().evaluate_records(dataset_name="pilot-sample", records=_records())

    assert result["evaluation_valid"] is True
    assert "chronological" in result["baselines"]
    assert "amount_desc" in result["baselines"]
    assert "random" in result["baselines"]
    assert result["best_baseline"] in {"chronological", "amount_desc", "heuristic", None}
    assert isinstance(result["summary_text"], str)
    assert "ALTHEA captured" in result["summary_text"]


def test_score_contract_helpers_are_deterministic_and_bounded() -> None:
    first = build_score_contract({"risk_score": 88.0, "model_version": "model-v1"}, priority_rank=3)
    second = build_score_contract({"risk_score": 88.0, "model_version": "model-v1"}, priority_rank=3, score_created_at=first["score_created_at"])

    assert normalize_risk_score(88.0) == 0.88
    assert derive_risk_band(88.0) == "High"
    assert derive_score_method("model-v1") == "production_model"
    assert 0.0 <= first["risk_score_normalized"] <= 1.0
    assert first["risk_band"] == second["risk_band"]
    assert first["score_method"] == second["score_method"]
    assert second["priority_rank"] == 3
