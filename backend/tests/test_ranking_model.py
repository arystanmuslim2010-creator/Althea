from __future__ import annotations

import pandas as pd

from models.ranking_model import fit_pairwise_ranker, fit_two_stage_reranker


def _ranking_frame() -> pd.DataFrame:
    rows = []
    for day in range(4):
        for index in range(8):
            positive = index == 0
            rows.append(
                {
                    "alert_id": f"A-{day}-{index}",
                    "created_at": f"2022-01-0{day + 1}T0{index % 4}:00:00Z",
                    "evaluation_label_is_sar": 1 if positive else 0,
                    "total_amount_usd": 400.0 if positive else float(100 + index),
                    "transaction_count": 4 if positive else 1 + (index % 2),
                    "unique_destination_accounts": 3 if positive else 1,
                    "score_hint": 1.0 if positive else 0.1 * index,
                }
            )
    return pd.DataFrame(rows)


def test_pairwise_ranker_returns_scores() -> None:
    frame = _ranking_frame()
    train = frame.iloc[:16].copy()
    validation = frame.iloc[16:24].copy()
    test = frame.iloc[24:].copy()
    result = fit_pairwise_ranker(train, validation, test, ["total_amount_usd", "transaction_count", "score_hint"])

    assert len(result.validation_scores) == len(validation)
    assert len(result.test_scores) == len(test)
    assert result.metadata["pairwise_examples"] > 0


def test_two_stage_reranker_returns_scores() -> None:
    frame = _ranking_frame()
    train = frame.iloc[:16].copy()
    validation = frame.iloc[16:24].copy()
    test = frame.iloc[24:].copy()
    result = fit_two_stage_reranker(train, validation, test, ["total_amount_usd", "transaction_count", "score_hint"])

    assert len(result.validation_scores) == len(validation)
    assert len(result.test_scores) == len(test)
    assert result.metadata["family"] == "two_stage_reranker"
