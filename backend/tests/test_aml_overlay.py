"""
Unit tests: hashing determinism, schema validator, idempotency, governance hard constraints,
and production-grade evaluation metrics (analyst disposition vs synthetic labels).
"""
from __future__ import annotations

import hashlib
import numpy as np
import pytest


def test_hashing_determinism():
    """Same input => same hash."""
    a = b"user_id,amount\nU1,100\nU2,200"
    h1 = hashlib.sha256(a).hexdigest()[:32]
    h2 = hashlib.sha256(a).hexdigest()[:32]
    assert h1 == h2


def test_schema_validator():
    """Normalized alert schema: required fields."""
    from src.domain.schemas import validate_normalized_alert_schema, NORMALIZED_ALERT_REQUIRED
    assert "alert_id" in NORMALIZED_ALERT_REQUIRED
    errs = validate_normalized_alert_schema({})
    assert any("alert_id" in e or "Missing" in e for e in errs)
    errs_full = validate_normalized_alert_schema({
        "alert_id": "A1", "entity_id": "E1", "source_system": "csv",
        "timestamp": "2025-01-01", "risk_score_source": "ingest", "typology": "x", "vendor_metadata": {}
    })
    assert len(errs_full) == 0


def test_idempotency_dedupe():
    """Dedupe by alert_id + tx_ref keeps first."""
    import pandas as pd
    from src.pipeline.stages.ingest import _idempotency_dedupe
    df = pd.DataFrame([
        {"alert_id": "A1", "tx_ref": "T1", "x": 1},
        {"alert_id": "A1", "tx_ref": "T1", "x": 2},
        {"alert_id": "A2", "tx_ref": "T2", "x": 3},
    ])
    out = _idempotency_dedupe(df, ["alert_id", "tx_ref"], per_run=True)
    assert len(out) == 2
    assert out["alert_id"].tolist() == ["A1", "A2"]


def test_governance_hard_constraints():
    """Hard constraints override: sanctions_hit => MANDATORY_REVIEW, in_queue=1."""
    from src.hard_constraints import evaluate_hard_constraints
    out = evaluate_hard_constraints({}, {"sanctions_hit": True}, {})
    assert out["hard_hit"] is True
    assert "SANCTIONS" in out.get("hard_code", "") or "sanctions" in out.get("hard_reason", "").lower()
    out2 = evaluate_hard_constraints({}, {"high_risk_country_critical": True}, {})
    assert out2["hard_hit"] is True
    out3 = evaluate_hard_constraints({}, {}, {})
    assert out3["hard_hit"] is False


# ---------------------------------------------------------------------------
# Evaluation metrics tests
# ---------------------------------------------------------------------------

def test_evaluation_with_analyst_labels():
    """Analyst disposition labels produce valid non-synthetic metrics."""
    import pandas as pd
    from src.evaluation_service import (
        EvaluationService, OutcomeLabelSource, detect_outcome_source,
    )

    n = 200
    rng = np.random.default_rng(42)
    df = pd.DataFrame({
        "risk_score":          rng.uniform(0, 100, n),
        "alert_eligible":      True,
        "analyst_disposition": rng.choice(
            ["SAR_FILED", "TP", "FP", "PENDING"],
            size=n, p=[0.05, 0.10, 0.60, 0.25],
        ),
        "timestamp": pd.date_range("2025-01-01", periods=n, freq="1h"),
    })

    source, col, warning = detect_outcome_source(df)
    assert source == OutcomeLabelSource.ANALYST_DISPOSITION
    assert warning == ""

    svc    = EvaluationService()
    result = svc.evaluate(df, capacity=50)
    assert result["is_production_valid"] is True
    assert result["metrics"].is_synthetic is False
    assert 0.0 <= result["metrics"].precision_at_k <= 1.0
    assert 0.0 <= result["metrics"].recall_at_k    <= 1.0
    assert result["metrics"].lift_at_k >= 0.0


def test_evaluation_warns_on_synthetic_labels():
    """Synthetic labels trigger warning and is_production_valid=False."""
    import pandas as pd
    from src.evaluation_service import (
        EvaluationService, OutcomeLabelSource, detect_outcome_source,
    )

    df = pd.DataFrame({
        "risk_score": [90, 80, 70, 60, 50, 40, 30, 20, 10, 5],
        "synthetic_true_suspicious": [
            "Yes", "No", "Yes", "No", "No", "No", "No", "No", "No", "No"
        ],
        "alert_eligible": True,
    })

    source, col, warning = detect_outcome_source(df)
    assert source == OutcomeLabelSource.SYNTHETIC
    assert len(warning) > 0

    svc    = EvaluationService()
    result = svc.evaluate(df, capacity=5)
    assert result["is_production_valid"] is False
    assert result["metrics"].is_synthetic is True


def test_temporal_holdout_respects_time_order():
    """Train window must be entirely before eval window."""
    import pandas as pd
    from src.evaluation_service import TemporalHoldoutEvaluator

    n  = 300
    df = pd.DataFrame({
        "risk_score":          np.random.default_rng(0).uniform(0, 100, n),
        "timestamp":           pd.date_range("2024-01-01", periods=n, freq="1D"),
        "analyst_disposition": np.random.default_rng(0).choice(["SAR_FILED", "FP"], n),
    })

    evaluator            = TemporalHoldoutEvaluator(train_pct=0.70, eval_pct=0.20)
    train_df, eval_df    = evaluator.split(df)

    assert train_df["timestamp"].max() < eval_df["timestamp"].min()
    assert len(train_df) > 0 and len(eval_df) > 0


def test_precision_recall_metrics_match_manual():
    """Verify precision@K and recall@K against manually computed values."""
    from src.evaluation_metrics import precision_at_k, recall_at_k, lift_at_k

    # 10 alerts, top-5 reviewed, 3 TPs in top-5, 4 total TPs
    y_true  = np.array([1, 1, 1, 0, 0, 1, 0, 0, 0, 0])
    y_score = np.array([9, 8, 7, 6, 5, 2, 3, 4, 1, 0])

    p_at_5 = precision_at_k(y_true, y_score, k=5)
    r_at_5 = recall_at_k(y_true, y_score, k=5)
    l_at_5 = lift_at_k(y_true, y_score, k=5)

    assert abs(p_at_5 - 0.6)  < 1e-6   # 3 TP in top-5
    assert abs(r_at_5 - 0.75) < 1e-6   # 3 of 4 total TPs captured
    assert l_at_5 > 1.0                  # must be better than random


def test_no_labels_returns_graceful_none():
    """System gracefully handles missing labels."""
    import pandas as pd
    from src.evaluation_service import EvaluationService

    df = pd.DataFrame({
        "risk_score":    [90, 80, 70, 60, 50],
        "alert_eligible": True,
    })

    svc    = EvaluationService()
    result = svc.evaluate(df, capacity=3)
    assert result["metrics"] is None
    assert result["is_production_valid"] is False
    assert "no label" in result["warnings"][0].lower()
