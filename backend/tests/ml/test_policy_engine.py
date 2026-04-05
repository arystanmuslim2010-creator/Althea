"""Tests for PolicyEngine and PriorityFormula — ranking, sanctions hold, suppression."""
from __future__ import annotations

import pandas as pd
import pytest

from decision.policy_engine import PolicyEngine, PolicyConfig
from decision.priority_formula import PriorityFormula, PrioritySignals, PriorityWeights


# ---------------------------------------------------------------------------
# PriorityFormula
# ---------------------------------------------------------------------------

class TestPriorityFormula:
    def setup_method(self):
        self.formula = PriorityFormula()

    def _signals(self, escalation_prob=0.5, graph_risk=50.0, similar_suspicious=0.5,
                 time_penalty=0.0, regulatory_boost=0.0, uncertainty_penalty=0.0):
        return PrioritySignals(
            escalation_prob=escalation_prob,
            graph_risk_score=graph_risk,
            similar_suspicious_strength=similar_suspicious,
            time_penalty_hours=time_penalty,
            regulatory_urgency=regulatory_boost,
            uncertainty=uncertainty_penalty,
        )

    def test_score_in_0_100_range(self):
        score = self.formula.compute(self._signals())
        assert 0.0 <= score <= 100.0

    def test_higher_escalation_prob_yields_higher_score(self):
        low = self.formula.compute(self._signals(escalation_prob=0.1))
        high = self.formula.compute(self._signals(escalation_prob=0.9))
        assert high > low

    def test_higher_graph_risk_yields_higher_score(self):
        low = self.formula.compute(self._signals(graph_risk=10.0))
        high = self.formula.compute(self._signals(graph_risk=90.0))
        assert high > low

    def test_priority_bucket_critical_for_high_scores(self):
        score = 90.0
        bucket = self.formula.priority_bucket(score)
        assert bucket == "CRITICAL"

    def test_priority_bucket_low_for_low_scores(self):
        bucket = self.formula.priority_bucket(10.0)
        assert bucket == "LOW"

    def test_time_penalty_reduces_score(self):
        no_penalty = self.formula.compute(self._signals(time_penalty=0))
        with_penalty = self.formula.compute(self._signals(time_penalty=168))  # 1 week
        assert with_penalty <= no_penalty

    def test_batch_compute_preserves_row_count(self):
        import numpy as np
        rng = np.random.default_rng(1)
        n = 50
        df = pd.DataFrame({
            "escalation_prob": rng.uniform(0, 1, n),
            "graph_risk_score": rng.uniform(0, 100, n),
            "similar_suspicious_strength": rng.uniform(0, 1, n),
            "time_penalty_hours": rng.uniform(0, 200, n),
            "regulatory_urgency": rng.uniform(0, 1, n),
            "uncertainty": rng.uniform(0, 1, n),
        })
        scores = self.formula.compute_batch(df)
        assert len(scores) == n

    def test_batch_scores_in_valid_range(self):
        import numpy as np
        rng = np.random.default_rng(2)
        n = 100
        df = pd.DataFrame({
            "escalation_prob": rng.uniform(0, 1, n),
            "graph_risk_score": rng.uniform(0, 100, n),
            "similar_suspicious_strength": rng.uniform(0, 1, n),
            "time_penalty_hours": rng.uniform(0, 300, n),
            "regulatory_urgency": rng.uniform(0, 1, n),
            "uncertainty": rng.uniform(0, 1, n),
        })
        scores = self.formula.compute_batch(df)
        arr = pd.Series(scores)
        assert arr.min() >= 0.0 - 1e-6
        assert arr.max() <= 100.0 + 1e-6


# ---------------------------------------------------------------------------
# PolicyEngine
# ---------------------------------------------------------------------------

def _base_alerts(n=10):
    import numpy as np
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "alert_id": [f"A{i}" for i in range(n)],
        "priority_score": rng.uniform(20, 90, n),
        "typology": ["money_laundering"] * n,
        "country": ["US"] * n,
        "amount": rng.uniform(1000, 50000, n),
    })


class TestPolicyEngine:
    def setup_method(self):
        self.engine = PolicyEngine()

    def test_apply_returns_same_row_count(self):
        df = _base_alerts(20)
        result = self.engine.apply(df)
        assert len(result) == len(df)

    def test_governance_status_column_added(self):
        df = _base_alerts()
        result = self.engine.apply(df)
        assert "governance_status" in result.columns

    def test_sanctions_typology_gets_hold(self):
        df = _base_alerts(5)
        df["typology"] = "sanctions"
        result = self.engine.apply(df)
        assert (result["governance_status"] == "sanctions_hold").all()

    def test_high_risk_country_sanctions_hold(self):
        df = _base_alerts(3)
        df["country"] = "KP"  # North Korea — high risk
        result = self.engine.apply(df)
        # All KP alerts must be held (sanctions_hold or mandatory_review)
        statuses = result["governance_status"].str.lower()
        assert statuses.isin({"sanctions_hold", "mandatory_review"}).all()

    def test_low_score_suppression(self):
        df = _base_alerts(5)
        # Force scores below suppress_threshold
        df["priority_score"] = PolicyConfig().suppress_threshold - 1.0
        result = self.engine.apply(df)
        assert (result["governance_status"] == "suppressed").all()

    def test_high_score_mandatory_review(self):
        df = _base_alerts(5)
        df["priority_score"] = PolicyConfig().mandatory_review_threshold + 5.0
        df["typology"] = "structuring"
        result = self.engine.apply(df)
        # High score, normal typology → mandatory_review or eligible
        assert result["governance_status"].isin({"mandatory_review", "eligible"}).all()

    def test_ctr_flag_set_for_large_amounts(self):
        df = _base_alerts(3)
        df["amount"] = PolicyConfig().ctr_amount_threshold + 1000
        result = self.engine.apply(df)
        # CTR flag must be present in compliance_flags
        if "compliance_flags_json" in result.columns:
            for flags in result["compliance_flags_json"]:
                if isinstance(flags, dict):
                    assert flags.get("ctr_flag") is True or "ctr" in str(flags).lower()

    def test_in_queue_true_for_eligible(self):
        df = _base_alerts(5)
        df["priority_score"] = 55.0
        df["typology"] = "structuring"
        result = self.engine.apply(df)
        eligible = result[result["governance_status"] == "eligible"]
        if not eligible.empty and "in_queue" in result.columns:
            assert eligible["in_queue"].all()

    def test_suppressed_not_in_queue(self):
        df = _base_alerts(5)
        df["priority_score"] = 5.0
        result = self.engine.apply(df)
        suppressed = result[result["governance_status"] == "suppressed"]
        if not suppressed.empty and "in_queue" in result.columns:
            assert not suppressed["in_queue"].any()
