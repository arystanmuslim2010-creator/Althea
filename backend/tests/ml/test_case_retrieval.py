"""Tests for case retrieval — nearest-neighbor returns reasonable neighbors."""
from __future__ import annotations

import numpy as np
import pytest

from retrieval.case_vectorizer import CaseVectorizer, VECTOR_DIM
from retrieval.case_index import CaseIndex, IndexedCase


def _payload(alert_id, typology="money_laundering", country_risk=0.5, risk_score=60.0, **kwargs):
    base = {
        "alert_id": alert_id,
        "typology": typology,
        "country_risk": country_risk,
        "risk_score": risk_score,
        "amount": 10000.0,
        "amount_log1p": float(np.log1p(10000.0)),
        "segment_code": 1,
        "txn_count_7d": 5,
        "amount_sum_7d": 50000.0,
        "cross_border_ratio": 0.3,
        "round_amount_ratio": 0.2,
        "dormant_reactivation": 0,
        "prior_alert_count": 2,
        "prior_escalation_rate": 0.5,
        "prior_sar_rate": 0.1,
        "graph_degree": 3,
        "suspicious_neighbor_ratio": 0.2,
        "fan_out_score": 0.1,
        "community_risk_score": 40.0,
    }
    base.update(kwargs)
    return base


def _outcome(alert_id, decision):
    return {"alert_id": alert_id, "analyst_decision": decision}


class TestCaseVectorizer:
    def setup_method(self):
        self.vec = CaseVectorizer()

    def test_vectorize_returns_correct_dim(self):
        p = _payload("A1")
        v = self.vec.vectorize(p)
        assert len(v) == VECTOR_DIM

    def test_vectorize_returns_unit_norm(self):
        p = _payload("A1")
        v = self.vec.vectorize(p)
        norm = np.linalg.norm(v)
        assert abs(norm - 1.0) < 1e-6, f"Expected unit vector, got norm={norm}"

    def test_vectorize_batch_shape(self):
        payloads = [_payload(f"A{i}") for i in range(10)]
        matrix = self.vec.vectorize_batch(payloads)
        assert matrix.shape == (10, VECTOR_DIM)

    def test_similar_payloads_have_high_cosine_sim(self):
        """Two nearly identical payloads must have cosine similarity close to 1."""
        p1 = _payload("A1", typology="money_laundering", risk_score=70.0)
        p2 = _payload("A2", typology="money_laundering", risk_score=71.0)
        v1 = self.vec.vectorize(p1)
        v2 = self.vec.vectorize(p2)
        sim = float(np.dot(v1, v2))
        assert sim > 0.98, f"Similar payloads should have cosine sim > 0.98, got {sim:.4f}"

    def test_different_typologies_have_lower_sim(self):
        p1 = _payload("A1", typology="sanctions", country_risk=1.0, prior_sar_rate=0.8)
        p2 = _payload("A2", typology="structuring", country_risk=0.1, prior_sar_rate=0.0)
        v1 = self.vec.vectorize(p1)
        v2 = self.vec.vectorize(p2)
        sim = float(np.dot(v1, v2))
        assert sim < 0.99, f"Different risk profiles should have cosine sim < 0.99, got {sim:.4f}"


class TestCaseIndex:
    def _build_index(self, n=50):
        payloads = [_payload(f"A{i}", typology="money_laundering" if i % 2 == 0 else "structuring") for i in range(n)]
        outcomes = [_outcome(f"A{i}", "true_positive" if i % 3 != 0 else "false_positive") for i in range(n)]
        index = CaseIndex()
        index.build(payloads, outcomes)
        return index

    def test_build_creates_non_empty_index(self):
        index = self._build_index(30)
        total = sum(len(v) for v in index._cases.values())
        assert total > 0

    def test_search_returns_top_k_results(self):
        index = self._build_index(50)
        vec = CaseVectorizer()
        query_payload = _payload("Q1")
        query_vec = vec.vectorize(query_payload)
        results = index.search(query_vector=query_vec, partition="suspicious", top_k=5)
        assert len(results) <= 5

    def test_search_results_have_scores(self):
        index = self._build_index(40)
        vec = CaseVectorizer()
        query_vec = vec.vectorize(_payload("Q"))
        results = index.search(query_vector=query_vec, partition="suspicious", top_k=3)
        for r in results:
            score = r.get("similarity") if isinstance(r, dict) else getattr(r, "similarity", None)
            assert score is not None, "Search result must include similarity score"
            assert 0.0 <= float(score) <= 1.0 + 1e-6

    def test_serialization_round_trip(self):
        """Index can be serialized and deserialized without losing data."""
        index = self._build_index(30)
        raw = index.serialize()
        restored = CaseIndex.deserialize(raw)
        original_count = sum(len(v) for v in index._cases.values())
        restored_count = sum(len(v) for v in restored._cases.values())
        assert original_count == restored_count

    def test_empty_partition_returns_empty_list(self):
        index = CaseIndex()
        index.build([], [])
        vec = CaseVectorizer()
        results = index.search(vec.vectorize(_payload("Q")), partition="sar", top_k=5)
        assert results == []
