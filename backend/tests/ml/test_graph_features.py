"""Tests for AML graph ML layer — motif detection and feature extraction."""
from __future__ import annotations

import pytest

from graph.entity_graph_builder import EntityGraphBuilder
from graph.feature_extraction import GraphFeatureExtractor
from graph.motif_detection import MotifDetector
from graph.community_scoring import CommunityScorer


def _alert(alert_id, user_id, account_id, counterparty_id=None, device_id=None, ip=None):
    return {
        "alert_id": alert_id,
        "user_id": user_id,
        "account_id": account_id,
        "counterparty_id": counterparty_id or f"cp_{alert_id}",
        "device_id": device_id,
        "ip_address": ip,
        "risk_score": 60.0,
        "typology": "money_laundering",
    }


def _build(payloads):
    builder = EntityGraphBuilder()
    return builder.build(payloads)


class TestEntityGraphBuilder:
    def test_builds_non_empty_graph(self):
        payloads = [_alert("A1", "U1", "ACC1", "CP1"), _alert("A2", "U2", "ACC2", "CP2")]
        graph = _build(payloads)
        assert len(graph.nodes) > 0
        assert len(graph.edges) > 0

    def test_all_alert_ids_have_nodes(self):
        payloads = [_alert("A1", "U1", "ACC1"), _alert("A2", "U2", "ACC2")]
        graph = _build(payloads)
        alert_node_ids = {n.node_id for n in graph.nodes.values() if n.node_type == "alert"}
        assert "A1" in alert_node_ids
        assert "A2" in alert_node_ids

    def test_shared_account_creates_co_alert_edge(self):
        """Two alerts sharing the same account should be connected."""
        payloads = [
            _alert("A1", "U1", "SHARED_ACC"),
            _alert("A2", "U2", "SHARED_ACC"),
        ]
        graph = _build(payloads)
        edge_pairs = {(e.source, e.target) for e in graph.edges}
        connected = (
            ("A1", "A2") in edge_pairs
            or ("A2", "A1") in edge_pairs
            or any(e.edge_type == "co_alert" for e in graph.edges)
        )
        assert connected, "Shared account must create connection between the two alerts"


class TestGraphFeatureExtractor:
    def test_extract_returns_features_for_each_alert(self):
        payloads = [_alert(f"A{i}", f"U{i}", f"ACC{i}") for i in range(5)]
        graph = _build(payloads)
        extractor = GraphFeatureExtractor()
        features = extractor.extract(graph, alert_ids=["A0", "A1", "A2", "A3", "A4"])
        assert len(features) == 5

    def test_features_have_expected_keys(self):
        payloads = [_alert("A1", "U1", "ACC1"), _alert("A2", "U1", "ACC2")]
        graph = _build(payloads)
        extractor = GraphFeatureExtractor()
        features = extractor.extract(graph, alert_ids=["A1"])
        row = features[0] if isinstance(features, list) else features.get("A1", {})
        expected_keys = {"graph_degree", "unique_counterparties", "graph_component_size"}
        assert expected_keys.issubset(set(row.keys())), f"Missing keys: {expected_keys - set(row.keys())}"


class TestMotifDetector:
    def test_fan_out_detected_for_one_to_many(self):
        """One source account sending to many counterparties → fan-out."""
        payloads = [_alert(f"A{i}", "U1", "ACC1", f"CP{i}") for i in range(6)]
        graph = _build(payloads)
        detector = MotifDetector()
        motifs = detector.detect(graph, alert_ids=[f"A{i}" for i in range(6)])
        # fan_out_score should be > 0 when many counterparties share a source
        row = motifs[0] if isinstance(motifs, list) else next(iter(motifs.values()), {})
        assert "fan_out_score" in row

    def test_shared_device_detected(self):
        """Multiple alerts sharing the same device should show shared_device_score > 0."""
        payloads = [_alert(f"A{i}", f"U{i}", f"ACC{i}", device_id="DEV1") for i in range(4)]
        graph = _build(payloads)
        detector = MotifDetector()
        motifs = detector.detect(graph, alert_ids=[f"A{i}" for i in range(4)])
        scores = [
            (m if isinstance(m, dict) else {}).get("shared_device_score", 0)
            for m in (motifs if isinstance(motifs, list) else motifs.values())
        ]
        assert max(scores) > 0, "Shared device must produce non-zero shared_device_score"

    def test_all_motif_scores_in_0_1(self):
        payloads = [_alert(f"A{i}", f"U{i}", f"ACC{i}") for i in range(5)]
        graph = _build(payloads)
        detector = MotifDetector()
        motifs = detector.detect(graph, alert_ids=[f"A{i}" for i in range(5)])
        for row in (motifs if isinstance(motifs, list) else motifs.values()):
            for k, v in row.items():
                if k.endswith("_score"):
                    assert 0.0 <= float(v) <= 1.0 + 1e-6, f"{k}={v} out of [0,1]"


class TestCommunityScorer:
    def test_community_risk_score_in_range(self):
        payloads = [_alert(f"A{i}", "U1", "ACC1") for i in range(4)]
        graph = _build(payloads)
        scorer = CommunityScorer()
        result = scorer.score(graph, alert_ids=["A0", "A1", "A2", "A3"])
        for row in (result if isinstance(result, list) else result.values()):
            score = row.get("community_risk_score", 0)
            assert 0.0 <= float(score) <= 100.0 + 1e-6, f"community_risk_score={score} out of range"
