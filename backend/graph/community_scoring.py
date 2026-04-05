"""Community risk scoring for entity graphs.

Assigns community-level risk scores to entities based on:
- density of suspicious neighbors in the community
- fraction of prior-escalated nodes in the community
- cluster concentration (Gini-style measure of how concentrated the community is)
- overall community risk score (weighted composite)

These signals capture guilt-by-association patterns that individual
node features cannot — a legitimate entity that happens to be embedded
in a high-risk cluster is statistically more likely to be involved.
"""
from __future__ import annotations

import logging
from typing import Any

from graph.entity_graph_builder import EntityGraph

logger = logging.getLogger("althea.graph.community_scoring")

_SUSPICIOUS_RISK_THRESHOLD = 70.0


class CommunityScorer:
    """Compute community-level risk scores for each alert node."""

    def score(
        self,
        graph: EntityGraph,
        alert_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Compute community risk features for each alert.

        Returns a list of row dicts with one entry per alert_id.
        """
        target_alerts = alert_ids or [
            nid for nid, node in graph.nodes.items() if node.node_type == "alert"
        ]

        # Identify communities via greedy BFS label propagation
        communities = self._detect_communities(graph)

        rows = []
        for alert_id in target_alerts:
            if alert_id not in graph.nodes:
                rows.append(self._zero_row(alert_id))
                continue

            community_id = communities.get(alert_id, alert_id)
            community_nodes = [nid for nid, cid in communities.items() if cid == community_id]

            rows.append({
                "alert_id": alert_id,
                **self._compute_community_features(graph, alert_id, community_nodes),
            })

        return rows

    @staticmethod
    def _feature_names() -> list[str]:
        return [
            "community_risk_score",
            "suspicious_density",
            "prior_escalated_ratio",
            "cluster_concentration",
            "community_size",
        ]

    @staticmethod
    def _zero_row(alert_id: str) -> dict[str, Any]:
        return {
            "alert_id": alert_id,
            "community_risk_score": 0.0,
            "suspicious_density": 0.0,
            "prior_escalated_ratio": 0.0,
            "cluster_concentration": 0.0,
            "community_size": 1.0,
        }

    def _compute_community_features(
        self,
        graph: EntityGraph,
        alert_id: str,
        community_nodes: list[str],
    ) -> dict[str, float]:
        n = len(community_nodes)
        if n == 0:
            return {k: 0.0 for k in self._feature_names()}

        # Suspicious density: fraction of nodes with risk_score >= threshold
        risk_scores = [
            float(graph.nodes[nid].attributes.get("risk_score", 0.0) or 0.0)
            for nid in community_nodes
            if nid in graph.nodes
        ]
        suspicious_count = sum(1 for r in risk_scores if r >= _SUSPICIOUS_RISK_THRESHOLD)
        suspicious_density = suspicious_count / max(n, 1)

        # Prior escalated ratio (from node attributes set externally)
        escalated_count = sum(
            1 for nid in community_nodes
            if graph.nodes.get(nid) and graph.nodes[nid].attributes.get("prior_escalated", False)
        )
        prior_escalated_ratio = escalated_count / max(n, 1)

        # Cluster concentration: edge density (actual / possible edges)
        actual_edges = sum(
            1 for e in graph.edges
            if e.source in set(community_nodes) and e.target in set(community_nodes)
        )
        possible_edges = max(n * (n - 1) / 2, 1)
        cluster_concentration = min(actual_edges / possible_edges, 1.0)

        # Composite community risk score
        mean_risk = sum(risk_scores) / max(len(risk_scores), 1)
        community_risk_score = float(
            mean_risk * 0.40
            + suspicious_density * 100.0 * 0.30
            + prior_escalated_ratio * 100.0 * 0.20
            + cluster_concentration * 100.0 * 0.10
        )
        community_risk_score = min(community_risk_score, 100.0)

        return {
            "community_risk_score": community_risk_score,
            "suspicious_density": suspicious_density,
            "prior_escalated_ratio": prior_escalated_ratio,
            "cluster_concentration": cluster_concentration,
            "community_size": float(n),
        }

    @staticmethod
    def _detect_communities(graph: EntityGraph) -> dict[str, str]:
        """Label-propagation-inspired community detection.

        Each node gets a community label (the earliest BFS-reached root in
        its connected component). Lightweight — no external library needed.
        """
        labels: dict[str, str] = {}
        for start in graph.nodes:
            if start in labels:
                continue
            queue = [start]
            while queue:
                node = queue.pop(0)
                if node in labels:
                    continue
                labels[node] = start
                for neighbor in graph.neighbors(node):
                    if neighbor not in labels:
                        queue.append(neighbor)
        return labels
