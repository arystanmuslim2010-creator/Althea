"""Graph feature extraction for ML models.

Extracts deterministic, per-node graph features from an EntityGraph.
These features feed directly into the escalation and time models
via CostFeatureBuilder and directly as ML signals.

Features extracted per alert node:
    graph_degree                 number of direct neighbors
    unique_counterparties        distinct counterparty nodes within 1 hop
    repeated_counterparty_ratio  fraction of counterparties seen > 1 time
    suspicious_neighbor_ratio    fraction of neighbors with high risk_score
    graph_component_size         size of connected component containing this alert
    centrality_proxy             degree / mean degree in component (local centrality)
    clustering_proxy             edge density in local neighborhood
"""
from __future__ import annotations

import logging
from typing import Any

from graph.entity_graph_builder import EntityGraph

logger = logging.getLogger("althea.graph.feature_extraction")

_SUSPICIOUS_RISK_THRESHOLD = 70.0


class GraphFeatureExtractor:
    """Extract per-alert ML features from an EntityGraph."""

    def extract(
        self,
        graph: EntityGraph,
        alert_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Compute graph features for each alert node.

        Parameters
        ----------
        graph     : EntityGraph built by EntityGraphBuilder
        alert_ids : optional subset of alert IDs to compute features for;
                    if None, computes for all alert nodes

        Returns a list of row dicts with one entry per alert_id.
        """
        target_alerts = alert_ids if alert_ids is not None else [
            nid for nid, node in graph.nodes.items() if node.node_type == "alert"
        ]

        # Pre-compute component sizes using BFS
        component_sizes = self._compute_components(graph)

        rows = []
        for alert_id in target_alerts:
            if alert_id not in graph.nodes:
                rows.append(self._zero_row(alert_id))
                continue

            neighbors = graph.neighbors(alert_id)
            degree = len(neighbors)

            # Counterparty analysis
            cp_neighbors = [
                nid for nid in neighbors
                if graph.nodes.get(nid, None) is not None and graph.nodes[nid].node_type == "counterparty"
            ]
            unique_cps = len(set(cp_neighbors))

            # Suspicious neighbor ratio (alerts and customers with high risk)
            suspicious = sum(
                1 for nid in neighbors
                if graph.nodes.get(nid) is not None
                and float(graph.nodes[nid].attributes.get("risk_score", 0.0) or 0.0) >= _SUSPICIOUS_RISK_THRESHOLD
            )
            suspicious_ratio = suspicious / max(degree, 1)

            # Component size
            comp_size = component_sizes.get(alert_id, 1)

            # Centrality proxy: degree relative to local neighborhood mean
            neighbor_degrees = [len(graph.neighbors(n)) for n in neighbors]
            mean_neighbor_degree = sum(neighbor_degrees) / max(len(neighbor_degrees), 1)
            centrality_proxy = float(degree) / max(mean_neighbor_degree, 1.0)

            # Clustering proxy: actual edges in neighborhood / possible edges
            neighbor_set = set(neighbors)
            actual_neighbor_edges = sum(
                1 for e in graph.edges
                if e.source in neighbor_set and e.target in neighbor_set
            )
            possible_neighbor_edges = max(len(neighbor_set) * (len(neighbor_set) - 1) / 2, 1)
            clustering_proxy = min(actual_neighbor_edges / possible_neighbor_edges, 1.0)

            # Repeated counterparty ratio
            repeated_cp_ratio = 0.0  # requires transaction-level data; default 0

            rows.append({
                "alert_id": alert_id,
                "graph_degree": float(degree),
                "unique_counterparties": float(unique_cps),
                "repeated_counterparty_ratio": repeated_cp_ratio,
                "suspicious_neighbor_ratio": suspicious_ratio,
                "graph_component_size": float(comp_size),
                "centrality_proxy": float(centrality_proxy),
                "clustering_proxy": float(clustering_proxy),
            })

        return rows

    @staticmethod
    def _feature_names() -> list[str]:
        return [
            "graph_degree",
            "unique_counterparties",
            "repeated_counterparty_ratio",
            "suspicious_neighbor_ratio",
            "graph_component_size",
            "centrality_proxy",
            "clustering_proxy",
        ]

    @staticmethod
    def _zero_row(alert_id: str) -> dict[str, Any]:
        return {
            "alert_id": alert_id,
            "graph_degree": 0.0,
            "unique_counterparties": 0.0,
            "repeated_counterparty_ratio": 0.0,
            "suspicious_neighbor_ratio": 0.0,
            "graph_component_size": 1.0,
            "centrality_proxy": 1.0,
            "clustering_proxy": 0.0,
        }

    @staticmethod
    def _compute_components(graph: EntityGraph) -> dict[str, int]:
        """BFS-based connected component size computation.

        Returns a dict mapping each node_id to the size of its component.
        """
        visited: set[str] = set()
        component_sizes: dict[str, int] = {}

        for start_node in graph.nodes:
            if start_node in visited:
                continue
            # BFS
            component: list[str] = []
            queue = [start_node]
            while queue:
                node = queue.pop(0)
                if node in visited:
                    continue
                visited.add(node)
                component.append(node)
                for neighbor in graph.neighbors(node):
                    if neighbor not in visited:
                        queue.append(neighbor)
            size = len(component)
            for nid in component:
                component_sizes[nid] = size

        return component_sizes
