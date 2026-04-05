"""AML motif detection on entity graphs.

Computes per-alert presence and strength of AML-relevant structural patterns
(motifs) in the entity graph. These are among the most interpretable signals
for compliance officers and model explainability.

Motifs detected:
    fan_in_score          many counterparties → single account (aggregation)
    fan_out_score         single account → many counterparties (layering)
    circularity_score     funds flow in closed loops (cycle proxy)
    layering_score        multi-hop transaction chains (> 2 hops)
    shared_device_score   multiple entities sharing a device
    shared_ip_score       multiple entities sharing an IP address
    shared_beneficiary_score  multiple accounts sharing a beneficiary

Each score is normalised to [0, 1] so they can be used directly as model
features or combined into a composite risk signal.
"""
from __future__ import annotations

import logging
from typing import Any

from graph.entity_graph_builder import EntityGraph

logger = logging.getLogger("althea.graph.motif_detection")


class MotifDetector:
    """Detect AML structural motifs in an EntityGraph."""

    def detect(
        self,
        graph: EntityGraph,
        alert_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Compute motif scores for each alert node.

        Returns a list of row dicts with one entry per alert_id.
        """
        target_alerts = alert_ids or [
            nid for nid, node in graph.nodes.items() if node.node_type == "alert"
        ]

        # Pre-compute shared-entity maps
        device_map = self._entity_share_map(graph, "device")
        ip_map = self._entity_share_map(graph, "ip")
        cp_map = self._beneficiary_share_map(graph)

        rows = []
        for alert_id in target_alerts:
            if alert_id not in graph.nodes:
                rows.append(self._zero_row(alert_id))
                continue

            rows.append({
                "alert_id": alert_id,
                "fan_in_score": self._fan_in_score(graph, alert_id),
                "fan_out_score": self._fan_out_score(graph, alert_id),
                "circularity_score": self._circularity_score(graph, alert_id),
                "layering_score": self._layering_score(graph, alert_id),
                "shared_device_score": device_map.get(alert_id, 0.0),
                "shared_ip_score": ip_map.get(alert_id, 0.0),
                "shared_beneficiary_score": cp_map.get(alert_id, 0.0),
            })

        return rows

    @staticmethod
    def _feature_names() -> list[str]:
        return [
            "fan_in_score", "fan_out_score", "circularity_score",
            "layering_score", "shared_device_score", "shared_ip_score",
            "shared_beneficiary_score",
        ]

    @staticmethod
    def _zero_row(alert_id: str) -> dict[str, Any]:
        return {
            "alert_id": alert_id,
            "fan_in_score": 0.0,
            "fan_out_score": 0.0,
            "circularity_score": 0.0,
            "layering_score": 0.0,
            "shared_device_score": 0.0,
            "shared_ip_score": 0.0,
            "shared_beneficiary_score": 0.0,
        }

    # ------------------------------------------------------------------
    # Individual motif detectors
    # ------------------------------------------------------------------

    def _fan_in_score(self, graph: EntityGraph, alert_id: str) -> float:
        """Many-to-one: multiple counterparties → one account.

        High fan-in suggests aggregation (smurfing / structuring).
        Score = normalized count of distinct counterparty ancestors.
        """
        account_neighbors = [
            nid for nid in graph.neighbors(alert_id)
            if graph.nodes.get(nid) and graph.nodes[nid].node_type == "account"
        ]
        if not account_neighbors:
            return 0.0

        cp_count = 0
        for acct in account_neighbors:
            cp_count += sum(
                1 for nid in graph.neighbors(acct)
                if graph.nodes.get(nid) and graph.nodes[nid].node_type == "counterparty"
            )

        return float(min(cp_count / 20.0, 1.0))

    def _fan_out_score(self, graph: EntityGraph, alert_id: str) -> float:
        """One-to-many: one account → many distinct counterparties.

        High fan-out suggests layering / dispersal.
        """
        account_neighbors = [
            nid for nid in graph.neighbors(alert_id)
            if graph.nodes.get(nid) and graph.nodes[nid].node_type == "account"
        ]
        if not account_neighbors:
            return 0.0

        cp_destinations: set[str] = set()
        for acct in account_neighbors:
            for nid in graph.neighbors(acct):
                if graph.nodes.get(nid) and graph.nodes[nid].node_type == "counterparty":
                    cp_destinations.add(nid)

        return float(min(len(cp_destinations) / 15.0, 1.0))

    def _circularity_score(self, graph: EntityGraph, alert_id: str) -> float:
        """Detect short cycles involving this alert's accounts.

        Uses BFS up to depth 4 to find paths that return to the origin.
        """
        acct_nodes = [
            nid for nid in graph.neighbors(alert_id)
            if graph.nodes.get(nid) and graph.nodes[nid].node_type == "account"
        ]
        if not acct_nodes:
            return 0.0

        cycles_found = 0
        for acct in acct_nodes[:3]:  # limit to 3 accounts for performance
            if self._has_short_cycle(graph, acct, max_depth=4):
                cycles_found += 1

        return float(min(cycles_found / 3.0, 1.0))

    def _layering_score(self, graph: EntityGraph, alert_id: str) -> float:
        """Measure transaction chain depth (layering proxy).

        Score = max hop count from this alert to a leaf node, normalized.
        """
        max_depth = self._bfs_max_depth(graph, alert_id, max_depth=5)
        return float(min(max(max_depth - 1, 0) / 4.0, 1.0))

    # ------------------------------------------------------------------
    # Shared-entity maps
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_share_map(graph: EntityGraph, entity_type: str) -> dict[str, float]:
        """Map alert_id → normalized count of OTHER alerts sharing the same entity."""
        entity_to_alerts: dict[str, list[str]] = {}
        for alert_id, node in graph.nodes.items():
            if node.node_type != "alert":
                continue

            related_entities: set[str] = set()
            for neighbor_id in graph.neighbors(alert_id):
                neighbor = graph.nodes.get(neighbor_id)
                if not neighbor:
                    continue
                if neighbor.node_type == entity_type:
                    related_entities.add(neighbor_id)
                    continue
                if neighbor.node_type in {"customer", "account"}:
                    for hop2 in graph.neighbors(neighbor_id):
                        hop2_node = graph.nodes.get(hop2)
                        if hop2_node and hop2_node.node_type == entity_type:
                            related_entities.add(hop2)

            for entity_id in related_entities:
                entity_to_alerts.setdefault(entity_id, []).append(alert_id)

        shared_map: dict[str, float] = {}
        for alerts in entity_to_alerts.values():
            if len(alerts) <= 1:
                continue
            score = float(min((len(alerts) - 1) / 10.0, 1.0))
            for aid in alerts:
                shared_map[aid] = max(shared_map.get(aid, 0.0), score)
        return shared_map

    @staticmethod
    def _beneficiary_share_map(graph: EntityGraph) -> dict[str, float]:
        """Alerts sharing a beneficiary (counterparty) account."""
        cp_to_alerts: dict[str, list[str]] = {}
        for alert_id, node in graph.nodes.items():
            if node.node_type != "alert":
                continue
            # Walk 2 hops: alert → account → counterparty
            for acct_id in graph.neighbors(alert_id):
                if graph.nodes.get(acct_id) and graph.nodes[acct_id].node_type == "account":
                    for cp_id in graph.neighbors(acct_id):
                        if graph.nodes.get(cp_id) and graph.nodes[cp_id].node_type == "counterparty":
                            cp_to_alerts.setdefault(cp_id, []).append(alert_id)

        shared_map: dict[str, float] = {}
        for alerts in cp_to_alerts.values():
            if len(alerts) <= 1:
                continue
            score = float(min((len(alerts) - 1) / 5.0, 1.0))
            for aid in alerts:
                shared_map[aid] = max(shared_map.get(aid, 0.0), score)
        return shared_map

    # ------------------------------------------------------------------
    # Graph traversal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _has_short_cycle(graph: EntityGraph, start: str, max_depth: int) -> bool:
        """Return True if there is a cycle reachable from start within max_depth hops."""
        visited: set[str] = set()
        stack = [(start, 0, {start})]
        while stack:
            node, depth, path = stack.pop()
            if depth > max_depth:
                continue
            for neighbor in graph.neighbors(node):
                if neighbor in path and depth >= 2:
                    return True
                if neighbor not in visited:
                    stack.append((neighbor, depth + 1, path | {neighbor}))
            visited.add(node)
        return False

    @staticmethod
    def _bfs_max_depth(graph: EntityGraph, start: str, max_depth: int) -> int:
        """BFS max depth from start node."""
        visited = {start}
        queue = [(start, 0)]
        max_d = 0
        while queue:
            node, depth = queue.pop(0)
            max_d = max(max_d, depth)
            if depth >= max_depth:
                continue
            for neighbor in graph.neighbors(node):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth + 1))
        return max_d
