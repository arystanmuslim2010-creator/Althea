"""Canonical entity graph builder.

Constructs a deterministic in-memory entity graph from alert payloads.
This graph is the shared backbone used by both:
- ML feature extraction (graph signals for models)
- Visualization layer (relationship_graph_service.py)

The graph is represented as a dict of adjacency lists rather than a
third-party graph library dependency to keep the deployment footprint
minimal. For production deployments with networkx available it will
use networkx.Graph for richer algorithm support.

Node types:
    customer     — KYC customer entity
    account      — bank account
    counterparty — external payment recipient
    device       — device fingerprint
    ip           — IP address
    alert        — the alert itself

Edge types:
    associated_with   customer ↔ alert
    source_account    account ↔ alert
    owns_account      customer ↔ account
    transacts_with    account ↔ counterparty
    uses_device       entity ↔ device
    uses_ip           entity ↔ ip
    co_alert          alert ↔ alert (shared entity linkage)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("althea.graph.entity_graph")

try:
    import networkx as nx
    _NX_AVAILABLE = True
except Exception:
    nx = None  # type: ignore
    _NX_AVAILABLE = False


@dataclass
class GraphNode:
    node_id: str
    node_type: str  # customer, account, counterparty, device, ip, alert
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphEdge:
    source: str
    target: str
    edge_type: str
    weight: float = 1.0
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class EntityGraph:
    nodes: dict[str, GraphNode]  # node_id → GraphNode
    edges: list[GraphEdge]
    adjacency: dict[str, set[str]]  # node_id → connected node_ids

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    def neighbors(self, node_id: str) -> set[str]:
        return self.adjacency.get(node_id, set())

    def subgraph(self, node_ids: set[str]) -> "EntityGraph":
        """Return a subgraph containing only the specified nodes and their edges."""
        nodes = {nid: self.nodes[nid] for nid in node_ids if nid in self.nodes}
        edges = [e for e in self.edges if e.source in node_ids and e.target in node_ids]
        adj: dict[str, set[str]] = {nid: set() for nid in node_ids}
        for e in edges:
            adj[e.source].add(e.target)
            adj[e.target].add(e.source)
        return EntityGraph(nodes=nodes, edges=edges, adjacency=adj)

    def to_networkx(self):
        """Convert to networkx.Graph if networkx is available."""
        if not _NX_AVAILABLE:
            raise ImportError("networkx is not installed")
        G = nx.Graph()
        for nid, node in self.nodes.items():
            G.add_node(nid, node_type=node.node_type, **node.attributes)
        for e in self.edges:
            G.add_edge(e.source, e.target, edge_type=e.edge_type, weight=e.weight)
        return G


class EntityGraphBuilder:
    """Build a canonical entity graph from a list of alert payload dicts."""

    # Hard limits to prevent runaway graph construction
    MAX_NODES = 5000
    MAX_EDGES = 10000

    def build(self, alert_payloads: list[dict[str, Any]]) -> EntityGraph:
        """Build an entity graph from alert payloads.

        Parameters
        ----------
        alert_payloads : list of alert row dicts, each containing fields like
            alert_id, user_id, customer_id, account_id, counterparty_id,
            device_id, ip_address, risk_score, etc.

        Returns
        -------
        EntityGraph with nodes, edges, and adjacency index.
        """
        nodes: dict[str, GraphNode] = {}
        edges: list[GraphEdge] = []
        adjacency: dict[str, set[str]] = {}

        def _add_node(nid: str, ntype: str, attrs: dict[str, Any] | None = None) -> bool:
            if len(nodes) >= self.MAX_NODES:
                return False
            if nid not in nodes:
                nodes[nid] = GraphNode(node_id=nid, node_type=ntype, attributes=attrs or {})
                adjacency[nid] = set()
            return True

        def _add_edge(src: str, tgt: str, etype: str, weight: float = 1.0) -> bool:
            if len(edges) >= self.MAX_EDGES:
                return False
            if src not in nodes or tgt not in nodes:
                return False
            edges.append(GraphEdge(source=src, target=tgt, edge_type=etype, weight=weight))
            adjacency.setdefault(src, set()).add(tgt)
            adjacency.setdefault(tgt, set()).add(src)
            return True

        for payload in alert_payloads:
            alert_id = str(payload.get("alert_id") or "").strip()
            if not alert_id:
                continue

            risk_score = float(payload.get("risk_score", 0.0) or 0.0)
            _add_node(alert_id, "alert", {
                "risk_score": risk_score,
                "typology": str(payload.get("typology") or ""),
                "amount": float(payload.get("amount", 0.0) or 0.0),
            })

            # Customer node
            customer_id = self._extract(payload, "user_id", "customer_id")
            if customer_id:
                cnode = f"cust:{customer_id}"
                _add_node(cnode, "customer", {"customer_id": customer_id})
                _add_edge(alert_id, cnode, "associated_with")

            # Source account node
            account_id = self._extract(payload, "account_id", "source_account")
            if account_id:
                anode = f"acct:{account_id}"
                _add_node(anode, "account", {"account_id": account_id})
                _add_edge(alert_id, anode, "source_account")
                if customer_id:
                    _add_edge(f"cust:{customer_id}", anode, "owns_account")

            # Counterparty nodes
            for cp_field in ("counterparty_id", "beneficiary_id", "counterparty_account", "beneficiary_account"):
                cp = self._extract(payload, cp_field)
                if cp:
                    cpnode = f"cp:{cp}"
                    _add_node(cpnode, "counterparty", {"counterparty_id": cp})
                    if account_id:
                        _add_edge(f"acct:{account_id}", cpnode, "transacts_with")
                    else:
                        _add_edge(alert_id, cpnode, "transacts_with")

            # Device node
            device_id = self._extract(payload, "device_id", "device_fingerprint")
            if device_id:
                dnode = f"dev:{device_id}"
                _add_node(dnode, "device", {"device_id": device_id})
                src = f"cust:{customer_id}" if customer_id else alert_id
                _add_edge(src, dnode, "uses_device")

            # IP node
            ip_addr = self._extract(payload, "ip_address", "ip")
            if ip_addr:
                ipnode = f"ip:{ip_addr}"
                _add_node(ipnode, "ip", {"ip": ip_addr})
                src = f"cust:{customer_id}" if customer_id else alert_id
                _add_edge(src, ipnode, "uses_ip")

        # Co-alert edges: connect alerts that share a customer / account / device
        self._add_co_alert_edges(alert_payloads, nodes, edges, adjacency)

        logger.debug(
            "EntityGraphBuilder: built graph nodes=%d edges=%d",
            len(nodes),
            len(edges),
        )
        return EntityGraph(nodes=nodes, edges=edges, adjacency=adjacency)

    def _add_co_alert_edges(
        self,
        payloads: list[dict[str, Any]],
        nodes: dict[str, GraphNode],
        edges: list[GraphEdge],
        adjacency: dict[str, set[str]],
    ) -> None:
        """Connect alert nodes that share an entity (customer, account, device)."""
        from collections import defaultdict
        entity_to_alerts: dict[str, list[str]] = defaultdict(list)
        for payload in payloads:
            alert_id = str(payload.get("alert_id") or "")
            for field in ("user_id", "customer_id", "account_id", "device_id"):
                val = self._extract(payload, field)
                if val:
                    entity_to_alerts[f"{field}:{val}"].append(alert_id)

        for _, alert_ids in entity_to_alerts.items():
            unique = list(set(alert_ids))
            for i in range(len(unique)):
                for j in range(i + 1, len(unique)):
                    a1, a2 = unique[i], unique[j]
                    if a1 in nodes and a2 in nodes and len(edges) < self.MAX_EDGES:
                        edges.append(GraphEdge(source=a1, target=a2, edge_type="co_alert", weight=0.5))
                        adjacency.setdefault(a1, set()).add(a2)
                        adjacency.setdefault(a2, set()).add(a1)

    @staticmethod
    def _extract(payload: dict[str, Any], *fields: str) -> str | None:
        for f in fields:
            val = str(payload.get(f) or "").strip()
            if val and val.lower() not in {"none", "null", "nan", ""}:
                return val
        return None
