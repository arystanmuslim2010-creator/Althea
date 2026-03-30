"""Relationship graph service for lightweight investigation visualization."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from core.observability import record_graph_generation, record_graph_generation_failure

logger = logging.getLogger("althea.graph.relationship")


class RelationshipGraphService:
    MAX_NODES = 50
    MAX_EDGES = 100

    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _risk_bucket(score: float | int | None) -> str:
        value = float(score or 0.0)
        if value >= 85:
            return "high"
        if value >= 70:
            return "medium"
        return "low"

    @staticmethod
    def _node(node_id: str, label: str, node_type: str, risk: str = "low", meta: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "id": node_id,
            "label": label,
            "type": node_type,
            "risk": risk,
            "meta": meta or {},
        }
        payload["properties"] = payload["meta"]
        return payload

    @staticmethod
    def _edge(source: str, target: str, edge_type: str, weight: float = 1.0, meta: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "source": source,
            "target": target,
            "type": edge_type,
            "weight": float(weight),
            "meta": meta or {},
        }
        payload["relation"] = edge_type
        return payload

    def _empty(self, alert_id: str) -> dict[str, Any]:
        return {
            "alert_id": alert_id,
            "nodes": [],
            "edges": [],
            "summary": {"node_count": 0, "edge_count": 0, "high_risk_nodes": 0},
            "node_count": 0,
            "edge_count": 0,
            "risk_signals": [],
            "relationship_types": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _non_empty_values(payload: dict[str, Any], *keys: str) -> list[tuple[str, str]]:
        values: list[tuple[str, str]] = []
        for key in keys:
            raw = payload.get(key)
            if raw is None:
                continue
            value = str(raw).strip()
            if value:
                values.append((key, value))
        return values

    def _build_related_alerts(self, payloads: list[dict[str, Any]], anchor: dict[str, Any], alert_id: str) -> list[dict[str, Any]]:
        user_id = str(anchor.get("user_id") or anchor.get("customer_id") or "").strip()
        account_id = str(anchor.get("account_id") or "").strip()
        device_id = str(anchor.get("device_id") or anchor.get("device_fingerprint") or "").strip()
        ip_addr = str(anchor.get("ip_address") or anchor.get("ip") or "").strip()

        related: list[dict[str, Any]] = []
        for row in payloads:
            rid = str(row.get("alert_id") or "").strip()
            if not rid or rid == alert_id:
                continue
            if user_id and str(row.get("user_id") or row.get("customer_id") or "").strip() == user_id:
                related.append(row)
                continue
            if account_id and str(row.get("account_id") or "").strip() == account_id:
                related.append(row)
                continue
            if device_id and str(row.get("device_id") or row.get("device_fingerprint") or "").strip() == device_id:
                related.append(row)
                continue
            if ip_addr and str(row.get("ip_address") or row.get("ip") or "").strip() == ip_addr:
                related.append(row)
                continue
        return related[:10]

    def _bounded(self, graph: dict[str, Any]) -> dict[str, Any]:
        nodes = list(graph.get("nodes") or [])
        edges = list(graph.get("edges") or [])
        if len(nodes) > self.MAX_NODES:
            nodes = nodes[: self.MAX_NODES]
        allowed_ids = {str(node.get("id")) for node in nodes}
        bounded_edges = [
            edge
            for edge in edges
            if str(edge.get("source")) in allowed_ids and str(edge.get("target")) in allowed_ids
        ]
        if len(bounded_edges) > self.MAX_EDGES:
            bounded_edges = bounded_edges[: self.MAX_EDGES]
        high_risk_nodes = sum(1 for node in nodes if str(node.get("risk")).lower() in {"high", "critical"})
        graph["nodes"] = nodes
        graph["edges"] = bounded_edges
        graph["summary"] = {
            "node_count": len(nodes),
            "edge_count": len(bounded_edges),
            "high_risk_nodes": high_risk_nodes,
        }
        graph["node_count"] = len(nodes)
        graph["edge_count"] = len(bounded_edges)
        graph["relationship_types"] = sorted({str(edge.get("type") or "") for edge in bounded_edges if edge.get("type")})
        return graph

    def build_graph(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        logger.info("graph_generation_started", extra={"tenant_id": tenant_id, "alert_id": alert_id})
        try:
            if not run_id:
                runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
                run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

            payloads = self._repository.list_alert_payloads_by_run(
                tenant_id=tenant_id,
                run_id=run_id or "",
                limit=500000,
            )
            payload = next((row for row in payloads if str(row.get("alert_id")) == str(alert_id)), None)
            if not payload:
                graph = self._empty(alert_id)
                record_graph_generation(time.perf_counter() - started)
                return graph

            nodes: dict[str, dict[str, Any]] = {}
            edges: list[dict[str, Any]] = []
            seen_edges: set[tuple[str, str, str]] = set()
            risk_bucket = self._risk_bucket(payload.get("risk_score"))
            anchor_id = f"alert:{alert_id}"

            nodes[anchor_id] = self._node(
                node_id=anchor_id,
                label=f"Alert {alert_id}",
                node_type="alert",
                risk=risk_bucket,
                meta={
                    "risk_score": payload.get("risk_score"),
                    "typology": payload.get("typology"),
                    "segment": payload.get("segment"),
                    "priority": payload.get("priority") or payload.get("risk_band"),
                },
            )

            customer = str(payload.get("customer_id") or payload.get("user_id") or "").strip()
            customer_id = None
            if customer:
                customer_id = f"customer:{customer}"
                nodes[customer_id] = self._node(
                    node_id=customer_id,
                    label=f"Customer {customer}",
                    node_type="customer",
                    risk=risk_bucket,
                    meta={"customer_id": customer},
                )
                seen_edges.add((anchor_id, customer_id, "associated_with"))
                edges.append(self._edge(anchor_id, customer_id, "associated_with", 1.0, {"source": "alert_payload"}))

            source_account = str(payload.get("account_id") or "").strip()
            source_account_id = None
            if source_account:
                source_account_id = f"account:{source_account}"
                nodes[source_account_id] = self._node(
                    node_id=source_account_id,
                    label=f"Source Account {source_account}",
                    node_type="source_account",
                    risk=risk_bucket,
                    meta={"account_id": source_account},
                )
                edge_key = (anchor_id, source_account_id, "source_account")
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(self._edge(anchor_id, source_account_id, "source_account", 1.0))
                if customer_id:
                    edge_key = (customer_id, source_account_id, "owns_account")
                    if edge_key not in seen_edges:
                        seen_edges.add(edge_key)
                        edges.append(self._edge(customer_id, source_account_id, "owns_account", 1.0))

            counterparty_values = self._non_empty_values(
                payload,
                "counterparty_id",
                "counterparty_account",
                "beneficiary_id",
                "beneficiary_account",
            )
            for source_key, value in counterparty_values:
                node_id = f"counterparty:{value}"
                if node_id not in nodes:
                    nodes[node_id] = self._node(
                        node_id=node_id,
                        label=f"Counterparty {value}",
                        node_type="counterparty",
                        risk=self._risk_bucket(payload.get("risk_score")),
                        meta={"value": value, "source_field": source_key},
                    )
                edge_source = source_account_id or anchor_id
                edge_key = (edge_source, node_id, "transaction")
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(
                        self._edge(
                            edge_source,
                            node_id,
                            "transaction",
                            1.0,
                            {"source_field": source_key, "amount": payload.get("amount")},
                        )
                    )

            for key, node_type, edge_type, label_prefix in [
                ("device_id", "device", "uses_device", "Device"),
                ("device_fingerprint", "device", "uses_device", "Device"),
                ("ip_address", "ip", "uses_ip", "IP"),
                ("ip", "ip", "uses_ip", "IP"),
            ]:
                value = str(payload.get(key) or "").strip()
                if not value:
                    continue
                node_id = f"{node_type}:{value}"
                if node_id not in nodes:
                    nodes[node_id] = self._node(
                        node_id=node_id,
                        label=f"{label_prefix} {value}",
                        node_type=node_type,
                        risk="medium",
                        meta={"value": value, "source_field": key},
                    )
                edge_key = (anchor_id, node_id, edge_type)
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(self._edge(anchor_id, node_id, edge_type, 1.0))

            for related in self._build_related_alerts(payloads, payload, alert_id):
                related_id = str(related.get("alert_id") or "").strip()
                if not related_id:
                    continue
                node_id = f"alert:{related_id}"
                if node_id not in nodes:
                    nodes[node_id] = self._node(
                        node_id=node_id,
                        label=f"Related Alert {related_id}",
                        node_type="related_alert",
                        risk=self._risk_bucket(related.get("risk_score")),
                        meta={
                            "risk_score": related.get("risk_score"),
                            "typology": related.get("typology"),
                        },
                    )
                edge_key = (anchor_id, node_id, "related_alert")
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(self._edge(anchor_id, node_id, "related_alert", 1.0))

            graph = {
                "alert_id": alert_id,
                "nodes": list(nodes.values()),
                "edges": edges,
                "summary": {"node_count": 0, "edge_count": 0, "high_risk_nodes": 0},
                "risk_signals": sorted({str(node.get("type")) for node in nodes.values() if node.get("type")}),
                "relationship_types": [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            bounded = self._bounded(graph)
            record_graph_generation(time.perf_counter() - started)
            logger.info(
                "graph_generation_succeeded",
                extra={
                    "tenant_id": tenant_id,
                    "alert_id": alert_id,
                    "node_count": bounded["summary"]["node_count"],
                    "edge_count": bounded["summary"]["edge_count"],
                },
            )
            return bounded
        except Exception:
            record_graph_generation_failure()
            logger.exception("graph_generation_failed", extra={"tenant_id": tenant_id, "alert_id": alert_id})
            return self._empty(alert_id)
