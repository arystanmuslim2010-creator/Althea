"""Relationship Graph Service — entity relationship graphs for AML investigation."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.graph.relationship")


class RelationshipGraphService:
    """Build entity relationship graphs from alert data connecting customers, accounts,
    beneficiaries, devices, IPs, and counterparties with risk signal annotations."""

    # Maps alert payload field → (node_type, label_prefix)
    _ENTITY_FIELDS: dict[str, tuple[str, str]] = {
        "user_id": ("customer", "Customer"),
        "customer_id": ("customer", "Customer"),
        "account_id": ("account", "Account"),
        "beneficiary_id": ("beneficiary", "Beneficiary"),
        "beneficiary_account": ("beneficiary", "Beneficiary"),
        "counterparty_id": ("counterparty", "Counterparty"),
        "counterparty_account": ("counterparty", "Counterparty"),
        "device_id": ("device", "Device"),
        "device_fingerprint": ("device", "Device"),
        "ip_address": ("ip", "IP"),
        "ip": ("ip", "IP"),
    }

    # High-risk countries for node annotation
    _HIGH_RISK_COUNTRIES = {"IR", "KP", "RU", "SY", "CU", "VE", "UA", "BY"}

    def __init__(self, repository) -> None:
        self._repository = repository

    @staticmethod
    def _parse_json(raw: Any, default: Any) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw.strip()) if raw.strip() else default
            except Exception:
                return default
        return default

    def _build_risk_signals(self, payload: dict, node_id: str, node_type: str) -> list[str]:
        signals: list[str] = []
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)

        if node_type == "customer":
            if risk_score >= 85:
                signals.append("customer_critical_risk")
            elif risk_score >= 70:
                signals.append("customer_high_risk")

        if node_type == "device":
            signals.append("device_linked_to_alert")

        if node_type == "ip":
            signals.append("ip_linked_to_alert")

        if node_type == "beneficiary":
            signals.append("new_or_suspicious_beneficiary")

        if node_type in ("counterparty", "beneficiary"):
            country = str(payload.get("country") or "").upper()
            if country in self._HIGH_RISK_COUNTRIES:
                signals.append(f"high_risk_jurisdiction:{country}")

        typology = str(payload.get("typology") or "").lower()
        if typology == "sanctions" and node_type in ("counterparty", "beneficiary"):
            signals.append("sanctions_exposure_risk")

        rules = self._parse_json(payload.get("rules_json"), [])
        if isinstance(rules, list) and rules:
            signals.append("rule_triggered")

        return signals

    def _build_nodes_and_edges(self, payload: dict) -> tuple[list[dict], list[dict]]:
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []

        # Anchor node: the alert itself
        alert_id = str(payload.get("alert_id") or payload.get("id") or "alert")
        risk_score = float(payload.get("risk_score", 0.0) or 0.0)
        risk_band = str(payload.get("risk_band") or "").upper()

        nodes[alert_id] = {
            "id": alert_id,
            "type": "alert",
            "label": f"Alert {alert_id}",
            "risk_score": risk_score,
            "risk_band": risk_band,
            "risk_signals": ["alert_node"],
            "properties": {
                "typology": payload.get("typology"),
                "segment": payload.get("segment"),
                "governance_status": payload.get("governance_status"),
            },
        }

        # Build entity nodes from known fields
        seen_pairs: set[tuple[str, str]] = set()
        for field, (node_type, label_prefix) in self._ENTITY_FIELDS.items():
            value = payload.get(field)
            if value is None or str(value).strip() == "":
                continue
            value_str = str(value).strip()
            node_id = f"{node_type}:{value_str}"

            if node_id not in nodes:
                risk_signals = self._build_risk_signals(payload, node_id, node_type)
                nodes[node_id] = {
                    "id": node_id,
                    "type": node_type,
                    "label": f"{label_prefix} {value_str}",
                    "risk_signals": risk_signals,
                    "properties": {"value": value_str, "source_field": field},
                }

            edge_key = (alert_id, node_id)
            if edge_key not in seen_pairs:
                seen_pairs.add(edge_key)
                edges.append(
                    {
                        "source": alert_id,
                        "target": node_id,
                        "relation": field,
                        "weight": 1.0,
                    }
                )

        # Link customer → account if both exist
        customer_id = str(payload.get("user_id") or payload.get("customer_id") or "")
        account_id = str(payload.get("account_id") or "")
        if customer_id and account_id:
            src = f"customer:{customer_id}"
            tgt = f"account:{account_id}"
            edge_key = (src, tgt)
            if edge_key not in seen_pairs and src in nodes and tgt in nodes:
                seen_pairs.add(edge_key)
                edges.append({"source": src, "target": tgt, "relation": "owns_account", "weight": 1.0})

        # Link account → beneficiary if both exist
        if account_id:
            benef_id = str(payload.get("beneficiary_id") or payload.get("beneficiary_account") or "")
            if benef_id:
                src = f"account:{account_id}"
                tgt = f"beneficiary:{benef_id}"
                edge_key = (src, tgt)
                if edge_key not in seen_pairs and src in nodes and tgt in nodes:
                    seen_pairs.add(edge_key)
                    edges.append(
                        {"source": src, "target": tgt, "relation": "transfers_to", "weight": 1.5}
                    )

        return list(nodes.values()), edges

    def _aggregate_risk_signals(self, nodes: list[dict]) -> list[str]:
        all_signals: set[str] = set()
        for node in nodes:
            for sig in node.get("risk_signals", []):
                if sig not in {"alert_node", "rule_triggered", "device_linked_to_alert", "ip_linked_to_alert"}:
                    all_signals.add(sig)
        return sorted(all_signals)

    def build_graph(
        self,
        tenant_id: str,
        alert_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        logger.info(
            "Building relationship graph",
            extra={"tenant_id": tenant_id, "alert_id": alert_id},
        )

        if not run_id:
            runs = self._repository.list_pipeline_runs(tenant_id, limit=20)
            run_id = next((str(r.get("run_id")) for r in runs if r.get("run_id")), "")

        payloads = self._repository.list_alert_payloads_by_run(
            tenant_id=tenant_id, run_id=run_id or "", limit=500000
        )
        payload = next((p for p in payloads if str(p.get("alert_id")) == str(alert_id)), {})

        nodes, edges = self._build_nodes_and_edges(payload)
        risk_signals = self._aggregate_risk_signals(nodes)

        result = {
            "alert_id": alert_id,
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "risk_signals": risk_signals,
            "relationship_types": list({e["relation"] for e in edges}),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        elapsed = time.perf_counter() - t0
        logger.info(
            "Relationship graph built",
            extra={
                "alert_id": alert_id,
                "nodes": len(nodes),
                "edges": len(edges),
                "latency_s": round(elapsed, 3),
            },
        )
        return result
