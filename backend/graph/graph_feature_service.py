"""Graph feature service — orchestrates graph construction and ML feature extraction.

This is the ML-facing entrypoint for all graph signals. It:
1. Builds the canonical entity graph (EntityGraphBuilder)
2. Extracts per-alert structural features (GraphFeatureExtractor)
3. Detects AML motifs (MotifDetector)
4. Computes community risk scores (CommunityScorer)
5. Merges all graph features into a single DataFrame per alert

The visualization-facing service (relationship_graph_service.py) uses
the same EntityGraph but for UI/API response construction only.
ML graph logic lives exclusively here.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from graph.community_scoring import CommunityScorer
from graph.entity_graph_builder import EntityGraph, EntityGraphBuilder
from graph.feature_extraction import GraphFeatureExtractor
from graph.motif_detection import MotifDetector

logger = logging.getLogger("althea.graph.feature_service")


class GraphFeatureService:
    """Build an entity graph and extract ML features for a batch of alerts."""

    def __init__(
        self,
        graph_builder: EntityGraphBuilder | None = None,
        feature_extractor: GraphFeatureExtractor | None = None,
        motif_detector: MotifDetector | None = None,
        community_scorer: CommunityScorer | None = None,
    ) -> None:
        self._builder = graph_builder or EntityGraphBuilder()
        self._extractor = feature_extractor or GraphFeatureExtractor()
        self._motif_detector = motif_detector or MotifDetector()
        self._community_scorer = community_scorer or CommunityScorer()

    def extract_features_for_batch(
        self,
        alerts_df: pd.DataFrame,
        context=None,
    ) -> pd.DataFrame:
        """Build entity graph and extract per-alert graph features.

        Parameters
        ----------
        alerts_df : normalized alerts DataFrame (must include alert_id)
        context   : BuilderContext (unused here, kept for builder interface consistency)

        Returns
        -------
        pd.DataFrame with alert_id + graph ML features.
        """
        if alerts_df.empty:
            return pd.DataFrame()

        payloads = alerts_df.to_dict("records")
        graph = self._builder.build(payloads)
        alert_ids = alerts_df["alert_id"].astype(str).tolist()

        # Extract structural features
        structural_df = self._to_frame(self._extractor.extract(graph, alert_ids))

        # Detect AML motifs
        motif_df = self._to_frame(self._motif_detector.detect(graph, alert_ids))

        # Community risk scores
        community_df = self._to_frame(self._community_scorer.score(graph, alert_ids))

        # Merge all graph feature frames
        merged = structural_df.copy()
        for df_part in (motif_df, community_df):
            if not df_part.empty and "alert_id" in df_part.columns:
                drop_cols = [c for c in df_part.columns if c != "alert_id" and c in merged.columns]
                merged = merged.merge(
                    df_part.drop(columns=drop_cols),
                    on="alert_id",
                    how="left",
                )

        logger.info(
            json.dumps(
                {
                    "event": "graph_features_extracted",
                    "alerts": len(alerts_df),
                    "graph_nodes": graph.node_count,
                    "graph_edges": graph.edge_count,
                    "feature_cols": len([c for c in merged.columns if c != "alert_id"]),
                },
                ensure_ascii=True,
            )
        )
        return merged.reset_index(drop=True)

    def extract_features_for_alert(
        self,
        alert_id: str,
        repository,
        tenant_id: str,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Extract graph features for a single alert using related historical alerts.

        Queries the repository for alerts linked to the same entity and builds
        a local graph for feature extraction.
        """
        # Fetch the anchor alert
        payloads = repository.list_alert_payloads_by_run(
            tenant_id=tenant_id,
            run_id=run_id or "",
            limit=1000,
        )

        # Filter to alerts with shared entities
        anchor = next(
            (row for row in payloads if str(row.get("alert_id", "")) == alert_id),
            None,
        )
        if not anchor:
            return self._empty_features(alert_id)

        # Build a local graph from nearby alerts
        related = self._find_related_alerts(anchor, payloads)
        local_payloads = [anchor] + related[:49]  # cap at 50 alerts

        graph = self._builder.build(local_payloads)
        structural = self._to_frame(self._extractor.extract(graph, [alert_id]))
        motif = self._to_frame(self._motif_detector.detect(graph, [alert_id]))
        community = self._to_frame(self._community_scorer.score(graph, [alert_id]))

        result: dict[str, Any] = {}
        for df_part in (structural, motif, community):
            if not df_part.empty:
                row = df_part[df_part["alert_id"] == alert_id]
                if not row.empty:
                    result.update({
                        k: float(v) if isinstance(v, (int, float)) else v
                        for k, v in row.iloc[0].to_dict().items()
                        if k != "alert_id"
                    })

        return {"alert_id": alert_id, **result}

    @staticmethod
    def _find_related_alerts(
        anchor: dict[str, Any],
        payloads: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Find alerts sharing an entity with the anchor."""
        match_fields = {"user_id", "customer_id", "account_id", "device_id", "ip_address"}
        anchor_vals = {
            k: str(anchor.get(k) or "").strip()
            for k in match_fields
            if str(anchor.get(k) or "").strip()
        }
        if not anchor_vals:
            return []

        related = []
        for row in payloads:
            if str(row.get("alert_id", "")) == str(anchor.get("alert_id", "")):
                continue
            for field, val in anchor_vals.items():
                if str(row.get(field) or "").strip() == val:
                    related.append(row)
                    break
        return related

    @staticmethod
    def _empty_features(alert_id: str) -> dict[str, Any]:
        return {
            "alert_id": alert_id,
            "graph_degree": 0.0,
            "unique_counterparties": 0.0,
            "suspicious_neighbor_ratio": 0.0,
            "graph_component_size": 1.0,
            "fan_in_score": 0.0,
            "fan_out_score": 0.0,
            "circularity_score": 0.0,
            "community_risk_score": 0.0,
        }

    @staticmethod
    def _to_frame(rows: Any) -> pd.DataFrame:
        if isinstance(rows, pd.DataFrame):
            return rows
        if isinstance(rows, list):
            if not rows:
                return pd.DataFrame(columns=["alert_id"])
            return pd.DataFrame(rows)
        if isinstance(rows, dict):
            return pd.DataFrame([rows])
        return pd.DataFrame(columns=["alert_id"])
