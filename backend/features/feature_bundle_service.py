"""Feature bundle service — orchestrates all feature builders.

Calls each builder in dependency order, merges outputs by alert_id,
and registers the resulting feature schema. This is the single entrypoint
for both training-time and inference-time feature construction.

Architecture:
    FeatureBundleService.build_bundle(df, context)
        → AlertFeatureBuilder.build(df, context)
        → BehaviorFeatureBuilder.build(df, context)
        → HistoryFeatureBuilder.build(df, context)
        → PeerFeatureBuilder.build(df, context)
        → CostFeatureBuilder.build(df, context)
        → [GraphFeatureService enrichment if available]
        → merge all feature frames on alert_id
        → validate schema
        → return FeatureBundle

The service is intentionally dependency-injectable so that builders
can be replaced (e.g. mocked in tests) without touching the pipeline.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from features.builders.alert_features import AlertFeatureBuilder
from features.builders.base import BuilderContext
from features.builders.behavior_features import BehaviorFeatureBuilder
from features.builders.cost_features import CostFeatureBuilder
from features.builders.history_features import HistoryFeatureBuilder
from features.builders.peer_features import PeerFeatureBuilder

logger = logging.getLogger("althea.features.bundle_service")


@dataclass
class FeatureBundle:
    """Holds the complete feature matrix for a batch of alerts."""
    alerts_df: pd.DataFrame          # normalized alert records
    feature_matrix: pd.DataFrame     # model-ready numeric matrix
    feature_schema: dict[str, Any]   # column schema for registry
    feature_groups: dict[str, list[str]]  # column → builder mapping
    metadata: dict[str, Any] = field(default_factory=dict)


class FeatureBundleService:
    """Orchestrate all feature builders and produce a unified feature matrix."""

    def __init__(
        self,
        alert_builder: AlertFeatureBuilder | None = None,
        behavior_builder: BehaviorFeatureBuilder | None = None,
        history_builder: HistoryFeatureBuilder | None = None,
        peer_builder: PeerFeatureBuilder | None = None,
        cost_builder: CostFeatureBuilder | None = None,
        graph_feature_service=None,
        schema_validator=None,
    ) -> None:
        self._alert_builder = alert_builder or AlertFeatureBuilder()
        self._behavior_builder = behavior_builder or BehaviorFeatureBuilder()
        self._history_builder = history_builder or HistoryFeatureBuilder()
        self._peer_builder = peer_builder or PeerFeatureBuilder()
        self._cost_builder = cost_builder or CostFeatureBuilder()
        self._graph_feature_service = graph_feature_service
        self._schema_validator = schema_validator

    def build_bundle(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> FeatureBundle:
        """Build the complete feature bundle for a batch of alerts.

        Parameters
        ----------
        df      : normalized alerts DataFrame. Must include at least:
                  alert_id, amount, timestamp, typology, segment, country.
        context : optional BuilderContext with pre-fetched enrichment data.
                  If None, all enriched features default to zero/baseline.

        Returns
        -------
        FeatureBundle with model-ready feature_matrix and schema.
        """
        if df.empty:
            return FeatureBundle(
                alerts_df=df,
                feature_matrix=pd.DataFrame(),
                feature_schema={},
                feature_groups={},
                metadata={"rows": 0},
            )

        # Ensure alert_id column exists
        out = df.copy()
        if "alert_id" not in out.columns:
            out["alert_id"] = [f"ALT{i+1:06d}" for i in range(len(out))]

        # ------------------------------------------------------------------
        # Run all builders (safe mode: never raise on partial failure)
        # ------------------------------------------------------------------
        alert_features = self._alert_builder.build_safe(out, context)
        behavior_features = self._behavior_builder.build_safe(out, context)
        history_features = self._history_builder.build_safe(out, context)
        peer_features = self._peer_builder.build_safe(out, context)
        cost_features = self._cost_builder.build_safe(out, context)

        # Optional graph feature enrichment
        graph_features_df = pd.DataFrame()
        if context is not None and getattr(context, "graph_features", None) is not None:
            graph_features_df = context.graph_features.copy()
        elif self._graph_feature_service is not None:
            try:
                graph_features_df = self._graph_feature_service.extract_features_for_batch(out, context)
            except Exception as exc:
                logger.warning("Graph feature extraction failed (non-fatal): %s", exc)

        # ------------------------------------------------------------------
        # Merge all frames on alert_id
        # ------------------------------------------------------------------
        merged = alert_features.copy()
        for frame, label in [
            (behavior_features, "behavior"),
            (history_features, "history"),
            (peer_features, "peer"),
            (cost_features, "cost"),
        ]:
            if not frame.empty and "alert_id" in frame.columns:
                merged = merged.merge(
                    frame.drop(columns=["alert_id"] if label == "alert" else []),
                    on="alert_id",
                    how="left",
                    suffixes=("", f"_{label}"),
                )

        if not graph_features_df.empty and "alert_id" in graph_features_df.columns:
            merged = merged.merge(graph_features_df, on="alert_id", how="left", suffixes=("", "_graph"))

        # ------------------------------------------------------------------
        # Extract numeric feature matrix (exclude alert_id and string cols)
        # ------------------------------------------------------------------
        non_feature_cols = {"alert_id"}
        feature_cols = [
            c for c in merged.columns
            if c not in non_feature_cols
            and pd.api.types.is_numeric_dtype(merged[c])
        ]

        import numpy as np
        feature_matrix = (
            merged[feature_cols]
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
        )

        # Add alert_id back as index label for downstream join
        feature_matrix_with_id = merged[["alert_id"]].join(feature_matrix)

        # ------------------------------------------------------------------
        # Build feature groups for registry / explainability
        # ------------------------------------------------------------------
        groups: dict[str, list[str]] = {
            "alert": self._alert_builder.feature_names,
            "behavior": [c for c in self._behavior_builder.feature_names if c in feature_cols],
            "history": [c for c in self._history_builder.feature_names if c in feature_cols],
            "peer": [c for c in self._peer_builder.feature_names if c in feature_cols],
            "cost": [c for c in self._cost_builder.feature_names if c in feature_cols],
        }
        if not graph_features_df.empty:
            graph_cols = [c for c in graph_features_df.columns if c != "alert_id" and c in feature_cols]
            groups["graph"] = graph_cols

        schema = self._build_schema(feature_matrix, feature_cols)

        logger.info(
            json.dumps(
                {
                    "event": "feature_bundle_built",
                    "rows": len(df),
                    "feature_count": len(feature_cols),
                    "groups": {k: len(v) for k, v in groups.items()},
                },
                ensure_ascii=True,
            )
        )

        return FeatureBundle(
            alerts_df=out,
            feature_matrix=feature_matrix,
            feature_schema=schema,
            feature_groups=groups,
            metadata={
                "rows": len(df),
                "feature_count": len(feature_cols),
                "feature_cols": feature_cols,
            },
        )

    # ------------------------------------------------------------------

    @staticmethod
    def _build_schema(
        feature_matrix: pd.DataFrame,
        feature_cols: list[str],
    ) -> dict[str, Any]:
        import hashlib
        columns = [
            {"name": col, "dtype": str(feature_matrix[col].dtype), "index": i}
            for i, col in enumerate(feature_cols)
        ]
        schema_hash = hashlib.sha256(
            json.dumps([c["name"] for c in columns], sort_keys=True).encode()
        ).hexdigest()[:16]
        return {
            "version": "v2",
            "schema_hash": schema_hash,
            "columns": columns,
            "feature_count": len(columns),
        }
