"""Feature service — normalization and orchestration entrypoint.

This module is now a thin orchestration layer that:
1. Normalizes raw alert DataFrames (fills missing columns, coerces types)
2. Delegates feature construction to FeatureBundleService
3. Returns a feature bundle compatible with both training and inference paths

The heavy feature computation previously in this file has been decomposed
into purpose-built builders under ``features/builders/``. This file now
has a single responsibility: canonical input normalization + dispatch.

Backward-compatible API is preserved so that existing callers
(pipeline_service, workers, tests) continue to work unchanged.
"""
from __future__ import annotations

from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd

from features.builders.base import BuilderContext
from features.feature_bundle_service import FeatureBundleService
from models.feature_schema import FeatureSchemaValidator


class EnterpriseFeatureService:
    """Normalized feature orchestration service.

    Delegates to FeatureBundleService for actual feature construction.
    Maintains backward-compatible public methods so pipeline code is
    not disrupted.
    """

    def __init__(
        self,
        schema_validator: FeatureSchemaValidator,
        bundle_service: FeatureBundleService | None = None,
    ) -> None:
        self._schema = schema_validator
        self._bundle_service = bundle_service or FeatureBundleService(schema_validator=schema_validator)

    def load_transactions_csv(self, payload: bytes) -> pd.DataFrame:
        return pd.read_csv(BytesIO(payload))

    # ------------------------------------------------------------------
    # Public API (called by pipeline and worker code)
    # ------------------------------------------------------------------

    def generate_features_batch(self, chunk: pd.DataFrame, context: BuilderContext | None = None) -> dict[str, Any]:
        """Build features for a chunk of alerts. Primary pipeline entrypoint."""
        normalized = self._normalize_input(chunk)
        bundle = self._bundle_service.build_bundle(normalized, context=context)
        return {
            "alerts_df": bundle.alerts_df,
            "feature_matrix": bundle.feature_matrix,
            "feature_schema": bundle.feature_schema,
            "feature_groups": bundle.feature_groups,
        }

    def generate_training_features(self, df: pd.DataFrame, context: BuilderContext | None = None) -> dict[str, Any]:
        """Build features for training. Alias for generate_features_batch."""
        return self.generate_features_batch(df, context=context)

    def generate_inference_features(self, df: pd.DataFrame, context: BuilderContext | None = None) -> dict[str, Any]:
        """Build features for inference. Alias for generate_features_batch."""
        return self.generate_features_batch(df, context=context)

    # Backward-compatible aliases used by older callers
    def build_training_features(self, df: pd.DataFrame) -> dict[str, Any]:
        return self.generate_training_features(df)

    def build_inference_features(self, df: pd.DataFrame) -> dict[str, Any]:
        return self.generate_inference_features(df)

    def validate_feature_schema(self, expected_schema: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        return self._schema.validate(expected_schema, df)

    # ------------------------------------------------------------------
    # Input normalization — canonical column names / types
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_input(df: pd.DataFrame) -> pd.DataFrame:
        """Coerce raw alert DataFrames to the canonical schema expected by builders."""
        out = df.copy()
        if out.empty:
            return out

        # alert_id
        if "alert_id" not in out.columns:
            out["alert_id"] = [f"ALT{i+1:06d}" for i in range(len(out))]

        # user_id (canonical entity identifier)
        if "user_id" not in out.columns:
            if "customer_id" in out.columns:
                out["user_id"] = out["customer_id"].astype(str)
            else:
                out["user_id"] = [f"USR{i+1:06d}" for i in range(len(out))]

        # amount
        if "amount" not in out.columns:
            out["amount"] = 0.0
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)

        # timestamp
        if "timestamp" not in out.columns and "timestamp_utc" in out.columns:
            out["timestamp"] = out["timestamp_utc"]
        if "timestamp" not in out.columns:
            out["timestamp"] = pd.Timestamp.utcnow().isoformat()
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
        out["timestamp"] = out["timestamp"].fillna(pd.Timestamp.utcnow())

        # categorical defaults
        for col, default in [
            ("segment", "retail"),
            ("typology", "anomaly"),
            ("country", "UNKNOWN"),
            ("source_system", "core_bank"),
        ]:
            if col not in out.columns:
                out[col] = default
            out[col] = out[col].astype(str).fillna(default)

        # Sort for stable ordering (needed for time-gap computation)
        out = out.sort_values(["user_id", "timestamp"], kind="stable").reset_index(drop=True)

        # time_gap (seconds between consecutive transactions per entity)
        if "time_gap" not in out.columns:
            diffs = out.groupby("user_id")["timestamp"].diff().dt.total_seconds()
            median_gap = diffs.median()
            out["time_gap"] = diffs.fillna(median_gap if pd.notna(median_gap) else 3600.0)
        out["time_gap"] = pd.to_numeric(out["time_gap"], errors="coerce").fillna(3600.0)

        # num_transactions
        if "num_transactions" not in out.columns:
            out["num_transactions"] = out.groupby("user_id").cumcount() + 1
        out["num_transactions"] = pd.to_numeric(out["num_transactions"], errors="coerce").fillna(1.0)

        return out
