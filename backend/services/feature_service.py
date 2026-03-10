from __future__ import annotations

from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd

from models.feature_schema import FeatureSchemaValidator


class EnterpriseFeatureService:
    def __init__(self, schema_validator: FeatureSchemaValidator) -> None:
        self._schema = schema_validator

    def load_transactions_csv(self, payload: bytes) -> pd.DataFrame:
        return pd.read_csv(BytesIO(payload))

    def _normalize_input(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            return out

        if "alert_id" not in out.columns:
            out["alert_id"] = [f"ALT{idx+1:06d}" for idx in range(len(out))]
        if "user_id" not in out.columns:
            if "customer_id" in out.columns:
                out["user_id"] = out["customer_id"].astype(str)
            else:
                out["user_id"] = [f"USR{idx+1:06d}" for idx in range(len(out))]
        if "amount" not in out.columns:
            out["amount"] = 0.0
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)

        if "timestamp" not in out.columns and "timestamp_utc" in out.columns:
            out["timestamp"] = out["timestamp_utc"]
        if "timestamp" not in out.columns:
            out["timestamp"] = pd.Timestamp.utcnow().isoformat()
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
        out["timestamp"] = out["timestamp"].fillna(pd.Timestamp.utcnow())

        if "segment" not in out.columns:
            out["segment"] = "retail"
        if "typology" not in out.columns:
            out["typology"] = "anomaly"
        if "country" not in out.columns:
            out["country"] = "UNKNOWN"
        if "source_system" not in out.columns:
            out["source_system"] = "core_bank"

        out["segment"] = out["segment"].astype(str).fillna("retail")
        out["typology"] = out["typology"].astype(str).fillna("anomaly")
        out["country"] = out["country"].astype(str).fillna("UNKNOWN")
        out["source_system"] = out["source_system"].astype(str).fillna("core_bank")

        out = out.sort_values(["user_id", "timestamp"], kind="stable").reset_index(drop=True)

        if "time_gap" not in out.columns:
            diffs = out.groupby("user_id")["timestamp"].diff().dt.total_seconds()
            out["time_gap"] = diffs.fillna(diffs.median() if pd.notna(diffs.median()) else 3600.0)
        out["time_gap"] = pd.to_numeric(out["time_gap"], errors="coerce").fillna(3600.0)

        if "num_transactions" not in out.columns:
            out["num_transactions"] = out.groupby("user_id").cumcount() + 1
        out["num_transactions"] = pd.to_numeric(out["num_transactions"], errors="coerce").fillna(1.0)
        return out

    def _encode_categorical(self, series: pd.Series) -> pd.Series:
        codes, _ = pd.factorize(series.astype(str), sort=True)
        return pd.Series(codes, index=series.index, dtype="int64")

    def _build_feature_matrix(self, normalized: pd.DataFrame) -> pd.DataFrame:
        if normalized.empty:
            return pd.DataFrame()

        frame = normalized.copy()
        frame["amount_log1p"] = np.log1p(np.clip(frame["amount"], 0.0, None))
        frame["hour_of_day"] = frame["timestamp"].dt.hour.astype(float)
        frame["day_of_week"] = frame["timestamp"].dt.dayofweek.astype(float)
        frame["is_weekend"] = (frame["day_of_week"] >= 5).astype(float)

        user_amount_mean = frame.groupby("user_id")["amount"].transform("mean")
        user_amount_std = frame.groupby("user_id")["amount"].transform("std").fillna(0.0)
        frame["user_amount_mean"] = user_amount_mean
        frame["user_amount_std"] = user_amount_std
        frame["amount_to_user_mean"] = frame["amount"] / (user_amount_mean + 1e-6)
        frame["amount_user_z"] = (frame["amount"] - user_amount_mean) / (user_amount_std + 1e-6)
        frame["user_tx_count"] = frame.groupby("user_id")["alert_id"].transform("count").astype(float)

        country_risk_map = {
            "UNKNOWN": 0.2,
            "US": 0.2,
            "GB": 0.2,
            "DE": 0.2,
            "AE": 0.6,
            "IR": 1.0,
            "KP": 1.0,
            "RU": 0.7,
        }
        frame["country_risk"] = frame["country"].map(country_risk_map).fillna(0.5).astype(float)
        frame["segment_code"] = self._encode_categorical(frame["segment"]).astype(float)
        frame["typology_code"] = self._encode_categorical(frame["typology"]).astype(float)
        frame["source_system_code"] = self._encode_categorical(frame["source_system"]).astype(float)

        feature_columns = [
            "amount",
            "amount_log1p",
            "time_gap",
            "num_transactions",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "user_amount_mean",
            "user_amount_std",
            "amount_to_user_mean",
            "amount_user_z",
            "user_tx_count",
            "country_risk",
            "segment_code",
            "typology_code",
            "source_system_code",
        ]
        feature_matrix = frame[feature_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0)
        return feature_matrix

    def generate_training_features(self, df: pd.DataFrame) -> dict[str, Any]:
        normalized = self._normalize_input(df)
        matrix = self._build_feature_matrix(normalized)
        schema = self._schema.from_frame(matrix)
        feature_groups = {"all_feature_cols": list(matrix.columns)}
        return {
            "alerts_df": normalized,
            "feature_groups": feature_groups,
            "feature_matrix": matrix,
            "feature_schema": schema,
        }

    def generate_inference_features(self, df: pd.DataFrame) -> dict[str, Any]:
        # Training and inference intentionally share one builder.
        return self.generate_training_features(df)

    def generate_features_batch(self, chunk: pd.DataFrame) -> dict[str, Any]:
        # Explicit batch API for chunked pipeline execution.
        return self.generate_inference_features(chunk)

    # Backward-compatible aliases for existing callers.
    def build_training_features(self, df: pd.DataFrame) -> dict[str, Any]:
        return self.generate_training_features(df)

    def build_inference_features(self, df: pd.DataFrame) -> dict[str, Any]:
        return self.generate_inference_features(df)

    def validate_feature_schema(self, expected_schema: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
        return self._schema.validate(expected_schema, df)
