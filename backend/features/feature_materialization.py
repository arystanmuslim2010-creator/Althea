from __future__ import annotations

from typing import Any

import pandas as pd

from features.feature_registry import FeatureRegistry
from features.offline_feature_store import OfflineFeatureStore
from features.online_feature_store import OnlineFeatureStore


class FeatureMaterializationService:
    def __init__(
        self,
        registry: FeatureRegistry,
        online_store: OnlineFeatureStore,
        offline_store: OfflineFeatureStore,
    ) -> None:
        self._registry = registry
        self._online_store = online_store
        self._offline_store = offline_store

    def _derive_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        if "amount" in out.columns:
            out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)
            out["feature_amount_z"] = (out["amount"] - out["amount"].mean()) / (out["amount"].std(ddof=0) + 1e-6)
        if "txn_count_24h" in out.columns:
            out["txn_count_24h"] = pd.to_numeric(out["txn_count_24h"], errors="coerce").fillna(0.0)
            out["feature_velocity_24h"] = out["txn_count_24h"]
        if "country_risk" in out.columns:
            out["feature_geo_risk"] = pd.to_numeric(out["country_risk"], errors="coerce").fillna(0.0)
        if "risk_score" in out.columns:
            out["feature_prior_score"] = pd.to_numeric(out["risk_score"], errors="coerce").fillna(0.0)
        return out

    def materialize_batch(
        self,
        tenant_id: str,
        run_id: str,
        alert_frame: pd.DataFrame,
        feature_version: str = "v1",
    ) -> list[dict[str, Any]]:
        if alert_frame.empty:
            return []
        transformed = self._derive_features(alert_frame)

        if "alert_id" not in transformed.columns and "id" in transformed.columns:
            transformed["alert_id"] = transformed["id"]

        # Register derived features for offline training and online inference consistency.
        for column in transformed.columns:
            if not str(column).startswith("feature_"):
                continue
            self._registry.register_feature(
                tenant_id=tenant_id,
                name=str(column),
                dtype=str(transformed[column].dtype),
                description=f"Derived feature {column}",
            )
            self._registry.register_feature_version(
                tenant_id=tenant_id,
                feature_name=str(column),
                version=feature_version,
                transformation_sql=f"derived:{column}",
                is_active=True,
            )

        rows = transformed.to_dict("records")
        for row in rows:
            alert_id = str(row.get("alert_id") or row.get("id") or "")
            if not alert_id:
                continue
            self._online_store.put_features(tenant_id=tenant_id, alert_id=alert_id, version=feature_version, features=row)

        self._offline_store.store_batch(
            tenant_id=tenant_id,
            run_id=run_id,
            feature_version=feature_version,
            feature_rows=rows,
        )
        return rows

    def load_online_features(self, tenant_id: str, alert_ids: list[str], feature_version: str = "v1") -> pd.DataFrame:
        return self._online_store.get_many(tenant_id=tenant_id, alert_ids=alert_ids, version=feature_version)

    def load_offline_training_set(self, tenant_id: str, run_id: str, feature_version: str = "v1") -> pd.DataFrame:
        return self._offline_store.get_training_set(tenant_id=tenant_id, run_id=run_id, feature_version=feature_version)
