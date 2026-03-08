from __future__ import annotations

import hashlib
from io import BytesIO
from types import SimpleNamespace
from typing import Any

import pandas as pd

from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository
from src import config as legacy_config
from src.services.ingestion_service import IngestionError
from src.services.ingestion_service import IngestionService as LegacyIngestionService
from src.synth_data import generate_synthetic_alerts, generate_synthetic_transactions


class EnterpriseIngestionService:
    def __init__(self, repository: EnterpriseRepository, object_storage: ObjectStorage) -> None:
        self._repository = repository
        self._object_storage = object_storage
        self._legacy = LegacyIngestionService()

    def _hash_df(self, df: pd.DataFrame) -> str:
        raw = df.head(5000).to_csv(index=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]

    def _legacy_cfg(self) -> SimpleNamespace:
        return SimpleNamespace(**{name: getattr(legacy_config, name) for name in dir(legacy_config) if name.isupper()})

    def _persist_dataset(
        self,
        tenant_id: str,
        user_scope: str,
        source: str,
        df: pd.DataFrame,
        raw_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        dataset_hash = hashlib.sha256(raw_bytes).hexdigest()[:16] if raw_bytes else self._hash_df(df)
        dataset_uri = f"datasets/{tenant_id}/{user_scope}/{dataset_hash}.csv"
        csv_bytes = raw_bytes or df.to_csv(index=False).encode("utf-8")
        self._object_storage.put_bytes(dataset_uri, csv_bytes)
        raw_uri = None
        if raw_bytes:
            raw_uri = f"datasets/{tenant_id}/{user_scope}/{dataset_hash}.raw.csv"
            self._object_storage.put_bytes(raw_uri, raw_bytes)
        context = self._repository.upsert_runtime_context(
            tenant_id,
            user_scope,
            run_source=source,
            dataset_hash=dataset_hash,
            dataset_artifact_uri=dataset_uri,
            raw_artifact_uri=raw_uri,
            row_count=len(df),
        )
        return {
            "rows": len(df),
            "source": source,
            "dataset_hash": dataset_hash,
            "dataset_artifact_uri": dataset_uri,
            "runtime_context": context,
        }

    def generate_synthetic(self, tenant_id: str, user_scope: str, n_rows: int) -> dict[str, Any]:
        cfg = self._legacy_cfg()
        if getattr(legacy_config, "OVERLAY_MODE", False):
            df = generate_synthetic_alerts(n_rows=n_rows, cfg=cfg, seed=getattr(legacy_config, "DEMO_SEED", 42))
        else:
            n_users = max(80, n_rows // 5)
            df = generate_synthetic_transactions(n_users=n_users, tx_per_user=n_rows, cfg=cfg)
        return self._persist_dataset(tenant_id, user_scope, "Synthetic", df)

    def upload_transactions_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes) -> dict[str, Any]:
        df = self._legacy.load_bank_alerts_csv(BytesIO(raw_bytes)) if getattr(legacy_config, "OVERLAY_MODE", False) else None
        if df is None:
            from src.services.feature_service import FeatureService as LegacyFeatureCsvService

            df = LegacyFeatureCsvService().load_transactions_csv(BytesIO(raw_bytes))
        return self._persist_dataset(tenant_id, user_scope, "CSV", df, raw_bytes=raw_bytes)

    def upload_bank_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes) -> dict[str, Any]:
        df = self._legacy.load_bank_alerts_csv(BytesIO(raw_bytes))
        return self._persist_dataset(tenant_id, user_scope, "BankCSV", df, raw_bytes=raw_bytes)

    def load_runtime_dataframe(self, context: dict[str, Any]) -> pd.DataFrame:
        artifact_uri = context.get("dataset_artifact_uri")
        if not artifact_uri:
            raise ValueError("No dataset is staged for the current tenant/session.")
        return pd.read_csv(BytesIO(self._object_storage.get_bytes(artifact_uri)))
