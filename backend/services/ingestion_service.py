from __future__ import annotations

import hashlib
from io import BytesIO
from typing import Any, Iterator

import numpy as np
import pandas as pd

from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository


class IngestionError(ValueError):
    pass


class EnterpriseIngestionService:
    def __init__(self, repository: EnterpriseRepository, object_storage: ObjectStorage) -> None:
        self._repository = repository
        self._object_storage = object_storage

    def _hash_df(self, df: pd.DataFrame) -> str:
        raw = df.head(5000).to_csv(index=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]

    def _validate_frame(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        if df is None or df.empty:
            raise IngestionError(f"{source} data is empty.")
        out = df.copy()
        if "amount" in out.columns:
            out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(0.0)
        elif source.lower().endswith("csv"):
            raise IngestionError("CSV must include an 'amount' column.")

        if "user_id" not in out.columns:
            if "customer_id" in out.columns:
                out["user_id"] = out["customer_id"].astype(str)
            elif "account_id" in out.columns:
                out["user_id"] = out["account_id"].astype(str)
            else:
                out["user_id"] = [f"USR{idx+1:06d}" for idx in range(len(out))]

        if "alert_id" not in out.columns:
            out["alert_id"] = [f"ALT{idx+1:06d}" for idx in range(len(out))]

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
        return out

    def _generate_synthetic_frame(self, n_rows: int, seed: int = 42) -> pd.DataFrame:
        n_rows = max(10, int(n_rows))
        rng = np.random.default_rng(seed)
        users = [f"USR{idx:05d}" for idx in range(max(100, n_rows // 4))]
        segments = np.array(["retail", "corporate", "private_banking"])
        typologies = np.array(["cross_border", "structuring", "sanctions", "high_amount_outlier", "flow_through"])
        countries = np.array(["US", "GB", "DE", "AE", "RU", "IR", "UNKNOWN"])

        amounts = rng.lognormal(mean=8.7, sigma=1.0, size=n_rows)
        base_time = pd.Timestamp.utcnow().floor("s") - pd.Timedelta(days=30)
        times = [base_time + pd.Timedelta(seconds=int(v)) for v in rng.integers(0, 30 * 24 * 3600, size=n_rows)]
        frame = pd.DataFrame(
            {
                "alert_id": [f"ALT{idx+1:06d}" for idx in range(n_rows)],
                "user_id": rng.choice(users, size=n_rows),
                "amount": amounts,
                "segment": rng.choice(segments, size=n_rows),
                "country": rng.choice(countries, size=n_rows),
                "typology": rng.choice(typologies, size=n_rows),
                "source_system": "core_bank",
                "timestamp": times,
                "channel": "bank_transfer",
            }
        )
        frame["time_gap"] = rng.uniform(300, 86400, size=n_rows)
        frame["num_transactions"] = rng.integers(1, 12, size=n_rows)
        return frame

    def _persist_dataset(
        self,
        tenant_id: str,
        user_scope: str,
        source: str,
        df: pd.DataFrame,
        raw_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        df = self._validate_frame(df, source)
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
        df = self._generate_synthetic_frame(n_rows=n_rows)
        return self._persist_dataset(tenant_id, user_scope, "Synthetic", df)

    def upload_transactions_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes) -> dict[str, Any]:
        if not raw_bytes:
            raise IngestionError("Uploaded file is empty.")
        try:
            df = pd.read_csv(BytesIO(raw_bytes))
        except Exception as exc:
            raise IngestionError(f"Invalid CSV payload: {exc}") from exc
        return self._persist_dataset(tenant_id, user_scope, "CSV", df, raw_bytes=raw_bytes)

    def upload_bank_csv(self, tenant_id: str, user_scope: str, raw_bytes: bytes) -> dict[str, Any]:
        if not raw_bytes:
            raise IngestionError("Uploaded bank CSV is empty.")
        try:
            df = pd.read_csv(BytesIO(raw_bytes))
        except Exception as exc:
            raise IngestionError(f"Invalid bank CSV payload: {exc}") from exc
        return self._persist_dataset(tenant_id, user_scope, "BankCSV", df, raw_bytes=raw_bytes)

    def generate_bank_alerts_csv(self, n_rows: int, seed: int) -> pd.DataFrame:
        df = self._generate_synthetic_frame(n_rows=n_rows, seed=seed)
        out = df[["alert_id", "user_id", "amount", "segment", "country", "typology", "source_system"]].copy()
        out["timestamp_utc"] = pd.to_datetime(df["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        out["channel"] = df.get("channel", "bank_transfer")
        out["time_gap"] = pd.to_numeric(df.get("time_gap", 86400), errors="coerce").fillna(86400).astype(float)
        out["num_transactions"] = pd.to_numeric(df.get("num_transactions", 1), errors="coerce").fillna(1).astype(int)
        return out

    def load_runtime_dataframe(self, context: dict[str, Any]) -> pd.DataFrame:
        artifact_uri = context.get("dataset_artifact_uri")
        if not artifact_uri:
            raise ValueError("No dataset is staged for the current tenant/session.")
        return pd.read_csv(BytesIO(self._object_storage.get_bytes(artifact_uri)))

    def stream_runtime_dataset(self, context: dict[str, Any], batch_size: int = 20000) -> Iterator[pd.DataFrame]:
        artifact_uri = context.get("dataset_artifact_uri")
        if not artifact_uri:
            raise ValueError("No dataset is staged for the current tenant/session.")
        path = self._object_storage.resolve_path(str(artifact_uri))
        if not path.exists():
            raise ValueError(f"Dataset artifact is missing: {artifact_uri}")
        for chunk in pd.read_csv(path, chunksize=max(1000, int(batch_size))):
            yield chunk
