from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text


class OfflineFeatureStore:
    def __init__(self, repository, root_dir: Path) -> None:
        self._repository = repository
        self._root_dir = Path(root_dir)
        self._root_dir.mkdir(parents=True, exist_ok=True)

    def store_batch(
        self,
        tenant_id: str,
        run_id: str,
        feature_version: str,
        feature_rows: list[dict[str, Any]],
    ) -> int:
        if not feature_rows:
            return 0
        frame = pd.DataFrame(feature_rows)
        out_dir = self._root_dir / tenant_id / "offline_features"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{run_id}_{feature_version}.parquet"
        try:
            frame.to_parquet(out_path, index=False)
        except Exception:
            # Fallback for environments where parquet engine/type extension support is unavailable.
            out_path = out_dir / f"{run_id}_{feature_version}.jsonl"
            frame.to_json(out_path, orient="records", lines=True, force_ascii=True)

        with self._repository.session(tenant_id=tenant_id) as session:
            for row in feature_rows:
                row_id = f"{tenant_id}:{run_id}:{row.get('alert_id')}"
                # Hard idempotency guard: stale/replayed events should overwrite existing row, never fail.
                session.execute(
                    text(
                        """
                        DELETE FROM offline_feature_store
                        WHERE id = :id
                        """
                    ),
                    {"id": row_id},
                )
                session.execute(
                    text(
                        """
                        INSERT INTO offline_feature_store (
                            id, tenant_id, run_id, alert_id, feature_version, features_json, parquet_uri, created_at
                        ) VALUES (
                            :id, :tenant_id, :run_id, :alert_id, :feature_version, :features_json, :parquet_uri, :created_at
                        )
                        ON CONFLICT (id)
                        DO UPDATE SET
                            tenant_id = EXCLUDED.tenant_id,
                            run_id = EXCLUDED.run_id,
                            alert_id = EXCLUDED.alert_id,
                            feature_version = EXCLUDED.feature_version,
                            features_json = EXCLUDED.features_json,
                            parquet_uri = EXCLUDED.parquet_uri,
                            created_at = EXCLUDED.created_at
                        """
                    ),
                    {
                        "id": row_id,
                        "tenant_id": tenant_id,
                        "run_id": run_id,
                        "alert_id": str(row.get("alert_id") or ""),
                        "feature_version": feature_version,
                        "features_json": json.dumps(dict(row), ensure_ascii=True),
                        "parquet_uri": str(out_path),
                        "created_at": datetime.now(timezone.utc),
                    },
                )
        return len(feature_rows)

    def get_training_set(self, tenant_id: str, run_id: str, feature_version: str) -> pd.DataFrame:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    """
                    SELECT features_json
                    FROM offline_feature_store
                    WHERE tenant_id = :tenant_id
                      AND run_id = :run_id
                      AND feature_version = :feature_version
                    ORDER BY created_at
                    """
                ),
                {"tenant_id": tenant_id, "run_id": run_id, "feature_version": feature_version},
            ).mappings().all()
        return pd.DataFrame([json.loads(str(row.get("features_json") or "{}")) for row in rows])
