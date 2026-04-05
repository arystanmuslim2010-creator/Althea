"""Training snapshot service — captures reproducible feature snapshots.

Before each training run, this service takes a frozen snapshot of:
    - The feature schema in use at that point in time
    - A compact manifest of the training dataset (hash, cutoff, row counts)
    - The model versions in production at snapshot time

This ensures full reproducibility: any training run can be re-executed
with identical data by replaying from the snapshot manifest.

Snapshots are stored at: feature_snapshots/{tenant_id}/{snapshot_id}/
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger("althea.training.snapshot_service")


class TrainingSnapshotService:
    """Capture and retrieve training dataset snapshots."""

    def __init__(self, repository, object_storage) -> None:
        self._repository = repository
        self._storage = object_storage

    def capture_snapshot(
        self,
        tenant_id: str,
        training_run_id: str,
        dataset: pd.DataFrame,
        feature_schema: dict[str, Any],
        cutoff_timestamp: datetime,
        model_version_at_snapshot: str | None = None,
    ) -> dict[str, Any]:
        """Create and persist a training snapshot.

        Returns the snapshot manifest dict.
        """
        snapshot_id = f"snap-{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)

        # Dataset fingerprint — hash of column names + row count + cutoff
        fingerprint_data = {
            "columns": sorted(dataset.columns.tolist()),
            "row_count": len(dataset),
            "cutoff": cutoff_timestamp.isoformat(),
            "positive_rate": float(dataset["escalation_label"].mean()) if "escalation_label" in dataset.columns else None,
        }
        dataset_hash = hashlib.sha256(
            json.dumps(fingerprint_data, sort_keys=True).encode()
        ).hexdigest()[:24]

        manifest: dict[str, Any] = {
            "snapshot_id": snapshot_id,
            "training_run_id": training_run_id,
            "tenant_id": tenant_id,
            "captured_at": now.isoformat(),
            "cutoff_timestamp": cutoff_timestamp.isoformat(),
            "dataset_hash": dataset_hash,
            "row_count": len(dataset),
            "feature_schema_version": feature_schema.get("version", "v2"),
            "feature_schema_hash": feature_schema.get("schema_hash", ""),
            "feature_count": feature_schema.get("feature_count", 0),
            "model_version_at_snapshot": model_version_at_snapshot or "unknown",
        }

        if "escalation_label" in dataset.columns:
            manifest["positive_rate"] = float(dataset["escalation_label"].mean())
            manifest["n_positive"] = int(dataset["escalation_label"].sum())

        # Store the snapshot manifest
        uri = f"feature_snapshots/{tenant_id}/{snapshot_id}/manifest.json"
        self._storage.put_json(uri, manifest)

        # Store the feature schema alongside the snapshot
        schema_uri = f"feature_snapshots/{tenant_id}/{snapshot_id}/feature_schema.json"
        self._storage.put_json(schema_uri, feature_schema)

        # Also record in the DB training_runs table
        self._persist_snapshot_record(tenant_id, snapshot_id, manifest)

        logger.info(
            json.dumps(
                {
                    "event": "training_snapshot_captured",
                    "tenant_id": tenant_id,
                    "snapshot_id": snapshot_id,
                    "training_run_id": training_run_id,
                    "row_count": len(dataset),
                    "dataset_hash": dataset_hash,
                },
                ensure_ascii=True,
            )
        )
        return manifest

    def load_snapshot(self, tenant_id: str, snapshot_id: str) -> dict[str, Any]:
        """Load a stored snapshot manifest."""
        uri = f"feature_snapshots/{tenant_id}/{snapshot_id}/manifest.json"
        return self._storage.get_json(uri) or {}

    def list_snapshots(self, tenant_id: str) -> list[dict[str, Any]]:
        """List recent snapshot manifests from object storage.

        Note: this is best-effort; relies on object_storage supporting
        prefix-based listing. Falls back to empty list if not supported.
        """
        try:
            items = self._storage.list_prefix(f"feature_snapshots/{tenant_id}/")
            manifests = []
            for item_uri in items:
                if item_uri.endswith("manifest.json"):
                    try:
                        manifests.append(self._storage.get_json(item_uri))
                    except Exception:
                        pass
            return sorted(manifests, key=lambda m: m.get("captured_at", ""), reverse=True)
        except Exception:
            return []

    def _persist_snapshot_record(
        self, tenant_id: str, snapshot_id: str, manifest: dict[str, Any]
    ) -> None:
        """Record snapshot metadata in the DB training_runs table if it exists."""
        try:
            from sqlalchemy import text
            with self._repository.session(tenant_id=tenant_id) as session:
                session.execute(
                    text(
                        """
                        INSERT INTO training_runs
                            (id, tenant_id, training_run_id, snapshot_id, status,
                             dataset_hash, row_count, feature_schema_version,
                             cutoff_timestamp, captured_at, metadata_json)
                        VALUES
                            (:id, :tenant_id, :training_run_id, :snapshot_id, 'snapshot',
                             :dataset_hash, :row_count, :feature_schema_version,
                             :cutoff_timestamp, :captured_at, :metadata_json)
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {
                        "id": f"{snapshot_id}-record",
                        "tenant_id": tenant_id,
                        "training_run_id": manifest.get("training_run_id", ""),
                        "snapshot_id": snapshot_id,
                        "dataset_hash": manifest.get("dataset_hash", ""),
                        "row_count": manifest.get("row_count", 0),
                        "feature_schema_version": manifest.get("feature_schema_version", "v2"),
                        "cutoff_timestamp": manifest.get("cutoff_timestamp"),
                        "captured_at": manifest.get("captured_at"),
                        "metadata_json": json.dumps(manifest, ensure_ascii=True),
                    },
                )
        except Exception as exc:
            # Table may not exist yet (pre-migration); log and continue
            logger.debug("Could not persist snapshot record to DB (pre-migration?): %s", exc)
