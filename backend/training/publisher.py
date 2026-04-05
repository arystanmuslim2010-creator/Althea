"""Model publisher — integrates training artifacts with the model registry.

Publishes the trained escalation model, optional time model, calibration
artifact, evaluation report, and feature schema to the existing
ModelRegistry and ObjectStorage infrastructure.

The publisher follows the existing registry conventions so that trained
models are immediately available for inference via
``InferenceService.predict()`` with ``strategy='active_approved'``.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("althea.training.publisher")


class ModelPublisher:
    """Publish training artifacts to the model registry.

    Wraps the existing ``ModelRegistry`` so that all training-time
    decisions about approval, activation, and versioning are centralised
    here rather than scattered across training scripts.
    """

    def __init__(
        self,
        model_registry,
        object_storage,
        auto_approve: bool = False,
        auto_activate: bool = False,
        approved_by: str = "training_pipeline",
    ) -> None:
        """
        Parameters
        ----------
        model_registry  : ModelRegistry instance
        object_storage  : ObjectStorage instance
        auto_approve    : if True, set approval_status='approved' immediately
        auto_activate   : if True, set the model as active after approval
                          (only relevant when auto_approve=True)
        approved_by     : actor name recorded in the approval trail
        """
        self._registry = model_registry
        self._storage = object_storage
        self._auto_approve = auto_approve
        self._auto_activate = auto_activate
        self._approved_by = approved_by

    def publish_escalation_model(
        self,
        tenant_id: str,
        artifact_bytes: bytes,
        feature_schema: dict[str, Any],
        metrics: dict[str, Any],
        training_metadata: dict[str, Any],
        calibration_artifact_bytes: bytes | None = None,
        evaluation_report: dict[str, Any] | None = None,
        training_run_id: str | None = None,
        dataset_hash: str = "",
        model_version: str | None = None,
    ) -> dict[str, Any]:
        """Publish escalation model artifacts to the registry.

        Returns the model record dict as stored by the registry.
        """
        version = model_version or f"esc-{uuid.uuid4().hex[:10]}"
        approval_status = "approved" if self._auto_approve else "draft"

        # Store calibration artifact alongside the model
        enriched_metadata = dict(training_metadata)
        enriched_metadata["model_purpose"] = "escalation"
        enriched_metadata["training_run_id"] = training_run_id or ""
        enriched_metadata["feature_schema_version"] = feature_schema.get("version", "v2")

        if calibration_artifact_bytes:
            calib_uri = f"models/{tenant_id}/{version}/calibrator.bin"
            self._storage.put_bytes(calib_uri, calibration_artifact_bytes)
            enriched_metadata["calibration_uri"] = calib_uri
            enriched_metadata["has_calibration"] = True
        else:
            enriched_metadata["has_calibration"] = False

        if evaluation_report:
            eval_uri = f"models/{tenant_id}/{version}/evaluation_report.json"
            self._storage.put_json(eval_uri, evaluation_report)
            enriched_metadata["evaluation_report_uri"] = eval_uri

        # The existing register_model handles artifact + schema + metrics storage
        record = self._registry.register_model(
            tenant_id=tenant_id,
            artifact_bytes=artifact_bytes,
            training_dataset_hash=dataset_hash,
            feature_schema=feature_schema,
            metrics=metrics,
            training_metadata=enriched_metadata,
            approval_status=approval_status,
            model_version=version,
            approved_by=self._approved_by if self._auto_approve else None,
            feature_schema_version=feature_schema.get("version", "v2"),
        )

        if self._auto_approve and self._auto_activate:
            self._registry.set_active_model(
                tenant_id=tenant_id,
                model_version=version,
                actor=self._approved_by,
            )

        logger.info(
            json.dumps(
                {
                    "event": "escalation_model_published",
                    "tenant_id": tenant_id,
                    "model_version": version,
                    "approval_status": approval_status,
                    "auto_activate": self._auto_approve and self._auto_activate,
                    "artifact_bytes": len(artifact_bytes),
                },
                ensure_ascii=True,
            )
        )
        return record

    def publish_time_model(
        self,
        tenant_id: str,
        artifact_bytes_p50: bytes,
        artifact_bytes_p90: bytes | None,
        feature_schema: dict[str, Any],
        metrics: dict[str, Any],
        training_metadata: dict[str, Any],
        training_run_id: str | None = None,
        dataset_hash: str = "",
        model_version: str | None = None,
    ) -> dict[str, Any]:
        """Publish investigation time model artifacts.

        The time model is stored under a separate version namespace
        (``time-*``) so it does not conflict with escalation models
        resolved via ``strategy='active_approved'``.
        """
        version = model_version or f"time-{uuid.uuid4().hex[:10]}"

        enriched_metadata = dict(training_metadata)
        enriched_metadata["model_purpose"] = "investigation_time"
        enriched_metadata["training_run_id"] = training_run_id or ""
        enriched_metadata["feature_schema_version"] = feature_schema.get("version", "v2")

        # Store p90 artifact separately if present
        if artifact_bytes_p90:
            p90_uri = f"models/{tenant_id}/{version}/time_model_p90.bin"
            self._storage.put_bytes(p90_uri, artifact_bytes_p90)
            enriched_metadata["p90_model_uri"] = p90_uri

        # Use 'draft' by default for time models; require explicit approval
        record = self._registry.register_model(
            tenant_id=tenant_id,
            artifact_bytes=artifact_bytes_p50,
            training_dataset_hash=dataset_hash,
            feature_schema=feature_schema,
            metrics=metrics,
            training_metadata=enriched_metadata,
            approval_status="draft",
            model_version=version,
            approved_by=None,
        )

        logger.info(
            json.dumps(
                {
                    "event": "time_model_published",
                    "tenant_id": tenant_id,
                    "model_version": version,
                    "has_p90": artifact_bytes_p90 is not None,
                },
                ensure_ascii=True,
            )
        )
        return record

    def publish_training_run_record(
        self,
        tenant_id: str,
        training_run_id: str,
        run_metadata: dict[str, Any],
    ) -> None:
        """Persist a training run record to object storage for audit and lineage."""
        uri = f"training_runs/{tenant_id}/{training_run_id}/run_record.json"
        record = {
            "training_run_id": training_run_id,
            "tenant_id": tenant_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
            **run_metadata,
        }
        self._storage.put_json(uri, record)
        logger.info(
            json.dumps(
                {
                    "event": "training_run_record_published",
                    "tenant_id": tenant_id,
                    "training_run_id": training_run_id,
                    "uri": uri,
                },
                ensure_ascii=True,
            )
        )
