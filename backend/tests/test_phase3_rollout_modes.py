from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from services.pipeline_service import PipelineService


class _StubRepository:
    def set_tenant_context(self, tenant_id: str) -> None:
        return None

    def list_recent_alert_ids(self, tenant_id: str, limit: int = 5000) -> list[str]:
        return []

    def save_alert_payloads(self, tenant_id: str, run_id: str, records: list[dict]) -> int:
        return len(records)

    def upsert_runtime_context(self, tenant_id: str, user_scope: str, **kwargs):
        return {"tenant_id": tenant_id, "user_scope": user_scope, **kwargs}

    def list_alert_payloads_by_run(self, tenant_id: str, run_id: str, limit: int = 500000) -> list[dict]:
        return []


class _StubAlertIngestionService:
    def __init__(self) -> None:
        self.last_kwargs: dict[str, object] = {}

    def ingest_jsonl(self, **kwargs):
        self.last_kwargs = dict(kwargs)
        return {
            "run_id": str(kwargs.get("run_id") or "run"),
            "total_rows": int(kwargs.get("max_upload_rows") or 0),
            "success_count": 0,
            "failed_count": 0,
            "warning_count": 0,
            "strict_mode_used": bool(kwargs.get("strict_validation", False)),
            "source_system": "alert_jsonl",
            "status": "rejected",
            "elapsed_ms": 1,
            "failure_reason_category": "none",
            "ingested_alert_count": 0,
            "ingested_transaction_count": 0,
            "data_quality_inconsistency_count": 0,
            "data_quality_counts": {},
            "alerts": [],
            "critical_issue_count": 0,
            "critical_data_quality_issues": [],
            "processing_time_per_alert_ms": 0.0,
        }


def _build_service(settings: SimpleNamespace, alert_ingestion_service: _StubAlertIngestionService) -> PipelineService:
    return PipelineService(
        settings=settings,  # type: ignore[arg-type]
        repository=_StubRepository(),  # type: ignore[arg-type]
        event_bus=SimpleNamespace(publish=lambda *args, **kwargs: None),  # type: ignore[arg-type]
        job_queue=SimpleNamespace(queue_depth=lambda *args, **kwargs: 0),  # type: ignore[arg-type]
        ingestion_service=SimpleNamespace(),  # type: ignore[arg-type]
        feature_service=SimpleNamespace(),  # type: ignore[arg-type]
        inference_service=SimpleNamespace(),  # type: ignore[arg-type]
        governance_service=SimpleNamespace(),  # type: ignore[arg-type]
        model_monitoring_service=SimpleNamespace(),  # type: ignore[arg-type]
        alert_ingestion_service=alert_ingestion_service,  # type: ignore[arg-type]
        feature_adapter=SimpleNamespace(alerts_to_dataframe=lambda alerts: pd.DataFrame()),  # type: ignore[arg-type]
    )


def _base_settings() -> SimpleNamespace:
    return SimpleNamespace(
        enable_alert_jsonl_ingestion=True,
        strict_ingestion_validation=False,
        alert_jsonl_max_upload_rows=1,
        enable_ibm_amlsim_import=False,
        enable_legacy_ingestion=False,
        default_tenant_id="tenant-a",
        pipeline_batch_size=1000,
        primary_ingestion_mode="alert_jsonl",
    )


def test_alert_jsonl_disabled_blocks_pipeline() -> None:
    settings = _base_settings()
    settings.enable_alert_jsonl_ingestion = False
    alert_service = _StubAlertIngestionService()
    service = _build_service(settings=settings, alert_ingestion_service=alert_service)
    with pytest.raises(RuntimeError, match="alert_jsonl_ingestion_disabled"):
        service.run_alert_ingestion_pipeline(
            file_path="alerts.jsonl",
            run_id="run-disabled",
            tenant_id="tenant-a",
            upload_row_count=1,
        )


def test_finalization_uses_configured_upload_row_limit() -> None:
    settings = _base_settings()
    alert_service = _StubAlertIngestionService()
    service = _build_service(settings=settings, alert_ingestion_service=alert_service)
    service.run_alert_ingestion_pipeline(
        file_path="alerts.jsonl",
        run_id="run-limit",
        tenant_id="tenant-a",
        upload_row_count=3,
        canary_override=False,
    )
    assert alert_service.last_kwargs["max_upload_rows"] == 1


def test_canary_override_no_longer_changes_row_limit_after_finalization() -> None:
    settings = _base_settings()
    alert_service = _StubAlertIngestionService()
    service = _build_service(settings=settings, alert_ingestion_service=alert_service)
    result = service.run_alert_ingestion_pipeline(
        file_path="alerts.jsonl",
        run_id="run-finalized",
        tenant_id="tenant-a",
        upload_row_count=3,
        canary_override=True,
    )
    assert result["rollout_mode"] == "full"
    assert alert_service.last_kwargs["max_upload_rows"] == 1
