from __future__ import annotations

from pathlib import Path
import os
import pytest

from core.observability import (
    MetricsRegistry,
    record_ingestion_attempt,
    record_legacy_path_access,
    record_ingestion_summary,
    record_legacy_ingestion_usage,
)
from services.ingestion_service import EnterpriseIngestionService
from services.ingestion_service import IngestionError
from storage.object_storage import ObjectStorage


class _FakeRepo:
    def __init__(self) -> None:
        self.last_context: dict | None = None

    def upsert_runtime_context(self, tenant_id: str, user_scope: str, **kwargs):
        payload = {"tenant_id": tenant_id, "user_scope": user_scope, **kwargs}
        self.last_context = payload
        return payload


def test_rollout_metrics_are_emitted_to_prometheus_registry() -> None:
    record_ingestion_attempt(source_system="alert_jsonl", strict_mode=True)
    record_ingestion_summary(
        {
            "source_system": "alert_jsonl",
            "status": "partially_ingested",
            "failure_reason_category": "validation_error",
            "failed_count": 2,
            "warning_count": 1,
            "elapsed_ms": 42,
            "success_count": 3,
            "ingested_transaction_count": 7,
            "data_quality_counts": {"duplicate_alert_ids": 1, "missing_transactions": 1},
        }
    )
    record_legacy_ingestion_usage(endpoint="upload_bank_csv", status="accepted")
    record_legacy_path_access(endpoint="upload_bank_csv", caller="api", blocked=True)
    body = MetricsRegistry().prometheus()
    assert "ingestion_attempt_total" in body
    assert "ingestion_success_total" in body
    assert "ingestion_failure_total" in body
    assert "ingestion_validation_failure_total" in body
    assert "ingestion_warning_total" in body
    assert "ingestion_duration_ms" in body
    assert "ingested_alert_count" in body
    assert "ingested_transaction_count" in body
    assert "ingestion_data_quality_inconsistency_total" in body
    assert "primary_ingestion_mode" in body
    assert "ingestion_path_used_total" in body
    assert "alerts_ingested_per_mode" in body
    assert "legacy_ingestion_usage_total" in body
    assert "legacy_path_access_attempt_total" in body
    assert "legacy_path_access_blocked_total" in body


def test_legacy_bank_csv_ingestion_path_still_works(tmp_path: Path) -> None:
    repo = _FakeRepo()
    storage = ObjectStorage(tmp_path / "storage")
    service = EnterpriseIngestionService(repository=repo, object_storage=storage)  # type: ignore[arg-type]
    previous_flag = os.environ.get("ALTHEA_ENABLE_LEGACY_INGESTION")
    os.environ["ALTHEA_ENABLE_LEGACY_INGESTION"] = "true"

    try:
        csv_bytes = (
            "alert_id,user_id,amount,segment,country,typology,source_system,timestamp_utc\n"
            "ALT001,U1,1500.00,retail,US,structuring,core_bank,2026-04-05T00:00:00Z\n"
        ).encode("utf-8")
        out = service.upload_bank_csv(tenant_id="tenant-a", user_scope="public", raw_bytes=csv_bytes)

        assert out["rows"] == 1
        assert out["source"] == "BankCSV"
        assert out["dataset_artifact_uri"].endswith(".csv")
        assert repo.last_context is not None
        assert int(repo.last_context.get("row_count") or 0) == 1
    finally:
        if previous_flag is None:
            os.environ.pop("ALTHEA_ENABLE_LEGACY_INGESTION", None)
        else:
            os.environ["ALTHEA_ENABLE_LEGACY_INGESTION"] = previous_flag


def test_legacy_service_path_is_hard_disabled_by_default(tmp_path: Path) -> None:
    repo = _FakeRepo()
    storage = ObjectStorage(tmp_path / "storage")
    service = EnterpriseIngestionService(repository=repo, object_storage=storage)  # type: ignore[arg-type]
    previous_flag = os.environ.get("ALTHEA_ENABLE_LEGACY_INGESTION")
    os.environ["ALTHEA_ENABLE_LEGACY_INGESTION"] = "false"

    try:
        csv_bytes = (
            "alert_id,user_id,amount,segment,country,typology,source_system,timestamp_utc\n"
            "ALT001,U1,1500.00,retail,US,structuring,core_bank,2026-04-05T00:00:00Z\n"
        ).encode("utf-8")
        with pytest.raises(IngestionError, match="legacy_ingestion_disabled"):
            service.upload_bank_csv(tenant_id="tenant-a", user_scope="public", raw_bytes=csv_bytes)
    finally:
        if previous_flag is None:
            os.environ.pop("ALTHEA_ENABLE_LEGACY_INGESTION", None)
        else:
            os.environ["ALTHEA_ENABLE_LEGACY_INGESTION"] = previous_flag
