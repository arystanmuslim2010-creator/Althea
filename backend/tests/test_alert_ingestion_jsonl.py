from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from services.alert_ingestion_service import AlertIngestionService, AlertIngestionValidationError


def _write_jsonl(tmp_path: Path, rows: list[dict[str, Any] | str]) -> Path:
    file_path = tmp_path / "alerts.jsonl"
    with file_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            if isinstance(row, str):
                handle.write(row + "\n")
            else:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")
    return file_path


def _valid_alert(alert_id: str) -> dict[str, Any]:
    return {
        "alert_id": alert_id,
        "created_at": "2026-01-01T00:00:00Z",
        "typology": "structuring",
        "is_sar": 1,
        "accounts": [{"account_id": f"acct-{alert_id}", "country": "US", "segment": "retail"}],
        "transactions": [
            {
                "transaction_id": f"tx-{alert_id}-1",
                "amount": 1000.0,
                "timestamp": "2026-01-01T00:10:00Z",
                "sender": "A",
                "receiver": "B",
            }
        ],
        "metadata": {"source_system": "core_bank"},
    }


def test_valid_jsonl_ingestion(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(tmp_path, [_valid_alert("ALERT-1"), _valid_alert("ALERT-2")])

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_valid")

    assert summary["total_rows"] == 2
    assert summary["success_count"] == 2
    assert summary["failed_count"] == 0
    assert summary["status"] == "accepted"
    assert summary["strict_mode_used"] is False
    assert summary["source_system"] == "core_bank"
    assert summary["elapsed_ms"] >= 0
    assert len(summary["alerts"]) == 2
    assert summary["alerts"][0]["run_id"] == "run_valid"
    assert isinstance(summary["alerts"][0]["transactions"], list)
    assert isinstance(summary["alerts"][0]["accounts"], list)


def test_missing_fields_are_defaulted(tmp_path: Path) -> None:
    service = AlertIngestionService()
    row = {
        "alert_id": "ALERT-MISSING",
        # created_at intentionally omitted
        # typology intentionally omitted
        # accounts intentionally omitted
        "transactions": [
            {
                "transaction_id": "tx-m-1",
                "amount": 50.0,
                "timestamp": "2026-02-01T08:00:00Z",
                "sender": "S1",
                "receiver": "R1",
            }
        ],
    }
    path = _write_jsonl(tmp_path, [row])

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_missing")
    alert = summary["alerts"][0]

    assert summary["total_rows"] == 1
    assert summary["success_count"] == 1
    assert summary["failed_count"] == 0
    assert alert["typology"] == "anomaly"
    assert alert["accounts"] == []
    assert alert["metadata"] == {}


def test_malformed_json_line_does_not_crash_ingestion(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(
        tmp_path,
        [
            _valid_alert("ALERT-OK"),
            '{"alert_id":"ALERT-BAD","transactions":[}',
            _valid_alert("ALERT-OK2"),
        ],
    )

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_malformed")

    assert summary["total_rows"] == 3
    assert summary["success_count"] == 2
    assert summary["failed_count"] == 1
    assert summary["status"] == "partially_ingested"
    assert len(summary["alerts"]) == 2


def test_empty_transactions_are_accepted(tmp_path: Path) -> None:
    service = AlertIngestionService()
    row = {
        "alert_id": "ALERT-EMPTY-TX",
        "created_at": "2026-03-01T00:00:00Z",
        "typology": "sanctions",
        "accounts": [{"account_id": "acct-empty"}],
        "transactions": [],
    }
    path = _write_jsonl(tmp_path, [row])

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_empty_tx")
    alert = summary["alerts"][0]

    assert summary["total_rows"] == 1
    assert summary["success_count"] == 1
    assert summary["failed_count"] == 0
    assert summary["warning_count"] >= 1
    assert summary["data_quality_counts"]["missing_transactions"] >= 1
    assert alert["transactions"] == []
    assert alert["num_transactions"] == 1


def test_duplicate_alert_id_is_counted_as_failure_without_crashing(tmp_path: Path) -> None:
    service = AlertIngestionService()
    first = _valid_alert("ALERT-DUP")
    second = _valid_alert("ALERT-DUP")
    second["transactions"][0]["transaction_id"] = "tx-duplicate-2"
    path = _write_jsonl(tmp_path, [first, second])

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_dup")

    assert summary["total_rows"] == 2
    assert summary["success_count"] == 1
    assert summary["failed_count"] == 1
    assert summary["data_quality_counts"]["duplicate_alert_ids"] == 1
    assert summary["critical_issue_count"] >= 1
    assert len(summary["alerts"]) == 1


def test_duplicate_alert_id_across_runs_is_detected_as_warning(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(tmp_path, [_valid_alert("ALERT-REPEAT")])
    summary = service.ingest_jsonl(
        file_path=str(path),
        run_id="run_repeat",
        known_recent_alert_ids={"ALERT-REPEAT"},
    )
    assert summary["success_count"] == 1
    assert summary["warning_count"] >= 1
    assert summary["data_quality_counts"]["duplicate_alert_ids_across_runs"] == 1


def test_strict_validation_rejects_upload_when_any_row_is_invalid(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(
        tmp_path,
        [
            _valid_alert("ALERT-OK"),
            '{"alert_id":"ALERT-BAD","transactions":[}',
        ],
    )

    with pytest.raises(AlertIngestionValidationError) as exc_info:
        service.ingest_jsonl(file_path=str(path), run_id="run_strict", strict_validation=True)
    assert exc_info.value.summary["status"] == "failed_validation"
    assert exc_info.value.summary["strict_mode_used"] is True
    assert exc_info.value.summary["failed_count"] >= 1


def test_missing_alert_id_is_detected_as_validation_failure(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(
        tmp_path,
        [
            {"created_at": "2026-01-01T00:00:00Z", "accounts": [], "transactions": []},
            _valid_alert("ALERT-OK"),
        ],
    )
    summary = service.ingest_jsonl(file_path=str(path), run_id="run_missing_id", strict_validation=False)
    assert summary["status"] == "partially_ingested"
    assert summary["failed_count"] == 1
    assert summary["data_quality_counts"]["missing_alert_id"] == 1


def test_ibm_amlsim_rows_are_rejected_when_ibm_import_flag_is_off(tmp_path: Path) -> None:
    service = AlertIngestionService()
    row = _valid_alert("ALERT-IBM")
    row["metadata"] = {"source_system": "ibm_amlsim"}
    path = _write_jsonl(tmp_path, [row])

    summary = service.ingest_jsonl(
        file_path=str(path),
        run_id="run_ibm_blocked",
        strict_validation=False,
        allow_ibm_amlsim_import=False,
    )
    assert summary["status"] == "rejected"
    assert summary["success_count"] == 0
    assert summary["failed_count"] == 1
    assert summary["data_quality_counts"]["ibm_amlsim_import_blocked"] == 1


def test_jsonl_max_upload_rows_guard_blocks_oversized_upload(tmp_path: Path) -> None:
    service = AlertIngestionService()
    path = _write_jsonl(tmp_path, [_valid_alert("A1"), _valid_alert("A2")])

    with pytest.raises(AlertIngestionValidationError) as exc_info:
        service.ingest_jsonl(
            file_path=str(path),
            run_id="run_canary_limit",
            strict_validation=False,
            max_upload_rows=1,
        )
    summary = exc_info.value.summary
    assert summary["status"] == "failed_validation"
    assert summary["failure_reason_category"] == "upload_limit_exceeded"
    assert summary["data_quality_counts"]["max_upload_rows_exceeded"] == 1


def test_empty_alert_is_rejected_as_critical_data_quality_issue(tmp_path: Path) -> None:
    service = AlertIngestionService()
    row = {
        "alert_id": "ALERT-EMPTY",
        "created_at": "2026-01-01T00:00:00Z",
        "typology": "anomaly",
        "accounts": [],
        "transactions": [],
    }
    path = _write_jsonl(tmp_path, [row])

    summary = service.ingest_jsonl(file_path=str(path), run_id="run_empty_alert")
    assert summary["status"] == "rejected"
    assert summary["failed_count"] == 1
    assert summary["data_quality_counts"]["empty_alerts"] == 1
    assert summary["critical_issue_count"] >= 1
    assert "empty_alerts" in summary["critical_data_quality_issues"]


def test_failed_rows_are_bounded_to_limit_for_memory_safety(tmp_path: Path) -> None:
    service = AlertIngestionService()
    malformed_rows = ['{"alert_id":"BROKEN","transactions":[}' for _ in range(260)]
    path = _write_jsonl(tmp_path, malformed_rows)
    summary = service.ingest_jsonl(file_path=str(path), run_id="run_many_failures")
    assert summary["failed_count"] == 260
    assert len(summary["failed_rows"]) == 200
    assert int(summary["failed_rows_truncated_count"]) == 60
