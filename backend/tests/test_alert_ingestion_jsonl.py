from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.alert_ingestion_service import AlertIngestionService


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
    assert len(summary["alerts"]) == 1

