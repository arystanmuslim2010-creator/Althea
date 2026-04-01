from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from schemas.alert_ingestion_schema import AlertPayload

logger = logging.getLogger("althea.alert_ingestion")


class AlertIngestionService:
    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def ingest_jsonl(self, file_path: str, run_id: str) -> dict[str, Any]:
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            raise ValueError(f"JSONL file does not exist: {file_path}")

        total_rows = 0
        success_count = 0
        failed_count = 0
        failed_rows: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []
        seen_alert_ids: set[str] = set()

        logger.info(
            "Alert JSONL ingestion started",
            extra={"run_id": run_id, "file_name": path.name},
        )

        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                payload_line = raw_line.strip()
                if not payload_line:
                    continue
                total_rows += 1

                try:
                    parsed = json.loads(payload_line)
                except json.JSONDecodeError as exc:
                    failed_count += 1
                    failed_rows.append(
                        {
                            "line_number": line_number,
                            "reason": "malformed_json",
                            "message": str(exc),
                        }
                    )
                    logger.warning(
                        "Alert JSONL row rejected: malformed JSON",
                        extra={"run_id": run_id, "line_number": line_number},
                    )
                    continue

                try:
                    alert = AlertPayload.model_validate(parsed)
                    if alert.alert_id in seen_alert_ids:
                        raise ValueError(f"duplicate alert_id in file: {alert.alert_id}")
                    seen_alert_ids.add(alert.alert_id)
                    alerts.append(self.transform_to_internal_format(alert, run_id=run_id))
                    success_count += 1
                except (ValidationError, ValueError) as exc:
                    failed_count += 1
                    failed_rows.append(
                        {
                            "line_number": line_number,
                            "reason": "validation_error",
                            "message": str(exc),
                        }
                    )
                    logger.warning(
                        "Alert JSONL row rejected by schema validation",
                        extra={"run_id": run_id, "line_number": line_number},
                    )

        summary: dict[str, Any] = {
            "total_rows": total_rows,
            "success_count": success_count,
            "failed_count": failed_count,
            "alerts": alerts,
            "failed_rows": failed_rows,
        }
        logger.info(
            "Alert JSONL ingestion completed",
            extra={
                "run_id": run_id,
                "total_rows": total_rows,
                "success_count": success_count,
                "failed_count": failed_count,
            },
        )
        return summary

    def transform_to_internal_format(self, alert: AlertPayload, run_id: str | None = None) -> dict[str, Any]:
        resolved_run_id = run_id or ""
        ingested_at = self._utcnow_iso()
        payload_json = alert.model_dump(mode="json")
        transactions = list(payload_json.get("transactions") or [])
        accounts = list(payload_json.get("accounts") or [])
        metadata = dict(payload_json.get("metadata") or {})

        total_amount = float(sum(float(tx.get("amount", 0.0) or 0.0) for tx in transactions))
        first_account = accounts[0] if accounts else {}
        user_id = str(first_account.get("account_id") or metadata.get("user_id") or "")
        country = str(first_account.get("country") or metadata.get("country") or "UNKNOWN")
        segment = str(first_account.get("segment") or metadata.get("segment") or "retail")
        timestamp = None
        if transactions:
            timestamp = transactions[0].get("timestamp")
        timestamp = timestamp or payload_json.get("created_at") or ingested_at
        source_system = str(metadata.get("source_system") or "alert_jsonl")

        return {
            "alert_id": payload_json["alert_id"],
            "created_at": payload_json.get("created_at"),
            "timestamp": timestamp,
            "typology": payload_json.get("typology") or "anomaly",
            "is_sar": payload_json.get("is_sar"),
            "accounts": accounts,
            "transactions": transactions,
            "metadata": metadata,
            "optional_fields": payload_json.get("optional_fields") or {},
            "amount": total_amount,
            "num_transactions": max(1, len(transactions)),
            "user_id": user_id or f"user_{payload_json['alert_id']}",
            "country": country or "UNKNOWN",
            "segment": segment or "retail",
            "source_system": source_system,
            "ingestion_timestamp": ingested_at,
            "run_id": resolved_run_id,
            # New alert-centric storage contract.
            "raw_payload_json": payload_json,
        }
