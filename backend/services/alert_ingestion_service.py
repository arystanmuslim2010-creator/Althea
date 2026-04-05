from __future__ import annotations

from collections import Counter
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from schemas.alert_ingestion_schema import AlertPayload

logger = logging.getLogger("althea.alert_ingestion")


class AlertIngestionValidationError(ValueError):
    def __init__(self, message: str, summary: dict[str, Any]) -> None:
        super().__init__(message)
        self.summary = summary


class AlertIngestionService:
    _ABNORMAL_TRANSACTION_THRESHOLD = 250
    _MAX_FAILED_ROWS_CAPTURE = 200

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _coerce_optional_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value in {0, 0.0}:
                return False
            if value in {1, 1.0}:
                return True
        raw = str(value).strip().lower()
        if raw in {"1", "true", "yes", "y"}:
            return True
        if raw in {"0", "false", "no", "n"}:
            return False
        return None

    @staticmethod
    def _normalize_source_system(parsed: dict[str, Any]) -> str:
        metadata = parsed.get("metadata")
        metadata_dict = metadata if isinstance(metadata, dict) else {}
        source = metadata_dict.get("source_system") or parsed.get("source_system") or "alert_jsonl"
        return str(source or "alert_jsonl").strip() or "alert_jsonl"

    @staticmethod
    def _is_ibm_amlsim_source(source_system: str) -> bool:
        normalized = str(source_system or "").strip().lower()
        return normalized in {"ibm_amlsim", "ibm-amlsim", "amlsim"}

    @staticmethod
    def _final_status(*, success_count: int, failed_count: int, strict_validation: bool, forced_validation_failure: bool) -> str:
        if forced_validation_failure or (strict_validation and failed_count > 0):
            return "failed_validation"
        if success_count > 0 and failed_count > 0:
            return "partially_ingested"
        if success_count > 0:
            return "accepted"
        return "rejected"

    @staticmethod
    def _failure_reason_category(
        failed_rows: list[dict[str, Any]],
        status: str,
        forced_validation_failure: bool,
    ) -> str:
        if forced_validation_failure:
            return "upload_limit_exceeded"
        if status == "failed_validation":
            return "validation_failure"
        reasons = [str(item.get("reason") or "").strip() for item in failed_rows]
        reasons = [reason for reason in reasons if reason]
        if not reasons:
            return "none"
        return str(Counter(reasons).most_common(1)[0][0])

    def ingest_jsonl(
        self,
        file_path: str,
        run_id: str,
        strict_validation: bool = False,
        max_upload_rows: int = 1000,
        allow_ibm_amlsim_import: bool = False,
        known_recent_alert_ids: set[str] | None = None,
        canary_override: bool = False,
        rollout_mode: str = "disabled",
    ) -> dict[str, Any]:
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            raise ValueError(f"JSONL file does not exist: {file_path}")

        started = perf_counter()
        total_rows = 0
        success_count = 0
        failed_count = 0
        warning_count = 0
        ingested_transaction_count = 0
        failed_rows: list[dict[str, Any]] = []
        failed_rows_truncated_count = 0
        alerts: list[dict[str, Any]] = []
        seen_alert_ids: set[str] = set()
        source_systems: set[str] = set()
        known_alert_ids = {str(item).strip() for item in (known_recent_alert_ids or set()) if str(item).strip()}
        forced_validation_failure = False
        data_quality_counts: dict[str, int] = {
            "missing_alert_id": 0,
            "missing_accounts": 0,
            "missing_transactions": 0,
            "inconsistent_sar_markers": 0,
            "inconsistent_sar_labeling_patterns": 0,
            "duplicate_alert_ids": 0,
            "duplicate_alert_ids_across_runs": 0,
            "empty_alerts": 0,
            "abnormal_alert_size": 0,
            "missing_critical_normalized_fields": 0,
            "ibm_amlsim_import_blocked": 0,
            "max_upload_rows_exceeded": 0,
        }
        data_quality_severity_counts = {"warning": 0, "critical": 0}
        critical_data_quality_issues: set[str] = set()

        def _mark_quality_issue(issue_type: str, severity: str) -> None:
            if severity == "critical":
                data_quality_severity_counts["critical"] += 1
                critical_data_quality_issues.add(issue_type)
            else:
                data_quality_severity_counts["warning"] += 1

        def _append_failed_row(entry: dict[str, Any]) -> None:
            nonlocal failed_rows_truncated_count
            if len(failed_rows) >= self._MAX_FAILED_ROWS_CAPTURE:
                failed_rows_truncated_count += 1
                return
            failed_rows.append(entry)

        logger.info(
            "Alert JSONL ingestion started",
            extra={
                "run_id": run_id,
                "file_name": path.name,
                "strict_mode": bool(strict_validation),
                "max_upload_rows": int(max_upload_rows),
            },
        )

        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                payload_line = raw_line.strip()
                if not payload_line:
                    continue
                total_rows += 1
                if total_rows > max(1, int(max_upload_rows)):
                    data_quality_counts["max_upload_rows_exceeded"] += 1
                    _mark_quality_issue("max_upload_rows_exceeded", "critical")
                    failed_count += 1
                    forced_validation_failure = True
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": "max_upload_rows_exceeded",
                            "message": f"Upload exceeds ALTHEA_ALERT_JSONL_MAX_UPLOAD_ROWS={int(max_upload_rows)}.",
                        }
                    )
                    break

                try:
                    parsed = json.loads(payload_line)
                except json.JSONDecodeError as exc:
                    failed_count += 1
                    _append_failed_row(
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
                if not isinstance(parsed, dict):
                    failed_count += 1
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": "validation_error",
                            "message": "payload must be a JSON object",
                        }
                    )
                    continue

                source_system = self._normalize_source_system(parsed)
                source_systems.add(source_system)
                if self._is_ibm_amlsim_source(source_system) and not bool(allow_ibm_amlsim_import):
                    data_quality_counts["ibm_amlsim_import_blocked"] += 1
                    failed_count += 1
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": "ibm_amlsim_import_disabled",
                            "message": "IBM AMLSim import is disabled by configuration.",
                        }
                    )
                    continue

                raw_alert_id = str(parsed.get("alert_id") or "").strip()
                if not raw_alert_id:
                    data_quality_counts["missing_alert_id"] += 1
                    _mark_quality_issue("missing_alert_id", "critical")
                    failed_count += 1
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": "missing_alert_id",
                            "message": "alert_id is required",
                        }
                    )
                    continue

                row_warning_codes: list[str] = []
                accounts = list(parsed.get("accounts") or [])
                transactions = list(parsed.get("transactions") or [])
                if not accounts:
                    data_quality_counts["missing_accounts"] += 1
                    _mark_quality_issue("missing_accounts", "warning")
                    warning_count += 1
                    row_warning_codes.append("missing_accounts")
                if not transactions:
                    data_quality_counts["missing_transactions"] += 1
                    _mark_quality_issue("missing_transactions", "warning")
                    warning_count += 1
                    row_warning_codes.append("missing_transactions")
                if not accounts and not transactions:
                    data_quality_counts["empty_alerts"] += 1
                    _mark_quality_issue("empty_alerts", "critical")
                    failed_count += 1
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": "empty_alert",
                            "message": "alert contains neither accounts nor transactions",
                        }
                    )
                    continue
                if len(transactions) > self._ABNORMAL_TRANSACTION_THRESHOLD:
                    data_quality_counts["abnormal_alert_size"] += 1
                    _mark_quality_issue("abnormal_alert_size", "warning")
                    warning_count += 1
                    row_warning_codes.append("abnormal_alert_size")
                if raw_alert_id in known_alert_ids:
                    data_quality_counts["duplicate_alert_ids_across_runs"] += 1
                    _mark_quality_issue("duplicate_alert_ids_across_runs", "warning")
                    warning_count += 1
                    row_warning_codes.append("duplicate_alert_ids_across_runs")

                sar_marker = self._coerce_optional_bool(parsed.get("is_sar"))
                eval_marker = self._coerce_optional_bool(parsed.get("evaluation_label_is_sar"))
                if sar_marker is not None and eval_marker is not None and sar_marker != eval_marker:
                    data_quality_counts["inconsistent_sar_markers"] += 1
                    data_quality_counts["inconsistent_sar_labeling_patterns"] += 1
                    _mark_quality_issue("inconsistent_sar_labeling_patterns", "warning")
                    warning_count += 1
                    row_warning_codes.append("inconsistent_sar_markers")
                metadata = parsed.get("metadata")
                metadata_dict = metadata if isinstance(metadata, dict) else {}
                metadata_sar = self._coerce_optional_bool(
                    metadata_dict.get("sar_label") or metadata_dict.get("is_sar")
                )
                if sar_marker is not None and metadata_sar is not None and sar_marker != metadata_sar:
                    data_quality_counts["inconsistent_sar_labeling_patterns"] += 1
                    _mark_quality_issue("inconsistent_sar_labeling_patterns", "warning")
                    warning_count += 1
                    row_warning_codes.append("inconsistent_sar_labeling_patterns")

                try:
                    alert = AlertPayload.model_validate(parsed)
                    if alert.alert_id in seen_alert_ids:
                        data_quality_counts["duplicate_alert_ids"] += 1
                        _mark_quality_issue("duplicate_alert_ids", "critical")
                        raise ValueError(f"duplicate alert_id in file: {alert.alert_id}")
                    seen_alert_ids.add(alert.alert_id)
                    internal_alert = self.transform_to_internal_format(
                        alert,
                        run_id=run_id,
                        warning_codes=row_warning_codes,
                    )
                    missing_critical_fields = [
                        key
                        for key in ("alert_id", "user_id", "timestamp", "source_system", "schema_version")
                        if not str(internal_alert.get(key) or "").strip()
                    ]
                    if missing_critical_fields:
                        data_quality_counts["missing_critical_normalized_fields"] += 1
                        _mark_quality_issue("missing_critical_normalized_fields", "critical")
                        failed_count += 1
                        _append_failed_row(
                            {
                                "line_number": line_number,
                                "reason": "missing_critical_normalized_fields",
                                "message": f"missing normalized fields: {', '.join(missing_critical_fields)}",
                            }
                        )
                        continue
                    alerts.append(internal_alert)
                    ingested_transaction_count += len(internal_alert.get("transactions") or [])
                    success_count += 1
                except (ValidationError, ValueError) as exc:
                    failed_count += 1
                    reason = "validation_error"
                    if "duplicate alert_id in file" in str(exc):
                        reason = "duplicate_alert_id"
                    _append_failed_row(
                        {
                            "line_number": line_number,
                            "reason": reason,
                            "message": str(exc),
                        }
                    )
                    logger.warning(
                        "Alert JSONL row rejected by schema validation",
                        extra={"run_id": run_id, "line_number": line_number},
                    )

        source_system = "unknown"
        if len(source_systems) == 1:
            source_system = next(iter(source_systems))
        elif len(source_systems) > 1:
            source_system = "mixed"

        status = self._final_status(
            success_count=success_count,
            failed_count=failed_count,
            strict_validation=bool(strict_validation),
            forced_validation_failure=forced_validation_failure,
        )
        failure_reason_category = self._failure_reason_category(
            failed_rows=failed_rows,
            status=status,
            forced_validation_failure=forced_validation_failure,
        )
        data_quality_inconsistency_count = sum(
            int(data_quality_counts.get(key, 0))
            for key in (
                "missing_accounts",
                "missing_transactions",
                "inconsistent_sar_markers",
                "inconsistent_sar_labeling_patterns",
                "duplicate_alert_ids",
                "duplicate_alert_ids_across_runs",
                "missing_alert_id",
                "empty_alerts",
                "abnormal_alert_size",
                "missing_critical_normalized_fields",
                "ibm_amlsim_import_blocked",
                "max_upload_rows_exceeded",
            )
        )
        elapsed_ms = int(max(0.0, (perf_counter() - started) * 1000.0))
        processing_time_per_alert_ms = float(elapsed_ms / success_count) if success_count > 0 else 0.0

        summary: dict[str, Any] = {
            "run_id": run_id,
            "total_rows": total_rows,
            "success_count": success_count,
            "failed_count": failed_count,
            "warning_count": warning_count,
            "strict_mode_used": bool(strict_validation),
            "source_system": source_system,
            "status": status,
            "elapsed_ms": elapsed_ms,
            "failure_reason_category": failure_reason_category,
            "ingested_alert_count": success_count,
            "ingested_transaction_count": ingested_transaction_count,
            "data_quality_inconsistency_count": data_quality_inconsistency_count,
            "data_quality_counts": data_quality_counts,
            "data_quality_severity_counts": dict(data_quality_severity_counts),
            "critical_issue_count": int(data_quality_severity_counts.get("critical", 0)),
            "critical_data_quality_issues": sorted(critical_data_quality_issues),
            "failed_rows_truncated_count": int(failed_rows_truncated_count),
            "processing_time_per_alert_ms": processing_time_per_alert_ms,
            "alerts": alerts,
            "failed_rows": failed_rows,
        }

        logger.info(
            "Alert JSONL ingestion completed",
            extra={
                "run_id": run_id,
                "status": status,
                "source_system": source_system,
                "strict_mode": bool(strict_validation),
                "total_rows": total_rows,
                "success_count": success_count,
                "failed_count": failed_count,
                "warning_count": warning_count,
                "elapsed_ms": elapsed_ms,
                "failure_reason_category": failure_reason_category,
                "critical_issue_count": int(data_quality_severity_counts.get("critical", 0)),
            },
        )
        if status == "failed_validation":
            message = "Alert JSONL ingestion failed validation."
            if failure_reason_category == "upload_limit_exceeded":
                message = "Alert JSONL upload exceeded the configured row limit."
            elif strict_validation and failed_count > 0:
                message = "Strict ingestion validation failed. Upload rejected."
            raise AlertIngestionValidationError(message, summary=summary)
        return summary

    def transform_to_internal_format(
        self,
        alert: AlertPayload,
        run_id: str | None = None,
        warning_codes: list[str] | None = None,
    ) -> dict[str, Any]:
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
        schema_version = str(payload_json.get("schema_version") or metadata.get("schema_version") or "alert_jsonl.v1")
        warnings: list[str] = list(warning_codes or [])
        if not payload_json.get("created_at"):
            warnings.append("created_at_missing")

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
            "schema_version": schema_version,
            "evaluation_label_is_sar": self._coerce_optional_bool(payload_json.get("is_sar")),
            "ingestion_metadata_json": {
                "source_system": source_system,
                "schema_version": schema_version,
                "ingestion_run_id": resolved_run_id,
                "ingestion_timestamp": ingested_at,
                "warnings": warnings,
                "context": {"format": "jsonl", "service": "alert_ingestion_service"},
            },
            # New alert-centric storage contract.
            "raw_payload_json": payload_json,
        }
