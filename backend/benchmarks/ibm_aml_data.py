from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from models.feature_schema import FeatureSchemaValidator
from models.inference_service import InferenceService
from models.model_registry import ModelRegistry
from services.feature_adapter import AlertFeatureAdapter
from services.feature_service import EnterpriseFeatureService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository

logger = logging.getLogger("althea.benchmarks.ibm_aml")

_CSV_HEADER_NORMALIZED = [
    "timestamp",
    "from_bank",
    "account",
    "to_bank",
    "account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
]
_PATTERN_BEGIN_RE = re.compile(r"^BEGIN LAUNDERING ATTEMPT - (?P<typology>[A-Z-]+)")
_PATTERN_END_RE = re.compile(r"^END LAUNDERING ATTEMPT")
_MAX_ERROR_EXAMPLES = 50
_DEFAULT_SOURCE_SYSTEM = "ibm_amlsim"
_DEFAULT_DATASET_NAME = "IBM AML-Data HI-Small"
_DEFAULT_ALERT_ID_PREFIX = "IBMHI"
_DEFAULT_SCHEMA_VERSION = "ibm_aml_alert_jsonl.v1"
_DEFAULT_GROUPING_RULE = "same_source_account_within_24h_anchored_window"
_DEFAULT_REPORT_RECALL_TARGET = 0.80
_DEFAULT_TOP_FRACTIONS = (0.10, 0.20)


@dataclass(slots=True)
class ParsedTransaction:
    line_number: int
    timestamp: datetime
    from_bank: str
    from_account: str
    to_bank: str
    to_account: str
    amount_received_raw: str
    receiving_currency: str
    amount_paid_raw: str
    payment_currency: str
    payment_format: str
    is_laundering: int
    amount_received: float
    amount_paid: float
    source_account_key: str
    destination_account_key: str
    signature: tuple[str, ...]

    @property
    def normalized_amount(self) -> float:
        if math.isfinite(self.amount_paid):
            return float(self.amount_paid)
        return float(self.amount_received)

    @property
    def normalized_currency(self) -> str:
        return str(self.payment_currency or self.receiving_currency or "").strip() or "UNKNOWN"


@dataclass(slots=True)
class PatternIndex:
    signature_to_typology: dict[tuple[str, ...], str]
    ambiguous_signatures: set[tuple[str, ...]]
    block_counts: dict[str, int]
    transaction_counts: dict[str, int]
    unmatched_rows: int = 0

    def lookup(self, signature: tuple[str, ...]) -> str:
        if signature in self.ambiguous_signatures:
            return "unknown"
        return self.signature_to_typology.get(signature, "unknown")

    def to_summary(self) -> dict[str, Any]:
        return {
            "unique_typed_transactions": int(len(self.signature_to_typology)),
            "ambiguous_transaction_signatures": int(len(self.ambiguous_signatures)),
            "pattern_block_counts": dict(sorted(self.block_counts.items())),
            "pattern_transaction_counts": dict(sorted(self.transaction_counts.items())),
            "unmatched_pattern_rows": int(self.unmatched_rows),
        }


@dataclass(slots=True)
class AlertWindow:
    source_account_key: str
    source_bank: str
    source_account: str
    start_time: datetime
    end_time: datetime
    first_line_number: int
    transactions: list[dict[str, Any]] = field(default_factory=list)
    counterparties: dict[str, dict[str, Any]] = field(default_factory=dict)
    currencies: set[str] = field(default_factory=set)
    matched_typologies: Counter[str] = field(default_factory=Counter)
    evaluation_label_is_sar: int = 0
    total_amount: float = 0.0

    @property
    def expiry(self) -> datetime:
        return self.start_time + timedelta(hours=24)

    def append_transaction(self, tx: ParsedTransaction, tx_typology: str) -> None:
        sender = tx.source_account_key
        receiver = tx.destination_account_key
        tx_payload = {
            "transaction_id": f"ibm-hi-small-tx-{tx.line_number}",
            "amount": tx.normalized_amount,
            "timestamp": _isoformat_utc(tx.timestamp),
            "sender": sender,
            "receiver": receiver,
            "currency": tx.normalized_currency,
            "channel": tx.payment_format,
            "source_system": _DEFAULT_SOURCE_SYSTEM,
            "optional_fields": {
                "from_bank": tx.from_bank,
                "from_account": tx.from_account,
                "to_bank": tx.to_bank,
                "to_account": tx.to_account,
                "amount_received": tx.amount_received_raw,
                "receiving_currency": tx.receiving_currency,
                "amount_paid": tx.amount_paid_raw,
                "payment_currency": tx.payment_currency,
                "is_laundering": tx.is_laundering,
                "pattern_typology": tx_typology,
            },
        }
        self.transactions.append(tx_payload)
        self.end_time = max(self.end_time, tx.timestamp)
        self.total_amount += float(tx.normalized_amount)
        self.currencies.add(tx.normalized_currency)
        if tx.is_laundering == 1:
            self.evaluation_label_is_sar = 1
        if tx_typology != "unknown":
            self.matched_typologies[tx_typology] += 1

        if receiver not in self.counterparties:
            self.counterparties[receiver] = {
                "account_id": receiver,
                "optional_fields": {
                    "bank_id": tx.to_bank,
                    "account_number": tx.to_account,
                    "role": "counterparty",
                },
            }


@dataclass(slots=True)
class BenchmarkAlertSummary:
    alert_id: str
    created_at: datetime
    evaluation_label_is_sar: int
    typology: str
    source_account_key: str
    transaction_count: int
    total_amount: float
    line_number: int


@dataclass(slots=True)
class BenchmarkResult:
    conversion_summary: dict[str, Any] | None
    dataset_stats: dict[str, Any]
    split_stats: dict[str, Any]
    ranking_metrics: dict[str, Any]
    chronology_checks: dict[str, Any]
    althea_baseline_status: dict[str, Any]
    summary_path: Path
    report_path: Path


class _NoopExplainabilityService:
    def generate_explanation(self, **_: Any) -> dict[str, Any]:
        return {}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _normalize_header_token(token: str) -> str:
    return str(token or "").strip().lower().replace("-", "_").replace(" ", "_").replace("\ufeff", "")


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(raw: str) -> float:
    return float(str(raw or "").strip())


def _safe_int(raw: str) -> int:
    return int(str(raw or "").strip())


def _parse_timestamp(raw: str) -> datetime:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("timestamp is missing")
    try:
        parsed = datetime.strptime(value, "%Y/%m/%d %H:%M")
    except ValueError:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _build_signature(cells: list[str]) -> tuple[str, ...]:
    normalized = [str(item or "").strip() for item in cells[: len(_CSV_HEADER_NORMALIZED)]]
    if len(normalized) < len(_CSV_HEADER_NORMALIZED):
        normalized.extend([""] * (len(_CSV_HEADER_NORMALIZED) - len(normalized)))
    return tuple(normalized)


def _validate_csv_header(header: list[str]) -> None:
    normalized = [_normalize_header_token(item) for item in header[: len(_CSV_HEADER_NORMALIZED)]]
    if normalized != _CSV_HEADER_NORMALIZED:
        raise ValueError(
            "Unexpected IBM AML transaction header. "
            f"Expected {_CSV_HEADER_NORMALIZED!r}, got {normalized!r}"
        )


def _parse_transaction_cells(cells: list[str], line_number: int) -> ParsedTransaction:
    if len(cells) < len(_CSV_HEADER_NORMALIZED):
        raise ValueError(f"expected at least {len(_CSV_HEADER_NORMALIZED)} columns, got {len(cells)}")
    timestamp = _parse_timestamp(cells[0])
    from_bank = str(cells[1]).strip()
    from_account = str(cells[2]).strip()
    to_bank = str(cells[3]).strip()
    to_account = str(cells[4]).strip()
    if not from_bank or not from_account:
        raise ValueError("source bank/account is missing")
    if not to_bank or not to_account:
        raise ValueError("destination bank/account is missing")

    amount_received_raw = str(cells[5]).strip()
    receiving_currency = str(cells[6]).strip()
    amount_paid_raw = str(cells[7]).strip()
    payment_currency = str(cells[8]).strip()
    payment_format = str(cells[9]).strip()
    is_laundering = _safe_int(cells[10])
    if is_laundering not in {0, 1}:
        raise ValueError("Is Laundering must be 0 or 1")

    amount_received = _safe_float(amount_received_raw)
    amount_paid = _safe_float(amount_paid_raw)
    source_account_key = f"{from_bank}:{from_account}"
    destination_account_key = f"{to_bank}:{to_account}"

    return ParsedTransaction(
        line_number=line_number,
        timestamp=timestamp,
        from_bank=from_bank,
        from_account=from_account,
        to_bank=to_bank,
        to_account=to_account,
        amount_received_raw=amount_received_raw,
        receiving_currency=receiving_currency,
        amount_paid_raw=amount_paid_raw,
        payment_currency=payment_currency,
        payment_format=payment_format,
        is_laundering=is_laundering,
        amount_received=amount_received,
        amount_paid=amount_paid,
        source_account_key=source_account_key,
        destination_account_key=destination_account_key,
        signature=_build_signature(cells),
    )


def parse_pattern_file(patterns_path: str | Path) -> PatternIndex:
    path = Path(patterns_path)
    if not path.exists() or not path.is_file():
        raise ValueError(f"Pattern file does not exist: {path}")

    signature_to_typology: dict[tuple[str, ...], str] = {}
    ambiguous_signatures: set[tuple[str, ...]] = set()
    block_counts: Counter[str] = Counter()
    transaction_counts: Counter[str] = Counter()
    current_typology: str | None = None
    unmatched_rows = 0

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            begin_match = _PATTERN_BEGIN_RE.match(line)
            if begin_match:
                current_typology = begin_match.group("typology").strip().upper()
                block_counts[current_typology] += 1
                continue
            if _PATTERN_END_RE.match(line):
                current_typology = None
                continue
            if current_typology is None:
                unmatched_rows += 1
                continue

            try:
                parsed_cells = next(csv.reader([raw_line]))
                tx = _parse_transaction_cells(parsed_cells, line_number=line_number)
            except Exception:
                unmatched_rows += 1
                continue

            transaction_counts[current_typology] += 1
            previous = signature_to_typology.get(tx.signature)
            if previous is None:
                signature_to_typology[tx.signature] = current_typology
            elif previous != current_typology:
                ambiguous_signatures.add(tx.signature)

    for signature in ambiguous_signatures:
        signature_to_typology.pop(signature, None)

    return PatternIndex(
        signature_to_typology=signature_to_typology,
        ambiguous_signatures=ambiguous_signatures,
        block_counts=dict(block_counts),
        transaction_counts=dict(transaction_counts),
        unmatched_rows=unmatched_rows,
    )


def _alert_id_prefix_for_dataset(dataset_name: str) -> str:
    normalized = str(dataset_name or "").upper()
    if "LI-SMALL" in normalized:
        return "IBMLI"
    if "HI-SMALL" in normalized:
        return "IBMHI"
    return _DEFAULT_ALERT_ID_PREFIX


def _deterministic_alert_id(
    source_account_key: str,
    start_time: datetime,
    first_line_number: int,
    *,
    alert_id_prefix: str = _DEFAULT_ALERT_ID_PREFIX,
) -> str:
    payload = f"{source_account_key}|{_isoformat_utc(start_time)}|{first_line_number}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12].upper()
    return f"{alert_id_prefix}-{digest}"


def _build_accounts(window: AlertWindow) -> list[dict[str, Any]]:
    accounts = [
        {
            "account_id": window.source_account_key,
            "optional_fields": {
                "bank_id": window.source_bank,
                "account_number": window.source_account,
                "role": "source",
            },
        }
    ]
    for key in sorted(window.counterparties):
        if key == window.source_account_key:
            continue
        accounts.append(window.counterparties[key])
    return accounts


def _resolve_alert_typology(window: AlertWindow) -> tuple[str, str]:
    if not window.matched_typologies:
        return "unknown", "no_pattern_match"
    if len(window.matched_typologies) == 1:
        return next(iter(window.matched_typologies)), "single_pattern_match"
    return "unknown", "multiple_pattern_matches"


def _build_alert_payload(window: AlertWindow, dataset_name: str, window_hours: int) -> dict[str, Any]:
    typology, typology_assignment = _resolve_alert_typology(window)
    created_at = _isoformat_utc(window.start_time)
    alert_id = _deterministic_alert_id(
        window.source_account_key,
        window.start_time,
        window.first_line_number,
        alert_id_prefix=_alert_id_prefix_for_dataset(dataset_name),
    )
    ingestion_metadata = {
        "dataset_name": dataset_name,
        "converter_version": _DEFAULT_SCHEMA_VERSION,
        "grouping_rule": _DEFAULT_GROUPING_RULE,
        "window_hours": int(window_hours),
        "transaction_count": int(len(window.transactions)),
        "window_start": created_at,
        "window_end": _isoformat_utc(window.end_time),
        "matched_typologies": sorted(window.matched_typologies.keys()),
        "typology_assignment": typology_assignment,
        "currency_set": sorted(window.currencies),
        "has_mixed_currencies": len(window.currencies) > 1,
        "amount_proxy_total": float(window.total_amount),
        "proxy_label_definition": "1 if any transaction in the synthetic alert has Is Laundering = 1",
    }
    metadata = {
        "source_system": _DEFAULT_SOURCE_SYSTEM,
        "schema_version": _DEFAULT_SCHEMA_VERSION,
        "dataset_name": dataset_name,
        "label_kind": "synthetic_proxy",
        "grouping_rule": _DEFAULT_GROUPING_RULE,
    }
    return {
        "alert_id": alert_id,
        "created_at": created_at,
        "source_system": _DEFAULT_SOURCE_SYSTEM,
        "source_account_key": window.source_account_key,
        "typology": typology,
        "is_sar": int(window.evaluation_label_is_sar),
        "evaluation_label_is_sar": int(window.evaluation_label_is_sar),
        "accounts": _build_accounts(window),
        "transactions": window.transactions,
        "metadata": metadata,
        "ingestion_metadata": ingestion_metadata,
    }


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _display_path(raw_path: str | Path | None) -> str:
    if not raw_path:
        return "not_recorded"
    path = Path(raw_path)
    try:
        resolved = path.resolve()
    except Exception:
        return str(raw_path)
    repo_root = _repo_root()
    try:
        return str(resolved.relative_to(repo_root))
    except Exception:
        return resolved.name


def _stage_sqlite_path(target_path: Path) -> Path:
    return target_path.with_suffix(".stage.sqlite")


def _create_staging_db(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        """
        CREATE TABLE staged_transactions (
            line_number INTEGER NOT NULL,
            timestamp_iso TEXT NOT NULL,
            source_account_key TEXT NOT NULL,
            from_bank TEXT NOT NULL,
            from_account TEXT NOT NULL,
            to_bank TEXT NOT NULL,
            to_account TEXT NOT NULL,
            amount_received_raw TEXT NOT NULL,
            receiving_currency TEXT NOT NULL,
            amount_paid_raw TEXT NOT NULL,
            payment_currency TEXT NOT NULL,
            payment_format TEXT NOT NULL,
            is_laundering INTEGER NOT NULL,
            amount_received REAL NOT NULL,
            amount_paid REAL NOT NULL,
            transaction_typology TEXT NOT NULL
        )
        """
    )
    return connection


def _stage_insert_tuple(tx: ParsedTransaction, tx_typology: str) -> tuple[Any, ...]:
    return (
        tx.line_number,
        _isoformat_utc(tx.timestamp),
        tx.source_account_key,
        tx.from_bank,
        tx.from_account,
        tx.to_bank,
        tx.to_account,
        tx.amount_received_raw,
        tx.receiving_currency,
        tx.amount_paid_raw,
        tx.payment_currency,
        tx.payment_format,
        tx.is_laundering,
        tx.amount_received,
        tx.amount_paid,
        tx_typology,
    )


def _staged_row_to_transaction(row: sqlite3.Row) -> ParsedTransaction:
    timestamp = datetime.fromisoformat(str(row["timestamp_iso"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    return ParsedTransaction(
        line_number=int(row["line_number"]),
        timestamp=timestamp,
        from_bank=str(row["from_bank"]),
        from_account=str(row["from_account"]),
        to_bank=str(row["to_bank"]),
        to_account=str(row["to_account"]),
        amount_received_raw=str(row["amount_received_raw"]),
        receiving_currency=str(row["receiving_currency"]),
        amount_paid_raw=str(row["amount_paid_raw"]),
        payment_currency=str(row["payment_currency"]),
        payment_format=str(row["payment_format"]),
        is_laundering=int(row["is_laundering"]),
        amount_received=float(row["amount_received"]),
        amount_paid=float(row["amount_paid"]),
        source_account_key=str(row["source_account_key"]),
        destination_account_key=f"{row['to_bank']}:{row['to_account']}",
        signature=tuple(),
    )


def convert_transactions_to_alert_jsonl(
    transactions_path: str | Path,
    patterns_path: str | Path,
    output_path: str | Path,
    *,
    window_hours: int = 24,
    dataset_name: str = _DEFAULT_DATASET_NAME,
    write_summary_path: str | Path | None = None,
) -> dict[str, Any]:
    source_path = Path(transactions_path)
    pattern_path = Path(patterns_path)
    target_path = Path(output_path)
    if not source_path.exists() or not source_path.is_file():
        raise ValueError(f"Transactions CSV does not exist: {source_path}")
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")

    pattern_index = parse_pattern_file(pattern_path)
    _ensure_parent_dir(target_path)
    summary_path = Path(write_summary_path) if write_summary_path else target_path.with_suffix(".summary.json")
    _ensure_parent_dir(summary_path)
    stage_path = _stage_sqlite_path(target_path)

    conversion_started = datetime.now(timezone.utc)

    total_rows = 0
    valid_transactions = 0
    invalid_rows = 0
    input_file_out_of_order_rows = 0
    positive_transactions = 0
    source_accounts_seen: set[str] = set()
    error_examples: list[dict[str, Any]] = []
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    previous_timestamp: datetime | None = None
    alerts_written = 0
    positive_alerts = 0
    alerts_with_typology = 0
    alerts_with_mixed_currencies = 0
    staging_removed = False

    def record_error(line_number: int, message: str) -> None:
        nonlocal invalid_rows
        invalid_rows += 1
        if len(error_examples) < _MAX_ERROR_EXAMPLES:
            error_examples.append({"line_number": line_number, "message": message})

    def finalize_window(window: AlertWindow, writer_handle) -> None:
        nonlocal alerts_written, positive_alerts, alerts_with_typology, alerts_with_mixed_currencies
        payload = _build_alert_payload(window, dataset_name=dataset_name, window_hours=window_hours)
        writer_handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        alerts_written += 1
        positive_alerts += int(payload["evaluation_label_is_sar"])
        if payload["typology"] != "unknown":
            alerts_with_typology += 1
        if bool(payload["ingestion_metadata"].get("has_mixed_currencies")):
            alerts_with_mixed_currencies += 1

    staging_connection = _create_staging_db(stage_path)
    try:
        insert_buffer: list[tuple[Any, ...]] = []
        with source_path.open("r", encoding="utf-8-sig", newline="") as input_handle:
            reader = csv.reader(input_handle)
            header = next(reader, None)
            if header is None:
                raise ValueError("Transactions CSV is empty")
            _validate_csv_header(header)

            for line_number, cells in enumerate(reader, start=2):
                if not cells or not any(str(cell).strip() for cell in cells):
                    continue
                total_rows += 1
                try:
                    tx = _parse_transaction_cells(cells, line_number=line_number)
                except Exception as exc:
                    record_error(line_number, str(exc))
                    continue

                valid_transactions += 1
                positive_transactions += int(tx.is_laundering)
                source_accounts_seen.add(tx.source_account_key)
                if previous_timestamp is not None and tx.timestamp < previous_timestamp:
                    input_file_out_of_order_rows += 1
                previous_timestamp = tx.timestamp
                first_timestamp = tx.timestamp if first_timestamp is None else min(first_timestamp, tx.timestamp)
                last_timestamp = tx.timestamp if last_timestamp is None else max(last_timestamp, tx.timestamp)

                tx_typology = pattern_index.lookup(tx.signature)
                insert_buffer.append(_stage_insert_tuple(tx, tx_typology))
                if len(insert_buffer) >= 10000:
                    staging_connection.executemany(
                        """
                        INSERT INTO staged_transactions (
                            line_number, timestamp_iso, source_account_key, from_bank, from_account,
                            to_bank, to_account, amount_received_raw, receiving_currency,
                            amount_paid_raw, payment_currency, payment_format, is_laundering,
                            amount_received, amount_paid, transaction_typology
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        insert_buffer,
                    )
                    staging_connection.commit()
                    insert_buffer.clear()

        if insert_buffer:
            staging_connection.executemany(
                """
                INSERT INTO staged_transactions (
                    line_number, timestamp_iso, source_account_key, from_bank, from_account,
                    to_bank, to_account, amount_received_raw, receiving_currency,
                    amount_paid_raw, payment_currency, payment_format, is_laundering,
                    amount_received, amount_paid, transaction_typology
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_buffer,
            )
            staging_connection.commit()

        staging_connection.execute(
            "CREATE INDEX idx_staged_transactions_source_time ON staged_transactions (source_account_key, timestamp_iso, line_number)"
        )
        staging_connection.commit()
        staging_connection.row_factory = sqlite3.Row

        with target_path.open("w", encoding="utf-8", newline="") as output_handle:
            current_window: AlertWindow | None = None
            cursor = staging_connection.execute(
                """
                SELECT
                    line_number, timestamp_iso, source_account_key, from_bank, from_account,
                    to_bank, to_account, amount_received_raw, receiving_currency,
                    amount_paid_raw, payment_currency, payment_format, is_laundering,
                    amount_received, amount_paid, transaction_typology
                FROM staged_transactions
                ORDER BY source_account_key, timestamp_iso, line_number
                """
            )
            for row in cursor:
                tx = _staged_row_to_transaction(row)
                tx_typology = str(row["transaction_typology"] or "unknown")
                if current_window is None:
                    current_window = AlertWindow(
                        source_account_key=tx.source_account_key,
                        source_bank=tx.from_bank,
                        source_account=tx.from_account,
                        start_time=tx.timestamp,
                        end_time=tx.timestamp,
                        first_line_number=tx.line_number,
                    )
                elif tx.source_account_key != current_window.source_account_key or tx.timestamp >= current_window.expiry:
                    finalize_window(current_window, output_handle)
                    current_window = AlertWindow(
                        source_account_key=tx.source_account_key,
                        source_bank=tx.from_bank,
                        source_account=tx.from_account,
                        start_time=tx.timestamp,
                        end_time=tx.timestamp,
                        first_line_number=tx.line_number,
                    )

                current_window.append_transaction(tx, tx_typology=tx_typology)

            if current_window is not None:
                finalize_window(current_window, output_handle)
    finally:
        staging_connection.close()
        if stage_path.exists():
            stage_path.unlink()
            staging_removed = True

    summary = {
        "dataset_name": dataset_name,
        "source_transactions_path": str(source_path.resolve()),
        "source_patterns_path": str(pattern_path.resolve()),
        "output_path": str(target_path.resolve()),
        "summary_path": str(summary_path.resolve()),
        "staging_mode": "sqlite_source_account_timestamp_sort",
        "staging_database_removed": bool(staging_removed),
        "window_hours": int(window_hours),
        "grouping_rule": _DEFAULT_GROUPING_RULE,
        "converter_schema_version": _DEFAULT_SCHEMA_VERSION,
        "conversion_started_at": _isoformat_utc(conversion_started),
        "conversion_completed_at": _isoformat_utc(datetime.now(timezone.utc)),
        "total_csv_rows": int(total_rows),
        "valid_transactions": int(valid_transactions),
        "invalid_rows": int(invalid_rows),
        "positive_transactions": int(positive_transactions),
        "alerts_written": int(alerts_written),
        "positive_alerts": int(positive_alerts),
        "negative_alerts": int(max(alerts_written - positive_alerts, 0)),
        "alerts_with_typology_assigned": int(alerts_with_typology),
        "alerts_with_unknown_typology": int(max(alerts_written - alerts_with_typology, 0)),
        "alerts_with_mixed_currencies": int(alerts_with_mixed_currencies),
        "distinct_source_accounts": int(len(source_accounts_seen)),
        "input_file_out_of_order_rows": int(input_file_out_of_order_rows),
        "out_of_order_rows": int(input_file_out_of_order_rows),
        "date_range": {
            "min_created_at": _isoformat_utc(first_timestamp) if first_timestamp else None,
            "max_created_at": _isoformat_utc(last_timestamp) if last_timestamp else None,
        },
        "pattern_summary": pattern_index.to_summary(),
        "error_examples": error_examples,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    logger.info("IBM AML conversion completed: %s alerts written", alerts_written)
    return summary


def _coerce_label(payload: dict[str, Any]) -> int:
    candidate = payload.get("evaluation_label_is_sar", payload.get("is_sar", 0))
    try:
        return 1 if int(candidate) == 1 else 0
    except Exception:
        return 0


def _coerce_created_at(payload: dict[str, Any]) -> datetime:
    created_at = payload.get("created_at")
    if not created_at:
        raise ValueError("created_at is missing")
    return _parse_timestamp(str(created_at).replace("-", "/")) if "/" in str(created_at) else datetime.fromisoformat(
        str(created_at).replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def _coerce_total_amount(payload: dict[str, Any]) -> float:
    transactions = list(payload.get("transactions") or [])
    total = 0.0
    for tx in transactions:
        try:
            total += float(tx.get("amount", 0.0) or 0.0)
        except Exception:
            continue
    return float(total)


def load_alert_summaries(alert_jsonl_path: str | Path) -> list[BenchmarkAlertSummary]:
    path = Path(alert_jsonl_path)
    if not path.exists() or not path.is_file():
        raise ValueError(f"Alert JSONL does not exist: {path}")

    summaries: list[BenchmarkAlertSummary] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            alert_id = str(payload.get("alert_id") or "").strip()
            if not alert_id:
                continue
            created_at = datetime.fromisoformat(str(payload.get("created_at")).replace("Z", "+00:00")).astimezone(timezone.utc)
            summaries.append(
                BenchmarkAlertSummary(
                    alert_id=alert_id,
                    created_at=created_at,
                    evaluation_label_is_sar=_coerce_label(payload),
                    typology=str(payload.get("typology") or "unknown").strip() or "unknown",
                    source_account_key=str(payload.get("source_account_key") or "").strip(),
                    transaction_count=len(list(payload.get("transactions") or [])),
                    total_amount=_coerce_total_amount(payload),
                    line_number=line_number,
                )
            )
    summaries.sort(key=lambda item: (item.created_at, item.alert_id))
    return summaries


def _split_alerts(alerts: list[BenchmarkAlertSummary]) -> dict[str, list[BenchmarkAlertSummary]]:
    total = len(alerts)
    if total == 0:
        return {"train": [], "validation": [], "test": []}
    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)
    train_end = max(1, train_end) if total >= 3 else min(total, train_end)
    validation_end = max(train_end + 1, validation_end) if total >= 5 else max(train_end, validation_end)
    validation_end = min(validation_end, total)
    return {
        "train": alerts[:train_end],
        "validation": alerts[train_end:validation_end],
        "test": alerts[validation_end:],
    }


def _dataset_stats(alerts: list[BenchmarkAlertSummary]) -> dict[str, Any]:
    total = len(alerts)
    positives = sum(item.evaluation_label_is_sar for item in alerts)
    typology_assigned = sum(1 for item in alerts if item.typology != "unknown")
    avg_transactions = float(sum(item.transaction_count for item in alerts) / total) if total else 0.0
    return {
        "total_alerts": int(total),
        "positive_alerts": int(positives),
        "negative_alerts": int(max(total - positives, 0)),
        "positive_rate": float(positives / total) if total else 0.0,
        "average_transactions_per_alert": avg_transactions,
        "alerts_with_typology_assigned": int(typology_assigned),
        "typology_assignment_rate": float(typology_assigned / total) if total else 0.0,
    }


def _chronology_checks(alerts: list[BenchmarkAlertSummary], conversion_summary: dict[str, Any] | None) -> dict[str, Any]:
    if not alerts:
        return {
            "status": "empty",
        }
    timestamps = [item.created_at for item in alerts]
    min_ts = timestamps[0]
    max_ts = timestamps[-1]
    unique_days = sorted({item.created_at.date().isoformat() for item in alerts})
    duplicate_timestamps = len(timestamps) - len({item.isoformat() for item in timestamps})
    span_days = (max_ts - min_ts).total_seconds() / 86400.0 if len(timestamps) >= 2 else 0.0
    warnings: list[str] = []
    if span_days < 7:
        warnings.append(
            "Observed alert horizon is shorter than seven days; chronological splits cover a compressed synthetic time span."
        )
    if len(unique_days) < 5:
        warnings.append("Only a small number of unique calendar days are present in the benchmark data.")
    if conversion_summary and int(conversion_summary.get("out_of_order_rows", 0)) > 0:
        warnings.append(
            f"Converter observed {int(conversion_summary.get('out_of_order_rows', 0))} out-of-order transaction rows."
        )
    return {
        "min_created_at": _isoformat_utc(min_ts),
        "max_created_at": _isoformat_utc(max_ts),
        "timespan_days": float(span_days),
        "unique_calendar_days": unique_days,
        "duplicate_created_at_count": int(duplicate_timestamps),
        "warnings": warnings,
    }


def _top_k_count(total: int, fraction: float) -> int:
    if total <= 0:
        return 0
    return max(1, math.ceil(total * fraction))


def _review_reduction_at_recall(sorted_items: list[BenchmarkAlertSummary], recall_target: float) -> dict[str, Any] | None:
    positives = sum(item.evaluation_label_is_sar for item in sorted_items)
    if positives <= 0:
        return None
    captured = 0
    for index, item in enumerate(sorted_items, start=1):
        captured += item.evaluation_label_is_sar
        if captured / positives >= recall_target:
            review_fraction = index / len(sorted_items)
            return {
                "target_recall": float(recall_target),
                "review_fraction": float(review_fraction),
                "review_reduction": float(max(0.0, 1.0 - review_fraction)),
                "alerts_reviewed": int(index),
            }
    return {
        "target_recall": float(recall_target),
        "review_fraction": 1.0,
        "review_reduction": 0.0,
        "alerts_reviewed": int(len(sorted_items)),
    }


def _ranking_metrics(sorted_items: list[BenchmarkAlertSummary]) -> dict[str, Any]:
    total = len(sorted_items)
    positives = sum(item.evaluation_label_is_sar for item in sorted_items)
    metrics = {
        "n_alerts": int(total),
        "n_positive_alerts": int(positives),
    }
    if total == 0 or positives == 0:
        metrics["recall_at_top_10pct"] = 0.0
        metrics["recall_at_top_20pct"] = 0.0
        metrics["precision_at_top_10pct"] = 0.0
        metrics["precision_at_top_20pct"] = 0.0
        metrics["review_reduction_at_80pct_recall"] = None
        return metrics

    for fraction in _DEFAULT_TOP_FRACTIONS:
        top_n = _top_k_count(total, fraction)
        reviewed = sorted_items[:top_n]
        captured = sum(item.evaluation_label_is_sar for item in reviewed)
        suffix = f"top_{int(fraction * 100)}pct"
        metrics[f"recall_at_{suffix}"] = float(captured / positives)
        metrics[f"precision_at_{suffix}"] = float(captured / top_n)

    metrics["review_reduction_at_80pct_recall"] = _review_reduction_at_recall(
        sorted_items, recall_target=_DEFAULT_REPORT_RECALL_TARGET
    )
    return metrics


def _baseline_rankings(split_alerts: list[BenchmarkAlertSummary]) -> dict[str, list[BenchmarkAlertSummary]]:
    chronological = sorted(split_alerts, key=lambda item: (item.created_at, item.alert_id))
    amount = sorted(split_alerts, key=lambda item: (-item.total_amount, item.created_at, item.alert_id))
    return {
        "chronological_queue": chronological,
        "amount_descending": amount,
    }


def _score_alerts_with_althea(
    alert_jsonl_path: str | Path,
    *,
    selected_ids: set[str],
    database_url: str,
    object_storage_root: str | Path,
    tenant_id: str,
    model_selection_strategy: str = "active_approved",
    batch_size: int = 5000,
) -> dict[str, Any]:
    if not selected_ids:
        return {"status": "empty", "scores": {}}

    repository = EnterpriseRepository(database_url)
    registry = ModelRegistry(repository=repository, object_storage=ObjectStorage(Path(object_storage_root)))
    model_record = registry.resolve_model(tenant_id=tenant_id, strategy=model_selection_strategy)
    if not model_record:
        return {"status": "unavailable", "reason": f"no_model_for_tenant:{tenant_id}"}

    schema_validator = FeatureSchemaValidator()
    inference_service = InferenceService(
        registry=registry,
        schema_validator=schema_validator,
        explainability_service=_NoopExplainabilityService(),
        allow_dev_models=False,
    )
    adapter = AlertFeatureAdapter()
    feature_service = EnterpriseFeatureService(schema_validator)

    scores: dict[str, float] = {}
    chunk: list[dict[str, Any]] = []

    def score_chunk(payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        alerts_df = adapter.alerts_to_dataframe(payloads)
        bundle = feature_service.generate_inference_features(alerts_df)
        inference = inference_service.predict(
            tenant_id=tenant_id,
            feature_frame=bundle["feature_matrix"],
            strategy=model_selection_strategy,
        )
        for alert_id, score in zip(alerts_df["alert_id"].tolist(), inference.get("scores", []), strict=False):
            scores[str(alert_id)] = float(score)

    with Path(alert_jsonl_path).open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            alert_id = str(payload.get("alert_id") or "").strip()
            if alert_id not in selected_ids:
                continue
            chunk.append(payload)
            if len(chunk) >= max(1, int(batch_size)):
                score_chunk(chunk)
                chunk.clear()

    if chunk:
        score_chunk(chunk)

    return {
        "status": "available",
        "tenant_id": tenant_id,
        "model_version": str(model_record.get("model_version") or "unknown"),
        "scores": scores,
    }


def _render_report(
    *,
    conversion_summary: dict[str, Any] | None,
    dataset_stats: dict[str, Any],
    split_stats: dict[str, Any],
    chronology_checks: dict[str, Any],
    ranking_metrics: dict[str, Any],
    benchmark_path: Path,
) -> str:
    lines = [
        "# ALTHEA Benchmark v1",
        "",
        "This report summarizes a first-pass synthetic benchmark built from IBM AML-Data `HI-Small`.",
        "",
        "Important scope note: this is synthetic benchmark validation only. It is not live bank validation, not production ROI proof, and not evidence of deployment readiness on real customer data.",
        "",
        "## Dataset",
        "",
        f"- Dataset: `{(conversion_summary or {}).get('dataset_name', _DEFAULT_DATASET_NAME)}`",
        f"- Transactions CSV: `{_display_path((conversion_summary or {}).get('source_transactions_path'))}`",
        f"- Patterns file: `{_display_path((conversion_summary or {}).get('source_patterns_path'))}`",
        f"- Alert JSONL: `{_display_path(benchmark_path)}`",
        "",
        "## Alert Construction",
        "",
        "- Grouping rule: one synthetic alert = all transactions from the same source account within an anchored 24-hour window.",
        "- Label rule: `evaluation_label_is_sar = 1` if any transaction in the alert has `Is Laundering = 1`; otherwise `0`.",
        "- Label semantics: this is a synthetic proxy label derived from IBM AML-Data transactions, not a real SAR outcome.",
        "- Typology enrichment: pattern-file transaction matches are lifted to alert level only when a single typology maps cleanly to the alert; otherwise typology is `unknown`.",
        "",
        "## Chronology",
        "",
        f"- Min alert timestamp: `{chronology_checks.get('min_created_at')}`",
        f"- Max alert timestamp: `{chronology_checks.get('max_created_at')}`",
        f"- Observed span: `{chronology_checks.get('timespan_days')}` days",
        f"- Unique calendar days: `{len(chronology_checks.get('unique_calendar_days', []))}`",
        "",
        "## Dataset Stats",
        "",
        f"- Total alerts: `{dataset_stats.get('total_alerts')}`",
        f"- Positive alerts: `{dataset_stats.get('positive_alerts')}`",
        f"- Negative alerts: `{dataset_stats.get('negative_alerts')}`",
        f"- Average transactions per alert: `{dataset_stats.get('average_transactions_per_alert'):.2f}`",
        f"- Alerts with typology assigned: `{dataset_stats.get('alerts_with_typology_assigned')}`",
        "",
        "## Split Logic",
        "",
        "- Chronological split by alert `created_at` after sorting ascending.",
        "- Train = first 60%, validation = next 20%, test = final 20%.",
        "",
        f"- Train alerts: `{split_stats['train']['total_alerts']}`",
        f"- Validation alerts: `{split_stats['validation']['total_alerts']}`",
        f"- Test alerts: `{split_stats['test']['total_alerts']}`",
        "",
        "## Baselines",
        "",
        "- Baseline A: chronological queue (`created_at` ascending).",
        "- Baseline B: simple amount heuristic (`total transaction amount` descending).",
    ]

    if ranking_metrics.get("althea_score"):
        status = ranking_metrics["althea_score"].get("status")
        if status == "available":
            lines.append(
                f"- Baseline C: ALTHEA score-based ranking using local model `{ranking_metrics['althea_score'].get('model_version', 'unknown')}`."
            )
        else:
            lines.append(f"- Baseline C: unavailable (`{ranking_metrics['althea_score'].get('reason', status)}`).")

    lines.extend(
        [
            "",
            "## Validation Metrics",
            "",
            "| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for baseline_name, metrics in ranking_metrics["validation"].items():
        review_reduction = metrics.get("review_reduction_at_80pct_recall") or {}
        lines.append(
            "| "
            + f"{baseline_name} | "
            + f"{metrics.get('recall_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('recall_at_top_20pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_20pct', 0.0):.4f} | "
            + (f"{review_reduction.get('review_reduction', 0.0):.4f}" if review_reduction else "n/a")
            + " |"
        )

    lines.extend(
        [
            "",
            "## Test Metrics",
            "",
            "| Baseline | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for baseline_name, metrics in ranking_metrics["test"].items():
        review_reduction = metrics.get("review_reduction_at_80pct_recall") or {}
        lines.append(
            "| "
            + f"{baseline_name} | "
            + f"{metrics.get('recall_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('recall_at_top_20pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_10pct', 0.0):.4f} | "
            + f"{metrics.get('precision_at_top_20pct', 0.0):.4f} | "
            + (f"{review_reduction.get('review_reduction', 0.0):.4f}" if review_reduction else "n/a")
            + " |"
        )

    lines.extend(
        [
            "",
            "## Known Limitations",
            "",
            "- The benchmark label is a transaction-derived proxy, not a true case or SAR disposition.",
            "- Alert grouping is synthetic and anchored to 24-hour source-account windows; it does not replicate a bank production alerting rule stack.",
            "- Amount-based comparisons are currency-naive in v1 because IBM AML-Data does not provide a stable FX-normalized benchmark amount.",
            "- The ALTHEA score baseline uses the currently registered local model as-is; it was not retrained or recalibrated on IBM-derived alert labels for this benchmark.",
            "- Typology enrichment depends on exact transaction matches against the pattern file, so some laundering alerts remain `unknown` at alert level.",
            "- The observed chronology spans a short synthetic period, so seasonal and operational drift are underrepresented.",
        ]
    )

    chronology_warnings = chronology_checks.get("warnings") or []
    if chronology_warnings:
        lines.extend(["", "## Chronology Warnings", ""])
        for warning in chronology_warnings:
            lines.append(f"- {warning}")

    return "\n".join(lines) + "\n"


def run_benchmark(
    alert_jsonl_path: str | Path,
    *,
    conversion_summary_path: str | Path | None = None,
    report_path: str | Path,
    summary_path: str | Path,
    database_url: str | None = None,
    object_storage_root: str | Path | None = None,
    tenant_id: str = "default-bank",
    include_althea_baseline: bool = True,
    model_selection_strategy: str = "active_approved",
) -> BenchmarkResult:
    alerts = load_alert_summaries(alert_jsonl_path)
    conversion_summary = None
    if conversion_summary_path:
        summary_source = Path(conversion_summary_path)
        if summary_source.exists():
            conversion_summary = json.loads(summary_source.read_text(encoding="utf-8"))

    dataset_stats = _dataset_stats(alerts)
    split_alerts = _split_alerts(alerts)
    split_stats = {name: _dataset_stats(items) for name, items in split_alerts.items()}
    chronology_checks = _chronology_checks(alerts, conversion_summary)

    ranking_metrics: dict[str, Any] = {"validation": {}, "test": {}}
    for split_name in ("validation", "test"):
        baselines = _baseline_rankings(split_alerts[split_name])
        for baseline_name, ranked in baselines.items():
            ranking_metrics[split_name][baseline_name] = _ranking_metrics(ranked)

    althea_status: dict[str, Any] = {"status": "skipped", "reason": "disabled"}
    if include_althea_baseline:
        resolved_database_url = database_url or f"sqlite:///{(_repo_root() / 'data' / 'althea_enterprise.db').as_posix()}"
        resolved_object_storage = Path(object_storage_root or (_repo_root() / "data" / "object_storage"))
        try:
            test_ids = {item.alert_id for item in split_alerts["test"]}
            validation_ids = {item.alert_id for item in split_alerts["validation"]}
            selected_ids = test_ids | validation_ids
            althea_status = _score_alerts_with_althea(
                alert_jsonl_path,
                selected_ids=selected_ids,
                database_url=resolved_database_url,
                object_storage_root=resolved_object_storage,
                tenant_id=tenant_id,
                model_selection_strategy=model_selection_strategy,
            )
            if althea_status.get("status") == "available":
                scores = dict(althea_status.get("scores") or {})
                for split_name in ("validation", "test"):
                    ranked = sorted(
                        split_alerts[split_name],
                        key=lambda item: (-float(scores.get(item.alert_id, float("-inf"))), item.created_at, item.alert_id),
                    )
                    ranking_metrics[split_name]["althea_score"] = _ranking_metrics(ranked)
                althea_status = {
                    "status": "available",
                    "tenant_id": althea_status.get("tenant_id"),
                    "model_version": althea_status.get("model_version"),
                    "scored_alert_count": int(len(scores)),
                }
                ranking_metrics["althea_score"] = dict(althea_status)
            else:
                ranking_metrics["althea_score"] = dict(althea_status)
        except Exception as exc:
            althea_status = {"status": "unavailable", "reason": str(exc)}
            ranking_metrics["althea_score"] = dict(althea_status)
    else:
        ranking_metrics["althea_score"] = dict(althea_status)

    summary_target = Path(summary_path)
    report_target = Path(report_path)
    _ensure_parent_dir(summary_target)
    _ensure_parent_dir(report_target)
    benchmark_summary = {
        "dataset_stats": dataset_stats,
        "split_stats": split_stats,
        "chronology_checks": chronology_checks,
        "ranking_metrics": ranking_metrics,
        "althea_baseline_status": althea_status,
        "conversion_summary_path": str(Path(conversion_summary_path).resolve()) if conversion_summary_path else None,
        "benchmark_generated_at": _isoformat_utc(datetime.now(timezone.utc)),
        "alert_jsonl_path": str(Path(alert_jsonl_path).resolve()),
    }
    summary_target.write_text(json.dumps(benchmark_summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report = _render_report(
        conversion_summary=conversion_summary,
        dataset_stats=dataset_stats,
        split_stats=split_stats,
        chronology_checks=chronology_checks,
        ranking_metrics=ranking_metrics,
        benchmark_path=Path(alert_jsonl_path).resolve(),
    )
    report_target.write_text(report, encoding="utf-8")

    return BenchmarkResult(
        conversion_summary=conversion_summary,
        dataset_stats=dataset_stats,
        split_stats=split_stats,
        ranking_metrics=ranking_metrics,
        chronology_checks=chronology_checks,
        althea_baseline_status=althea_status,
        summary_path=summary_target,
        report_path=report_target,
    )
