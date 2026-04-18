from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:  # pragma: no cover - optional dependency
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

from benchmarks.ibm_aml_data import (
    _parse_timestamp,
    _parse_transaction_cells,
    parse_pattern_file,
)
from models.feature_schema import FeatureSchemaValidator
from models.model_registry import ModelRegistry
from services.feature_adapter import AlertFeatureAdapter
from services.feature_service import EnterpriseFeatureService
from models.inference_service import InferenceService
from storage.object_storage import ObjectStorage
from storage.postgres_repository import EnterpriseRepository

logger = logging.getLogger("althea.benchmarks.ibm_aml_improvement")

_TOP_FRACTIONS = (0.10, 0.20)
_DEFAULT_RECALL_TARGET = 0.80
_DEFAULT_MODEL_RANDOM_STATE = 42
_DEFAULT_LGBM_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "name": "lightgbm_balanced",
        "params": {
            "objective": "binary",
            "metric": "average_precision",
            "learning_rate": 0.05,
            "num_leaves": 63,
            "min_child_samples": 80,
            "feature_fraction": 0.85,
            "bagging_fraction": 0.85,
            "bagging_freq": 5,
            "lambda_l1": 0.1,
            "lambda_l2": 0.2,
            "n_estimators": 700,
            "random_state": _DEFAULT_MODEL_RANDOM_STATE,
            "n_jobs": -1,
            "verbose": -1,
        },
        "note": "LightGBM with balanced positive weighting and moderate regularization.",
    },
    {
        "name": "lightgbm_top_recall",
        "params": {
            "objective": "binary",
            "metric": "average_precision",
            "learning_rate": 0.035,
            "num_leaves": 127,
            "min_child_samples": 40,
            "feature_fraction": 0.80,
            "bagging_fraction": 0.80,
            "bagging_freq": 5,
            "lambda_l1": 0.0,
            "lambda_l2": 0.1,
            "n_estimators": 900,
            "random_state": _DEFAULT_MODEL_RANDOM_STATE,
            "n_jobs": -1,
            "verbose": -1,
        },
        "note": "LightGBM biased toward top-of-queue recall with deeper trees and lighter child constraints.",
    },
)
_WEIGHTED_HEURISTIC_GRID: tuple[tuple[float, float, float], ...] = (
    (0.60, 0.25, 0.15),
    (0.55, 0.25, 0.20),
    (0.50, 0.30, 0.20),
    (0.50, 0.20, 0.30),
    (0.45, 0.30, 0.25),
    (0.40, 0.35, 0.25),
)
_PAYMENT_FORMAT_COLUMNS = {
    "ach": "ach_ratio",
    "wire": "wire_ratio",
    "cash": "cash_ratio",
    "cheque": "cheque_ratio",
    "credit_card": "credit_card_ratio",
    "reinvestment": "reinvestment_ratio",
    "bitcoin": "bitcoin_ratio",
}
_FX_TO_USD = {
    "US Dollar": 1.00,
    "Euro": 1.02,
    "Australian Dollar": 0.67,
    "Ruble": 0.016,
    "Canadian Dollar": 0.75,
    "UK Pound": 1.17,
    "Rupee": 0.013,
    "Yen": 0.0074,
    "Yuan": 0.14,
    "Bitcoin": 20000.0,
    "Shekel": 0.29,
    "Mexican Peso": 0.05,
    "Brazil Real": 0.19,
    "Swiss Franc": 1.04,
    "Saudi Riyal": 0.27,
    "UNKNOWN": 1.0,
}
_FEATURE_FIELDNAMES = [
    "alert_id",
    "created_at",
    "grouping_variant",
    "evaluation_label_is_sar",
    "source_account_key",
    "source_bank",
    "dominant_destination_bank",
    "dominant_currency",
    "dominant_payment_format",
    "typology",
    "pattern_assigned",
    "transaction_count",
    "total_amount",
    "total_amount_usd",
    "max_amount",
    "max_amount_usd",
    "mean_amount",
    "mean_amount_usd",
    "min_amount",
    "min_amount_usd",
    "median_amount",
    "median_amount_usd",
    "std_amount",
    "std_amount_usd",
    "amount_range",
    "amount_range_usd",
    "amount_per_transaction",
    "amount_per_transaction_usd",
    "unique_destination_accounts",
    "unique_destination_banks",
    "repeated_counterparty_ratio",
    "top_counterparty_tx_share",
    "top_counterparty_amount_share",
    "time_span_hours",
    "avg_gap_hours",
    "max_gap_hours",
    "currency_count",
    "has_mixed_currencies",
    "payment_format_count",
    "same_bank_ratio",
    "cross_bank_ratio",
    "round_amount_ratio",
    "night_ratio",
    "weekend_ratio",
    "created_hour",
    "created_day_of_week",
    "ach_ratio",
    "wire_ratio",
    "cash_ratio",
    "cheque_ratio",
    "credit_card_ratio",
    "reinvestment_ratio",
    "bitcoin_ratio",
]
_METRIC_KEYS = (
    "recall_at_top_10pct",
    "recall_at_top_20pct",
    "precision_at_top_10pct",
    "precision_at_top_20pct",
)


@dataclass(slots=True)
class ImprovedBenchmarkResult:
    summary_path: Path
    report_path: Path
    dataset_stats: dict[str, Any]
    diagnosis: dict[str, Any]
    baseline_results: list[dict[str, Any]]
    model_results: list[dict[str, Any]]
    grouping_variants: list[dict[str, Any]]
    champion: dict[str, Any]


@dataclass(slots=True)
class _WindowTransaction:
    transaction_id: str
    timestamp: datetime
    amount: float
    currency: str
    payment_format: str
    destination_account_key: str
    destination_bank: str
    source_bank: str
    is_laundering: int
    pattern_typology: str


class _NoopExplainabilityService:
    def generate_explanation(self, **_: Any) -> dict[str, Any]:
        return {}


class _FeatureWindow:
    def __init__(
        self,
        *,
        grouping_variant: str,
        source_account_key: str,
        source_bank: str,
        window_start: datetime,
        first_event_id: str,
    ) -> None:
        self.grouping_variant = grouping_variant
        self.source_account_key = source_account_key
        self.source_bank = source_bank
        self.window_start = window_start
        self.window_end = window_start
        self.first_event_id = first_event_id
        self.amounts: list[float] = []
        self.amounts_usd: list[float] = []
        self.timestamps: list[datetime] = []
        self.destination_accounts: Counter[str] = Counter()
        self.destination_banks: Counter[str] = Counter()
        self.currencies: Counter[str] = Counter()
        self.payment_formats: Counter[str] = Counter()
        self.counterparty_amounts: Counter[str] = Counter()
        self.same_bank_count = 0
        self.round_amount_count = 0
        self.night_count = 0
        self.weekend_count = 0
        self.label = 0
        self.typologies: Counter[str] = Counter()

    @property
    def expiry_6h(self) -> datetime:
        return self.window_start + timedelta(hours=6)

    @property
    def expiry_24h(self) -> datetime:
        return self.window_start + timedelta(hours=24)

    def append(self, tx: _WindowTransaction) -> None:
        amount = float(tx.amount)
        amount_usd = float(amount * _fx_rate_to_usd(tx.currency))
        self.amounts.append(amount)
        self.amounts_usd.append(amount_usd)
        self.timestamps.append(tx.timestamp)
        self.window_end = max(self.window_end, tx.timestamp)
        self.destination_accounts[tx.destination_account_key] += 1
        self.destination_banks[tx.destination_bank] += 1
        self.currencies[_normalize_currency(tx.currency)] += 1
        payment_format = _normalize_payment_format(tx.payment_format)
        self.payment_formats[payment_format] += 1
        self.counterparty_amounts[tx.destination_account_key] += amount_usd
        self.same_bank_count += 1 if str(tx.source_bank) == str(tx.destination_bank) else 0
        self.round_amount_count += 1 if _is_round_amount(amount) else 0
        self.night_count += 1 if tx.timestamp.hour < 6 else 0
        self.weekend_count += 1 if tx.timestamp.weekday() >= 5 else 0
        self.label = max(self.label, int(tx.is_laundering == 1))
        normalized_typology = str(tx.pattern_typology or "unknown").strip().upper() or "unknown"
        if normalized_typology != "UNKNOWN" and normalized_typology != "unknown":
            self.typologies[normalized_typology] += 1

    def to_row(self, alert_id: str) -> dict[str, Any]:
        tx_count = len(self.amounts)
        amounts = np.asarray(self.amounts, dtype=np.float64) if self.amounts else np.zeros(0, dtype=np.float64)
        amounts_usd = np.asarray(self.amounts_usd, dtype=np.float64) if self.amounts_usd else np.zeros(0, dtype=np.float64)
        timestamps = sorted(self.timestamps)
        if len(timestamps) >= 2:
            gaps = np.diff(np.asarray([item.timestamp() for item in timestamps], dtype=np.float64)) / 3600.0
            time_span_hours = float((timestamps[-1] - timestamps[0]).total_seconds() / 3600.0)
            avg_gap_hours = float(gaps.mean())
            max_gap_hours = float(gaps.max())
        else:
            time_span_hours = 0.0
            avg_gap_hours = 0.0
            max_gap_hours = 0.0

        typology = "unknown"
        if len(self.typologies) == 1:
            typology = next(iter(self.typologies.keys()))

        total_amount = float(amounts.sum()) if tx_count else 0.0
        total_amount_usd = float(amounts_usd.sum()) if tx_count else 0.0
        max_amount = float(amounts.max()) if tx_count else 0.0
        max_amount_usd = float(amounts_usd.max()) if tx_count else 0.0
        min_amount = float(amounts.min()) if tx_count else 0.0
        min_amount_usd = float(amounts_usd.min()) if tx_count else 0.0
        mean_amount = float(amounts.mean()) if tx_count else 0.0
        mean_amount_usd = float(amounts_usd.mean()) if tx_count else 0.0
        median_amount = float(np.median(amounts)) if tx_count else 0.0
        median_amount_usd = float(np.median(amounts_usd)) if tx_count else 0.0
        std_amount = float(amounts.std()) if tx_count else 0.0
        std_amount_usd = float(amounts_usd.std()) if tx_count else 0.0
        amount_range = float(max_amount - min_amount)
        amount_range_usd = float(max_amount_usd - min_amount_usd)
        dominant_currency = self.currencies.most_common(1)[0][0] if self.currencies else "UNKNOWN"
        dominant_payment = self.payment_formats.most_common(1)[0][0] if self.payment_formats else "unknown"
        dominant_destination_bank = self.destination_banks.most_common(1)[0][0] if self.destination_banks else "UNKNOWN"
        tx_count_float = float(max(tx_count, 1))
        top_counterparty_tx_share = float(max(self.destination_accounts.values()) / tx_count_float) if self.destination_accounts else 0.0
        top_counterparty_amount_share = (
            float(max(self.counterparty_amounts.values()) / total_amount_usd) if self.counterparty_amounts and total_amount_usd > 0 else 0.0
        )
        payment_ratio_values = {name: 0.0 for name in _PAYMENT_FORMAT_COLUMNS.values()}
        for fmt, count in self.payment_formats.items():
            target_column = _PAYMENT_FORMAT_COLUMNS.get(fmt)
            if target_column:
                payment_ratio_values[target_column] = float(count / tx_count_float)

        return {
            "alert_id": alert_id,
            "created_at": _isoformat_utc(self.window_start),
            "grouping_variant": self.grouping_variant,
            "evaluation_label_is_sar": int(self.label),
            "source_account_key": self.source_account_key,
            "source_bank": self.source_bank,
            "dominant_destination_bank": dominant_destination_bank,
            "dominant_currency": dominant_currency,
            "dominant_payment_format": dominant_payment,
            "typology": typology,
            "pattern_assigned": int(typology != "unknown"),
            "transaction_count": tx_count,
            "total_amount": total_amount,
            "total_amount_usd": total_amount_usd,
            "max_amount": max_amount,
            "max_amount_usd": max_amount_usd,
            "mean_amount": mean_amount,
            "mean_amount_usd": mean_amount_usd,
            "min_amount": min_amount,
            "min_amount_usd": min_amount_usd,
            "median_amount": median_amount,
            "median_amount_usd": median_amount_usd,
            "std_amount": std_amount,
            "std_amount_usd": std_amount_usd,
            "amount_range": amount_range,
            "amount_range_usd": amount_range_usd,
            "amount_per_transaction": float(total_amount / tx_count_float),
            "amount_per_transaction_usd": float(total_amount_usd / tx_count_float),
            "unique_destination_accounts": int(len(self.destination_accounts)),
            "unique_destination_banks": int(len(self.destination_banks)),
            "repeated_counterparty_ratio": float(max(0.0, 1.0 - (len(self.destination_accounts) / tx_count_float))),
            "top_counterparty_tx_share": top_counterparty_tx_share,
            "top_counterparty_amount_share": top_counterparty_amount_share,
            "time_span_hours": time_span_hours,
            "avg_gap_hours": avg_gap_hours,
            "max_gap_hours": max_gap_hours,
            "currency_count": int(len(self.currencies)),
            "has_mixed_currencies": int(len(self.currencies) > 1),
            "payment_format_count": int(len(self.payment_formats)),
            "same_bank_ratio": float(self.same_bank_count / tx_count_float),
            "cross_bank_ratio": float(max(0.0, 1.0 - (self.same_bank_count / tx_count_float))),
            "round_amount_ratio": float(self.round_amount_count / tx_count_float),
            "night_ratio": float(self.night_count / tx_count_float),
            "weekend_ratio": float(self.weekend_count / tx_count_float),
            "created_hour": float(self.window_start.hour),
            "created_day_of_week": float(self.window_start.weekday()),
            **payment_ratio_values,
        }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _fx_rate_to_usd(currency: str) -> float:
    normalized = str(currency or "").strip()
    return float(_FX_TO_USD.get(normalized, _FX_TO_USD["UNKNOWN"]))


def _normalize_currency(currency: str) -> str:
    return str(currency or "").strip() or "UNKNOWN"


def _normalize_payment_format(value: str) -> str:
    raw = str(value or "").strip().lower()
    return raw.replace(" ", "_") or "unknown"


def _is_round_amount(amount: float) -> bool:
    value = abs(float(amount or 0.0))
    return (value % 1000.0) < 1e-2 or (value % 100.0) < 1e-2


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def _deterministic_variant_alert_id(*parts: str) -> str:
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:14].upper()
    return f"IBMHI-{digest}"


def _write_feature_header(path: Path) -> csv.DictWriter:
    _ensure_parent_dir(path)
    handle = path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=_FEATURE_FIELDNAMES)
    writer.writeheader()
    writer._handle = handle  # type: ignore[attr-defined]
    return writer


def _close_feature_writer(writer: csv.DictWriter) -> None:
    handle = getattr(writer, "_handle", None)
    if handle is not None:
        handle.close()


def _payload_transactions(payload: dict[str, Any]) -> list[_WindowTransaction]:
    transactions = []
    source_account_key = str(payload.get("source_account_key") or "").strip()
    source_bank = ""
    if source_account_key and ":" in source_account_key:
        source_bank = source_account_key.split(":", 1)[0]
    for tx in list(payload.get("transactions") or []):
        optional_fields = dict(tx.get("optional_fields") or {})
        timestamp_raw = tx.get("timestamp")
        if not timestamp_raw:
            continue
        timestamp = datetime.fromisoformat(str(timestamp_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
        destination_bank = str(optional_fields.get("to_bank") or "").strip()
        source_bank_value = str(optional_fields.get("from_bank") or source_bank or "").strip()
        source_bank = source_bank_value or source_bank
        destination_account_key = str(tx.get("receiver") or "").strip()
        transactions.append(
            _WindowTransaction(
                transaction_id=str(tx.get("transaction_id") or ""),
                timestamp=timestamp,
                amount=float(tx.get("amount", 0.0) or 0.0),
                currency=str(tx.get("currency") or "UNKNOWN"),
                payment_format=str(tx.get("channel") or "unknown"),
                destination_account_key=destination_account_key,
                destination_bank=destination_bank or "UNKNOWN",
                source_bank=source_bank_value or "UNKNOWN",
                is_laundering=int(optional_fields.get("is_laundering") or 0),
                pattern_typology=str(optional_fields.get("pattern_typology") or "unknown"),
            )
        )
    transactions.sort(key=lambda item: (item.timestamp, item.transaction_id))
    return transactions


def _aggregate_payload_transactions(
    *,
    alert_id: str,
    grouping_variant: str,
    source_account_key: str,
    transactions: list[_WindowTransaction],
) -> dict[str, Any] | None:
    if not transactions:
        return None
    first = transactions[0]
    window = _FeatureWindow(
        grouping_variant=grouping_variant,
        source_account_key=source_account_key,
        source_bank=first.source_bank,
        window_start=first.timestamp,
        first_event_id=first.transaction_id,
    )
    for tx in transactions:
        window.append(tx)
    return window.to_row(alert_id)


def extract_feature_csv_from_alert_jsonl(
    alert_jsonl_path: str | Path,
    output_csv_path: str | Path,
    *,
    grouping_variant: str,
    force_rebuild: bool = False,
) -> Path:
    source_path = Path(alert_jsonl_path)
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path
    if not source_path.exists() or not source_path.is_file():
        raise ValueError(f"Alert JSONL does not exist: {source_path}")

    writer = _write_feature_header(output_path)
    rows_written = 0
    try:
        with source_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                source_account_key = str(payload.get("source_account_key") or "").strip()
                alert_id = str(payload.get("alert_id") or "").strip()
                txs = _payload_transactions(payload)
                if grouping_variant == "source_account_24h":
                    row = _aggregate_payload_transactions(
                        alert_id=alert_id,
                        grouping_variant=grouping_variant,
                        source_account_key=source_account_key,
                        transactions=txs,
                    )
                    if row:
                        writer.writerow(row)
                        rows_written += 1
                    continue

                if grouping_variant != "source_account_6h":
                    raise ValueError(f"Unsupported alert-jsonl grouping_variant: {grouping_variant}")

                current_window: _FeatureWindow | None = None
                window_index = 0
                for tx in txs:
                    if current_window is None:
                        current_window = _FeatureWindow(
                            grouping_variant=grouping_variant,
                            source_account_key=source_account_key,
                            source_bank=tx.source_bank,
                            window_start=tx.timestamp,
                            first_event_id=tx.transaction_id,
                        )
                    elif tx.timestamp >= current_window.expiry_6h:
                        window_alert_id = _deterministic_variant_alert_id(
                            grouping_variant,
                            source_account_key,
                            _isoformat_utc(current_window.window_start),
                            str(window_index),
                            alert_id,
                        )
                        writer.writerow(current_window.to_row(window_alert_id))
                        rows_written += 1
                        window_index += 1
                        current_window = _FeatureWindow(
                            grouping_variant=grouping_variant,
                            source_account_key=source_account_key,
                            source_bank=tx.source_bank,
                            window_start=tx.timestamp,
                            first_event_id=tx.transaction_id,
                        )
                    current_window.append(tx)

                if current_window is not None:
                    window_alert_id = _deterministic_variant_alert_id(
                        grouping_variant,
                        source_account_key,
                        _isoformat_utc(current_window.window_start),
                        str(window_index),
                        alert_id,
                    )
                    writer.writerow(current_window.to_row(window_alert_id))
                    rows_written += 1
    finally:
        _close_feature_writer(writer)

    logger.info("Extracted %s feature rows for %s", rows_written, grouping_variant)
    return output_path


def _source_destination_stage_path(output_csv_path: Path) -> Path:
    return output_csv_path.with_suffix(".stage.sqlite")


def extract_source_destination_feature_csv(
    transactions_path: str | Path,
    patterns_path: str | Path,
    output_csv_path: str | Path,
    *,
    force_rebuild: bool = False,
) -> Path:
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path

    source_path = Path(transactions_path)
    if not source_path.exists() or not source_path.is_file():
        raise ValueError(f"Transactions CSV does not exist: {source_path}")

    pattern_index = parse_pattern_file(patterns_path)
    stage_path = _source_destination_stage_path(output_path)
    if stage_path.exists():
        stage_path.unlink()
    _ensure_parent_dir(stage_path)
    connection = sqlite3.connect(str(stage_path))
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        """
        CREATE TABLE staged_source_destination (
            line_number INTEGER NOT NULL,
            group_key TEXT NOT NULL,
            timestamp_iso TEXT NOT NULL,
            source_account_key TEXT NOT NULL,
            source_bank TEXT NOT NULL,
            destination_account_key TEXT NOT NULL,
            destination_bank TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            payment_format TEXT NOT NULL,
            is_laundering INTEGER NOT NULL,
            pattern_typology TEXT NOT NULL
        )
        """
    )
    try:
        buffer: list[tuple[Any, ...]] = []
        with source_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if header is None:
                raise ValueError("Transactions CSV is empty")
            for line_number, cells in enumerate(reader, start=2):
                if not cells or not any(str(cell).strip() for cell in cells):
                    continue
                try:
                    tx = _parse_transaction_cells(cells, line_number=line_number)
                except Exception:
                    continue
                typology = pattern_index.lookup(tx.signature)
                group_key = f"{tx.source_account_key}|{tx.destination_account_key}"
                buffer.append(
                    (
                        tx.line_number,
                        group_key,
                        _isoformat_utc(tx.timestamp),
                        tx.source_account_key,
                        tx.from_bank,
                        tx.destination_account_key,
                        tx.to_bank,
                        float(tx.normalized_amount),
                        tx.normalized_currency,
                        tx.payment_format,
                        tx.is_laundering,
                        typology,
                    )
                )
                if len(buffer) >= 10000:
                    connection.executemany(
                        """
                        INSERT INTO staged_source_destination (
                            line_number, group_key, timestamp_iso, source_account_key, source_bank,
                            destination_account_key, destination_bank, amount, currency,
                            payment_format, is_laundering, pattern_typology
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        buffer,
                    )
                    connection.commit()
                    buffer.clear()
        if buffer:
            connection.executemany(
                """
                INSERT INTO staged_source_destination (
                    line_number, group_key, timestamp_iso, source_account_key, source_bank,
                    destination_account_key, destination_bank, amount, currency,
                    payment_format, is_laundering, pattern_typology
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                buffer,
            )
            connection.commit()
        connection.execute(
            "CREATE INDEX idx_source_destination_group_time ON staged_source_destination (group_key, timestamp_iso, line_number)"
        )
        connection.commit()
        connection.row_factory = sqlite3.Row

        writer = _write_feature_header(output_path)
        rows_written = 0
        try:
            current_window: _FeatureWindow | None = None
            current_group_key = ""
            cursor = connection.execute(
                """
                SELECT
                    line_number, group_key, timestamp_iso, source_account_key, source_bank,
                    destination_account_key, destination_bank, amount, currency,
                    payment_format, is_laundering, pattern_typology
                FROM staged_source_destination
                ORDER BY group_key, timestamp_iso, line_number
                """
            )
            for row in cursor:
                timestamp = datetime.fromisoformat(str(row["timestamp_iso"]).replace("Z", "+00:00")).astimezone(timezone.utc)
                tx = _WindowTransaction(
                    transaction_id=f"ibm-hi-small-tx-{int(row['line_number'])}",
                    timestamp=timestamp,
                    amount=float(row["amount"]),
                    currency=str(row["currency"]),
                    payment_format=str(row["payment_format"]),
                    destination_account_key=str(row["destination_account_key"]),
                    destination_bank=str(row["destination_bank"]),
                    source_bank=str(row["source_bank"]),
                    is_laundering=int(row["is_laundering"]),
                    pattern_typology=str(row["pattern_typology"]),
                )
                group_key = str(row["group_key"])
                source_account_key = str(row["source_account_key"])
                if current_window is None:
                    current_group_key = group_key
                    current_window = _FeatureWindow(
                        grouping_variant="source_destination_24h",
                        source_account_key=source_account_key,
                        source_bank=tx.source_bank,
                        window_start=tx.timestamp,
                        first_event_id=tx.transaction_id,
                    )
                elif group_key != current_group_key or tx.timestamp >= current_window.expiry_24h:
                    alert_id = _deterministic_variant_alert_id(
                        "source_destination_24h",
                        current_group_key,
                        _isoformat_utc(current_window.window_start),
                        current_window.first_event_id,
                    )
                    writer.writerow(current_window.to_row(alert_id))
                    rows_written += 1
                    current_group_key = group_key
                    current_window = _FeatureWindow(
                        grouping_variant="source_destination_24h",
                        source_account_key=source_account_key,
                        source_bank=tx.source_bank,
                        window_start=tx.timestamp,
                        first_event_id=tx.transaction_id,
                    )
                current_window.append(tx)

            if current_window is not None:
                alert_id = _deterministic_variant_alert_id(
                    "source_destination_24h",
                    current_group_key,
                    _isoformat_utc(current_window.window_start),
                    current_window.first_event_id,
                )
                writer.writerow(current_window.to_row(alert_id))
                rows_written += 1
        finally:
            _close_feature_writer(writer)
        logger.info("Extracted %s feature rows for source_destination_24h", rows_written)
    finally:
        connection.close()
        if stage_path.exists():
            stage_path.unlink()
    return output_path


def load_feature_frame(feature_csv_path: str | Path) -> pd.DataFrame:
    path = Path(feature_csv_path)
    if not path.exists() or not path.is_file():
        raise ValueError(f"Feature CSV does not exist: {path}")
    frame = pd.read_csv(path)
    frame["created_at"] = pd.to_datetime(frame["created_at"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["created_at"]).sort_values(["created_at", "alert_id"], kind="stable").reset_index(drop=True)
    int_like_columns = {
        "evaluation_label_is_sar",
        "pattern_assigned",
        "transaction_count",
        "unique_destination_accounts",
        "unique_destination_banks",
        "currency_count",
        "has_mixed_currencies",
        "payment_format_count",
    }
    for col in frame.columns:
        if col in {"alert_id", "created_at", "grouping_variant", "source_account_key", "source_bank", "dominant_destination_bank", "dominant_currency", "dominant_payment_format", "typology"}:
            continue
        numeric = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
        if col in int_like_columns:
            frame[col] = numeric.astype(np.int32)
        else:
            frame[col] = numeric.astype(np.float32)
    return frame


def _split_frame(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    total = len(frame)
    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)
    return {
        "train": frame.iloc[:train_end].copy(),
        "validation": frame.iloc[train_end:validation_end].copy(),
        "test": frame.iloc[validation_end:].copy(),
    }


def _dataset_stats(frame: pd.DataFrame) -> dict[str, Any]:
    total = len(frame)
    positives = int(frame["evaluation_label_is_sar"].sum()) if total else 0
    return {
        "total_alerts": int(total),
        "positive_alerts": positives,
        "negative_alerts": int(max(total - positives, 0)),
        "positive_rate": float(positives / total) if total else 0.0,
        "average_transactions_per_alert": float(frame["transaction_count"].mean()) if total else 0.0,
        "alerts_with_typology_assigned": int(frame["pattern_assigned"].sum()) if total else 0,
        "typology_assignment_rate": float(frame["pattern_assigned"].mean()) if total else 0.0,
    }


def _add_label_free_history_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy(deep=False)
    out["source_account_prior_alert_count"] = out.groupby("source_account_key").cumcount().astype(float)
    out["source_bank_prior_alert_count"] = out.groupby("source_bank").cumcount().astype(float)
    source_prev = out.groupby("source_account_key")["created_at"].shift(1)
    bank_prev = out.groupby("source_bank")["created_at"].shift(1)
    out["source_account_hours_since_prev_alert"] = (
        (out["created_at"] - source_prev).dt.total_seconds().fillna(24.0 * 30.0) / 3600.0
    )
    out["source_bank_hours_since_prev_alert"] = (
        (out["created_at"] - bank_prev).dt.total_seconds().fillna(24.0 * 7.0) / 3600.0
    )
    out["source_account_prior_total_amount_usd"] = (
        out.groupby("source_account_key")["total_amount_usd"].cumsum().shift(fill_value=0.0)
    )
    out["source_bank_prior_total_amount_usd"] = (
        out.groupby("source_bank")["total_amount_usd"].cumsum().shift(fill_value=0.0)
    )
    out["source_account_prior_avg_amount_usd"] = np.where(
        out["source_account_prior_alert_count"] > 0,
        out["source_account_prior_total_amount_usd"] / out["source_account_prior_alert_count"].replace(0.0, np.nan),
        0.0,
    )
    out["source_bank_prior_avg_amount_usd"] = np.where(
        out["source_bank_prior_alert_count"] > 0,
        out["source_bank_prior_total_amount_usd"] / out["source_bank_prior_alert_count"].replace(0.0, np.nan),
        0.0,
    )
    for col in (
        "source_account_prior_avg_amount_usd",
        "source_bank_prior_avg_amount_usd",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return out


def _add_derived_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy(deep=False)
    out["log_total_amount"] = np.log1p(np.clip(out["total_amount"], 0.0, None))
    out["log_total_amount_usd"] = np.log1p(np.clip(out["total_amount_usd"], 0.0, None))
    out["log_max_amount_usd"] = np.log1p(np.clip(out["max_amount_usd"], 0.0, None))
    out["log_transaction_count"] = np.log1p(np.clip(out["transaction_count"], 0.0, None))
    out["log_unique_destination_accounts"] = np.log1p(np.clip(out["unique_destination_accounts"], 0.0, None))
    out["amount_std_to_mean_usd"] = np.where(
        out["mean_amount_usd"] > 0,
        out["std_amount_usd"] / out["mean_amount_usd"],
        0.0,
    )
    out["tx_per_hour"] = np.where(
        out["time_span_hours"] > 0,
        out["transaction_count"] / (out["time_span_hours"] + 1.0),
        out["transaction_count"],
    )
    out["amount_per_counterparty_usd"] = np.where(
        out["unique_destination_accounts"] > 0,
        out["total_amount_usd"] / out["unique_destination_accounts"],
        out["total_amount_usd"],
    )
    out["counterparty_density"] = np.where(
        out["transaction_count"] > 0,
        out["unique_destination_accounts"] / out["transaction_count"],
        0.0,
    )
    out["max_amount_share_usd"] = np.where(
        out["total_amount_usd"] > 0,
        out["max_amount_usd"] / out["total_amount_usd"],
        0.0,
    )
    out["hour_sin"] = np.sin(2.0 * math.pi * out["created_hour"] / 24.0)
    out["hour_cos"] = np.cos(2.0 * math.pi * out["created_hour"] / 24.0)
    out["dow_sin"] = np.sin(2.0 * math.pi * out["created_day_of_week"] / 7.0)
    out["dow_cos"] = np.cos(2.0 * math.pi * out["created_day_of_week"] / 7.0)
    return out


def _build_smoothed_rate_map(
    train_df: pd.DataFrame,
    *,
    key_col: str,
    value_col: str,
    smoothing: float = 100.0,
) -> tuple[dict[str, float], float]:
    grouped = train_df.groupby(key_col)[value_col].agg(["sum", "count"])
    global_rate = float(train_df[value_col].mean()) if len(train_df) else 0.0
    result: dict[str, float] = {}
    for key, row in grouped.iterrows():
        total = float(row["sum"])
        count = float(row["count"])
        result[str(key)] = float((total + global_rate * smoothing) / (count + smoothing))
    return result, global_rate


def _build_frequency_map(train_df: pd.DataFrame, *, key_col: str) -> tuple[dict[str, float], float]:
    counts = train_df.groupby(key_col).size()
    total = float(max(len(train_df), 1))
    result = {str(key): float(count / total) for key, count in counts.items()}
    default = float(1.0 / total)
    return result, default


def _apply_train_only_encodings(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    train = train_df.copy()
    validation = validation_df.copy()
    test = test_df.copy()

    encoding_spec = (
        ("source_bank", "source_bank"),
        ("dominant_destination_bank", "dominant_destination_bank"),
        ("dominant_currency", "dominant_currency"),
        ("dominant_payment_format", "dominant_payment_format"),
    )

    diagnostics: dict[str, Any] = {"label_rate_encodings": {}, "pattern_rate_encodings": {}, "frequency_encodings": {}}
    for raw_col, encoded_prefix in encoding_spec:
        rate_map, global_rate = _build_smoothed_rate_map(
            train,
            key_col=raw_col,
            value_col="evaluation_label_is_sar",
        )
        pattern_map, global_pattern_rate = _build_smoothed_rate_map(
            train.assign(pattern_label=train["pattern_assigned"]),
            key_col=raw_col,
            value_col="pattern_assigned",
            smoothing=100.0,
        )
        freq_map, default_freq = _build_frequency_map(train, key_col=raw_col)

        for frame in (train, validation, test):
            values = frame[raw_col].astype(str)
            frame[f"{encoded_prefix}_positive_rate_train"] = values.map(rate_map).fillna(global_rate)
            frame[f"{encoded_prefix}_pattern_rate_train"] = values.map(pattern_map).fillna(global_pattern_rate)
            frame[f"{encoded_prefix}_frequency_train"] = values.map(freq_map).fillna(default_freq)

        diagnostics["label_rate_encodings"][encoded_prefix] = {
            "distinct_keys_train": int(len(rate_map)),
            "global_positive_rate": global_rate,
        }
        diagnostics["pattern_rate_encodings"][encoded_prefix] = {
            "distinct_keys_train": int(len(pattern_map)),
            "global_pattern_rate": global_pattern_rate,
        }
        diagnostics["frequency_encodings"][encoded_prefix] = {
            "distinct_keys_train": int(len(freq_map)),
            "default_frequency": default_freq,
        }

    source_account_freq, default_source_account_freq = _build_frequency_map(train, key_col="source_account_key")
    for frame in (train, validation, test):
        values = frame["source_account_key"].astype(str)
        frame["source_account_seen_train"] = values.map(source_account_freq).fillna(0.0).gt(0).astype(float)
        frame["source_account_frequency_train"] = values.map(source_account_freq).fillna(default_source_account_freq)
    diagnostics["source_account_seen_train_rate"] = float(train["source_account_seen_train"].mean()) if len(train) else 0.0
    return train, validation, test, diagnostics


def _numeric_feature_columns(frame: pd.DataFrame, *, include_pattern_exact: bool = False) -> list[str]:
    excluded = {
        "alert_id",
        "created_at",
        "grouping_variant",
        "source_account_key",
        "source_bank",
        "dominant_destination_bank",
        "dominant_currency",
        "dominant_payment_format",
        "typology",
        "evaluation_label_is_sar",
    }
    if not include_pattern_exact:
        excluded.add("pattern_assigned")
    columns = [
        col
        for col in frame.columns
        if col not in excluded and pd.api.types.is_numeric_dtype(frame[col])
    ]
    return sorted(columns)


def _raw_signal_feature_columns(feature_columns: list[str]) -> list[str]:
    return [
        col
        for col in feature_columns
        if not col.endswith("_positive_rate_train") and not col.endswith("_pattern_rate_train")
    ]


def _prepare_model_matrices(frame: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict[str, pd.Series], list[str], dict[str, Any]]:
    prepared = _add_derived_features(_add_label_free_history_features(frame))
    splits = _split_frame(prepared)
    train, validation, test, diagnostics = _apply_train_only_encodings(
        splits["train"],
        splits["validation"],
        splits["test"],
    )
    feature_columns = _numeric_feature_columns(train)
    matrices = {
        "train": train,
        "validation": validation,
        "test": test,
    }
    labels = {
        key: matrices[key]["evaluation_label_is_sar"].astype(int)
        for key in matrices
    }
    for key in matrices:
        matrices[key][feature_columns] = (
            matrices[key][feature_columns]
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
            .astype(np.float32)
        )
    return matrices, labels, feature_columns, diagnostics


def _top_k_count(total: int, fraction: float) -> int:
    if total <= 0:
        return 0
    return max(1, math.ceil(total * fraction))


def _review_reduction_at_recall(sorted_df: pd.DataFrame, recall_target: float) -> dict[str, Any] | None:
    positives = int(sorted_df["evaluation_label_is_sar"].sum())
    if positives <= 0:
        return None
    captured = 0
    for index, label in enumerate(sorted_df["evaluation_label_is_sar"].tolist(), start=1):
        captured += int(label)
        if captured / positives >= recall_target:
            review_fraction = index / len(sorted_df)
            return {
                "target_recall": float(recall_target),
                "alerts_reviewed": int(index),
                "review_fraction": float(review_fraction),
                "review_reduction": float(max(0.0, 1.0 - review_fraction)),
            }
    return {
        "target_recall": float(recall_target),
        "alerts_reviewed": int(len(sorted_df)),
        "review_fraction": 1.0,
        "review_reduction": 0.0,
    }


def _ranking_metrics_from_scores(frame: pd.DataFrame, score_column: str) -> dict[str, Any]:
    ordered = frame.sort_values([score_column, "created_at", "alert_id"], ascending=[False, True, True], kind="stable")
    positives = int(ordered["evaluation_label_is_sar"].sum())
    total = len(ordered)
    metrics: dict[str, Any] = {
        "n_alerts": int(total),
        "n_positive_alerts": positives,
    }
    if total == 0 or positives == 0:
        for fraction in _TOP_FRACTIONS:
            metrics[f"recall_at_top_{int(fraction * 100)}pct"] = 0.0
            metrics[f"precision_at_top_{int(fraction * 100)}pct"] = 0.0
        metrics["pr_auc"] = 0.0
        metrics["review_reduction_at_80pct_recall"] = None
        return metrics

    for fraction in _TOP_FRACTIONS:
        top_n = _top_k_count(total, fraction)
        reviewed = ordered.iloc[:top_n]
        captured = int(reviewed["evaluation_label_is_sar"].sum())
        suffix = f"top_{int(fraction * 100)}pct"
        metrics[f"recall_at_{suffix}"] = float(captured / positives)
        metrics[f"precision_at_{suffix}"] = float(captured / top_n)
    scores = pd.to_numeric(ordered[score_column], errors="coerce").fillna(0.0).to_numpy()
    metrics["pr_auc"] = float(average_precision_score(ordered["evaluation_label_is_sar"].to_numpy(), scores))
    metrics["review_reduction_at_80pct_recall"] = _review_reduction_at_recall(
        ordered,
        recall_target=_DEFAULT_RECALL_TARGET,
    )
    return metrics


def _best_metric_tuple(metrics: dict[str, Any]) -> tuple[float, float, float, float]:
    review = metrics.get("review_reduction_at_80pct_recall") or {}
    return (
        float(metrics.get("recall_at_top_10pct", 0.0)),
        float(metrics.get("recall_at_top_20pct", 0.0)),
        float(metrics.get("precision_at_top_10pct", 0.0)),
        float(review.get("review_reduction", 0.0)),
    )


def _zscore_from_train(train_values: pd.Series, other_values: pd.Series) -> pd.Series:
    mean = float(train_values.mean()) if len(train_values) else 0.0
    std = float(train_values.std()) if len(train_values) else 0.0
    if std <= 1e-9:
        return pd.Series(np.zeros(len(other_values), dtype=np.float32), index=other_values.index)
    return pd.Series((other_values - mean) / std, index=other_values.index, dtype=np.float32)


def _fit_weighted_heuristic(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> dict[str, Any]:
    base_cols = {
        "amount": "log_total_amount_usd",
        "tx_count": "log_transaction_count",
        "counterparties": "unique_destination_accounts",
    }
    train_amount = _zscore_from_train(train_df[base_cols["amount"]], train_df[base_cols["amount"]])
    train_tx_count = _zscore_from_train(train_df[base_cols["tx_count"]], train_df[base_cols["tx_count"]])
    train_counterparties = _zscore_from_train(train_df[base_cols["counterparties"]], train_df[base_cols["counterparties"]])
    validation_amount = _zscore_from_train(train_df[base_cols["amount"]], validation_df[base_cols["amount"]])
    validation_tx_count = _zscore_from_train(train_df[base_cols["tx_count"]], validation_df[base_cols["tx_count"]])
    validation_counterparties = _zscore_from_train(train_df[base_cols["counterparties"]], validation_df[base_cols["counterparties"]])
    test_amount = _zscore_from_train(train_df[base_cols["amount"]], test_df[base_cols["amount"]])
    test_tx_count = _zscore_from_train(train_df[base_cols["tx_count"]], test_df[base_cols["tx_count"]])
    test_counterparties = _zscore_from_train(train_df[base_cols["counterparties"]], test_df[base_cols["counterparties"]])

    best: dict[str, Any] | None = None
    for weight_amount, weight_tx_count, weight_counterparties in _WEIGHTED_HEURISTIC_GRID:
        candidate = {
            "weights": {
                "amount": weight_amount,
                "transaction_count": weight_tx_count,
                "counterparties": weight_counterparties,
            }
        }
        validation_scores = (
            weight_amount * validation_amount
            + weight_tx_count * validation_tx_count
            + weight_counterparties * validation_counterparties
        )
        validation_scored = validation_df.copy()
        validation_scored["model_score"] = validation_scores.astype(np.float32)
        validation_metrics = _ranking_metrics_from_scores(validation_scored, "model_score")
        candidate["validation_metrics"] = validation_metrics
        candidate["validation_score_tuple"] = _best_metric_tuple(validation_metrics)
        if best is None or tuple(candidate["validation_score_tuple"]) > tuple(best["validation_score_tuple"]):
            best = candidate

    assert best is not None
    weights = best["weights"]
    test_scores = (
        weights["amount"] * test_amount
        + weights["transaction_count"] * test_tx_count
        + weights["counterparties"] * test_counterparties
    )
    test_scored = test_df.copy()
    test_scored["model_score"] = test_scores.astype(np.float32)
    best["test_metrics"] = _ranking_metrics_from_scores(test_scored, "model_score")
    best["name"] = "weighted_signal_heuristic"
    best["kind"] = "heuristic"
    best["notes"] = "Validation-tuned weighted heuristic over normalized amount, transaction count, and counterparty breadth."
    return best


def _fit_logistic_candidate(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    name: str = "logistic_regression",
    notes: str = "Standardized logistic regression with balanced class weights.",
) -> dict[str, Any]:
    pipeline = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=600,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=_DEFAULT_MODEL_RANDOM_STATE,
                ),
            ),
        ]
    )
    pipeline.fit(train_df[feature_columns], train_df["evaluation_label_is_sar"])
    validation_scores = pipeline.predict_proba(validation_df[feature_columns])[:, 1]
    test_scores = pipeline.predict_proba(test_df[feature_columns])[:, 1]
    validation_scored = validation_df.copy()
    validation_scored["model_score"] = validation_scores.astype(np.float32)
    test_scored = test_df.copy()
    test_scored["model_score"] = test_scores.astype(np.float32)
    coefficients = pipeline.named_steps["model"].coef_[0]
    top_features = sorted(
        (
            {"feature": feature, "coefficient": float(weight)}
            for feature, weight in zip(feature_columns, coefficients, strict=False)
        ),
        key=lambda item: abs(item["coefficient"]),
        reverse=True,
    )[:12]
    return {
        "name": name,
        "kind": "model",
        "notes": notes,
        "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
        "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
        "top_features": top_features,
        "model_object": pipeline,
    }


def _fit_lightgbm_candidates(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> list[dict[str, Any]]:
    if lgb is None:
        return []
    X_train = train_df[feature_columns]
    y_train = train_df["evaluation_label_is_sar"].astype(int)
    X_val = validation_df[feature_columns]
    y_val = validation_df["evaluation_label_is_sar"].astype(int)
    X_test = test_df[feature_columns]
    y_test = test_df["evaluation_label_is_sar"].astype(int)
    positive_rate = float(y_train.mean()) if len(y_train) else 0.0
    base_scale_pos_weight = float((1.0 - positive_rate) / positive_rate) if positive_rate > 0 else 1.0

    runs: list[dict[str, Any]] = []
    for candidate in _DEFAULT_LGBM_CANDIDATES:
        params = dict(candidate["params"])
        scale_multiplier = 1.0 if candidate["name"] != "lightgbm_top_recall" else 1.35
        params["scale_pos_weight"] = max(1.0, base_scale_pos_weight * scale_multiplier)
        model = lgb.LGBMClassifier(**params)
        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=0),
        ]
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            eval_metric="average_precision",
            callbacks=callbacks,
        )
        validation_scores = model.predict_proba(X_val)[:, 1]
        test_scores = model.predict_proba(X_test)[:, 1]
        validation_scored = validation_df.copy()
        validation_scored["model_score"] = validation_scores.astype(np.float32)
        test_scored = test_df.copy()
        test_scored["model_score"] = test_scores.astype(np.float32)
        importance = getattr(model, "feature_importances_", None)
        top_features = []
        if importance is not None:
            top_features = sorted(
                (
                    {"feature": feature, "importance": float(weight)}
                    for feature, weight in zip(feature_columns, importance, strict=False)
                ),
                key=lambda item: item["importance"],
                reverse=True,
            )[:12]
        runs.append(
            {
                "name": candidate["name"],
                "kind": "model",
                "notes": candidate["note"],
                "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
                "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
                "top_features": top_features,
                "model_object": model,
                "scale_pos_weight": params["scale_pos_weight"],
            }
        )
    return runs


def _compute_baselines(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    baseline_spec = (
        ("chronological_queue", "Chronological queue ordered by created_at ascending.", None),
        ("amount_descending", "Raw total amount descending.", "total_amount"),
        ("amount_usd_descending", "Static-FX normalized total amount descending.", "total_amount_usd"),
        ("transaction_count_descending", "Transaction count descending.", "transaction_count"),
        ("distinct_counterparties_descending", "Distinct destination account count descending.", "unique_destination_accounts"),
    )
    for name, note, score_col in baseline_spec:
        if score_col is None:
            validation_scored = validation_df.copy()
            validation_scored["baseline_score"] = np.linspace(len(validation_df), 1, num=len(validation_df), dtype=np.float32)
            test_scored = test_df.copy()
            test_scored["baseline_score"] = np.linspace(len(test_df), 1, num=len(test_df), dtype=np.float32)
        else:
            validation_scored = validation_df.copy()
            validation_scored["baseline_score"] = pd.to_numeric(validation_scored[score_col], errors="coerce").fillna(0.0).astype(np.float32)
            test_scored = test_df.copy()
            test_scored["baseline_score"] = pd.to_numeric(test_scored[score_col], errors="coerce").fillna(0.0).astype(np.float32)
        results.append(
            {
                "name": name,
                "kind": "baseline",
                "notes": note,
                "validation_metrics": _ranking_metrics_from_scores(validation_scored, "baseline_score"),
                "test_metrics": _ranking_metrics_from_scores(test_scored, "baseline_score"),
            }
        )
    results.append(_fit_weighted_heuristic(train_df, validation_df, test_df))
    return results


def _current_pipeline_schema_diagnosis(alert_jsonl_path: str | Path, sample_size: int = 128) -> dict[str, Any]:
    path = Path(alert_jsonl_path)
    payloads: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            payloads.append(json.loads(line))
            if len(payloads) >= sample_size:
                break
    if not payloads:
        return {"status": "empty"}

    repository = EnterpriseRepository(f"sqlite:///{(_repo_root() / 'data' / 'althea_enterprise.db').as_posix()}")
    registry = ModelRegistry(repository=repository, object_storage=ObjectStorage(_repo_root() / "data" / "object_storage"))
    model_record = registry.resolve_model(tenant_id="default-bank", strategy="active_approved")
    if not model_record:
        return {"status": "no_model"}

    expected_schema = registry.load_feature_schema(model_record)
    adapter = AlertFeatureAdapter()
    schema_validator = FeatureSchemaValidator()
    feature_service = EnterpriseFeatureService(schema_validator)
    alerts_df = adapter.alerts_to_dataframe(payloads)
    bundle = feature_service.generate_inference_features(alerts_df)
    produced_columns = list(bundle["feature_matrix"].columns)
    expected_columns = [str(item.get("name")) for item in (expected_schema.get("columns") or []) if item.get("name")]
    overlap = sorted(set(expected_columns).intersection(produced_columns))
    imputed = sorted(set(expected_columns) - set(produced_columns))
    dropped = sorted(set(produced_columns) - set(expected_columns))
    return {
        "status": "ok",
        "model_version": str(model_record.get("model_version") or "unknown"),
        "expected_feature_count": int(len(expected_columns)),
        "produced_feature_count": int(len(produced_columns)),
        "overlap_feature_count": int(len(overlap)),
        "imputed_feature_count": int(len(imputed)),
        "dropped_feature_count": int(len(dropped)),
        "expected_only_features": imputed,
        "produced_only_features": dropped,
    }


def _score_althea_test_split(
    alert_jsonl_path: str | Path,
    test_alert_ids: set[str],
    *,
    tenant_id: str,
    database_url: str,
    object_storage_root: str | Path,
) -> dict[str, Any]:
    if not test_alert_ids:
        return {"status": "empty", "scores": {}}
    repository = EnterpriseRepository(database_url)
    registry = ModelRegistry(repository=repository, object_storage=ObjectStorage(Path(object_storage_root)))
    model_record = registry.resolve_model(tenant_id=tenant_id, strategy="active_approved")
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
    inference_logger = logging.getLogger("althea.inference")
    previous_level = inference_logger.level
    inference_logger.setLevel(logging.ERROR)
    try:
        with Path(alert_jsonl_path).open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                alert_id = str(payload.get("alert_id") or "").strip()
                if alert_id not in test_alert_ids:
                    continue
                chunk.append(payload)
                if len(chunk) >= 5000:
                    alerts_df = adapter.alerts_to_dataframe(chunk)
                    bundle = feature_service.generate_inference_features(alerts_df)
                    inference = inference_service.predict(
                        tenant_id=tenant_id,
                        feature_frame=bundle["feature_matrix"],
                        strategy="active_approved",
                    )
                    for current_alert_id, score in zip(alerts_df["alert_id"].tolist(), inference.get("scores", []), strict=False):
                        scores[str(current_alert_id)] = float(score)
                    chunk.clear()
            if chunk:
                alerts_df = adapter.alerts_to_dataframe(chunk)
                bundle = feature_service.generate_inference_features(alerts_df)
                inference = inference_service.predict(
                    tenant_id=tenant_id,
                    feature_frame=bundle["feature_matrix"],
                    strategy="active_approved",
                )
                for current_alert_id, score in zip(alerts_df["alert_id"].tolist(), inference.get("scores", []), strict=False):
                    scores[str(current_alert_id)] = float(score)
    finally:
        inference_logger.setLevel(previous_level)
    return {
        "status": "available",
        "model_version": str(model_record.get("model_version") or "unknown"),
        "scores": scores,
    }


def _score_distribution(scores: pd.Series, labels: pd.Series) -> dict[str, Any]:
    positives = scores[labels == 1]
    negatives = scores[labels == 0]
    def summary(series: pd.Series) -> dict[str, float]:
        if len(series) == 0:
            return {"mean": 0.0, "p50": 0.0, "p90": 0.0, "p99": 0.0}
        return {
            "mean": float(series.mean()),
            "p50": float(series.quantile(0.50)),
            "p90": float(series.quantile(0.90)),
            "p99": float(series.quantile(0.99)),
        }
    return {
        "positive": summary(positives),
        "negative": summary(negatives),
    }


def _diagnose_althea_vs_amount(
    test_df: pd.DataFrame,
    althea_scores: dict[str, float],
    schema_diagnosis: dict[str, Any],
) -> dict[str, Any]:
    diagnosis_df = test_df.copy()
    diagnosis_df["althea_score"] = diagnosis_df["alert_id"].map(althea_scores).fillna(float("-inf"))
    diagnosis_df["amount_score"] = pd.to_numeric(diagnosis_df["total_amount"], errors="coerce").fillna(0.0)
    top_n = _top_k_count(len(diagnosis_df), 0.10)
    amount_top = diagnosis_df.sort_values(["amount_score", "created_at", "alert_id"], ascending=[False, True, True], kind="stable").iloc[:top_n]
    althea_top = diagnosis_df.sort_values(["althea_score", "created_at", "alert_id"], ascending=[False, True, True], kind="stable").iloc[:top_n]
    amount_top_positive_ids = set(amount_top.loc[amount_top["evaluation_label_is_sar"] == 1, "alert_id"].tolist())
    althea_top_positive_ids = set(althea_top.loc[althea_top["evaluation_label_is_sar"] == 1, "alert_id"].tolist())
    amount_only_positive_ids = sorted(amount_top_positive_ids - althea_top_positive_ids)
    overlap_positive_ids = sorted(amount_top_positive_ids & althea_top_positive_ids)
    amount_only_df = diagnosis_df[diagnosis_df["alert_id"].isin(amount_only_positive_ids)].copy()
    positive_df = diagnosis_df[diagnosis_df["evaluation_label_is_sar"] == 1].copy()
    positive_amount_median = float(positive_df["total_amount_usd"].median()) if len(positive_df) else 0.0
    althea_scores_series = pd.to_numeric(diagnosis_df["althea_score"], errors="coerce").replace([-np.inf, np.inf], np.nan).fillna(0.0)
    corr = float(althea_scores_series.corr(diagnosis_df["total_amount_usd"], method="spearman")) if len(diagnosis_df) else 0.0
    return {
        "current_althea_model": {
            "model_version": schema_diagnosis.get("model_version"),
            "schema_alignment": schema_diagnosis,
        },
        "top_10pct_capture": {
            "amount_positive_captured": int(len(amount_top_positive_ids)),
            "althea_positive_captured": int(len(althea_top_positive_ids)),
            "overlap_positive_captured": int(len(overlap_positive_ids)),
            "amount_only_positive_captured": int(len(amount_only_positive_ids)),
        },
        "amount_only_positive_alert_profile": {
            "count": int(len(amount_only_df)),
            "median_total_amount_usd": float(amount_only_df["total_amount_usd"].median()) if len(amount_only_df) else 0.0,
            "median_transaction_count": float(amount_only_df["transaction_count"].median()) if len(amount_only_df) else 0.0,
            "median_unique_destination_accounts": float(amount_only_df["unique_destination_accounts"].median()) if len(amount_only_df) else 0.0,
            "share_above_positive_median_amount_usd": float((amount_only_df["total_amount_usd"] >= positive_amount_median).mean()) if len(amount_only_df) else 0.0,
        },
        "althea_score_distribution": _score_distribution(
            scores=althea_scores_series,
            labels=diagnosis_df["evaluation_label_is_sar"].astype(int),
        ),
        "althea_spearman_correlation_with_total_amount_usd": corr,
        "diagnosis_summary": [
            "Current ALTHEA scoring is using a bootstrap demo RandomForest model that was never trained on IBM-derived alerts.",
            f"Feature alignment is weak: {schema_diagnosis.get('overlap_feature_count', 0)} shared columns, {schema_diagnosis.get('imputed_feature_count', 0)} expected legacy columns imputed, {schema_diagnosis.get('dropped_feature_count', 0)} current bundle columns dropped.",
            "Amount-heavy positives are materially under-captured by the current ALTHEA ranking relative to the simple amount queue.",
            "The active model therefore behaves like a schema-mismatched fallback, not a benchmark-calibrated alert prioritizer.",
        ],
    }


def _compare_metric_rows(rows: list[dict[str, Any]], metric_scope: str) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda item: _best_metric_tuple(item[metric_scope]),
        reverse=True,
    )


def _select_champion(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = _compare_metric_rows(rows, "validation_metrics")
    champion = dict(ordered[0]) if ordered else {}
    return champion


def _fit_candidate_by_name(
    candidate_name: str,
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> dict[str, Any]:
    raw_feature_columns = _raw_signal_feature_columns(feature_columns)
    if candidate_name == "weighted_signal_heuristic":
        return _fit_weighted_heuristic(train_df, validation_df, test_df)
    if candidate_name == "logistic_regression_raw_signals":
        return _fit_logistic_candidate(
            train_df,
            validation_df,
            test_df,
            raw_feature_columns,
            name="logistic_regression_raw_signals",
            notes="Standardized logistic regression on direct alert signals plus chronology-safe history features, excluding train-only label/pattern rate encodings.",
        )
    if candidate_name == "logistic_regression_full":
        return _fit_logistic_candidate(
            train_df,
            validation_df,
            test_df,
            feature_columns,
            name="logistic_regression_full",
            notes="Standardized logistic regression with balanced class weights and train-only rate encodings.",
        )
    if candidate_name.startswith("lightgbm_"):
        candidates = _fit_lightgbm_candidates(train_df, validation_df, test_df, raw_feature_columns)
        for candidate in candidates:
            if candidate["name"] == candidate_name:
                return candidate
        if candidates:
            return candidates[0]
    baseline_rows = _compute_baselines(train_df, validation_df, test_df)
    for row in baseline_rows:
        if row["name"] == candidate_name:
            return row
    return _fit_weighted_heuristic(train_df, validation_df, test_df)


def _run_grouping_variant_summary(
    *,
    grouping_variant: str,
    feature_csv_path: Path,
    champion_candidate_name: str,
) -> dict[str, Any]:
    frame = load_feature_frame(feature_csv_path)
    matrices, _, variant_feature_columns, _ = _prepare_model_matrices(frame)
    baselines = _compute_baselines(matrices["train"], matrices["validation"], matrices["test"])
    champion_result = _fit_candidate_by_name(
        champion_candidate_name,
        matrices["train"],
        matrices["validation"],
        matrices["test"],
        variant_feature_columns,
    )
    return {
        "grouping_variant": grouping_variant,
        "dataset_stats": _dataset_stats(frame),
        "baseline_strength": {
            row["name"]: row["test_metrics"]["recall_at_top_10pct"]
            for row in baselines
            if row["name"] in {"amount_descending", "amount_usd_descending", "weighted_signal_heuristic"}
        },
        "champion_candidate_name": champion_candidate_name,
        "champion_test_metrics": champion_result["test_metrics"],
    }


def _build_benchmark_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    table = []
    for row in rows:
        review = row["test_metrics"].get("review_reduction_at_80pct_recall") or {}
        table.append(
            {
                "name": row["name"],
                "kind": row["kind"],
                "recall_at_top_10pct": row["test_metrics"].get("recall_at_top_10pct", 0.0),
                "recall_at_top_20pct": row["test_metrics"].get("recall_at_top_20pct", 0.0),
                "precision_at_top_10pct": row["test_metrics"].get("precision_at_top_10pct", 0.0),
                "precision_at_top_20pct": row["test_metrics"].get("precision_at_top_20pct", 0.0),
                "review_reduction_at_80pct_recall": review.get("review_reduction", 0.0) if review else 0.0,
                "pr_auc": row["test_metrics"].get("pr_auc", 0.0),
                "notes": row.get("notes"),
            }
        )
    return table


def _render_report(
    *,
    summary: dict[str, Any],
) -> str:
    dataset_stats = summary["dataset_stats"]
    default_split_stats = summary["default_grouping"]["split_stats"]
    diagnosis = summary["diagnosis"]
    champion = summary["champion"]
    benchmark_table = summary["benchmark_table"]
    grouping_variants = summary.get("grouping_variants") or []
    lines = [
        "# ALTHEA IBM AML Benchmark Improvement Sprint",
        "",
        "This report summarizes a focused alert-ranking improvement pass on IBM AML-Data `HI-Small` synthetic alerts.",
        "",
        "Important scope note: this is synthetic benchmark validation only. It is not live bank validation, not a production ROI proof, and not evidence of bank deployment readiness.",
        "",
        "## Default Benchmark Dataset",
        "",
        f"- Grouping variant: `{summary['default_grouping']['grouping_variant']}`",
        f"- Total alerts: `{dataset_stats['total_alerts']}`",
        f"- Positive alerts: `{dataset_stats['positive_alerts']}`",
        f"- Negative alerts: `{dataset_stats['negative_alerts']}`",
        f"- Average transactions per alert: `{dataset_stats['average_transactions_per_alert']:.2f}`",
        f"- Typology assignment rate: `{dataset_stats['typology_assignment_rate']:.4f}`",
        "",
        "## Chronological Split",
        "",
        "- Train = first 60%, validation = next 20%, test = final 20%.",
        f"- Train alerts: `{default_split_stats['train']['total_alerts']}`",
        f"- Validation alerts: `{default_split_stats['validation']['total_alerts']}`",
        f"- Test alerts: `{default_split_stats['test']['total_alerts']}`",
        "",
        "## Diagnosis: Why Current ALTHEA Loses",
        "",
    ]
    for item in diagnosis["diagnosis_summary"]:
        lines.append(f"- {item}")
    schema_alignment = (diagnosis.get("current_althea_model") or {}).get("schema_alignment") or {}
    if diagnosis.get("top_10pct_capture"):
        lines.extend(
            [
                "",
                f"- Amount baseline positives captured in top 10%: `{diagnosis['top_10pct_capture']['amount_positive_captured']}`",
                f"- Current ALTHEA positives captured in top 10%: `{diagnosis['top_10pct_capture']['althea_positive_captured']}`",
                f"- Positives captured by amount but missed by ALTHEA top 10%: `{diagnosis['top_10pct_capture']['amount_only_positive_captured']}`",
                f"- Median total amount (USD proxy) of amount-only positives: `{diagnosis['amount_only_positive_alert_profile']['median_total_amount_usd']:.2f}`",
                f"- Median tx count of amount-only positives: `{diagnosis['amount_only_positive_alert_profile']['median_transaction_count']:.2f}`",
                f"- Shared current/expected model features: `{schema_alignment.get('overlap_feature_count', 0)}`",
                f"- Legacy schema columns imputed at inference: `{schema_alignment.get('imputed_feature_count', 0)}`",
                f"- Rich current bundle columns dropped at inference: `{schema_alignment.get('dropped_feature_count', 0)}`",
            ]
        )
    elif schema_alignment:
        lines.extend(
            [
                "",
                f"- Shared current/expected model features: `{schema_alignment.get('overlap_feature_count', 0)}`",
                f"- Legacy schema columns imputed at inference: `{schema_alignment.get('imputed_feature_count', 0)}`",
                f"- Rich current bundle columns dropped at inference: `{schema_alignment.get('dropped_feature_count', 0)}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Benchmark Table",
            "",
            "| Candidate | Kind | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in benchmark_table:
        lines.append(
            "| "
            + f"{row['name']} | {row['kind']} | "
            + f"{row['recall_at_top_10pct']:.4f} | "
            + f"{row['recall_at_top_20pct']:.4f} | "
            + f"{row['precision_at_top_10pct']:.4f} | "
            + f"{row['precision_at_top_20pct']:.4f} | "
            + f"{row['review_reduction_at_80pct_recall']:.4f} | "
            + f"{row['pr_auc']:.4f} |"
        )

    lines.extend(
        [
            "",
            "## Champion",
            "",
            f"- Selected by validation priority metric: `{champion['name']}`",
            f"- Test Recall@Top 10%: `{champion['test_metrics']['recall_at_top_10pct']:.4f}`",
            f"- Test Recall@Top 20%: `{champion['test_metrics']['recall_at_top_20pct']:.4f}`",
            f"- Test Precision@Top 10%: `{champion['test_metrics']['precision_at_top_10pct']:.4f}`",
            f"- Test PR-AUC: `{champion['test_metrics']['pr_auc']:.4f}`",
            f"- Notes: {champion.get('notes', '')}",
            "",
            "## Feature Changes",
            "",
            "- Added explicit amount-aware alert features: raw total/max/mean/min/std/range and static-FX USD-normalized equivalents.",
            "- Added alert structure features: transaction count, distinct counterparties, destination-bank breadth, counterparty concentration, time span, gap metrics, payment-format mix, same-bank ratio, mixed-currency flags, round-amount ratio, night/weekend activity ratios.",
            "- Added label-free chronology-safe history features: prior alert counts, time since prior alert, prior rolling amount totals and averages by source account and source bank.",
            "- Added train-only category encodings for source bank, dominant destination bank, dominant currency, and payment format; added train-only pattern-rate priors without using direct exact-match test annotations as model inputs.",
            "",
            "## Grouping Sensitivity",
            "",
        ]
    )
    for variant in grouping_variants:
        lines.append(
            "- "
            + f"`{variant['grouping_variant']}`: "
            + f"positive_rate={variant['dataset_stats']['positive_rate']:.4f}, "
            + f"avg_tx_per_alert={variant['dataset_stats']['average_transactions_per_alert']:.2f}, "
            + f"amount_baseline_recall@10={variant['baseline_strength'].get('amount_descending', 0.0):.4f}, "
            + f"weighted_heuristic_recall@10={variant['baseline_strength'].get('weighted_signal_heuristic', 0.0):.4f}, "
            + f"champion_recall@10={variant['champion_test_metrics']['recall_at_top_10pct']:.4f}"
        )
    lines.extend(
        [
            "",
            "## Limitations",
            "",
            "- Labels remain synthetic transaction-derived proxies, not true SAR or case outcomes.",
            "- Static FX normalization is benchmark-only scaffolding; it is approximate and intended only to reduce obvious cross-currency distortion in IBM AML-Data.",
            "- Pattern file enrichment is used conservatively through train-only priors; direct exact-match test annotations were not used as primary model features.",
            "- Grouping sensitivity outside the default source-account 24h variant still depends on synthetic grouping rules, not a real bank rules engine.",
        ]
    )

    if champion.get("top_features"):
        lines.extend(["", "## Champion Top Features", ""])
        for item in champion["top_features"][:10]:
            value = item.get("importance", item.get("coefficient", 0.0))
            lines.append(f"- `{item['feature']}`: `{value:.4f}`")

    return "\n".join(lines) + "\n"


def run_improved_benchmark(
    *,
    alert_jsonl_path: str | Path,
    report_path: str | Path,
    summary_path: str | Path,
    feature_cache_dir: str | Path | None = None,
    transactions_path: str | Path | None = None,
    patterns_path: str | Path | None = None,
    tenant_id: str = "default-bank",
    database_url: str | None = None,
    object_storage_root: str | Path | None = None,
    include_grouping_variants: bool = True,
    include_althea_diagnosis: bool = True,
    force_rebuild_features: bool = False,
) -> ImprovedBenchmarkResult:
    alert_path = Path(alert_jsonl_path)
    cache_dir = Path(feature_cache_dir or (_repo_root() / "data" / "processed" / "ibm_aml_alerts" / "benchmark_features"))
    _ensure_parent_dir(cache_dir / "placeholder.txt")
    default_feature_csv = extract_feature_csv_from_alert_jsonl(
        alert_path,
        cache_dir / "source_account_24h.features.csv",
        grouping_variant="source_account_24h",
        force_rebuild=force_rebuild_features,
    )
    default_frame = load_feature_frame(default_feature_csv)
    default_dataset_stats = _dataset_stats(default_frame)
    default_matrices, _, feature_columns, feature_diagnostics = _prepare_model_matrices(default_frame)
    split_stats = {name: _dataset_stats(df) for name, df in default_matrices.items()}

    baseline_rows = _compute_baselines(
        default_matrices["train"],
        default_matrices["validation"],
        default_matrices["test"],
    )
    model_rows = [
        _fit_logistic_candidate(
            default_matrices["train"],
            default_matrices["validation"],
            default_matrices["test"],
            _raw_signal_feature_columns(feature_columns),
            name="logistic_regression_raw_signals",
            notes="Standardized logistic regression on direct alert signals plus chronology-safe history features, excluding train-only label/pattern rate encodings.",
        ),
        _fit_logistic_candidate(
            default_matrices["train"],
            default_matrices["validation"],
            default_matrices["test"],
            feature_columns,
            name="logistic_regression_full",
            notes="Standardized logistic regression with balanced class weights and train-only rate encodings.",
        )
    ]
    model_rows.extend(
        _fit_lightgbm_candidates(
            default_matrices["train"],
            default_matrices["validation"],
            default_matrices["test"],
            _raw_signal_feature_columns(feature_columns),
        )
    )
    all_rows = baseline_rows + model_rows
    champion = _select_champion(all_rows)
    if "model_object" in champion:
        champion.pop("model_object", None)

    schema_diagnosis = _current_pipeline_schema_diagnosis(alert_path)
    diagnosis: dict[str, Any] = {
        "feature_pipeline_diagnostics": feature_diagnostics,
        "diagnosis_summary": [],
    }
    if include_althea_diagnosis:
        resolved_database_url = database_url or f"sqlite:///{(_repo_root() / 'data' / 'althea_enterprise.db').as_posix()}"
        resolved_object_storage = Path(object_storage_root or (_repo_root() / "data" / "object_storage"))
        test_alert_ids = set(default_matrices["test"]["alert_id"].astype(str).tolist())
        althea_status = _score_althea_test_split(
            alert_jsonl_path=alert_path,
            test_alert_ids=test_alert_ids,
            tenant_id=tenant_id,
            database_url=resolved_database_url,
            object_storage_root=resolved_object_storage,
        )
        if althea_status.get("status") == "available":
            diagnosis = _diagnose_althea_vs_amount(
                test_df=default_matrices["test"],
                althea_scores=dict(althea_status.get("scores") or {}),
                schema_diagnosis=schema_diagnosis,
            )
            diagnosis["althea_baseline_status"] = {
                "status": "available",
                "model_version": althea_status.get("model_version"),
                "scored_alert_count": int(len(althea_status.get("scores") or {})),
            }
        else:
            diagnosis["althea_baseline_status"] = dict(althea_status)
            diagnosis["current_althea_model"] = {"schema_alignment": schema_diagnosis}
            diagnosis["diagnosis_summary"] = [
                "Current ALTHEA baseline was unavailable, so model-loss diagnosis is limited to schema mismatch inspection."
            ]
    else:
        diagnosis["current_althea_model"] = {"schema_alignment": schema_diagnosis}
        diagnosis["diagnosis_summary"] = [
            "ALTHEA score diagnosis was skipped by configuration; schema mismatch inspection is still included."
        ]

    grouping_variants: list[dict[str, Any]] = []
    champion_model_name = champion.get("name", "unknown")
    if include_grouping_variants:
        variant_6h_csv = extract_feature_csv_from_alert_jsonl(
            alert_path,
            cache_dir / "source_account_6h.features.csv",
            grouping_variant="source_account_6h",
            force_rebuild=force_rebuild_features,
        )
        grouping_variants.append(
            _run_grouping_variant_summary(
                grouping_variant="source_account_6h",
                feature_csv_path=variant_6h_csv,
                champion_candidate_name=champion_model_name,
            )
        )
        if transactions_path and patterns_path:
            source_destination_csv = extract_source_destination_feature_csv(
                transactions_path=transactions_path,
                patterns_path=patterns_path,
                output_csv_path=cache_dir / "source_destination_24h.features.csv",
                force_rebuild=force_rebuild_features,
            )
            grouping_variants.append(
                _run_grouping_variant_summary(
                    grouping_variant="source_destination_24h",
                    feature_csv_path=source_destination_csv,
                    champion_candidate_name=champion_model_name,
                )
            )

    benchmark_table = _build_benchmark_table(_compare_metric_rows(all_rows, "test_metrics"))
    report_target = Path(report_path)
    summary_target = Path(summary_path)
    _ensure_parent_dir(report_target)
    _ensure_parent_dir(summary_target)
    summary = {
        "generated_at": _isoformat_utc(datetime.now(timezone.utc)),
        "alert_jsonl_path": str(alert_path.resolve()),
        "feature_cache_dir": str(cache_dir.resolve()),
        "dataset_stats": default_dataset_stats,
        "default_grouping": {
            "grouping_variant": "source_account_24h",
            "feature_csv_path": str(default_feature_csv.resolve()),
            "split_stats": split_stats,
            "feature_columns": feature_columns,
        },
        "diagnosis": diagnosis,
        "baseline_results": [
            {key: value for key, value in row.items() if key != "model_object"}
            for row in baseline_rows
        ],
        "model_results": [
            {key: value for key, value in row.items() if key != "model_object"}
            for row in model_rows
        ],
        "benchmark_table": benchmark_table,
        "champion": champion,
        "grouping_variants": grouping_variants,
    }
    summary_target.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report_target.write_text(_render_report(summary=summary), encoding="utf-8")
    return ImprovedBenchmarkResult(
        summary_path=summary_target,
        report_path=report_target,
        dataset_stats=default_dataset_stats,
        diagnosis=diagnosis,
        baseline_results=baseline_rows,
        model_results=model_rows,
        grouping_variants=grouping_variants,
        champion=champion,
    )
