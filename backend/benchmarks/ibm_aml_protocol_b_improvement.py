from __future__ import annotations

import csv
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
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:  # pragma: no cover - optional dependency
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

from benchmarks.ibm_aml_improvement import (
    _add_derived_features,
    _compute_baselines,
    _deterministic_variant_alert_id,
    _fit_logistic_candidate,
    _fx_rate_to_usd,
    _numeric_feature_columns,
    _ranking_metrics_from_scores,
    _split_frame,
)
from benchmarks.ibm_aml_protocol_b import (
    _AMOUNT_ABLATION_COLUMNS,
    _HISTORY_ABLATION_COLUMNS,
    _DEFAULT_GROUPING_VARIANT,
    _DEFAULT_OBSERVATION_HOURS,
    _DEFAULT_OUTCOME_HOURS,
    _PATTERN_FEATURES,
    _PAYMENT_CURRENCY_ABLATION_COLUMNS,
    _ProtocolBTransaction,
    _display_path,
    _ensure_parent_dir,
    _isoformat_utc,
    _parse_iso_utc,
    _repo_root,
    _row_to_transaction,
    _safe_ratio,
    _stage_path,
    _stage_transactions_from_alert_jsonl,
    load_protocol_b_feature_frame,
)

logger = logging.getLogger("althea.benchmarks.ibm_aml_protocol_b_improvement")

_DATASET_NAME = "IBM AML-Data HI-Small Protocol B v2"
_REFERENCE_PROTOCOL_B_SUMMARY = "benchmark_protocol_b_v1.json"
_CURRENT_PROTOCOL_B_RECALL = 0.5223880597014925
_TOP_ALERT_SAMPLE = 12
_DEFAULT_HARD_NEGATIVE_WEIGHTS = {
    "top_10pct_negative_weight": 4.0,
    "top_20pct_negative_weight": 2.5,
}
_DEFAULT_WEIGHTED_HEURISTIC = {
    "amount": 0.45,
    "transaction_count": 0.30,
    "counterparties": 0.25,
}
_EXTRA_FEATURE_FIELDNAMES = [
    "alert_id",
    "tx_count_1h",
    "tx_count_6h",
    "amount_total_usd_1h",
    "amount_total_usd_6h",
    "amount_share_1h",
    "amount_share_6h",
    "tx_share_1h",
    "tx_share_6h",
    "unique_destination_accounts_1h",
    "unique_destination_accounts_6h",
    "unique_destination_banks_1h",
    "unique_destination_banks_6h",
    "currency_entropy",
    "payment_format_entropy",
    "counterparty_hhi_tx",
    "counterparty_hhi_amount",
    "bank_hhi_tx",
    "bank_hhi_amount",
    "fan_out_ratio",
    "fan_in_ratio",
    "self_transfer_ratio",
    "new_counterparty_ratio_hist",
    "seen_counterparty_tx_ratio_hist",
    "seen_counterparty_amount_share_hist",
    "new_bank_ratio_hist",
    "seen_bank_tx_ratio_hist",
    "repeat_counterparty_mean_recency_hours",
    "repeat_counterparty_min_recency_hours",
    "prior_raw_tx_count",
    "prior_raw_amount_usd",
    "hours_since_account_first_seen",
    "tx_count_vs_prior_anchor_avg",
    "total_amount_usd_vs_prior_anchor_avg",
    "unique_counterparties_vs_prior_anchor_avg",
    "unique_banks_vs_prior_anchor_avg",
    "currency_count_vs_prior_anchor_avg",
    "payment_format_count_vs_prior_anchor_avg",
    "cross_bank_ratio_vs_prior_anchor_avg",
    "round_amount_ratio_vs_prior_anchor_avg",
    "currency_entropy_vs_prior_anchor_avg",
    "payment_format_entropy_vs_prior_anchor_avg",
]
_GRAPH_FEATURE_COLUMNS = {
    "counterparty_hhi_tx",
    "counterparty_hhi_amount",
    "bank_hhi_tx",
    "bank_hhi_amount",
    "fan_out_ratio",
    "fan_in_ratio",
    "self_transfer_ratio",
    "new_counterparty_ratio_hist",
    "seen_counterparty_tx_ratio_hist",
    "seen_counterparty_amount_share_hist",
    "new_bank_ratio_hist",
    "seen_bank_tx_ratio_hist",
    "repeat_counterparty_mean_recency_hours",
    "repeat_counterparty_min_recency_hours",
}
_TEMPORAL_FEATURE_COLUMNS = {
    "tx_count_1h",
    "tx_count_6h",
    "amount_total_usd_1h",
    "amount_total_usd_6h",
    "amount_share_1h",
    "amount_share_6h",
    "tx_share_1h",
    "tx_share_6h",
    "hours_since_account_first_seen",
    "prior_raw_tx_count",
    "prior_raw_amount_usd",
    "tx_count_vs_prior_anchor_avg",
    "total_amount_usd_vs_prior_anchor_avg",
    "unique_counterparties_vs_prior_anchor_avg",
    "unique_banks_vs_prior_anchor_avg",
    "currency_count_vs_prior_anchor_avg",
    "payment_format_count_vs_prior_anchor_avg",
    "cross_bank_ratio_vs_prior_anchor_avg",
    "round_amount_ratio_vs_prior_anchor_avg",
}
_PAYMENT_CURRENCY_EXTRA_COLUMNS = {
    "currency_entropy",
    "payment_format_entropy",
    "currency_entropy_vs_prior_anchor_avg",
    "payment_format_entropy_vs_prior_anchor_avg",
}
_AMOUNT_EXTRA_COLUMNS = {
    "amount_total_usd_1h",
    "amount_total_usd_6h",
    "amount_share_1h",
    "amount_share_6h",
    "prior_raw_amount_usd",
    "total_amount_usd_vs_prior_anchor_avg",
}


@dataclass(slots=True)
class ProtocolBImprovementResult:
    summary_path: Path
    report_path: Path
    base_feature_csv_path: Path
    extra_feature_csv_path: Path
    dataset_stats: dict[str, Any]
    baseline_results: list[dict[str, Any]]
    candidate_results: list[dict[str, Any]]
    champion: dict[str, Any]
    ablations: list[dict[str, Any]]


def _write_extra_feature_header(path: Path) -> csv.DictWriter:
    _ensure_parent_dir(path)
    handle = path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=_EXTRA_FEATURE_FIELDNAMES)
    writer.writeheader()
    writer._handle = handle  # type: ignore[attr-defined]
    return writer


def _close_feature_writer(writer: csv.DictWriter) -> None:
    handle = getattr(writer, "_handle", None)
    if handle is not None:
        handle.close()


def _protocol_b_v2_stage_path(extra_feature_csv_path: Path) -> Path:
    return extra_feature_csv_path.with_suffix(".protocol_b_v2.stage.sqlite")


def _entropy(values: list[str]) -> float:
    if not values:
        return 0.0
    total = len(values)
    counts = Counter(values)
    return float(-sum((count / total) * math.log(count / total + 1e-12) for count in counts.values()))


def _hhi(values: list[str], *, weights: list[float] | None = None) -> float:
    if not values:
        return 0.0
    if weights is None:
        counts = Counter(values)
        total = sum(counts.values())
        return float(sum((count / total) ** 2 for count in counts.values()))
    weighted = defaultdict(float)
    for key, value in zip(values, weights, strict=False):
        weighted[str(key)] += float(value)
    total = sum(weighted.values())
    if total <= 0:
        return 0.0
    return float(sum((amount / total) ** 2 for amount in weighted.values()))


def _relative_change(current: float, baseline: float) -> float:
    return float((current - baseline) / max(abs(baseline), 1.0))


def _window_stats(
    current_window: list[_ProtocolBTransaction],
    *,
    source_account_key: str,
    prior_counterparties: set[str],
    prior_banks: set[str],
    prior_last_seen_counterparty: dict[str, datetime],
    prior_anchor_averages: dict[str, float],
    account_first_seen: datetime,
    anchor_time: datetime,
    prior_raw_tx_count: int,
    prior_raw_amount_usd: float,
) -> dict[str, Any]:
    amounts_usd = [float(tx.amount) * _fx_rate_to_usd(str(tx.currency)) for tx in current_window]
    total_amount_usd = float(sum(amounts_usd))
    tx_count = len(current_window)
    dest_accounts = [str(tx.destination_account_key) for tx in current_window]
    dest_banks = [str(tx.destination_bank) for tx in current_window]
    currencies = [str(tx.currency) for tx in current_window]
    payment_formats = [str(tx.payment_format) for tx in current_window]
    source_banks = [str(tx.source_bank) for tx in current_window]

    one_hour_boundary = anchor_time - timedelta(hours=1)
    six_hour_boundary = anchor_time - timedelta(hours=6)
    window_1h = [tx for tx in current_window if tx.timestamp > one_hour_boundary]
    window_6h = [tx for tx in current_window if tx.timestamp > six_hour_boundary]

    def _window_amounts(rows: list[_ProtocolBTransaction]) -> float:
        return float(sum(float(tx.amount) * _fx_rate_to_usd(str(tx.currency)) for tx in rows))

    unique_destination_accounts = len(set(dest_accounts))
    unique_destination_banks = len(set(dest_banks))
    currency_count = len(set(currencies))
    payment_format_count = len(set(payment_formats))
    cross_bank_ratio = _safe_ratio(sum(1 for tx in current_window if tx.destination_bank != tx.source_bank), tx_count)
    round_amount_ratio = _safe_ratio(
        sum(1 for tx in current_window if abs(float(tx.amount) - round(float(tx.amount))) < 1e-6),
        tx_count,
    )
    currency_entropy = _entropy(currencies)
    payment_format_entropy = _entropy(payment_formats)

    prior_seen_counterparty_tx = sum(1 for tx in current_window if tx.destination_account_key in prior_counterparties)
    prior_seen_counterparty_amount = sum(
        amount
        for tx, amount in zip(current_window, amounts_usd, strict=False)
        if tx.destination_account_key in prior_counterparties
    )
    prior_seen_bank_tx = sum(1 for tx in current_window if tx.destination_bank in prior_banks)

    recency_values = []
    seen_counterparties_this_window = set()
    for tx in current_window:
        destination = str(tx.destination_account_key)
        if destination in prior_last_seen_counterparty and destination not in seen_counterparties_this_window:
            recency_values.append((anchor_time - prior_last_seen_counterparty[destination]).total_seconds() / 3600.0)
        seen_counterparties_this_window.add(destination)

    row = {
        "tx_count_1h": int(len(window_1h)),
        "tx_count_6h": int(len(window_6h)),
        "amount_total_usd_1h": _window_amounts(window_1h),
        "amount_total_usd_6h": _window_amounts(window_6h),
        "amount_share_1h": _safe_ratio(_window_amounts(window_1h), total_amount_usd),
        "amount_share_6h": _safe_ratio(_window_amounts(window_6h), total_amount_usd),
        "tx_share_1h": _safe_ratio(len(window_1h), tx_count),
        "tx_share_6h": _safe_ratio(len(window_6h), tx_count),
        "unique_destination_accounts_1h": int(len({tx.destination_account_key for tx in window_1h})),
        "unique_destination_accounts_6h": int(len({tx.destination_account_key for tx in window_6h})),
        "unique_destination_banks_1h": int(len({tx.destination_bank for tx in window_1h})),
        "unique_destination_banks_6h": int(len({tx.destination_bank for tx in window_6h})),
        "currency_entropy": currency_entropy,
        "payment_format_entropy": payment_format_entropy,
        "counterparty_hhi_tx": _hhi(dest_accounts),
        "counterparty_hhi_amount": _hhi(dest_accounts, weights=amounts_usd),
        "bank_hhi_tx": _hhi(dest_banks),
        "bank_hhi_amount": _hhi(dest_banks, weights=amounts_usd),
        "fan_out_ratio": _safe_ratio(unique_destination_accounts, tx_count),
        "fan_in_ratio": _safe_ratio(sum(1 for value in Counter(dest_accounts).values() if value > 1), max(unique_destination_accounts, 1)),
        "self_transfer_ratio": _safe_ratio(sum(1 for tx in current_window if tx.destination_account_key == source_account_key), tx_count),
        "new_counterparty_ratio_hist": _safe_ratio(sum(1 for tx in current_window if tx.destination_account_key not in prior_counterparties), tx_count),
        "seen_counterparty_tx_ratio_hist": _safe_ratio(prior_seen_counterparty_tx, tx_count),
        "seen_counterparty_amount_share_hist": _safe_ratio(prior_seen_counterparty_amount, total_amount_usd),
        "new_bank_ratio_hist": _safe_ratio(sum(1 for tx in current_window if tx.destination_bank not in prior_banks), tx_count),
        "seen_bank_tx_ratio_hist": _safe_ratio(prior_seen_bank_tx, tx_count),
        "repeat_counterparty_mean_recency_hours": float(np.mean(recency_values)) if recency_values else float(_DEFAULT_OBSERVATION_HOURS * 30),
        "repeat_counterparty_min_recency_hours": float(min(recency_values)) if recency_values else float(_DEFAULT_OBSERVATION_HOURS * 30),
        "prior_raw_tx_count": int(prior_raw_tx_count),
        "prior_raw_amount_usd": float(prior_raw_amount_usd),
        "hours_since_account_first_seen": float((anchor_time - account_first_seen).total_seconds() / 3600.0),
        "tx_count_vs_prior_anchor_avg": _relative_change(tx_count, prior_anchor_averages["transaction_count"]),
        "total_amount_usd_vs_prior_anchor_avg": _relative_change(total_amount_usd, prior_anchor_averages["total_amount_usd"]),
        "unique_counterparties_vs_prior_anchor_avg": _relative_change(unique_destination_accounts, prior_anchor_averages["unique_destination_accounts"]),
        "unique_banks_vs_prior_anchor_avg": _relative_change(unique_destination_banks, prior_anchor_averages["unique_destination_banks"]),
        "currency_count_vs_prior_anchor_avg": _relative_change(currency_count, prior_anchor_averages["currency_count"]),
        "payment_format_count_vs_prior_anchor_avg": _relative_change(payment_format_count, prior_anchor_averages["payment_format_count"]),
        "cross_bank_ratio_vs_prior_anchor_avg": _relative_change(cross_bank_ratio, prior_anchor_averages["cross_bank_ratio"]),
        "round_amount_ratio_vs_prior_anchor_avg": _relative_change(round_amount_ratio, prior_anchor_averages["round_amount_ratio"]),
        "currency_entropy_vs_prior_anchor_avg": _relative_change(currency_entropy, prior_anchor_averages["currency_entropy"]),
        "payment_format_entropy_vs_prior_anchor_avg": _relative_change(payment_format_entropy, prior_anchor_averages["payment_format_entropy"]),
    }
    return row


def extract_protocol_b_extra_feature_csv_from_alert_jsonl(
    alert_jsonl_path: str | Path,
    output_csv_path: str | Path,
    *,
    force_rebuild: bool = False,
) -> tuple[Path, dict[str, Any]]:
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path, {"reused_existing_extra_feature_csv": True}

    _ensure_parent_dir(output_path)
    stage_path = _protocol_b_v2_stage_path(output_path)
    flatten_summary = _stage_transactions_from_alert_jsonl(alert_jsonl_path, stage_path)
    writer = _write_extra_feature_header(output_path)
    rows_written = 0
    accounts_processed = 0
    connection = sqlite3.connect(str(stage_path))
    connection.row_factory = sqlite3.Row
    try:
        current_account: str | None = None
        current_rows: list[_ProtocolBTransaction] = []
        cursor = connection.execute(
            """
            SELECT
                transaction_id, source_account_key, source_bank, destination_account_key, destination_bank,
                amount, currency, payment_format, timestamp_iso, is_laundering, pattern_typology
            FROM staged_transactions
            ORDER BY source_account_key, timestamp_iso, transaction_id
            """
        )

        def flush_account(rows: list[_ProtocolBTransaction]) -> None:
            nonlocal rows_written, accounts_processed
            if not rows:
                return
            accounts_processed += 1
            anchors: list[tuple[int, datetime]] = []
            current_anchor_start: datetime | None = None
            observation_delta = timedelta(hours=_DEFAULT_OBSERVATION_HOURS)
            outcome_delta = timedelta(hours=_DEFAULT_OUTCOME_HOURS)
            for index, tx in enumerate(rows):
                if current_anchor_start is None or tx.timestamp >= current_anchor_start + outcome_delta:
                    anchors.append((index, tx.timestamp))
                    current_anchor_start = tx.timestamp

            past_left = 0
            past_right = 0
            future_right = 0
            history_right = 0
            prior_counterparties: set[str] = set()
            prior_banks: set[str] = set()
            prior_last_seen_counterparty: dict[str, datetime] = {}
            prior_raw_tx_count = 0
            prior_raw_amount_usd = 0.0
            prior_anchor_count = 0
            prior_anchor_sums = defaultdict(float)
            account_first_seen = rows[0].timestamp

            for anchor_index, anchor_time in anchors:
                while history_right < len(rows) and rows[history_right].timestamp < anchor_time:
                    historical_tx = rows[history_right]
                    prior_counterparties.add(str(historical_tx.destination_account_key))
                    prior_banks.add(str(historical_tx.destination_bank))
                    prior_last_seen_counterparty[str(historical_tx.destination_account_key)] = historical_tx.timestamp
                    prior_raw_tx_count += 1
                    prior_raw_amount_usd += float(historical_tx.amount) * _fx_rate_to_usd(str(historical_tx.currency))
                    history_right += 1
                while past_left < len(rows) and rows[past_left].timestamp < anchor_time - observation_delta:
                    past_left += 1
                while past_right < len(rows) and rows[past_right].timestamp <= anchor_time:
                    past_right += 1
                while future_right < len(rows) and rows[future_right].timestamp <= anchor_time + outcome_delta:
                    future_right += 1
                past_window = rows[past_left:past_right]
                future_window = rows[past_right:future_right]
                if not past_window:
                    continue

                prior_anchor_averages = {
                    "transaction_count": _safe_ratio(prior_anchor_sums["transaction_count"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "total_amount_usd": _safe_ratio(prior_anchor_sums["total_amount_usd"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "unique_destination_accounts": _safe_ratio(prior_anchor_sums["unique_destination_accounts"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "unique_destination_banks": _safe_ratio(prior_anchor_sums["unique_destination_banks"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "currency_count": _safe_ratio(prior_anchor_sums["currency_count"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "payment_format_count": _safe_ratio(prior_anchor_sums["payment_format_count"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "cross_bank_ratio": _safe_ratio(prior_anchor_sums["cross_bank_ratio"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "round_amount_ratio": _safe_ratio(prior_anchor_sums["round_amount_ratio"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "currency_entropy": _safe_ratio(prior_anchor_sums["currency_entropy"], prior_anchor_count) if prior_anchor_count else 0.0,
                    "payment_format_entropy": _safe_ratio(prior_anchor_sums["payment_format_entropy"], prior_anchor_count) if prior_anchor_count else 0.0,
                }
                extra_row = _window_stats(
                    past_window,
                    source_account_key=rows[anchor_index].source_account_key,
                    prior_counterparties=prior_counterparties,
                    prior_banks=prior_banks,
                    prior_last_seen_counterparty=prior_last_seen_counterparty,
                    prior_anchor_averages=prior_anchor_averages,
                    account_first_seen=account_first_seen,
                    anchor_time=anchor_time,
                    prior_raw_tx_count=prior_raw_tx_count,
                    prior_raw_amount_usd=prior_raw_amount_usd,
                )
                row = {
                    "alert_id": _deterministic_variant_alert_id(
                        _DEFAULT_GROUPING_VARIANT,
                        rows[anchor_index].source_account_key,
                        _isoformat_utc(anchor_time),
                        rows[anchor_index].transaction_id,
                    ),
                    **extra_row,
                }
                writer.writerow(row)
                rows_written += 1
                prior_anchor_count += 1
                prior_anchor_sums["transaction_count"] += len(past_window)
                prior_anchor_sums["total_amount_usd"] += float(sum(float(tx.amount) * _fx_rate_to_usd(str(tx.currency)) for tx in past_window))
                prior_anchor_sums["unique_destination_accounts"] += len({tx.destination_account_key for tx in past_window})
                prior_anchor_sums["unique_destination_banks"] += len({tx.destination_bank for tx in past_window})
                prior_anchor_sums["currency_count"] += len({tx.currency for tx in past_window})
                prior_anchor_sums["payment_format_count"] += len({tx.payment_format for tx in past_window})
                prior_anchor_sums["cross_bank_ratio"] += _safe_ratio(sum(1 for tx in past_window if tx.destination_bank != tx.source_bank), len(past_window))
                prior_anchor_sums["round_amount_ratio"] += _safe_ratio(
                    sum(1 for tx in past_window if abs(float(tx.amount) - round(float(tx.amount))) < 1e-6),
                    len(past_window),
                )
                prior_anchor_sums["currency_entropy"] += _entropy([str(tx.currency) for tx in past_window])
                prior_anchor_sums["payment_format_entropy"] += _entropy([str(tx.payment_format) for tx in past_window])

        for raw in cursor:
            tx = _row_to_transaction(raw)
            if current_account is None:
                current_account = tx.source_account_key
            if tx.source_account_key != current_account:
                flush_account(current_rows)
                current_rows = []
                current_account = tx.source_account_key
            current_rows.append(tx)
        flush_account(current_rows)
    finally:
        connection.close()
        _close_feature_writer(writer)
        if stage_path.exists():
            stage_path.unlink()

    return output_path, {
        **flatten_summary,
        "rows_written": int(rows_written),
        "accounts_processed": int(accounts_processed),
        "reused_existing_extra_feature_csv": False,
    }


def _add_protocol_b_v2_derived_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy(deep=False)
    out["log_tx_count_1h"] = np.log1p(np.clip(out["tx_count_1h"], 0.0, None))
    out["log_tx_count_6h"] = np.log1p(np.clip(out["tx_count_6h"], 0.0, None))
    out["log_amount_total_usd_1h"] = np.log1p(np.clip(out["amount_total_usd_1h"], 0.0, None))
    out["log_amount_total_usd_6h"] = np.log1p(np.clip(out["amount_total_usd_6h"], 0.0, None))
    out["log_prior_raw_tx_count"] = np.log1p(np.clip(out["prior_raw_tx_count"], 0.0, None))
    out["log_prior_raw_amount_usd"] = np.log1p(np.clip(out["prior_raw_amount_usd"], 0.0, None))
    out["log_hours_since_account_first_seen"] = np.log1p(np.clip(out["hours_since_account_first_seen"], 0.0, None))
    out["counterparty_recency_inverse"] = 1.0 / (1.0 + np.clip(out["repeat_counterparty_mean_recency_hours"], 0.0, None))
    out["counterparty_recency_min_inverse"] = 1.0 / (1.0 + np.clip(out["repeat_counterparty_min_recency_hours"], 0.0, None))
    out["recent_bank_diversity_share"] = np.where(
        out["unique_destination_banks"] > 0,
        out["unique_destination_banks_6h"] / out["unique_destination_banks"],
        0.0,
    )
    out["recent_counterparty_diversity_share"] = np.where(
        out["unique_destination_accounts"] > 0,
        out["unique_destination_accounts_6h"] / out["unique_destination_accounts"],
        0.0,
    )
    out["recent_amount_intensity"] = np.where(
        out["total_amount_usd"] > 0,
        out["amount_total_usd_6h"] / out["total_amount_usd"],
        0.0,
    )
    return out


def load_protocol_b_improved_feature_frame(
    base_feature_csv_path: str | Path,
    extra_feature_csv_path: str | Path,
) -> pd.DataFrame:
    base_frame = load_protocol_b_feature_frame(base_feature_csv_path)
    extra_frame = pd.read_csv(extra_feature_csv_path)
    int_like = {
        "tx_count_1h",
        "tx_count_6h",
        "unique_destination_accounts_1h",
        "unique_destination_accounts_6h",
        "unique_destination_banks_1h",
        "unique_destination_banks_6h",
        "prior_raw_tx_count",
    }
    for column in extra_frame.columns:
        if column == "alert_id":
            extra_frame[column] = extra_frame[column].astype(str)
            continue
        numeric = pd.to_numeric(extra_frame[column], errors="coerce").fillna(0.0)
        extra_frame[column] = numeric.astype(np.int32 if column in int_like else np.float32)
    merged = base_frame.merge(extra_frame, on="alert_id", how="left", validate="one_to_one")
    for column in _EXTRA_FEATURE_FIELDNAMES:
        if column == "alert_id" or column not in merged.columns:
            continue
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0.0)
    merged = _add_protocol_b_v2_derived_features(_add_derived_features(merged))
    numeric_columns = [
        col for col in merged.columns
        if col not in {
            "alert_id",
            "created_at",
            "grouping_variant",
            "source_account_key",
            "source_bank",
            "dominant_destination_bank",
            "dominant_currency",
            "dominant_payment_format",
            "typology",
        }
    ]
    merged[numeric_columns] = merged[numeric_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return merged


def _dataset_stats(frame: pd.DataFrame) -> dict[str, Any]:
    total = len(frame)
    positives = int(frame["evaluation_label_is_sar"].sum()) if total else 0
    return {
        "total_alerts": int(total),
        "positive_alerts": positives,
        "negative_alerts": int(max(total - positives, 0)),
        "positive_rate": float(positives / total) if total else 0.0,
        "average_transactions_per_alert": float(frame["transaction_count"].mean()) if total else 0.0,
    }


def _primary_feature_columns(frame: pd.DataFrame) -> list[str]:
    feature_columns = _numeric_feature_columns(frame, include_pattern_exact=True)
    excluded = {"pattern_assigned", *(_PATTERN_FEATURES)}
    return [column for column in feature_columns if column not in excluded]


def _metric_tuple(metrics: dict[str, Any]) -> tuple[float, float, float, float, float]:
    review = metrics.get("review_reduction_at_80pct_recall") or {}
    return (
        float(metrics.get("recall_at_top_10pct", 0.0)),
        float(metrics.get("recall_at_top_20pct", 0.0)),
        float(metrics.get("precision_at_top_10pct", 0.0)),
        float(metrics.get("precision_at_top_20pct", 0.0)),
        float(review.get("review_reduction", 0.0)),
    )


def _series_zscore(values: pd.Series) -> pd.Series:
    mean = float(values.mean()) if len(values) else 0.0
    std = float(values.std()) if len(values) else 0.0
    if std <= 1e-9:
        return pd.Series(np.zeros(len(values), dtype=np.float32), index=values.index)
    return pd.Series((values - mean) / std, index=values.index, dtype=np.float32)


def _rank_percentile(values: pd.Series) -> pd.Series:
    return values.rank(method="average", pct=True).astype(np.float32)


def _compute_hard_negative_weights(train_df: pd.DataFrame) -> tuple[np.ndarray, dict[str, Any]]:
    labels = train_df["evaluation_label_is_sar"].astype(int)
    weights = np.ones(len(train_df), dtype=np.float32)
    amount_score = _rank_percentile(pd.to_numeric(train_df["log_total_amount_usd"], errors="coerce").fillna(0.0))
    tx_score = _rank_percentile(pd.to_numeric(train_df["log_transaction_count"], errors="coerce").fillna(0.0))
    breadth_score = _rank_percentile(pd.to_numeric(train_df["unique_destination_accounts"], errors="coerce").fillna(0.0))
    heuristic_score = (
        _DEFAULT_WEIGHTED_HEURISTIC["amount"] * amount_score
        + _DEFAULT_WEIGHTED_HEURISTIC["transaction_count"] * tx_score
        + _DEFAULT_WEIGHTED_HEURISTIC["counterparties"] * breadth_score
    )
    negative_mask = labels == 0
    top_10 = negative_mask & ((amount_score >= 0.90) | (heuristic_score >= 0.90))
    top_20 = negative_mask & ~top_10 & ((amount_score >= 0.80) | (heuristic_score >= 0.80))
    weights[top_10.to_numpy()] = _DEFAULT_HARD_NEGATIVE_WEIGHTS["top_10pct_negative_weight"]
    weights[top_20.to_numpy()] = _DEFAULT_HARD_NEGATIVE_WEIGHTS["top_20pct_negative_weight"]
    diagnostics = {
        "negative_alerts": int(negative_mask.sum()),
        "top_10pct_hard_negatives": int(top_10.sum()),
        "top_20pct_hard_negatives": int(top_20.sum()),
        "top_10pct_negative_weight": float(_DEFAULT_HARD_NEGATIVE_WEIGHTS["top_10pct_negative_weight"]),
        "top_20pct_negative_weight": float(_DEFAULT_HARD_NEGATIVE_WEIGHTS["top_20pct_negative_weight"]),
    }
    return weights, diagnostics


def _fit_logistic_candidate_with_weights(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    name: str,
    notes: str,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any]:
    pipeline = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=500,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )
    fit_kwargs: dict[str, Any] = {}
    if sample_weight is not None:
        fit_kwargs["model__sample_weight"] = sample_weight
    pipeline.fit(train_df[feature_columns], train_df["evaluation_label_is_sar"], **fit_kwargs)
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
    )[:15]
    return {
        "name": name,
        "kind": "model",
        "family": "logistic_regression",
        "notes": notes,
        "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
        "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
        "top_features": top_features,
        "test_scores": test_scores.astype(np.float32),
        "validation_scores": validation_scores.astype(np.float32),
        "model_object": pipeline,
        "uses_hard_negative_weighting": sample_weight is not None,
        "feature_columns": list(feature_columns),
    }


def _fit_lightgbm_candidate(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    name: str,
    notes: str,
    sample_weight: np.ndarray | None = None,
) -> dict[str, Any] | None:
    if lgb is None:
        return None
    y_train = train_df["evaluation_label_is_sar"].astype(int)
    positive_rate = float(y_train.mean()) if len(y_train) else 0.0
    scale_pos_weight = float((1.0 - positive_rate) / positive_rate) if positive_rate > 0 else 1.0
    model = lgb.LGBMClassifier(
        objective="binary",
        metric="average_precision",
        learning_rate=0.035 if sample_weight is not None else 0.05,
        num_leaves=127 if sample_weight is not None else 63,
        min_child_samples=45 if sample_weight is not None else 80,
        feature_fraction=0.85,
        bagging_fraction=0.85,
        bagging_freq=5,
        lambda_l1=0.0 if sample_weight is not None else 0.1,
        lambda_l2=0.1 if sample_weight is not None else 0.2,
        n_estimators=850 if sample_weight is not None else 700,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
        scale_pos_weight=scale_pos_weight,
    )
    callbacks = [
        lgb.early_stopping(stopping_rounds=50, verbose=False),
        lgb.log_evaluation(period=0),
    ]
    model.fit(
        train_df[feature_columns],
        y_train,
        sample_weight=sample_weight,
        eval_set=[(validation_df[feature_columns], validation_df["evaluation_label_is_sar"].astype(int))],
        eval_metric="average_precision",
        callbacks=callbacks,
    )
    validation_scores = model.predict_proba(validation_df[feature_columns])[:, 1]
    test_scores = model.predict_proba(test_df[feature_columns])[:, 1]
    validation_scored = validation_df.copy()
    validation_scored["model_score"] = validation_scores.astype(np.float32)
    test_scored = test_df.copy()
    test_scored["model_score"] = test_scores.astype(np.float32)
    top_features = sorted(
        (
            {"feature": feature, "importance": float(weight)}
            for feature, weight in zip(feature_columns, model.feature_importances_, strict=False)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:15]
    return {
        "name": name,
        "kind": "model",
        "family": "lightgbm",
        "notes": notes,
        "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
        "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
        "top_features": top_features,
        "test_scores": test_scores.astype(np.float32),
        "validation_scores": validation_scores.astype(np.float32),
        "model_object": model,
        "scale_pos_weight": scale_pos_weight,
        "uses_hard_negative_weighting": sample_weight is not None,
        "feature_columns": list(feature_columns),
    }


def _fit_sparse_logistic_candidate(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    name: str,
    notes: str,
    c_value: float = 0.2,
    l1_ratio: float = 1.0,
) -> dict[str, Any]:
    pipeline = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1800,
                    solver="saga",
                    class_weight="balanced",
                    random_state=42,
                    C=c_value,
                    l1_ratio=l1_ratio,
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
    )[:15]
    return {
        "name": name,
        "kind": "model",
        "family": "sparse_logistic_regression",
        "notes": notes,
        "validation_metrics": _ranking_metrics_from_scores(validation_scored, "model_score"),
        "test_metrics": _ranking_metrics_from_scores(test_scored, "model_score"),
        "top_features": top_features,
        "validation_scores": validation_scores.astype(np.float32),
        "test_scores": test_scores.astype(np.float32),
        "model_object": pipeline,
        "uses_hard_negative_weighting": False,
        "feature_columns": list(feature_columns),
        "regularization": {
            "c_value": float(c_value),
            "l1_ratio": float(l1_ratio),
        },
    }


def _score_distribution(scores: np.ndarray, labels: pd.Series) -> dict[str, Any]:
    series = pd.Series(scores, index=labels.index, dtype=np.float32)
    positive_scores = series[labels == 1]
    negative_scores = series[labels == 0]

    def _summary(values: pd.Series) -> dict[str, float]:
        if values.empty:
            return {"mean": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0}
        return {
            "mean": float(values.mean()),
            "p10": float(values.quantile(0.10)),
            "p50": float(values.quantile(0.50)),
            "p90": float(values.quantile(0.90)),
        }

    return {
        "positive_scores": _summary(positive_scores),
        "negative_scores": _summary(negative_scores),
    }


def _decile_breakdown(frame: pd.DataFrame, scores: np.ndarray) -> list[dict[str, Any]]:
    scored = frame[["alert_id", "evaluation_label_is_sar"]].copy()
    scored["model_score"] = pd.Series(scores, index=frame.index, dtype=np.float32)
    ordered = scored.sort_values("model_score", ascending=False, kind="stable").reset_index(drop=True)
    base_rate = float(ordered["evaluation_label_is_sar"].mean()) if len(ordered) else 0.0
    rows: list[dict[str, Any]] = []
    if ordered.empty:
        return rows
    for decile in range(10):
        start = int(len(ordered) * decile / 10)
        end = int(len(ordered) * (decile + 1) / 10)
        bucket = ordered.iloc[start:end]
        positive_rate = float(bucket["evaluation_label_is_sar"].mean()) if len(bucket) else 0.0
        rows.append(
            {
                "decile": int(decile + 1),
                "alerts": int(len(bucket)),
                "positive_alerts": int(bucket["evaluation_label_is_sar"].sum()),
                "positive_rate": positive_rate,
                "uplift_vs_base_rate": float(positive_rate / base_rate) if base_rate > 0 else 0.0,
            }
        )
    return rows


def _top_k_mask(frame: pd.DataFrame, scores: np.ndarray, fraction: float = 0.10) -> pd.Series:
    scored = frame[["alert_id"]].copy()
    scored["model_score"] = pd.Series(scores, index=frame.index, dtype=np.float32)
    ordered_index = scored.sort_values(["model_score", "alert_id"], ascending=[False, True], kind="stable").index
    top_n = max(1, math.ceil(len(scored) * fraction))
    mask = pd.Series(False, index=frame.index)
    mask.loc[ordered_index[:top_n]] = True
    return mask


def _sample_alerts(frame: pd.DataFrame, mask: pd.Series, *, limit: int = _TOP_ALERT_SAMPLE) -> list[dict[str, Any]]:
    columns = [
        "alert_id",
        "created_at",
        "source_account_key",
        "transaction_count",
        "total_amount_usd",
        "unique_destination_accounts",
        "currency_count",
        "payment_format_count",
    ]
    subset = frame.loc[mask, columns].head(limit).copy()
    subset["created_at"] = subset["created_at"].astype(str)
    return subset.to_dict(orient="records")


def _current_champion_diagnosis(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = _fit_logistic_candidate(
        train_df,
        validation_df,
        test_df,
        feature_columns,
        name="althea_protocol_b_logistic_regression_reference",
        notes="Reference Protocol B v1 logistic regression retrained on the base strict feature set.",
    )
    weighted_candidates = _compute_baselines(train_df, validation_df, test_df)
    weighted = next(row for row in weighted_candidates if row["name"] == "weighted_signal_heuristic")
    weighted_scores = pd.Series(dtype=np.float32)
    weighted_weights = weighted.get("weights") or _DEFAULT_WEIGHTED_HEURISTIC
    amount_z = _series_zscore(pd.to_numeric(test_df["log_total_amount_usd"], errors="coerce").fillna(0.0))
    tx_z = _series_zscore(pd.to_numeric(test_df["log_transaction_count"], errors="coerce").fillna(0.0))
    breadth_z = _series_zscore(pd.to_numeric(test_df["unique_destination_accounts"], errors="coerce").fillna(0.0))
    weighted_scores = (
        weighted_weights["amount"] * amount_z
        + weighted_weights["transaction_count"] * tx_z
        + weighted_weights["counterparties"] * breadth_z
    ).to_numpy(dtype=np.float32)
    current_model = current.get("model_object")
    if current_model is None:
        raise RuntimeError("Reference Protocol B model object is missing for diagnosis")
    current_scores = np.asarray(current_model.predict_proba(test_df[feature_columns])[:, 1], dtype=np.float32)
    current_top = _top_k_mask(test_df, current_scores)
    weighted_top = _top_k_mask(test_df, weighted_scores)
    positives = test_df["evaluation_label_is_sar"].astype(int) == 1
    false_negative_mask = positives & ~current_top
    heuristic_only_mask = positives & weighted_top & ~current_top

    diagnostics = {
        "reference_metrics": current["test_metrics"],
        "feature_importance": {
            "top_positive": [row for row in current["top_features"] if row["coefficient"] > 0][:10],
            "top_negative": [row for row in current["top_features"] if row["coefficient"] < 0][:10],
        },
        "score_distribution": _score_distribution(current_scores, test_df["evaluation_label_is_sar"]),
        "decile_breakdown": _decile_breakdown(test_df, current_scores),
        "top_false_negatives": _sample_alerts(
            test_df.sort_values(
                by=["evaluation_label_is_sar", "total_amount_usd", "transaction_count"],
                ascending=[False, False, False],
                kind="stable",
            ),
            false_negative_mask.reindex(test_df.sort_values(
                by=["evaluation_label_is_sar", "total_amount_usd", "transaction_count"],
                ascending=[False, False, False],
                kind="stable",
            ).index, fill_value=False),
        ),
        "positives_captured_by_weighted_heuristic_but_missed_by_current": _sample_alerts(
            test_df.sort_values(by=["total_amount_usd", "transaction_count"], ascending=[False, False], kind="stable"),
            heuristic_only_mask.reindex(
                test_df.sort_values(by=["total_amount_usd", "transaction_count"], ascending=[False, False], kind="stable").index,
                fill_value=False,
            ),
        ),
        "heuristic_only_positive_count_top_10pct": int(heuristic_only_mask.sum()),
        "current_false_negative_count_top_10pct": int(false_negative_mask.sum()),
    }
    current.pop("model_object", None)
    current.pop("validation_scores", None)
    current.pop("test_scores", None)
    return diagnostics, current


def _serialize_result(result: dict[str, Any]) -> dict[str, Any]:
    row = dict(result)
    row.pop("model_object", None)
    row.pop("validation_scores", None)
    row.pop("test_scores", None)
    return row


def _run_candidate_models(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    base_feature_columns: list[str],
    *,
    include_lightgbm: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    hard_negative_weights, weighting_diagnostics = _compute_hard_negative_weights(train_df)
    candidates: list[dict[str, Any]] = []
    sparse_feature_sets = {
        "protocol_b_v2_sparse_logistic_base": list(base_feature_columns),
        "protocol_b_v2_sparse_logistic_graph_entropy": list(
            base_feature_columns
            + [
                column
                for column in feature_columns
                if column in {
                    "counterparty_hhi_tx",
                    "counterparty_hhi_amount",
                    "bank_hhi_tx",
                    "bank_hhi_amount",
                    "seen_counterparty_tx_ratio_hist",
                    "seen_counterparty_amount_share_hist",
                    "new_counterparty_ratio_hist",
                    "new_bank_ratio_hist",
                    "currency_entropy",
                    "payment_format_entropy",
                    "currency_entropy_vs_prior_anchor_avg",
                    "payment_format_entropy_vs_prior_anchor_avg",
                }
            ]
        ),
        "protocol_b_v2_sparse_logistic_recent_entropy": list(
            base_feature_columns
            + [
                column
                for column in feature_columns
                if column in {
                    "tx_count_6h",
                    "tx_share_6h",
                    "amount_share_6h",
                    "recent_amount_intensity",
                    "recent_counterparty_diversity_share",
                    "recent_bank_diversity_share",
                    "currency_entropy",
                    "payment_format_entropy",
                    "currency_entropy_vs_prior_anchor_avg",
                    "payment_format_entropy_vs_prior_anchor_avg",
                }
            ]
        ),
        "protocol_b_v2_sparse_logistic_behavior_shift": list(
            base_feature_columns
            + [
                column
                for column in feature_columns
                if column in {
                    "tx_count_vs_prior_anchor_avg",
                    "total_amount_usd_vs_prior_anchor_avg",
                    "unique_counterparties_vs_prior_anchor_avg",
                    "unique_banks_vs_prior_anchor_avg",
                    "currency_count_vs_prior_anchor_avg",
                    "payment_format_count_vs_prior_anchor_avg",
                }
            ]
        ),
    }
    for name, columns in sparse_feature_sets.items():
        candidates.append(
            _fit_sparse_logistic_candidate(
                train_df,
                validation_df,
                test_df,
                columns,
                name=name,
                notes=(
                    "Sparse saga logistic regression over the strict Protocol B base feature space."
                    if name.endswith("_base")
                    else "Sparse saga logistic regression over the strict Protocol B base feature space plus a validation-selected past-only feature subset."
                ),
            )
        )
    logistic_enhanced = _fit_logistic_candidate_with_weights(
        train_df,
        validation_df,
        test_df,
        feature_columns,
        name="protocol_b_v2_logistic_regression",
        notes="Enhanced strict Protocol B logistic regression with past-only temporal, graph, and behavior-shift features.",
    )
    candidates.append(logistic_enhanced)
    logistic_hardneg = _fit_logistic_candidate_with_weights(
        train_df,
        validation_df,
        test_df,
        feature_columns,
        name="protocol_b_v2_logistic_regression_hardneg",
        notes="Enhanced strict Protocol B logistic regression with hard-negative weighting for alerts that rank highly on amount and heuristic baselines but remain negative.",
        sample_weight=hard_negative_weights,
    )
    candidates.append(logistic_hardneg)
    if include_lightgbm:
        lightgbm_plain = _fit_lightgbm_candidate(
            train_df,
            validation_df,
            test_df,
            feature_columns,
            name="protocol_b_v2_lightgbm",
            notes="Enhanced strict Protocol B LightGBM over past-only temporal, graph, and behavior-shift features.",
        )
        if lightgbm_plain is not None:
            candidates.append(lightgbm_plain)
        lightgbm_hardneg = _fit_lightgbm_candidate(
            train_df,
            validation_df,
            test_df,
            feature_columns,
            name="protocol_b_v2_lightgbm_hardneg",
            notes="Enhanced strict Protocol B LightGBM with hard-negative weighting focused on amount-heavy false-positive lookalikes.",
            sample_weight=hard_negative_weights,
        )
        if lightgbm_hardneg is not None:
            candidates.append(lightgbm_hardneg)
    champion = max(candidates, key=lambda row: _metric_tuple(row["validation_metrics"]))
    return candidates, {
        "hard_negative_strategy": {
            "description": "Negative alerts in the top 10%-20% of amount/weighted-heuristic rankings receive extra loss weight during training.",
            **weighting_diagnostics,
        },
        "validation_selection_metric": [
            "recall_at_top_10pct",
            "recall_at_top_20pct",
            "precision_at_top_10pct",
            "precision_at_top_20pct",
            "review_reduction_at_80pct_recall",
        ],
        "selected_champion_name": champion["name"],
    }


def _run_champion_ablation(
    *,
    champion: dict[str, Any],
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> list[dict[str, Any]]:
    base_family = str(champion.get("family") or "logistic_regression")
    use_hardneg = bool(champion.get("uses_hard_negative_weighting"))
    hard_negative_weights, _ = _compute_hard_negative_weights(train_df)
    champion_feature_columns = list(champion.get("feature_columns") or feature_columns)

    def fit_ablation(name: str, columns: list[str], notes: str) -> dict[str, Any]:
        if base_family == "lightgbm":
            row = _fit_lightgbm_candidate(
                train_df,
                validation_df,
                test_df,
                columns,
                name=name,
                notes=notes,
                sample_weight=hard_negative_weights if use_hardneg else None,
            )
            assert row is not None
            return row
        if base_family == "sparse_logistic_regression":
            return _fit_sparse_logistic_candidate(
                train_df,
                validation_df,
                test_df,
                columns,
                name=name,
                notes=notes,
            )
        return _fit_logistic_candidate_with_weights(
            train_df,
            validation_df,
            test_df,
            columns,
            name=name,
            notes=notes,
            sample_weight=hard_negative_weights if use_hardneg else None,
        )

    ablations = []
    specs = (
        (
            "ablation_amount_features_v2",
            _AMOUNT_ABLATION_COLUMNS | _AMOUNT_EXTRA_COLUMNS,
            "Removed amount totals, normalized amount, and recent amount-share features.",
        ),
        (
            "ablation_history_temporal_features_v2",
            _HISTORY_ABLATION_COLUMNS | _TEMPORAL_FEATURE_COLUMNS,
            "Removed prior-anchor history, multi-window temporal, and behavior-shift features.",
        ),
        (
            "ablation_payment_currency_mix_features_v2",
            _PAYMENT_CURRENCY_ABLATION_COLUMNS | _PAYMENT_CURRENCY_EXTRA_COLUMNS,
            "Removed payment-format, currency-mix, entropy, and mix-shift features.",
        ),
        (
            "ablation_graph_network_features_v2",
            _GRAPH_FEATURE_COLUMNS,
            "Removed counterparty concentration, breadth, novelty, and network-shape features.",
        ),
    )
    champion_recall = float(champion["test_metrics"]["recall_at_top_10pct"])
    for name, excluded, notes in specs:
        cols = [column for column in champion_feature_columns if column not in excluded]
        result = fit_ablation(name, cols, notes)
        result["delta_recall_at_top_10pct_vs_champion"] = float(result["test_metrics"]["recall_at_top_10pct"] - champion_recall)
        ablations.append(_serialize_result(result))
    return ablations


def _comparison_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comparison: list[dict[str, Any]] = []
    for row in rows:
        metrics = row["test_metrics"]
        review = metrics.get("review_reduction_at_80pct_recall") or {}
        comparison.append(
            {
                "name": row["name"],
                "kind": row["kind"],
                "family": row.get("family"),
                "recall_at_top_10pct": float(metrics.get("recall_at_top_10pct", 0.0)),
                "recall_at_top_20pct": float(metrics.get("recall_at_top_20pct", 0.0)),
                "precision_at_top_10pct": float(metrics.get("precision_at_top_10pct", 0.0)),
                "precision_at_top_20pct": float(metrics.get("precision_at_top_20pct", 0.0)),
                "review_reduction_at_80pct_recall": float(review.get("review_reduction", 0.0)),
                "pr_auc": float(metrics.get("pr_auc", 0.0)),
                "notes": row.get("notes"),
            }
        )
    return comparison


def _load_protocol_b_reference_summary() -> dict[str, Any] | None:
    path = _repo_root() / "reports" / _REFERENCE_PROTOCOL_B_SUMMARY
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _render_report(summary: dict[str, Any]) -> str:
    benchmark_rows = summary["benchmark_rows"]
    current = summary["current_protocol_b_reference"]["test_metrics"]
    champion = summary["champion"]["test_metrics"]
    lines = [
        "# ALTHEA IBM AML Benchmark Protocol B v2",
        "",
        "This run keeps Protocol B strict:",
        "- past-only feature construction",
        "- future-only labels",
        "- chronological split",
        "- no pattern-derived shortcut features in the primary score",
        "",
        "## Dataset",
        "",
        f"- Source alerts: `{_display_path(summary['source_alert_jsonl_path'])}`",
        f"- Reused base feature cache: `{_display_path(summary['base_feature_csv_path'])}`",
        f"- Extra strict v2 feature cache: `{_display_path(summary['extra_feature_csv_path'])}`",
        f"- Total alerts: `{summary['dataset_stats']['total_alerts']}`",
        f"- Positive alerts: `{summary['dataset_stats']['positive_alerts']}`",
        f"- Negative alerts: `{summary['dataset_stats']['negative_alerts']}`",
        "",
        "## Current Champion Diagnosis",
        "",
        f"- Current Protocol B v1 Recall@Top 10%: `{current['recall_at_top_10pct']:.4f}`",
        f"- Current Protocol B v1 Precision@Top 10%: `{current['precision_at_top_10pct']:.4f}`",
        f"- Positives captured by weighted heuristic but missed by current champion in top decile: `{summary['diagnosis']['heuristic_only_positive_count_top_10pct']}`",
        f"- Current top-decile false negatives: `{summary['diagnosis']['current_false_negative_count_top_10pct']}`",
        "",
        "## Model Comparison",
        "",
        "| Candidate | Kind | Family | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Precision@Top 20% | Review reduction @ 80% recall | PR-AUC |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in benchmark_rows:
        lines.append(
            "| "
            + f"{row['name']} | {row['kind']} | {row.get('family') or '-'} | "
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
            f"- Selected champion: `{summary['champion']['name']}`",
            f"- Recall@Top 10%: `{champion['recall_at_top_10pct']:.4f}`",
            f"- Recall@Top 20%: `{champion['recall_at_top_20pct']:.4f}`",
            f"- Precision@Top 10%: `{champion['precision_at_top_10pct']:.4f}`",
            f"- Precision@Top 20%: `{champion['precision_at_top_20pct']:.4f}`",
            f"- PR-AUC: `{champion['pr_auc']:.4f}`",
            f"- Improved beyond current Protocol B reference `{_CURRENT_PROTOCOL_B_RECALL:.4f}`: `{champion['recall_at_top_10pct'] > _CURRENT_PROTOCOL_B_RECALL}`",
            "",
            "## Ablation Safety Check",
            "",
            "| Ablation | Recall@Top 10% | Recall@Top 20% | Precision@Top 10% | Delta vs champion |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["ablations"]:
        metrics = row["test_metrics"]
        lines.append(
            "| "
            + f"{row['name']} | "
            + f"{metrics['recall_at_top_10pct']:.4f} | "
            + f"{metrics['recall_at_top_20pct']:.4f} | "
            + f"{metrics['precision_at_top_10pct']:.4f} | "
            + f"{row['delta_recall_at_top_10pct_vs_champion']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Strictness Guardrails",
            "",
            "- Benchmark convenience reduced further: `True`",
            "- Future information added to features: `False`",
            "- Pattern-derived shortcuts restored into primary score: `False`",
            "- Test data used for tuning: `False`",
            "",
            "## Notes",
            "",
            "- This remains a synthetic IBM-derived benchmark, not live bank validation.",
            "- Stronger numbers here should still be treated as internal benchmark evidence only.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_protocol_b_improvement_benchmark(
    *,
    alert_jsonl_path: str | Path,
    base_feature_csv_path: str | Path,
    extra_feature_csv_path: str | Path,
    report_path: str | Path,
    summary_path: str | Path,
    force_rebuild_extra_features: bool = False,
    include_lightgbm: bool = True,
) -> ProtocolBImprovementResult:
    extra_feature_path, extra_extraction_summary = extract_protocol_b_extra_feature_csv_from_alert_jsonl(
        alert_jsonl_path,
        extra_feature_csv_path,
        force_rebuild=force_rebuild_extra_features,
    )
    frame = load_protocol_b_improved_feature_frame(base_feature_csv_path, extra_feature_path)
    dataset_stats = _dataset_stats(frame)
    splits = _split_frame(frame)
    split_stats = {name: _dataset_stats(dataset) for name, dataset in splits.items()}
    feature_columns = _primary_feature_columns(frame)
    for split in splits.values():
        split[feature_columns] = split[feature_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0).astype(np.float32)

    reference_frame = _add_derived_features(load_protocol_b_feature_frame(base_feature_csv_path))
    reference_splits = _split_frame(reference_frame)
    reference_feature_columns = _primary_feature_columns(reference_frame)
    for split in reference_splits.values():
        split[reference_feature_columns] = (
            split[reference_feature_columns]
            .replace([np.inf, -np.inf], 0.0)
            .fillna(0.0)
            .astype(np.float32)
        )
    diagnosis, current_reference = _current_champion_diagnosis(
        reference_splits["train"],
        reference_splits["validation"],
        reference_splits["test"],
        reference_feature_columns,
    )

    baseline_results = [
        _serialize_result(row)
        for row in _compute_baselines(splits["train"], splits["validation"], splits["test"])
        if row["name"] in {
            "chronological_queue",
            "amount_descending",
            "transaction_count_descending",
            "distinct_counterparties_descending",
            "weighted_signal_heuristic",
        }
    ]
    candidate_results_raw, training_strategy = _run_candidate_models(
        splits["train"],
        splits["validation"],
        splits["test"],
        feature_columns,
        reference_feature_columns,
        include_lightgbm=include_lightgbm,
    )
    candidate_results = [_serialize_result(row) for row in candidate_results_raw]
    champion_raw = max(candidate_results_raw, key=lambda row: _metric_tuple(row["validation_metrics"]))
    champion = _serialize_result(champion_raw)
    ablations = _run_champion_ablation(
        champion=champion_raw,
        train_df=splits["train"],
        validation_df=splits["validation"],
        test_df=splits["test"],
        feature_columns=feature_columns,
    )
    benchmark_rows = _comparison_rows(baseline_results + [current_reference] + candidate_results)
    benchmark_rows = sorted(benchmark_rows, key=lambda row: _metric_tuple({"recall_at_top_10pct": row["recall_at_top_10pct"], "recall_at_top_20pct": row["recall_at_top_20pct"], "precision_at_top_10pct": row["precision_at_top_10pct"], "precision_at_top_20pct": row["precision_at_top_20pct"], "review_reduction_at_80pct_recall": {"review_reduction": row["review_reduction_at_80pct_recall"]}}), reverse=True)

    reference_summary = _load_protocol_b_reference_summary()
    summary = {
        "generated_at": _isoformat_utc(datetime.now(timezone.utc)),
        "dataset_name": _DATASET_NAME,
        "grouping_variant": _DEFAULT_GROUPING_VARIANT,
        "source_alert_jsonl_path": str(Path(alert_jsonl_path).resolve()),
        "base_feature_csv_path": str(Path(base_feature_csv_path).resolve()),
        "extra_feature_csv_path": str(extra_feature_path.resolve()),
        "extra_feature_extraction_summary": extra_extraction_summary,
        "dataset_stats": dataset_stats,
        "split_stats": split_stats,
        "feature_groups_added": {
            "temporal_dynamics": sorted(_TEMPORAL_FEATURE_COLUMNS),
            "graph_network": sorted(_GRAPH_FEATURE_COLUMNS),
            "payment_currency_mix": sorted(_PAYMENT_CURRENCY_EXTRA_COLUMNS),
            "safer_amount": sorted(_AMOUNT_EXTRA_COLUMNS),
        },
        "diagnosis": diagnosis,
        "baseline_results": baseline_results,
        "current_protocol_b_reference": current_reference,
        "candidate_results": candidate_results,
        "champion": champion,
        "ablations": ablations,
        "benchmark_rows": benchmark_rows,
        "training_strategy": training_strategy,
        "protocol_b_reference_summary": reference_summary,
        "protocol_safety_claims": {
            "past_only_features": True,
            "future_only_labels": True,
            "chronological_split": True,
            "pattern_shortcuts_removed_from_primary": True,
            "test_data_used_for_tuning": False,
        },
    }
    summary_target = Path(summary_path)
    report_target = Path(report_path)
    _ensure_parent_dir(summary_target)
    _ensure_parent_dir(report_target)
    summary_target.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    report_target.write_text(_render_report(summary), encoding="utf-8")
    return ProtocolBImprovementResult(
        summary_path=summary_target,
        report_path=report_target,
        base_feature_csv_path=Path(base_feature_csv_path),
        extra_feature_csv_path=extra_feature_path,
        dataset_stats=dataset_stats,
        baseline_results=baseline_results,
        candidate_results=candidate_results,
        champion=champion,
        ablations=ablations,
    )
