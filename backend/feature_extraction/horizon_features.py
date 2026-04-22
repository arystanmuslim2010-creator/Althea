from __future__ import annotations

import csv
import math
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


_STRING_COLUMNS = {
    "alert_id",
    "grouping_variant",
    "source_account_key",
    "source_bank",
    "dominant_destination_bank",
    "dominant_currency",
    "dominant_payment_format",
    "typology",
}
_WINDOW_HOURS = {
    "1h": 1.0,
    "6h": 6.0,
    "24h": 24.0,
    "7d": 24.0 * 7.0,
}
_HALF_LIFE_HOURS = {
    "6h": 6.0,
    "24h": 24.0,
    "7d": 24.0 * 7.0,
}
_FEATURE_FIELDNAMES = [
    "alert_id",
    "hist_alert_count_1h",
    "hist_alert_count_6h",
    "hist_alert_count_24h",
    "hist_alert_count_7d",
    "hist_tx_count_1h",
    "hist_tx_count_6h",
    "hist_tx_count_24h",
    "hist_tx_count_7d",
    "hist_amount_total_usd_1h",
    "hist_amount_total_usd_6h",
    "hist_amount_total_usd_24h",
    "hist_amount_total_usd_7d",
    "hist_avg_amount_usd_1h",
    "hist_avg_amount_usd_6h",
    "hist_avg_amount_usd_24h",
    "hist_avg_amount_usd_7d",
    "hist_tx_count_vs_24h_mean",
    "hist_tx_count_vs_7d_mean",
    "hist_total_amount_vs_24h_mean",
    "hist_total_amount_vs_7d_mean",
    "hist_mean_amount_vs_24h_mean",
    "hist_mean_amount_vs_7d_mean",
    "hist_recency_weighted_tx_count_6h",
    "hist_recency_weighted_tx_count_24h",
    "hist_recency_weighted_amount_usd_24h",
    "hist_recency_weighted_amount_usd_7d",
    "hist_recency_weighted_counterparty_diversity_24h",
    "hist_recency_weighted_bank_diversity_24h",
    "fingerprint_total_amount_usd_median",
    "fingerprint_tx_count_median",
    "fingerprint_counterparty_diversity_median",
    "fingerprint_currency_entropy_mean",
    "fingerprint_payment_entropy_mean",
    "fingerprint_dominant_currency_prior_share",
    "fingerprint_dominant_payment_prior_share",
    "deviation_total_amount_vs_fingerprint",
    "deviation_tx_count_vs_fingerprint",
    "deviation_counterparty_diversity_vs_fingerprint",
    "deviation_currency_entropy_vs_fingerprint",
    "deviation_payment_entropy_vs_fingerprint",
    "deviation_cross_bank_ratio_vs_history_mean",
    "deviation_fan_out_ratio_vs_history_mean",
    "history_active_windows",
    "history_depth_hours",
]
HORIZON_FEATURE_COLUMNS = [column for column in _FEATURE_FIELDNAMES if column != "alert_id"]


@dataclass(slots=True)
class _AlertProfile:
    created_at: datetime
    transaction_count: float
    total_amount_usd: float
    mean_amount_usd: float
    counterparty_diversity: float
    bank_diversity: float
    currency_entropy: float
    payment_entropy: float
    cross_bank_ratio: float
    fan_out_ratio: float
    dominant_currency: str
    dominant_payment: str


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


def _safe_ratio(current: float, baseline: float) -> float:
    if abs(baseline) <= 1e-9:
        return 0.0
    return float((current - baseline) / max(abs(baseline), 1.0))


def _entropy_from_series(values: list[str]) -> float:
    if not values:
        return 0.0
    counts = Counter(values)
    total = float(sum(counts.values()))
    return float(-sum((count / total) * math.log((count / total) + 1e-12) for count in counts.values()))


def _normalize_category(value: Any) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized else "UNKNOWN"


def _alert_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    if "created_at" not in prepared.columns:
        raise KeyError("Expected `created_at` column in horizon feature frame input")
    prepared["created_at"] = pd.to_datetime(prepared["created_at"], utc=True, errors="coerce")
    prepared = prepared.dropna(subset=["created_at"]).sort_values(
        ["source_account_key", "created_at", "alert_id"],
        kind="stable",
    )
    for column in prepared.columns:
        if column == "created_at":
            continue
        if column in _STRING_COLUMNS:
            prepared[column] = prepared[column].astype(str)
            continue
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0)
    if "currency_entropy" not in prepared.columns:
        prepared["currency_entropy"] = 0.0
    if "payment_format_entropy" not in prepared.columns:
        prepared["payment_format_entropy"] = 0.0
    if "fan_out_ratio" not in prepared.columns:
        prepared["fan_out_ratio"] = 0.0
    return prepared.reset_index(drop=True)


def _ewma(history: list[_AlertProfile], *, anchor_time: datetime, attribute: str, half_life_hours: float) -> float:
    if not history:
        return 0.0
    weighted_sum = 0.0
    total_weight = 0.0
    for item in history:
        gap_hours = max((anchor_time - item.created_at).total_seconds() / 3600.0, 0.0)
        weight = math.exp(-math.log(2.0) * gap_hours / max(half_life_hours, 1e-6))
        weighted_sum += weight * float(getattr(item, attribute))
        total_weight += weight
    if total_weight <= 1e-9:
        return 0.0
    return float(weighted_sum / total_weight)


def _window_summary(window: deque[_AlertProfile], *, anchor_time: datetime, horizon_hours: float) -> dict[str, float]:
    while window and (anchor_time - window[0].created_at).total_seconds() / 3600.0 > horizon_hours:
        window.popleft()
    if not window:
        return {
            "alert_count": 0.0,
            "tx_count": 0.0,
            "amount_total_usd": 0.0,
            "avg_amount_usd": 0.0,
        }
    tx_count = float(sum(item.transaction_count for item in window))
    total_amount_usd = float(sum(item.total_amount_usd for item in window))
    return {
        "alert_count": float(len(window)),
        "tx_count": tx_count,
        "amount_total_usd": total_amount_usd,
        "avg_amount_usd": float(total_amount_usd / max(tx_count, 1.0)),
    }


def build_horizon_feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = _alert_frame(frame)
    output_rows: list[dict[str, float | str]] = []
    for _, group in prepared.groupby("source_account_key", sort=False):
        history: list[_AlertProfile] = []
        window_state = {label: deque() for label in _WINDOW_HOURS}
        dominant_currency_counts: Counter[str] = Counter()
        dominant_payment_counts: Counter[str] = Counter()
        for row in group.itertuples(index=False):
            created_at = getattr(row, "created_at")
            current = _AlertProfile(
                created_at=created_at,
                transaction_count=_safe_float(getattr(row, "transaction_count")),
                total_amount_usd=_safe_float(getattr(row, "total_amount_usd")),
                mean_amount_usd=_safe_float(getattr(row, "mean_amount_usd")),
                counterparty_diversity=_safe_float(getattr(row, "unique_destination_accounts")),
                bank_diversity=_safe_float(getattr(row, "unique_destination_banks")),
                currency_entropy=_safe_float(getattr(row, "currency_entropy")),
                payment_entropy=_safe_float(getattr(row, "payment_format_entropy")),
                cross_bank_ratio=_safe_float(getattr(row, "cross_bank_ratio")),
                fan_out_ratio=_safe_float(getattr(row, "fan_out_ratio")),
                dominant_currency=_normalize_category(getattr(row, "dominant_currency")),
                dominant_payment=_normalize_category(getattr(row, "dominant_payment_format")),
            )
            record: dict[str, float | str] = {"alert_id": str(getattr(row, "alert_id"))}
            for label, hours in _WINDOW_HOURS.items():
                summary = _window_summary(window_state[label], anchor_time=created_at, horizon_hours=hours)
                record[f"hist_alert_count_{label}"] = summary["alert_count"]
                record[f"hist_tx_count_{label}"] = summary["tx_count"]
                record[f"hist_amount_total_usd_{label}"] = summary["amount_total_usd"]
                record[f"hist_avg_amount_usd_{label}"] = summary["avg_amount_usd"]

            mean_24h_tx = float(record["hist_tx_count_24h"] / max(record["hist_alert_count_24h"], 1.0))
            mean_7d_tx = float(record["hist_tx_count_7d"] / max(record["hist_alert_count_7d"], 1.0))
            mean_24h_amount = float(record["hist_amount_total_usd_24h"] / max(record["hist_alert_count_24h"], 1.0))
            mean_7d_amount = float(record["hist_amount_total_usd_7d"] / max(record["hist_alert_count_7d"], 1.0))
            mean_24h_avg_amount = float(record["hist_avg_amount_usd_24h"])
            mean_7d_avg_amount = float(record["hist_avg_amount_usd_7d"])
            record["hist_tx_count_vs_24h_mean"] = _safe_ratio(current.transaction_count, mean_24h_tx)
            record["hist_tx_count_vs_7d_mean"] = _safe_ratio(current.transaction_count, mean_7d_tx)
            record["hist_total_amount_vs_24h_mean"] = _safe_ratio(current.total_amount_usd, mean_24h_amount)
            record["hist_total_amount_vs_7d_mean"] = _safe_ratio(current.total_amount_usd, mean_7d_amount)
            record["hist_mean_amount_vs_24h_mean"] = _safe_ratio(current.mean_amount_usd, mean_24h_avg_amount)
            record["hist_mean_amount_vs_7d_mean"] = _safe_ratio(current.mean_amount_usd, mean_7d_avg_amount)

            for label, half_life in _HALF_LIFE_HOURS.items():
                record[f"hist_recency_weighted_tx_count_{label}"] = _ewma(
                    history,
                    anchor_time=created_at,
                    attribute="transaction_count",
                    half_life_hours=half_life,
                )
            record["hist_recency_weighted_amount_usd_24h"] = _ewma(
                history,
                anchor_time=created_at,
                attribute="total_amount_usd",
                half_life_hours=_HALF_LIFE_HOURS["24h"],
            )
            record["hist_recency_weighted_amount_usd_7d"] = _ewma(
                history,
                anchor_time=created_at,
                attribute="total_amount_usd",
                half_life_hours=_HALF_LIFE_HOURS["7d"],
            )
            record["hist_recency_weighted_counterparty_diversity_24h"] = _ewma(
                history,
                anchor_time=created_at,
                attribute="counterparty_diversity",
                half_life_hours=_HALF_LIFE_HOURS["24h"],
            )
            record["hist_recency_weighted_bank_diversity_24h"] = _ewma(
                history,
                anchor_time=created_at,
                attribute="bank_diversity",
                half_life_hours=_HALF_LIFE_HOURS["24h"],
            )

            prior_total_amounts = [item.total_amount_usd for item in history]
            prior_tx_counts = [item.transaction_count for item in history]
            prior_counterparty_diversity = [item.counterparty_diversity for item in history]
            prior_currency_entropy = [item.currency_entropy for item in history]
            prior_payment_entropy = [item.payment_entropy for item in history]
            prior_cross_bank_ratio = [item.cross_bank_ratio for item in history]
            prior_fan_out_ratio = [item.fan_out_ratio for item in history]

            record["fingerprint_total_amount_usd_median"] = float(np.median(prior_total_amounts)) if prior_total_amounts else 0.0
            record["fingerprint_tx_count_median"] = float(np.median(prior_tx_counts)) if prior_tx_counts else 0.0
            record["fingerprint_counterparty_diversity_median"] = (
                float(np.median(prior_counterparty_diversity)) if prior_counterparty_diversity else 0.0
            )
            record["fingerprint_currency_entropy_mean"] = float(np.mean(prior_currency_entropy)) if prior_currency_entropy else 0.0
            record["fingerprint_payment_entropy_mean"] = float(np.mean(prior_payment_entropy)) if prior_payment_entropy else 0.0
            record["fingerprint_dominant_currency_prior_share"] = _safe_ratio(
                float(dominant_currency_counts[current.dominant_currency]),
                float(sum(dominant_currency_counts.values())),
            )
            record["fingerprint_dominant_payment_prior_share"] = _safe_ratio(
                float(dominant_payment_counts[current.dominant_payment]),
                float(sum(dominant_payment_counts.values())),
            )
            record["deviation_total_amount_vs_fingerprint"] = _safe_ratio(
                current.total_amount_usd,
                float(record["fingerprint_total_amount_usd_median"]),
            )
            record["deviation_tx_count_vs_fingerprint"] = _safe_ratio(
                current.transaction_count,
                float(record["fingerprint_tx_count_median"]),
            )
            record["deviation_counterparty_diversity_vs_fingerprint"] = _safe_ratio(
                current.counterparty_diversity,
                float(record["fingerprint_counterparty_diversity_median"]),
            )
            record["deviation_currency_entropy_vs_fingerprint"] = _safe_ratio(
                current.currency_entropy,
                float(record["fingerprint_currency_entropy_mean"]),
            )
            record["deviation_payment_entropy_vs_fingerprint"] = _safe_ratio(
                current.payment_entropy,
                float(record["fingerprint_payment_entropy_mean"]),
            )
            record["deviation_cross_bank_ratio_vs_history_mean"] = _safe_ratio(
                current.cross_bank_ratio,
                float(np.mean(prior_cross_bank_ratio)) if prior_cross_bank_ratio else 0.0,
            )
            record["deviation_fan_out_ratio_vs_history_mean"] = _safe_ratio(
                current.fan_out_ratio,
                float(np.mean(prior_fan_out_ratio)) if prior_fan_out_ratio else 0.0,
            )
            record["history_active_windows"] = float(len(history))
            if history:
                record["history_depth_hours"] = max((created_at - history[0].created_at).total_seconds() / 3600.0, 0.0)
            else:
                record["history_depth_hours"] = 0.0
            output_rows.append(record)

            history.append(current)
            dominant_currency_counts[current.dominant_currency] += 1
            dominant_payment_counts[current.dominant_payment] += 1
            for window in window_state.values():
                window.append(current)
    output = pd.DataFrame(output_rows, columns=_FEATURE_FIELDNAMES)
    for column in HORIZON_FEATURE_COLUMNS:
        output[column] = pd.to_numeric(output[column], errors="coerce").fillna(0.0).astype(np.float32)
    output["alert_id"] = output["alert_id"].astype(str)
    return output


def extract_horizon_feature_csv(
    frame: pd.DataFrame,
    output_csv_path: str | Path,
    *,
    force_rebuild: bool = False,
) -> tuple[Path, dict[str, Any]]:
    output_path = Path(output_csv_path)
    if output_path.exists() and not force_rebuild:
        return output_path, {"reused_existing_horizon_feature_csv": True}
    features = build_horizon_feature_frame(frame)
    _ensure_parent_dir(output_path)
    features.to_csv(output_path, index=False, quoting=csv.QUOTE_MINIMAL)
    return output_path, {
        "reused_existing_horizon_feature_csv": False,
        "rows_written": int(len(features)),
        "feature_columns": list(HORIZON_FEATURE_COLUMNS),
    }


def load_horizon_feature_frame(base_frame: pd.DataFrame, horizon_feature_csv_path: str | Path) -> pd.DataFrame:
    horizon_frame = pd.read_csv(horizon_feature_csv_path)
    horizon_frame["alert_id"] = horizon_frame["alert_id"].astype(str)
    for column in HORIZON_FEATURE_COLUMNS:
        horizon_frame[column] = pd.to_numeric(horizon_frame[column], errors="coerce").fillna(0.0).astype(np.float32)
    merged = base_frame.merge(horizon_frame, on="alert_id", how="left", validate="one_to_one")
    for column in HORIZON_FEATURE_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0.0)
    return merged
