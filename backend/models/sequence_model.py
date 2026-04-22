from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SEQUENCE_FEATURE_COLUMNS = [
    "sequence_recent_alert_count",
    "sequence_recent_amount_mean",
    "sequence_recent_amount_std",
    "sequence_recent_tx_mean",
    "sequence_recent_tx_std",
    "sequence_recent_counterparty_mean",
    "sequence_recent_counterparty_std",
    "sequence_recent_gap_mean_hours",
    "sequence_recent_gap_std_hours",
    "sequence_amount_trend_slope",
    "sequence_tx_trend_slope",
    "sequence_counterparty_trend_slope",
    "sequence_currency_shift_rate",
    "sequence_payment_shift_rate",
    "sequence_recent_high_risk_velocity_share",
]


@dataclass(slots=True)
class SequenceEncodingConfig:
    max_history: int = 8
    high_velocity_gap_hours: float = 6.0


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


def _linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = np.arange(len(values), dtype=np.float32)
    ys = np.asarray(values, dtype=np.float32)
    xs = xs - float(xs.mean())
    denom = float(np.square(xs).sum())
    if denom <= 1e-9:
        return 0.0
    return float(np.dot(xs, ys - float(ys.mean())) / denom)


def build_sequence_feature_frame(
    frame: pd.DataFrame,
    *,
    config: SequenceEncodingConfig | None = None,
) -> pd.DataFrame:
    cfg = config or SequenceEncodingConfig()
    prepared = frame.copy()
    prepared["created_at"] = pd.to_datetime(prepared["created_at"], utc=True, errors="coerce")
    prepared = prepared.dropna(subset=["created_at"]).sort_values(
        ["source_account_key", "created_at", "alert_id"],
        kind="stable",
    ).reset_index(drop=True)
    rows: list[dict[str, float | str]] = []
    for _, group in prepared.groupby("source_account_key", sort=False):
        history: list[dict[str, Any]] = []
        for row in group.itertuples(index=False):
            recent = history[-cfg.max_history :]
            amount_values = [_safe_float(item["total_amount_usd"]) for item in recent]
            tx_values = [_safe_float(item["transaction_count"]) for item in recent]
            counterparty_values = [_safe_float(item["unique_destination_accounts"]) for item in recent]
            gap_values = [_safe_float(item["gap_hours"]) for item in recent]
            currency_values = [str(item["dominant_currency"]) for item in recent if str(item["dominant_currency"])]
            payment_values = [str(item["dominant_payment_format"]) for item in recent if str(item["dominant_payment_format"])]
            high_velocity_share = (
                float(sum(1 for value in gap_values if value <= cfg.high_velocity_gap_hours)) / len(gap_values)
                if gap_values
                else 0.0
            )
            rows.append(
                {
                    "alert_id": str(getattr(row, "alert_id")),
                    "sequence_recent_alert_count": float(len(recent)),
                    "sequence_recent_amount_mean": float(np.mean(amount_values)) if amount_values else 0.0,
                    "sequence_recent_amount_std": float(np.std(amount_values)) if amount_values else 0.0,
                    "sequence_recent_tx_mean": float(np.mean(tx_values)) if tx_values else 0.0,
                    "sequence_recent_tx_std": float(np.std(tx_values)) if tx_values else 0.0,
                    "sequence_recent_counterparty_mean": float(np.mean(counterparty_values)) if counterparty_values else 0.0,
                    "sequence_recent_counterparty_std": float(np.std(counterparty_values)) if counterparty_values else 0.0,
                    "sequence_recent_gap_mean_hours": float(np.mean(gap_values)) if gap_values else 0.0,
                    "sequence_recent_gap_std_hours": float(np.std(gap_values)) if gap_values else 0.0,
                    "sequence_amount_trend_slope": _linear_slope(amount_values),
                    "sequence_tx_trend_slope": _linear_slope(tx_values),
                    "sequence_counterparty_trend_slope": _linear_slope(counterparty_values),
                    "sequence_currency_shift_rate": (
                        float(sum(1 for left, right in zip(currency_values, currency_values[1:], strict=False) if left != right))
                        / max(len(currency_values) - 1, 1)
                        if len(currency_values) > 1
                        else 0.0
                    ),
                    "sequence_payment_shift_rate": (
                        float(sum(1 for left, right in zip(payment_values, payment_values[1:], strict=False) if left != right))
                        / max(len(payment_values) - 1, 1)
                        if len(payment_values) > 1
                        else 0.0
                    ),
                    "sequence_recent_high_risk_velocity_share": high_velocity_share,
                }
            )
            previous_created_at = history[-1]["created_at"] if history else None
            gap_hours = 0.0
            if previous_created_at is not None:
                gap_hours = max((getattr(row, "created_at") - previous_created_at).total_seconds() / 3600.0, 0.0)
            history.append(
                {
                    "created_at": getattr(row, "created_at"),
                    "total_amount_usd": _safe_float(getattr(row, "total_amount_usd", 0.0)),
                    "transaction_count": _safe_float(getattr(row, "transaction_count", 0.0)),
                    "unique_destination_accounts": _safe_float(getattr(row, "unique_destination_accounts", 0.0)),
                    "dominant_currency": str(getattr(row, "dominant_currency", "")),
                    "dominant_payment_format": str(getattr(row, "dominant_payment_format", "")),
                    "gap_hours": gap_hours,
                }
            )
    output = pd.DataFrame(rows, columns=["alert_id", *SEQUENCE_FEATURE_COLUMNS])
    output["alert_id"] = output["alert_id"].astype(str)
    for column in SEQUENCE_FEATURE_COLUMNS:
        output[column] = pd.to_numeric(output[column], errors="coerce").fillna(0.0).astype(np.float32)
    return output


def merge_sequence_features(base_frame: pd.DataFrame, sequence_frame: pd.DataFrame) -> pd.DataFrame:
    merged = base_frame.merge(sequence_frame, on="alert_id", how="left", validate="one_to_one")
    for column in SEQUENCE_FEATURE_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0.0)
    return merged
