from __future__ import annotations

from pathlib import Path

import pandas as pd

from feature_extraction.horizon_features import (
    HORIZON_FEATURE_COLUMNS,
    build_horizon_feature_frame,
    extract_horizon_feature_csv,
    load_horizon_feature_frame,
)


def _toy_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "alert_id": "A1",
                "created_at": "2022-01-01T00:00:00Z",
                "source_account_key": "acct-1",
                "transaction_count": 2,
                "total_amount_usd": 100.0,
                "mean_amount_usd": 50.0,
                "unique_destination_accounts": 1,
                "unique_destination_banks": 1,
                "currency_entropy": 0.0,
                "payment_format_entropy": 0.0,
                "cross_bank_ratio": 0.0,
                "fan_out_ratio": 0.5,
                "dominant_currency": "USD",
                "dominant_payment_format": "WIRE",
            },
            {
                "alert_id": "A2",
                "created_at": "2022-01-01T03:00:00Z",
                "source_account_key": "acct-1",
                "transaction_count": 4,
                "total_amount_usd": 400.0,
                "mean_amount_usd": 100.0,
                "unique_destination_accounts": 3,
                "unique_destination_banks": 2,
                "currency_entropy": 0.4,
                "payment_format_entropy": 0.2,
                "cross_bank_ratio": 0.5,
                "fan_out_ratio": 1.0,
                "dominant_currency": "EUR",
                "dominant_payment_format": "ACH",
            },
            {
                "alert_id": "A3",
                "created_at": "2022-01-02T05:00:00Z",
                "source_account_key": "acct-1",
                "transaction_count": 1,
                "total_amount_usd": 20.0,
                "mean_amount_usd": 20.0,
                "unique_destination_accounts": 1,
                "unique_destination_banks": 1,
                "currency_entropy": 0.0,
                "payment_format_entropy": 0.0,
                "cross_bank_ratio": 0.0,
                "fan_out_ratio": 0.0,
                "dominant_currency": "USD",
                "dominant_payment_format": "WIRE",
            },
        ]
    )


def test_horizon_features_use_only_prior_history() -> None:
    features = build_horizon_feature_frame(_toy_frame())
    first = features.loc[features["alert_id"] == "A1"].iloc[0]
    second = features.loc[features["alert_id"] == "A2"].iloc[0]
    third = features.loc[features["alert_id"] == "A3"].iloc[0]

    assert first["hist_alert_count_24h"] == 0.0
    assert second["hist_alert_count_24h"] == 1.0
    assert second["hist_tx_count_24h"] == 2.0
    assert second["hist_total_amount_vs_24h_mean"] > 0.0
    assert third["hist_alert_count_24h"] == 0.0
    assert third["hist_alert_count_7d"] == 2.0


def test_horizon_feature_csv_roundtrip(tmp_path: Path) -> None:
    base = _toy_frame()
    path, summary = extract_horizon_feature_csv(base, tmp_path / "horizon.csv", force_rebuild=True)
    merged = load_horizon_feature_frame(base, path)

    assert path.exists()
    assert summary["rows_written"] == len(base)
    assert set(HORIZON_FEATURE_COLUMNS).issubset(merged.columns)
