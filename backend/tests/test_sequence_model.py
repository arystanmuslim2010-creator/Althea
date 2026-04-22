from __future__ import annotations

import pandas as pd

from models.sequence_model import SEQUENCE_FEATURE_COLUMNS, build_sequence_feature_frame


def test_sequence_features_follow_prior_alert_order() -> None:
    frame = pd.DataFrame(
        [
            {
                "alert_id": "A1",
                "created_at": "2022-01-01T00:00:00Z",
                "source_account_key": "acct-1",
                "total_amount_usd": 50.0,
                "transaction_count": 1,
                "unique_destination_accounts": 1,
                "dominant_currency": "USD",
                "dominant_payment_format": "WIRE",
            },
            {
                "alert_id": "A2",
                "created_at": "2022-01-01T04:00:00Z",
                "source_account_key": "acct-1",
                "total_amount_usd": 150.0,
                "transaction_count": 3,
                "unique_destination_accounts": 2,
                "dominant_currency": "EUR",
                "dominant_payment_format": "ACH",
            },
            {
                "alert_id": "A3",
                "created_at": "2022-01-01T08:00:00Z",
                "source_account_key": "acct-1",
                "total_amount_usd": 300.0,
                "transaction_count": 4,
                "unique_destination_accounts": 3,
                "dominant_currency": "EUR",
                "dominant_payment_format": "ACH",
            },
        ]
    )
    encoded = build_sequence_feature_frame(frame)
    first = encoded.loc[encoded["alert_id"] == "A1"].iloc[0]
    third = encoded.loc[encoded["alert_id"] == "A3"].iloc[0]

    assert first["sequence_recent_alert_count"] == 0.0
    assert third["sequence_recent_alert_count"] == 2.0
    assert third["sequence_amount_trend_slope"] > 0.0
    assert set(SEQUENCE_FEATURE_COLUMNS).issubset(encoded.columns)
