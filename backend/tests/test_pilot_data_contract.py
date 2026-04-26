from __future__ import annotations

import pytest
from pydantic import ValidationError

from schemas.bank_pilot_alert_schema import PilotBatch


def test_valid_pilot_payload_accepts_hashed_identifiers():
    payload = {
        "tenant_id": "bank-a",
        "batch_id": "batch-001",
        "alerts": [
            {
                "alert_id": "A1",
                "customer_id_hash": "cust_hash_123456",
                "account_id_hash": "acct_hash_123456",
                "alert_timestamp": "2026-04-01T00:00:00Z",
                "scenario_name": "structuring",
            }
        ],
        "transactions": [
            {
                "transaction_id": "T1",
                "timestamp": "2026-04-01T01:00:00Z",
                "amount": 100.0,
                "currency": "USD",
                "from_account_hash": "acct_hash_123456",
            }
        ],
    }
    batch = PilotBatch.model_validate(payload)
    assert batch.batch_id == "batch-001"


def test_pilot_payload_rejects_raw_pii_and_negative_amount():
    payload = {
        "tenant_id": "bank-a",
        "batch_id": "batch-001",
        "alerts": [{"alert_id": "A1", "alert_timestamp": "2026-04-01T00:00:00Z", "name": "Jane Doe"}],
        "transactions": [{"transaction_id": "T1", "timestamp": "2026-04-01T01:00:00Z", "amount": -1, "currency": "USD"}],
    }
    with pytest.raises(ValidationError):
        PilotBatch.model_validate(payload)
