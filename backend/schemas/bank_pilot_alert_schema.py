from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_RAW_PII_KEYS = {
    "name",
    "full_name",
    "first_name",
    "last_name",
    "passport",
    "iin",
    "ssn",
    "tax_id",
    "document",
    "phone",
    "email",
    "address",
}
_HASH_RE = re.compile(r"^[A-Za-z0-9:_-]{8,256}$")


def _reject_raw_pii(data: Any) -> Any:
    if isinstance(data, dict):
        forbidden = sorted(_RAW_PII_KEYS & {str(key).lower() for key in data})
        if forbidden:
            raise ValueError(f"Raw PII fields are not allowed: {', '.join(forbidden)}")
    return data


def _validate_hash(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if not _HASH_RE.match(cleaned):
        raise ValueError("identifier must be a hashed or tokenized value")
    return cleaned


class _PilotBase(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    @model_validator(mode="before")
    @classmethod
    def _no_raw_pii(cls, data: Any) -> Any:
        return _reject_raw_pii(data)


class PilotAlertRecord(_PilotBase):
    alert_id: str
    customer_id_hash: str | None = None
    account_id_hash: str | None = None
    alert_timestamp: datetime
    scenario_name: str | None = None
    original_risk_score: float | None = Field(default=None, ge=0)
    alert_status: str | None = None
    rule_name: str | None = None
    source_system: str | None = None

    _hash_customer = field_validator("customer_id_hash")(_validate_hash)
    _hash_account = field_validator("account_id_hash")(_validate_hash)


class PilotTransactionRecord(_PilotBase):
    transaction_id: str
    timestamp: datetime
    amount: float = Field(ge=0)
    currency: str
    from_account_hash: str | None = None
    to_account_hash: str | None = None
    direction: str | None = None
    payment_channel: str | None = None
    counterparty_id_hash: str | None = None
    counterparty_bank_hash: str | None = None
    country: str | None = None

    _hash_from = field_validator("from_account_hash")(_validate_hash)
    _hash_to = field_validator("to_account_hash")(_validate_hash)
    _hash_counterparty = field_validator("counterparty_id_hash")(_validate_hash)
    _hash_bank = field_validator("counterparty_bank_hash")(_validate_hash)


class PilotOutcomeRecord(_PilotBase):
    alert_id: str
    outcome: str | None = None
    disposition: str | None = None
    false_positive: bool | None = None
    escalated: bool | None = None
    sar_str_filed: bool | None = None
    decision_timestamp: datetime | None = None
    closure_reason_category: str | None = None


class PilotBatch(_PilotBase):
    tenant_id: str
    batch_id: str
    alerts: list[PilotAlertRecord]
    transactions: list[PilotTransactionRecord]
    outcomes: list[PilotOutcomeRecord] = Field(default_factory=list)
