from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _collect_optional_fields(data: Any, known_fields: set[str]) -> Any:
    if not isinstance(data, dict):
        return data
    values = dict(data)
    explicit_optional = values.pop("optional_fields", None)
    merged_optional: dict[str, Any] = dict(explicit_optional) if isinstance(explicit_optional, dict) else {}
    for key in list(values.keys()):
        if key not in known_fields:
            merged_optional[key] = values.pop(key)
    values["optional_fields"] = merged_optional
    return values


class AlertTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    transaction_id: str
    amount: float
    timestamp: datetime
    sender: str
    receiver: str
    currency: str | None = None
    channel: str | None = None
    source_system: str | None = None
    optional_fields: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _merge_unknown_fields(cls, data: Any) -> Any:
        known = {
            "transaction_id",
            "amount",
            "timestamp",
            "sender",
            "receiver",
            "currency",
            "channel",
            "source_system",
            "optional_fields",
        }
        return _collect_optional_fields(data, known)

    @field_validator("transaction_id", "sender", "receiver")
    @classmethod
    def _required_non_empty_strings(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("field must not be empty")
        return cleaned

    @field_validator("amount", mode="before")
    @classmethod
    def _cast_amount(cls, value: Any) -> float:
        amount = float(value)
        if not math.isfinite(amount):
            raise ValueError("amount must be a finite number")
        return amount


class AlertAccount(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    account_id: str
    customer_id: str | None = None
    country: str | None = None
    segment: str | None = None
    risk_rating: str | None = None
    optional_fields: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _merge_unknown_fields(cls, data: Any) -> Any:
        known = {
            "account_id",
            "customer_id",
            "country",
            "segment",
            "risk_rating",
            "optional_fields",
        }
        return _collect_optional_fields(data, known)

    @field_validator("account_id")
    @classmethod
    def _required_non_empty_account_id(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("account_id must not be empty")
        return cleaned


class AlertPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    alert_id: str
    created_at: datetime = Field(default_factory=_utcnow)
    typology: str = "anomaly"
    is_sar: int | None = None
    accounts: list[AlertAccount] = Field(default_factory=list)
    transactions: list[AlertTransaction] = Field(default_factory=list)
    metadata: dict[str, Any] | None = None
    optional_fields: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _merge_unknown_fields_and_defaults(cls, data: Any) -> Any:
        known = {
            "alert_id",
            "created_at",
            "typology",
            "is_sar",
            "accounts",
            "transactions",
            "metadata",
            "optional_fields",
        }
        values = _collect_optional_fields(data, known)
        if isinstance(values, dict):
            values.setdefault("accounts", [])
            values.setdefault("transactions", [])
            if values.get("typology") in (None, ""):
                values["typology"] = "anomaly"
        return values

    @field_validator("alert_id")
    @classmethod
    def _required_non_empty_alert_id(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("alert_id must not be empty")
        return cleaned

    @field_validator("typology")
    @classmethod
    def _default_typology(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        return cleaned or "anomaly"

    @field_validator("is_sar", mode="before")
    @classmethod
    def _normalize_is_sar(cls, value: Any) -> int | None:
        if value is None or value == "":
            return None
        cast_value = int(value)
        if cast_value not in {0, 1}:
            raise ValueError("is_sar must be 0, 1, or null")
        return cast_value
