"""Schema validation for normalized alerts, context_json, decision_trace (reproducibility)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


class OverlayInputError(ValueError):
    """Raised when input is transaction-level or does not conform to alert-only overlay requirements."""

    def __init__(self, message: str = "Overlay requires alert-level input from AML monitoring systems."):
        super().__init__(message)
        self.message = message


# Columns that indicate transaction-level data; presence of any => reject for overlay
TRANSACTION_FORBIDDEN_COLUMNS = frozenset([
    "transaction_id", "transaction_time", "merchant_id", "counterparty",
    "account_balance", "velocity_windows", "tx_id", "tx_time", "txn_id",
    "merchant", "counterparty_id", "balance", "rolling_velocity",
])

# Required fields for normalized alert (per spec)
NORMALIZED_ALERT_REQUIRED = [
    "alert_id",
    "entity_id",
    "source_system",
    "timestamp",
    "risk_score_source",
    "typology",
    "vendor_metadata",
]


def assert_overlay_alert_only_columns(columns: List[str]) -> None:
    """
    Raise OverlayInputError if any column indicates transaction-level data.
    Overlay accepts only alert-level input from AML monitoring systems.
    """
    cols_lower = {str(c).strip().lower() for c in columns if c}
    for forbidden in TRANSACTION_FORBIDDEN_COLUMNS:
        if forbidden.lower() in cols_lower:
            raise OverlayInputError(
                "Overlay requires alert-level input from AML monitoring systems. "
                f"Transaction-level column '{forbidden}' is not allowed."
            )


def validate_normalized_alert_schema(record: Dict[str, Any]) -> List[str]:
    """
    Validate a single normalized alert record. Returns list of error messages (empty if valid).
    entity_id can be stored as user_id in DataFrame; we accept either key.
    """
    errors: List[str] = []
    if not record:
        return ["Record is empty"]
    # entity_id may be present as user_id in payload
    if "entity_id" not in record and record.get("user_id") is None:
        errors.append("Missing entity_id (or user_id)")
    for key in ["alert_id", "source_system", "timestamp", "risk_score_source", "typology"]:
        if key not in record or record.get(key) is None:
            errors.append(f"Missing required field: {key}")
    if "vendor_metadata" not in record:
        errors.append("Missing required field: vendor_metadata")
    elif not isinstance(record.get("vendor_metadata"), dict):
        errors.append("vendor_metadata must be a dict")
    return errors


def validate_context_json(obj: Any) -> List[str]:
    """Validate context_json structure. Returns list of error messages."""
    errors: List[str] = []
    if not isinstance(obj, dict):
        return ["context_json must be a dict"]
    for key in ["behavioral_baseline", "historical_alerts", "peer_comparison", "external_signals"]:
        if key not in obj:
            continue  # optional keys
        if not isinstance(obj[key], dict):
            errors.append(f"context_json.{key} must be a dict")
    return errors


def validate_decision_trace_schema(obj: Any) -> List[str]:
    """Validate decision_trace schema. Returns list of error messages."""
    errors: List[str] = []
    if not isinstance(obj, dict):
        return ["decision_trace must be a dict"]
    for key in ["alert_id", "run_id", "input_summary", "features_summary", "model_output", "rules", "governance", "outcome"]:
        if key not in obj:
            errors.append(f"decision_trace missing key: {key}")
    if "rules" in obj and not isinstance(obj["rules"], list):
        errors.append("decision_trace.rules must be a list")
    return errors
