"""Utility helpers for timestamps and logging."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime

import pandas as pd


def now_iso() -> str:
    """Return current UTC timestamp in ISO format (Z suffix)."""

    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def setup_logging(level: int | str = logging.INFO) -> None:
    """Configure root logging once for the app."""

    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    """Get a module-scoped logger."""

    return logging.getLogger(name)


def build_alert_id(row: dict) -> str:
    """
    Generate a deterministic alert_id from transaction data.
    
    Uses SHA256 hash of canonical string: user_id|segment|amount|timestamp_or_time_index|counterparty_or_typology|channel(optional)
    
    Args:
        row: Dictionary or Series with transaction data
        
    Returns:
        Deterministic alert_id string (hex digest, first 16 chars)
    """
    # Extract fields with fallbacks
    user_id = str(row.get("user_id", "")).strip()
    segment = str(row.get("segment", "")).strip()
    amount = str(row.get("amount", "")).strip()
    
    # Timestamp or time index - prefer timestamp, then created_at, then time_index, then row index
    timestamp = ""
    if "timestamp" in row and pd.notna(row.get("timestamp")):
        timestamp = str(row.get("timestamp"))
    elif "created_at" in row and pd.notna(row.get("created_at")):
        timestamp = str(row.get("created_at"))
    elif "time_index" in row and pd.notna(row.get("time_index")):
        timestamp = str(row.get("time_index"))
    else:
        # Fallback: use row index if available
        timestamp = str(row.get("_index", row.get("index", "")))
    
    # Counterparty or typology
    counterparty = str(row.get("counterparty", "")).strip()
    typology = str(row.get("typology", "")).strip()
    counterparty_or_typology = counterparty if counterparty else typology
    
    # Channel (optional)
    channel = str(row.get("channel", "")).strip()
    
    # Build canonical string
    canonical = f"{user_id}|{segment}|{amount}|{timestamp}|{counterparty_or_typology}|{channel}"
    
    # Generate SHA256 hash
    hash_obj = hashlib.sha256(canonical.encode("utf-8"))
    alert_id_hex = hash_obj.hexdigest()[:16]  # Use first 16 chars for readability
    
    return f"ALERT_{alert_id_hex}"
