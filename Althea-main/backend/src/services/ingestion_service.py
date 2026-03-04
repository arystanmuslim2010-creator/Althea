"""Ingestion service for bank-produced AML alert CSVs.

Reads a CSV of pre-existing bank alerts, normalises column names,
validates mandatory fields, fills defaults, and returns a DataFrame
ready for the scoring/governance pipeline.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd

from .. import utils

# ---------------------------------------------------------------------------
# Required & accepted column names
# ---------------------------------------------------------------------------

# Mandatory (at least one form must be present)
_MANDATORY_COLS = ["user_id", "amount", "segment", "country", "channel"]

# The pipeline itself needs these three numeric columns
_PIPELINE_COLS = ["amount", "time_gap", "num_transactions"]

# Accepted alternative names  (normalised_name -> list of aliases)
_ALIASES = {
    "alert_id":          ["alert_id", "alertid", "id", "alert_ref"],
    "user_id":           ["user_id", "userid", "customer_id", "customerid", "cust_id", "account_id"],
    "amount":            ["amount", "transaction_amount", "txn_amount", "amt"],
    "timestamp_utc":     ["timestamp_utc", "timestamp", "event_time", "tx_time", "datetime", "created_at"],
    "time_gap":          ["time_gap", "timegap", "time_gap_seconds"],
    "num_transactions":  ["num_transactions", "numtransactions", "transactions_count_24h", "tx_count", "transaction_count"],
    "segment":           ["segment", "customer_segment", "cust_segment"],
    "country":           ["country", "country_code", "jurisdiction"],
    "channel":           ["channel", "tx_channel", "transaction_channel"],
    "typology":          ["typology", "alert_type", "scenario", "rule_name"],
    "source_system":     ["source_system", "sourcesystem", "origin"],
    "rule_hits":         ["rule_hits", "rulehits", "triggered_rules"],
    "external_versions_json": ["external_versions_json", "external_versions"],
}


class IngestionError(ValueError):
    """Raised when the uploaded CSV does not satisfy the schema."""

    def __init__(self, message: str, missing: Optional[List[str]] = None):
        super().__init__(message)
        self.missing = missing or []


class IngestionService:
    """Loads, validates, and normalises bank alert CSVs."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or utils.get_logger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_bank_alerts_csv(self, uploaded_file: Any) -> pd.DataFrame:
        """Read CSV, normalise, validate, fill defaults.

        Returns a DataFrame with at least:
            user_id, amount, time_gap, num_transactions, segment,
            country, channel, typology, alert_id
        ready for the scoring pipeline.
        """
        df = pd.read_csv(uploaded_file)
        if df.empty:
            raise IngestionError("Uploaded CSV is empty.")

        # 1. Normalise column names
        df = self._normalise_columns(df)

        # 2. Validate mandatory columns
        self._validate_mandatory(df)

        # 3. Generate stable alert_id if missing
        df = self._ensure_alert_id(df)

        # 4. Derive time_gap from timestamps if needed
        df = self._ensure_time_gap(df)

        # 5. Derive num_transactions if missing
        df = self._ensure_num_transactions(df)

        # 6. Fill optional columns with defaults
        df = self._fill_defaults(df)

        self._logger.info(
            "Ingested %d bank alerts (%d columns): %s",
            len(df), len(df.columns), list(df.columns),
        )
        return df

    def get_schema_description(self) -> str:
        """Return a human-readable schema requirement string."""
        return (
            "**Mandatory columns:** `user_id`, `amount`, `segment`, `country`, `channel`\n\n"
            "**Time:** `timestamp_utc` (ISO) **or** `time_gap` (seconds) — at least one required\n\n"
            "**Volume:** `num_transactions` **or** `transactions_count_24h` — defaults to 1 if missing\n\n"
            "**Optional:** `alert_id`, `typology`, `source_system`, `rule_hits`, `external_versions_json`"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase + strip column names, then map aliases."""
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        rename_map = {}
        for canonical, aliases in _ALIASES.items():
            if canonical in df.columns:
                continue  # already present
            for alias in aliases:
                if alias in df.columns:
                    rename_map[alias] = canonical
                    break
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    @staticmethod
    def _validate_mandatory(df: pd.DataFrame) -> None:
        """Raise IngestionError if required columns are missing."""
        missing = [c for c in _MANDATORY_COLS if c not in df.columns]
        # Also need at least one time source
        has_time = "time_gap" in df.columns or "timestamp_utc" in df.columns
        if not has_time:
            missing.append("time_gap or timestamp_utc")
        if missing:
            raise IngestionError(
                f"Missing required columns: {', '.join(missing)}.\n"
                f"Present columns: {', '.join(df.columns.tolist())}",
                missing=missing,
            )

    @staticmethod
    def _ensure_alert_id(df: pd.DataFrame) -> pd.DataFrame:
        """Generate deterministic alert_id from row content if missing."""
        if "alert_id" in df.columns:
            df["alert_id"] = df["alert_id"].astype(str)
            # Fill blanks
            mask = df["alert_id"].isin(["", "nan", "None", "NaN"])
            if mask.any():
                df.loc[mask, "alert_id"] = df.loc[mask].apply(
                    lambda r: "ALERT_" + hashlib.sha1(
                        str(r.values.tolist()).encode()
                    ).hexdigest()[:12],
                    axis=1,
                )
            return df

        # No alert_id column at all — generate from row content
        df["alert_id"] = df.apply(
            lambda r: "ALERT_" + hashlib.sha1(
                str(r.values.tolist()).encode()
            ).hexdigest()[:12],
            axis=1,
        )
        return df

    @staticmethod
    def _ensure_time_gap(df: pd.DataFrame) -> pd.DataFrame:
        """If time_gap is missing, derive it from timestamp_utc."""
        if "time_gap" in df.columns:
            df["time_gap"] = pd.to_numeric(df["time_gap"], errors="coerce").fillna(86400)
            return df

        if "timestamp_utc" not in df.columns:
            # Should not reach here (validated earlier), but be safe
            df["time_gap"] = 86400
            return df

        ts = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
        df["_parsed_ts"] = ts

        # Sort per user, compute diff in seconds
        df = df.sort_values(["user_id", "_parsed_ts"])
        df["time_gap"] = df.groupby("user_id")["_parsed_ts"].diff().dt.total_seconds()

        # First event per user: fill with median of the user or global median
        global_median = df["time_gap"].median()
        if pd.isna(global_median) or global_median <= 0:
            global_median = 86400.0
        df["time_gap"] = df["time_gap"].fillna(global_median).clip(lower=0)

        df = df.drop(columns=["_parsed_ts"], errors="ignore")
        return df

    @staticmethod
    def _ensure_num_transactions(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure num_transactions column exists."""
        if "num_transactions" in df.columns:
            df["num_transactions"] = pd.to_numeric(
                df["num_transactions"], errors="coerce"
            ).fillna(1).astype(int)
            return df

        # Missing entirely — default to 1 and flag
        df["num_transactions"] = 1
        df["missing_num_transactions"] = 1
        return df

    @staticmethod
    def _fill_defaults(df: pd.DataFrame) -> pd.DataFrame:
        """Fill optional columns with sensible defaults."""
        if "typology" not in df.columns:
            df["typology"] = "bank_alert"
        else:
            df["typology"] = df["typology"].fillna("bank_alert").astype(str)

        if "source_system" not in df.columns:
            df["source_system"] = "external_csv"

        # Ensure segment is string
        df["segment"] = df["segment"].fillna("unknown").astype(str)
        df["country"] = df["country"].fillna("XX").astype(str)
        df["channel"] = df["channel"].fillna("unknown").astype(str)

        # Ensure numeric types
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["time_gap"] = pd.to_numeric(df["time_gap"], errors="coerce").fillna(86400.0)

        return df
