"""
INGEST stage: CSV/JSON/DB pull, schema validation, field mapping, idempotency, retries, dead-letter.
Read-only; no write-backs to external systems.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ... import config as app_config
from ...domain.schemas import (
    validate_normalized_alert_schema,
    NORMALIZED_ALERT_REQUIRED,
    OverlayInputError,
    assert_overlay_alert_only_columns,
)
from ...observability.logging import get_logger
from ...services.ingestion_service import IngestionService, IngestionError

logger = get_logger("ingest")

# Retry settings for file read/parse
MAX_RETRIES = 3
RETRY_DELAY_SEC = 0.5


def _load_mapping_config(config: Dict[str, Any], data_dir: Path) -> Dict[str, str]:
    """Load field mapping from config or config/ingest_mapping.yaml."""
    mapping = config.get("ingest_field_mapping") or config.get("field_mapping")
    if isinstance(mapping, dict):
        return mapping
    try:
        p = data_dir / "config" / "ingest_mapping.yaml"
        if not p.is_file():
            p = Path(__file__).resolve().parent.parent.parent.parent / "config" / "ingest_mapping.yaml"
        if p.is_file():
            import yaml
            with open(p, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data.get("mapping") or {}
    except Exception:
        pass
    return {}


def _schema_validate_rows(df: pd.DataFrame, run_id: Optional[str] = None) -> tuple[pd.DataFrame, List[Dict], List[str]]:
    """
    Validate each row against normalized alert schema. Returns (valid_df, valid_records, errors).
    We require alert_id, entity_id (or user_id), source_system, timestamp, risk_score_source, typology, vendor_metadata.
    For ingestion we relax: risk_score_source can be defaulted; vendor_metadata can be {}.
    """
    errors: List[str] = []
    bad_rows: List[Dict] = []
    valid_indices = []
    for idx in df.index:
        row = df.loc[idx].to_dict()
        # Normalize: entity_id from user_id if missing
        if "entity_id" not in row and row.get("user_id") is not None:
            row["entity_id"] = row["user_id"]
        if "timestamp" not in row or row.get("timestamp") is None:
            row["timestamp"] = row.get("timestamp_utc") or row.get("created_at") or ""
        row.setdefault("risk_score_source", "ingest")
        row.setdefault("source_system", "external_csv")
        row.setdefault("vendor_metadata", {})
        errs = validate_normalized_alert_schema(row)
        if errs:
            errors.extend([f"Row {idx}: {e}" for e in errs])
            bad_rows.append({**row, "_row_index": int(idx), "_errors": errs})
        else:
            valid_indices.append(idx)
    valid_df = df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()
    return valid_df, bad_rows, errors


def _dataset_hash(df: pd.DataFrame, source: str, input_bytes: Optional[bytes] = None) -> str:
    """Deterministic hash for reproducibility: same input => same hash."""
    if input_bytes is not None:
        return hashlib.sha256(input_bytes).hexdigest()[:32]
    # Hash from dataframe content (sorted columns + first 1000 rows)
    cols = sorted([c for c in df.columns])
    sample = df[cols].head(1000) if len(df) > 0 else pd.DataFrame(columns=cols)
    raw = source + "|" + "|".join(cols) + "|" + str(len(df)) + "|" + str(sample.values.tobytes())
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:32]


def _idempotency_dedupe(df: pd.DataFrame, key_fields: List[str], per_run: bool) -> pd.DataFrame:
    """Dedupe by key_fields (e.g. alert_id, tx_ref). Keep first occurrence."""
    key_cols = [c for c in key_fields if c in df.columns]
    if not key_cols:
        return df
    return df.drop_duplicates(subset=key_cols, keep="first")


def _write_dead_letter(rows: List[Dict], run_id: str, reason: str, dead_letter_dir: Path) -> None:
    """Write bad rows to data/dead_letter/<run_id>.csv with reason column."""
    if not rows:
        return
    dead_letter_dir.mkdir(parents=True, exist_ok=True)
    out_path = dead_letter_dir / f"{run_id}_dead_letter.csv"
    out_df = pd.DataFrame(rows)
    if "_errors" in out_df.columns:
        out_df["_reason"] = out_df["_errors"].apply(lambda x: "; ".join(x) if isinstance(x, list) else str(x))
    else:
        out_df["_reason"] = reason
    out_df.to_csv(out_path, index=False, encoding="utf-8")


def run_ingest(
    source: str,
    input_path: Optional[Path] = None,
    input_df: Optional[pd.DataFrame] = None,
    input_bytes: Optional[bytes] = None,
    config: Optional[Dict[str, Any]] = None,
    dead_letter_dir: Optional[Path] = None,
    data_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Run ingest stage. Returns dict with keys: df, dataset_hash, dead_letter_count, errors.
    Supports CSV, JSON, dataframe. Schema validation fails loudly with actionable errors.
    """
    config = config or {}
    dead_letter_dir = dead_letter_dir or Path("data/dead_letter")
    data_dir = data_dir or Path("data")
    mapping = _load_mapping_config(config, data_dir)

    df: Optional[pd.DataFrame] = None
    dataset_hash = ""
    last_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            if source == "dataframe" and input_df is not None:
                df = input_df.copy()
                dataset_hash = _dataset_hash(df, source)
                break
            if source in ("csv", "json") and input_bytes is not None:
                from io import BytesIO
                if source == "csv":
                    df = pd.read_csv(BytesIO(input_bytes))
                else:
                    data = json.loads(input_bytes.decode("utf-8"))
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict) and "records" in data:
                        df = pd.DataFrame(data["records"])
                    else:
                        df = pd.DataFrame([data])
                dataset_hash = _dataset_hash(None, source, input_bytes)
                break
            if source in ("csv", "json") and input_path is not None and input_path.is_file():
                raw = input_path.read_bytes()
                if source == "csv":
                    df = pd.read_csv(input_path)
                else:
                    data = json.loads(raw.decode("utf-8"))
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict) and "records" in data:
                        df = pd.DataFrame(data["records"])
                    else:
                        df = pd.DataFrame([data])
                dataset_hash = _dataset_hash(df, source, raw)
                break
            # Fallback: try ingestion_service for CSV (bank alerts)
            if (source == "csv" and (input_path or input_bytes)) or (df is None and input_df is not None):
                svc = IngestionService()
                if input_bytes is not None:
                    from io import BytesIO
                    df = svc.load_bank_alerts_csv(BytesIO(input_bytes))
                elif input_path is not None:
                    df = svc.load_bank_alerts_csv(input_path)
                elif input_df is not None:
                    df = input_df.copy()
                    svc._validate_mandatory(df)
                    df = svc._ensure_alert_id(svc._normalise_columns(df))
                    df = svc._ensure_time_gap(df)
                    df = svc._ensure_num_transactions(df)
                    df = svc._fill_defaults(df)
                if df is not None:
                    dataset_hash = _dataset_hash(df, source, input_bytes)
                break
        except IngestionError as e:
            last_error = e
            raise  # do not retry schema/validation errors
        except Exception as e:
            last_error = e
            if attempt + 1 >= MAX_RETRIES:
                raise
            import time
            time.sleep(RETRY_DELAY_SEC)

    if df is None or df.empty:
        if getattr(app_config, "OVERLAY_MODE", False):
            raise OverlayInputError("Overlay requires alert-level input from AML monitoring systems. No data or empty input.")
        return {"df": pd.DataFrame(), "dataset_hash": dataset_hash or "none", "dead_letter_count": 0, "errors": [str(last_error or "No data")]}

    # Overlay-only: reject transaction-level columns
    if getattr(app_config, "OVERLAY_MODE", False):
        assert_overlay_alert_only_columns(list(df.columns))
    # Require alert_id for overlay
    if getattr(app_config, "OVERLAY_MODE", False) and "alert_id" not in df.columns:
        raise OverlayInputError("Overlay requires alert-level input from AML monitoring systems. Missing alert_id.")

    # Ensure pipeline-required columns (alert_id, time_gap, num_transactions) via ingestion_service helpers
    try:
        svc = IngestionService()
        df = svc._normalise_columns(df)
        if "user_id" in df.columns or "userid" in df.columns:
            svc._validate_mandatory(df)
        df = svc._ensure_alert_id(df)
        df = svc._ensure_time_gap(df)
        df = svc._ensure_num_transactions(df)
        df = svc._fill_defaults(df)
    except IngestionError:
        pass  # already have required cols or will fail at schema validation
    except Exception:
        pass

    # Apply field mapping (rename columns)
    if mapping:
        rename = {k: v for k, v in mapping.items() if k in df.columns and v != k}
        if rename:
            df = df.rename(columns=rename)

    # Schema validation: fail loudly with actionable errors
    valid_df, bad_rows, errors = _schema_validate_rows(df)
    if bad_rows and errors:
        _write_dead_letter(bad_rows, dataset_hash[:16], "schema_validation", dead_letter_dir)
    # If we have strict schema we could use only valid_df; for backward compat use full df and log errors
    use_df = valid_df if valid_df.shape[0] > 0 else df
    if use_df.empty and not df.empty:
        # All rows failed validation
        if getattr(app_config, "OVERLAY_MODE", False):
            raise OverlayInputError(
                "Overlay requires alert-level input from AML monitoring systems. "
                "Input did not conform to NormalizedAlert schema (alert_id, entity_id, source_system, timestamp, typology, risk_score_source, vendor_metadata)."
            )
        return {"df": pd.DataFrame(), "dataset_hash": dataset_hash, "dead_letter_count": len(bad_rows), "errors": errors[:20]}

    # Idempotency: dedupe by alert_id + tx_ref
    idem_cfg = config.get("idempotency", {})
    if idem_cfg.get("dedupe_per_run", True):
        key_fields = idem_cfg.get("key_fields", ["alert_id", "tx_ref"])
        use_df = _idempotency_dedupe(use_df, key_fields, per_run=True)

    # Overlay: only alert-level data reaches pipeline
    dataset_type = "alert" if (getattr(app_config, "OVERLAY_MODE", False) and not use_df.empty) else "alert"
    return {
        "df": use_df,
        "dataset_hash": dataset_hash,
        "dead_letter_count": len(bad_rows),
        "errors": errors[:20] if errors else [],
        "dataset_type": dataset_type,
    }
