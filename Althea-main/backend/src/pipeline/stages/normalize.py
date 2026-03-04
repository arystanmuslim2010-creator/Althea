"""
NORMALIZE stage: Convert ingested records to NormalizedAlert schema; enforce schema versioning.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from ...domain.schemas import validate_normalized_alert_schema

SCHEMA_VERSION = "1.0"


def run_normalize(
    df: pd.DataFrame,
    run_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Normalize DataFrame to required schema. Add entity_id, timestamp, risk_score_source, vendor_metadata, schema_version.
    """
    config = config or {}
    out = df.copy()
    if "entity_id" not in out.columns and "user_id" in out.columns:
        out["entity_id"] = out["user_id"].astype(str)
    if "timestamp" not in out.columns:
        if "timestamp_utc" in out.columns:
            out["timestamp"] = out["timestamp_utc"].astype(str)
        elif "created_at" in out.columns:
            out["timestamp"] = out["created_at"].astype(str)
        else:
            out["timestamp"] = ""
    out["timestamp"] = out["timestamp"].astype(str)
    if "risk_score_source" not in out.columns:
        out["risk_score_source"] = "ingest"
    if "vendor_metadata" not in out.columns:
        out["vendor_metadata"] = out.apply(lambda _: {}, axis=1)
    if "source_system" not in out.columns:
        out["source_system"] = "external_csv"
    out["schema_version"] = config.get("schema_version", SCHEMA_VERSION)
    if run_id:
        out["run_id"] = run_id
    return out
