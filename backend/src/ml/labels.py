"""
Unified label interface for AML alert outcome (expected investigative yield).

- y_sar: 1 if alert outcome indicates SAR/STR/confirmed suspicious (or escalated+confirmed), else 0.
- y_escalated: 1 if escalated for deeper review, else 0.

Label mapping is configurable via config file (e.g. backend/config/ml.yaml).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Default disposition/label column candidates (overlay alert-level)
DEFAULT_LABEL_COLUMN_CANDIDATES = [
    "disposition",
    "outcome",
    "case_outcome",
    "alert_outcome",
    "synthetic_true_suspicious",  # legacy demo
    "is_suspicious",
    "label",
    "y_true",
    "true_label",
    "ground_truth",
]


def get_label_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load label mapping from YAML config. If path is None, use default mapping.

    Expected config structure:
      labels:
        disposition_column: "disposition"   # or outcome, etc.
        sar_values: ["SAR", "STR", "CONFIRMED_TP", "ESCALATED_CONFIRMED"]  # -> y_sar=1
        escalated_values: ["ESCALATED", "IN_REVIEW", "PENDING"]             # -> y_escalated=1
        # Optional: explicit negative values (else not in sar_values -> 0)
        negative_values: ["FP", "CLOSED_FP", "FALSE_POSITIVE"]
    """
    default_config = {
        "labels": {
            "disposition_column": None,  # auto-detect from candidates
            "sar_values": [
                "SAR", "STR", "CONFIRMED", "CONFIRMED_TP", "ESCALATED_CONFIRMED",
                "Yes", "true", "1", "TP",
            ],
            "escalated_values": [
                "ESCALATED", "IN_REVIEW", "PENDING", "OPEN", "ASSIGNED",
            ],
            "negative_values": [
                "FP", "CLOSED_FP", "FALSE_POSITIVE", "No", "false", "0",
            ],
        },
        "two_stage": {
            "stage1_target": "y_escalated",
            "stage2_target": "y_sar",
            "stage2_train_on_escalated_only": True,
        },
    }

    if config_path is None:
        try:
            from pathlib import Path
            import yaml
            backend = Path(__file__).resolve().parent.parent.parent
            cfg_file = backend / "config" / "ml.yaml"
            if cfg_file.exists():
                with open(cfg_file, "r", encoding="utf-8") as f:
                    loaded = yaml.safe_load(f) or {}
                labels_cfg = loaded.get("labels", default_config["labels"])
                two_stage = loaded.get("two_stage", default_config["two_stage"])
                default_config["labels"] = {**default_config["labels"], **labels_cfg}
                default_config["two_stage"] = {**default_config["two_stage"], **two_stage}
        except Exception:
            pass
    else:
        try:
            import yaml
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            labels_cfg = loaded.get("labels", {})
            two_stage = loaded.get("two_stage", {})
            default_config["labels"] = {**default_config["labels"], **labels_cfg}
            default_config["two_stage"] = {**default_config["two_stage"], **two_stage}
        except Exception:
            pass

    return default_config


def _normalize_for_match(val: Any) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip().upper()
    return s


def _map_to_binary(
    series: pd.Series,
    positive_values: List[str],
    negative_values: Optional[List[str]] = None,
) -> pd.Series:
    """Map disposition-like column to binary 0/1. Unmatched -> 0 (or NaN if strict)."""
    normalized = series.map(_normalize_for_match)
    pos_set = {str(v).strip().upper() for v in positive_values}
    neg_set = set()
    if negative_values:
        neg_set = {str(v).strip().upper() for v in negative_values}
    out = pd.Series(0, index=series.index, dtype=int)
    for idx, v in normalized.items():
        if v in pos_set:
            out.loc[idx] = 1
        elif neg_set and v in neg_set:
            out.loc[idx] = 0
        # else leave 0 (or could set NaN for unknown)
    return out


def compute_labels(
    df: pd.DataFrame,
    config_path: Optional[str] = None,
    disposition_column: Optional[str] = None,
    sar_values: Optional[List[str]] = None,
    escalated_values: Optional[List[str]] = None,
    negative_values: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Compute y_sar and y_escalated from disposition/outcome column.

    - y_sar = 1 if outcome in sar_values (SAR/STR/confirmed), else 0.
    - y_escalated = 1 if outcome in escalated_values, else 0.

    Returns:
        (df_with_columns_y_sar_and_y_escalated, dict of used column names)
    """
    cfg = get_label_config(config_path)
    labels_cfg = cfg["labels"]

    col = disposition_column or labels_cfg.get("disposition_column")
    if not col:
        for c in DEFAULT_LABEL_COLUMN_CANDIDATES:
            if c in df.columns:
                col = c
                break
    if col is None or col not in df.columns:
        raise ValueError(
            "Label/disposition column not found. Required one of: "
            + ", ".join(DEFAULT_LABEL_COLUMN_CANDIDATES)
            + ". Add disposition_column in config/ml.yaml or pass disposition_column=..."
        )

    sar_vals = sar_values if sar_values is not None else labels_cfg.get("sar_values", [])
    esc_vals = escalated_values if escalated_values is not None else labels_cfg.get("escalated_values", [])
    neg_vals = negative_values if negative_values is not None else labels_cfg.get("negative_values")

    df = df.copy()
    df["y_sar"] = _map_to_binary(df[col], sar_vals, neg_vals)
    df["y_escalated"] = _map_to_binary(df[col], esc_vals, neg_vals)

    # If disposition is already binary-like (Yes/No), use it for y_sar when sar_values include Yes
    if df["y_sar"].sum() == 0 and col in df.columns:
        uniq = df[col].dropna().astype(str).str.strip().unique()
        if set(uniq).issubset({"Yes", "No", "1", "0", "true", "false", "True", "False"}):
            df["y_sar"] = (df[col].astype(str).str.strip().str.lower().isin(["yes", "true", "1"])).astype(int)

    used = {"disposition_column": col}
    return df, used


def get_two_stage_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Return two_stage config (stage1_target, stage2_target, stage2_train_on_escalated_only)."""
    cfg = get_label_config(config_path)
    return cfg.get("two_stage", {})
