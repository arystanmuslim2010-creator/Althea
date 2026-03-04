"""
Active learning loop scaffolding: export uncertain/top-impact alerts for labeling, ingest labels back.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


REQUIRED_EXPORT_COLUMNS = [
    "alert_id",
    "entity_id",
    "rule_id",
    "created_at",
    "model_score",
    "suggested_action",
]


def export_label_batch(
    df_scored: pd.DataFrame,
    strategy: str = "uncertain",
    n: int = 200,
    out_path: Union[str, Path] = "label_batch.csv",
    alert_id_col: str = "alert_id",
    entity_id_col: str = "entity_id",
    rule_id_col: str = "rule_id",
    created_at_col: str = "created_at",
    score_col: str = "model_score",
    key_feature_cols: Optional[List[str]] = None,
) -> str:
    """
    Export alerts for human labeling.

    strategy:
      - "uncertain": probs around 0.5 (or high entropy)
      - "top_impact": near suppression threshold or top of queue (high score)
    """
    df = df_scored.copy()
    out_path = Path(out_path)

    # Resolve score column
    if score_col not in df.columns:
        for c in ["model_score", "risk_score_raw", "ml_proba", "risk_prob"]:
            if c in df.columns:
                score_col = c
                break
        else:
            raise ValueError("No score column found. Provide model_score or risk_score_raw/ml_proba.")

    scores = pd.to_numeric(df[score_col], errors="coerce").fillna(0.0)

    if strategy == "uncertain":
        # Entropy proxy: distance from 0.5
        distance = (scores - 0.5).abs()
        # Take smallest distance (most uncertain) up to n
        order = distance.nsmallest(min(n, len(df))).index
        batch = df.loc[order].head(n)
    elif strategy == "top_impact":
        # Top n by score (near top of queue)
        batch = df.nlargest(n, score_col)
    else:
        raise ValueError(f"Unknown strategy: {strategy}. Use 'uncertain' or 'top_impact'.")

    # Build export columns
    alert_id = batch[alert_id_col] if alert_id_col in batch.columns else batch.index.astype(str)
    entity_id = batch[entity_id_col] if entity_id_col in batch.columns else ""
    rule_id = batch[rule_id_col] if rule_id_col in batch.columns else ""
    created_at = batch[created_at_col] if created_at_col in batch.columns else ""
    model_score = batch[score_col]

    suggested = "review" if strategy == "top_impact" else "review_uncertain"
    out = pd.DataFrame({
        "alert_id": alert_id,
        "entity_id": entity_id,
        "rule_id": rule_id,
        "created_at": created_at,
        "model_score": model_score,
        "suggested_action": suggested,
    })

    if key_feature_cols:
        for c in key_feature_cols:
            if c in batch.columns:
                out[c] = batch[c]

    out["_strategy"] = strategy
    out["_export_n"] = n
    out.to_csv(out_path, index=False)
    return str(out_path)


def ingest_labels(
    path: Union[str, Path],
    disposition_column: str = "disposition",
    alert_id_column: str = "alert_id",
) -> pd.DataFrame:
    """
    Ingest labeled CSV and return standardized label DataFrame (alert_id, disposition, y_sar, y_escalated).

    Expects CSV with at least alert_id and a disposition/outcome column (or label column).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Label file not found: {path}")

    df = pd.read_csv(path)
    if alert_id_column not in df.columns:
        raise ValueError(f"Label file must contain '{alert_id_column}'.")

    # Detect disposition/label column
    disp_col = disposition_column
    if disp_col not in df.columns:
        for c in ["disposition", "outcome", "label", "y_true", "case_outcome"]:
            if c in df.columns:
                disp_col = c
                break
        else:
            raise ValueError("Label file must contain disposition/outcome/label column.")

    out = df[[alert_id_column, disp_col]].copy()
    out = out.rename(columns={disp_col: "disposition"})
    out = out.dropna(subset=["disposition"])
    # Map to y_sar, y_escalated via simple rules (can be overridden by labels.compute_labels)
    disp = out["disposition"].astype(str).str.strip().str.upper()
    sar_vals = {"SAR", "STR", "CONFIRMED", "YES", "1", "TP"}
    esc_vals = {"ESCALATED", "IN_REVIEW", "PENDING", "OPEN", "ASSIGNED"}
    out["y_sar"] = disp.isin(sar_vals).astype(int)
    out["y_escalated"] = disp.isin(esc_vals).astype(int)
    return out
