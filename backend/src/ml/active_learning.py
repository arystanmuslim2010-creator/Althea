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


def ingest_labeled_batch(
    labeled_csv_path: Union[str, Path],
    training_data_path: Union[str, Path],
    label_col: str = "disposition",
    alert_id_col: str = "alert_id",
    output_path: Optional[Union[str, Path]] = None,
    dedup: bool = True,
) -> str:
    """
    Ingest a human-labeled CSV batch back into the training dataset.

    Workflow:
        1. export_label_batch() -> analysts label the CSV
        2. ingest_labeled_batch() -> labeled CSV merged into training data
        3. scripts/train_model.py -> retrain on updated dataset

    Args:
        labeled_csv_path: Path to the CSV exported by export_label_batch() and labeled
                          by analysts. Must contain alert_id and label_col columns.
        training_data_path: Path to the existing training dataset CSV.
        label_col: Column name that analysts filled with dispositions
                   (e.g. "SAR", "FP", "ESCALATED"). Default: "disposition".
        alert_id_col: Column used to match labeled alerts to training data.
        output_path: Where to save the updated training dataset. If None, overwrites
                     training_data_path.
        dedup: If True, de-duplicate by alert_id (keep most recent label).

    Returns:
        Path to the updated training dataset.

    Raises:
        ValueError: If labeled_csv has no valid labels or alert_id column is missing.
        FileNotFoundError: If labeled_csv_path or training_data_path do not exist.
    """
    labeled_csv_path = Path(labeled_csv_path)
    training_data_path = Path(training_data_path)

    if not labeled_csv_path.exists():
        raise FileNotFoundError(f"Labeled CSV not found: {labeled_csv_path}")
    if not training_data_path.exists():
        raise FileNotFoundError(f"Training data not found: {training_data_path}")

    labeled = pd.read_csv(labeled_csv_path)
    training = pd.read_csv(training_data_path)

    if alert_id_col not in labeled.columns:
        raise ValueError(
            f"alert_id column '{alert_id_col}' not found in labeled CSV. "
            f"Available columns: {list(labeled.columns)}"
        )
    if label_col not in labeled.columns:
        raise ValueError(
            f"Label column '{label_col}' not found in labeled CSV. "
            f"Available columns: {list(labeled.columns)}. "
            f"Analysts must fill the '{label_col}' column before ingesting."
        )

    # Drop rows where analyst left the label blank
    labeled = labeled.dropna(subset=[label_col])
    labeled = labeled[labeled[label_col].astype(str).str.strip() != ""]
    if len(labeled) == 0:
        raise ValueError(
            f"No labeled rows found in {labeled_csv_path}. "
            f"Ensure analysts have filled the '{label_col}' column."
        )

    # Merge: update existing rows or append new labeled rows
    if alert_id_col in training.columns:
        # For alerts already in training data, update their label
        training = training.set_index(alert_id_col)
        labeled_indexed = labeled.set_index(alert_id_col)[[label_col]]
        training.update(labeled_indexed)
        training = training.reset_index()

        # For alerts NOT in training data, append them as new rows
        new_alerts = labeled[~labeled[alert_id_col].isin(training[alert_id_col])]
        if len(new_alerts) > 0:
            training = pd.concat([training, new_alerts], ignore_index=True)
    else:
        # Training data has no alert_id column - just append
        training = pd.concat([training, labeled], ignore_index=True)

    if dedup and alert_id_col in training.columns:
        training = training.drop_duplicates(subset=[alert_id_col], keep="last")

    out = Path(output_path) if output_path else training_data_path
    training.to_csv(out, index=False)

    n_labeled = len(labeled)
    n_total = len(training)
    print(
        f"Ingested {n_labeled} labeled alerts from {labeled_csv_path.name} -> "
        f"{out} (total training rows: {n_total})"
    )
    return str(out)
