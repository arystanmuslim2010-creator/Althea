"""Label builder for ALTHEA training datasets.

Maps analyst investigation outcomes to normalized supervised learning labels:
- escalation_label: binary (0 = not suspicious, 1 = escalated/suspicious)
- sar_label: binary (0 = no SAR filed, 1 = SAR/STR filed)
- label_status: finality and confidence of the label
- label_weight: optional per-sample training weight

Outcome taxonomy:
    false_positive   → escalation=0, sar=0
    benign_activity  → escalation=0, sar=0
    true_positive    → escalation=1, sar=0
    escalated        → escalation=1, sar=0
    sar_filed        → escalation=1, sar=1
    confirmed_suspicious → escalation=1, sar=0  (future extension)
    pending          → excluded from training (label not final)
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger("althea.training.label_builder")

# Decisions that map to escalation = 1
POSITIVE_DECISIONS = frozenset(
    {"true_positive", "escalated", "sar_filed", "confirmed_suspicious"}
)

# Decisions that map to escalation = 0
NEGATIVE_DECISIONS = frozenset({"false_positive", "benign_activity"})

# Decisions that indicate a SAR/STR was filed
SAR_DECISIONS = frozenset({"sar_filed"})

# Decisions that are not yet final — excluded from supervised training
PENDING_DECISIONS = frozenset({"pending", "under_review", "re_reviewed"})

# All known decisions
ALL_KNOWN_DECISIONS = POSITIVE_DECISIONS | NEGATIVE_DECISIONS | PENDING_DECISIONS

# Training weights for high-value decisions
# SAR cases are rare and extremely high signal — up-weight them
_DECISION_WEIGHTS: dict[str, float] = {
    "sar_filed": 3.0,
    "escalated": 1.5,
    "true_positive": 1.2,
    "confirmed_suspicious": 1.5,
    "false_positive": 1.0,
    "benign_activity": 1.0,
}


class LabelBuilder:
    """Convert raw analyst decisions into training labels and weights."""

    def build_escalation_labels(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Add ``escalation_label``, ``sar_label``, ``label_status``, and
        ``sample_weight`` columns to the dataset.

        Rows with non-final (pending) decisions are removed.
        Rows with unrecognized decisions are tagged as ``label_status='unknown'``
        and excluded from training unless ``include_unknown=True``.
        """
        if dataset.empty:
            return dataset.copy()

        out = dataset.copy()
        decisions = out["analyst_decision"].astype(str).str.lower().str.strip()

        out["escalation_label"] = decisions.map(
            lambda d: 1 if d in POSITIVE_DECISIONS else (0 if d in NEGATIVE_DECISIONS else -1)
        ).astype(int)

        out["sar_label"] = decisions.map(
            lambda d: 1 if d in SAR_DECISIONS else 0
        ).astype(int)

        out["label_status"] = decisions.map(self._label_status)

        out["sample_weight"] = decisions.map(
            lambda d: _DECISION_WEIGHTS.get(d, 1.0)
        ).astype(float)

        # Exclude rows with non-final or unrecognized decisions
        before = len(out)
        out = out[out["label_status"] == "final"].reset_index(drop=True)
        excluded = before - len(out)
        if excluded > 0:
            logger.info(
                "LabelBuilder: excluded %d rows with non-final/unknown decisions",
                excluded,
            )

        if out.empty:
            raise ValueError(
                "No finalized labels remain after filtering. "
                "Ensure analyst decisions are from: "
                + str(sorted(POSITIVE_DECISIONS | NEGATIVE_DECISIONS))
            )

        pos_rate = float(out["escalation_label"].mean())
        sar_rate = float(out["sar_label"].mean())
        logger.info(
            "LabelBuilder: %d labeled rows — escalation_positive_rate=%.3f, sar_rate=%.3f",
            len(out),
            pos_rate,
            sar_rate,
        )
        return out

    def build_time_labels(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Validate and return the time dataset with ``resolution_hours`` as label.

        The ``resolution_hours`` column must already be present (added by
        TrainingDatasetBuilder.build_time_dataset). This method validates
        the range and adds a log-transformed version for direct use in
        regression models.
        """
        if dataset.empty:
            return dataset.copy()

        out = dataset.copy()
        if "resolution_hours" not in out.columns:
            raise ValueError("'resolution_hours' column not found. Call build_time_dataset first.")

        out["resolution_hours"] = pd.to_numeric(out["resolution_hours"], errors="coerce")
        before = len(out)
        out = out.dropna(subset=["resolution_hours"])
        out = out[out["resolution_hours"] >= 0.0].reset_index(drop=True)
        excluded = before - len(out)
        if excluded > 0:
            logger.info("LabelBuilder: excluded %d rows with invalid resolution_hours", excluded)

        import numpy as np
        # log1p transform reduces skew in resolution time distributions
        out["resolution_hours_log"] = np.log1p(out["resolution_hours"])

        logger.info(
            "LabelBuilder: %d time-labeled rows — median_hours=%.1f, p90_hours=%.1f",
            len(out),
            float(out["resolution_hours"].median()),
            float(out["resolution_hours"].quantile(0.90)),
        )
        return out

    def label_summary(self, labeled_dataset: pd.DataFrame) -> dict[str, Any]:
        """Return a summary dict describing label distribution and quality."""
        if labeled_dataset.empty:
            return {"rows": 0}

        summary: dict[str, Any] = {"rows": len(labeled_dataset)}

        if "escalation_label" in labeled_dataset.columns:
            vc = labeled_dataset["escalation_label"].value_counts().to_dict()
            summary["escalation_label_counts"] = {int(k): int(v) for k, v in vc.items()}
            summary["escalation_positive_rate"] = float(labeled_dataset["escalation_label"].mean())

        if "sar_label" in labeled_dataset.columns:
            summary["sar_rate"] = float(labeled_dataset["sar_label"].mean())

        if "analyst_decision" in labeled_dataset.columns:
            summary["decision_counts"] = (
                labeled_dataset["analyst_decision"]
                .value_counts()
                .to_dict()
            )

        if "resolution_hours" in labeled_dataset.columns:
            summary["resolution_hours_median"] = float(labeled_dataset["resolution_hours"].median())
            summary["resolution_hours_p90"] = float(labeled_dataset["resolution_hours"].quantile(0.90))

        return summary

    @staticmethod
    def _label_status(decision: str) -> str:
        if decision in POSITIVE_DECISIONS or decision in NEGATIVE_DECISIONS:
            return "final"
        if decision in PENDING_DECISIONS:
            return "pending"
        return "unknown"
