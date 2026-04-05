"""Time-based dataset splitter for AML model training.

Uses temporal ordering only — never random splits — to prevent future
leakage and mimic production deployment conditions.

Optional entity-based deduplication ensures a customer/entity does not
appear in both training and validation partitions (cross-contamination guard).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

logger = logging.getLogger("althea.training.splitter")


@dataclass
class DataSplit:
    """Holds train / validation / test DataFrames and split metadata."""
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def sizes(self) -> dict[str, int]:
        return {
            "train": len(self.train),
            "validation": len(self.validation),
            "test": len(self.test),
        }


class TimeBasedSplitter:
    """Split a labeled dataset into train / val / test using temporal order.

    The default fractions (0.70 / 0.15 / 0.15) approximate a common
    walk-forward backtesting regime for AML models.

    ``entity_col`` — when specified, ensures entities (e.g. user_id,
    customer_id) do not appear in more than one partition. Entities
    are assigned to the partition that contains their earliest alert.
    This is a conservative leakage guard at the cost of slightly
    smaller validation/test sets.
    """

    def __init__(
        self,
        train_frac: float = 0.70,
        val_frac: float = 0.15,
        time_col: str = "alert_created_at",
        entity_col: str | None = None,
    ) -> None:
        if not (0.0 < train_frac < 1.0):
            raise ValueError("train_frac must be in (0, 1)")
        if not (0.0 < val_frac < 1.0):
            raise ValueError("val_frac must be in (0, 1)")
        if train_frac + val_frac >= 1.0:
            raise ValueError("train_frac + val_frac must be < 1")

        self._train_frac = train_frac
        self._val_frac = val_frac
        self._time_col = time_col
        self._entity_col = entity_col

    def split(self, dataset: pd.DataFrame) -> DataSplit:
        """Perform a time-based three-way split.

        Parameters
        ----------
        dataset : pd.DataFrame
            Labeled dataset with at least the time column present.

        Returns
        -------
        DataSplit
            Named train / validation / test DataFrames with split metadata.
        """
        if dataset.empty:
            raise ValueError("Input dataset is empty; cannot perform time-based split.")

        df = dataset.copy()

        # Parse timestamp column; fall back to positional ordering if absent
        if self._time_col in df.columns:
            df["_sort_ts"] = pd.to_datetime(df[self._time_col], utc=True, errors="coerce")
            # Fill missing timestamps with min so they go to training set
            df["_sort_ts"] = df["_sort_ts"].fillna(df["_sort_ts"].min())
        else:
            logger.warning(
                "TimeBasedSplitter: column '%s' not found — using row order as proxy",
                self._time_col,
            )
            df["_sort_ts"] = pd.to_datetime("2000-01-01", utc=True)

        df = df.sort_values("_sort_ts", kind="stable").reset_index(drop=True)
        n = len(df)

        if self._entity_col and self._entity_col in df.columns:
            train, val, test = self._entity_aware_split(df, n)
        else:
            train, val, test = self._positional_split(df, n)

        train = train.drop(columns=["_sort_ts"], errors="ignore").reset_index(drop=True)
        val = val.drop(columns=["_sort_ts"], errors="ignore").reset_index(drop=True)
        test = test.drop(columns=["_sort_ts"], errors="ignore").reset_index(drop=True)

        cutoff_val = str(df["_sort_ts"].iloc[int(n * self._train_frac)]) if n > 0 else "N/A"
        cutoff_test = str(df["_sort_ts"].iloc[int(n * (self._train_frac + self._val_frac))]) if n > 0 else "N/A"

        metadata = {
            "split_method": "time_based" + ("_entity_aware" if self._entity_col else ""),
            "total_rows": n,
            "train_rows": len(train),
            "val_rows": len(val),
            "test_rows": len(test),
            "train_size": len(train),
            "val_size": len(val),
            "test_size": len(test),
            "train_frac_actual": len(train) / n if n > 0 else 0.0,
            "val_frac_actual": len(val) / n if n > 0 else 0.0,
            "test_frac_actual": len(test) / n if n > 0 else 0.0,
            "time_col": self._time_col,
            "entity_col": self._entity_col,
            "cutoff_train_val": cutoff_val,
            "cutoff_val_test": cutoff_test,
        }

        if "escalation_label" in train.columns:
            metadata["train_positive_rate"] = float(train["escalation_label"].mean())
            metadata["val_positive_rate"] = float(val["escalation_label"].mean()) if not val.empty else None
            metadata["test_positive_rate"] = float(test["escalation_label"].mean()) if not test.empty else None

        logger.info(
            "TimeBasedSplitter: train=%d val=%d test=%d",
            len(train),
            len(val),
            len(test),
        )
        return DataSplit(train=train, validation=val, test=test, metadata=metadata)

    # ------------------------------------------------------------------
    # Internal split strategies
    # ------------------------------------------------------------------

    def _positional_split(
        self, df: pd.DataFrame, n: int
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        train_end = int(n * self._train_frac)
        val_end = int(n * (self._train_frac + self._val_frac))
        return (
            df.iloc[:train_end],
            df.iloc[train_end:val_end],
            df.iloc[val_end:],
        )

    def _entity_aware_split(
        self, df: pd.DataFrame, n: int
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Assign entities to partitions based on the timestamp of their
        earliest alert. An entity stays in a single partition to prevent
        cross-contamination."""
        train_end_idx = int(n * self._train_frac)
        val_end_idx = int(n * (self._train_frac + self._val_frac))

        train_cutoff = df["_sort_ts"].iloc[train_end_idx] if train_end_idx < n else df["_sort_ts"].iloc[-1]
        val_cutoff = df["_sort_ts"].iloc[val_end_idx] if val_end_idx < n else df["_sort_ts"].iloc[-1]

        # First alert timestamp per entity
        entity_first = df.groupby(self._entity_col)["_sort_ts"].min()
        train_entities = set(entity_first[entity_first < train_cutoff].index)
        val_entities = set(entity_first[(entity_first >= train_cutoff) & (entity_first < val_cutoff)].index)
        test_entities = set(entity_first[entity_first >= val_cutoff].index)

        entity_col = df[self._entity_col]
        train_mask = entity_col.isin(train_entities)
        val_mask = entity_col.isin(val_entities)
        test_mask = entity_col.isin(test_entities)

        # Fallback for rows with null entity: assign to train
        null_mask = entity_col.isna() | (entity_col.astype(str) == "")
        train_mask = train_mask | null_mask

        return df[train_mask], df[val_mask], df[test_mask]
