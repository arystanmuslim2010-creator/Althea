"""Base interface for all ALTHEA feature builders.

All concrete builders inherit from ``BaseFeatureBuilder`` and implement
``build(df, context)``. The builder contract:

- Input ``df`` is a raw/normalized alerts DataFrame.
- ``context`` carries optional enrichment data (transaction history, graph
  features, peer stats, etc.) that the builder may consume.
- Output is a DataFrame with one row per alert_id, columns named after
  the features produced by this builder.
- Builders must be stateless — no mutable state is kept between calls.
- Builders must never raise on missing optional context; they return zero
  or NaN for features they cannot compute.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BuilderContext:
    """Carries optional enrichment data passed to feature builders.

    All fields are optional; builders should guard against None values.
    """

    def __init__(
        self,
        transaction_history: pd.DataFrame | None = None,
        outcome_history: pd.DataFrame | None = None,
        peer_stats: pd.DataFrame | None = None,
        graph_features: pd.DataFrame | None = None,
        case_history: pd.DataFrame | None = None,
        tenant_id: str = "",
        as_of_timestamp: Any = None,
    ) -> None:
        self.transaction_history = transaction_history
        self.outcome_history = outcome_history
        self.peer_stats = peer_stats
        self.graph_features = graph_features
        self.case_history = case_history
        self.tenant_id = tenant_id
        self.as_of_timestamp = as_of_timestamp


class BaseFeatureBuilder(ABC):
    """Abstract base for all feature builders."""

    @property
    @abstractmethod
    def feature_names(self) -> list[str]:
        """Return the list of feature columns this builder produces."""

    @abstractmethod
    def build(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        """Build features for a batch of alerts.

        Parameters
        ----------
        df      : normalized alerts DataFrame. Must contain at least ``alert_id``.
        context : optional enrichment data.

        Returns
        -------
        pd.DataFrame with columns: ``alert_id`` + one column per feature.
        Rows correspond to the same alerts as ``df`` (same index order).
        """

    def build_safe(
        self,
        df: pd.DataFrame,
        context: BuilderContext | None = None,
    ) -> pd.DataFrame:
        """Like ``build`` but returns zeros on any unhandled exception.

        Use in production inference paths where a single builder failing
        must not bring down the entire feature pipeline.
        """
        try:
            return self.build(df, context)
        except Exception:
            import logging
            logging.getLogger("althea.features.builder").exception(
                "Builder %s failed; returning zero features",
                self.__class__.__name__,
            )
            zeros = pd.DataFrame({"alert_id": df["alert_id"].tolist() if "alert_id" in df.columns else []})
            for name in self.feature_names:
                zeros[name] = 0.0
            return zeros
