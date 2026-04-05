"""Composite priority ranking formula.

Combines multiple model signals and heuristic adjustments into a single
priority score that determines queue ordering.

Formula (default weights):
    priority_score =
        escalation_prob * 100   × 0.45  (primary ML signal)
        + graph_risk_score      × 0.15  (structural risk)
        + similar_suspicious    × 100   × 0.15  (precedent risk)
        - expected_time_penalty × 0.10  (investigation cost)
        + regulatory_boost      × 0.10  (compliance urgency)
        - uncertainty_penalty   × 0.05  (model confidence discount)

All input signals are normalised before combining. The output is
clipped to [0, 100] and represents the recommended investigation
priority, NOT the escalation probability itself.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class PriorityWeights:
    """Configurable weights for the priority formula."""
    escalation_prob: float = 0.45
    graph_risk: float = 0.15
    similar_suspicious: float = 0.15
    time_penalty: float = 0.10
    regulatory_boost: float = 0.10
    uncertainty_penalty: float = 0.05

    def validate(self) -> None:
        total = (
            self.escalation_prob + self.graph_risk + self.similar_suspicious
            + self.time_penalty + self.regulatory_boost + self.uncertainty_penalty
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Priority weights must sum to 1.0, got {total:.4f}")


@dataclass
class PrioritySignals:
    """Input signals for the priority formula (per alert)."""
    escalation_prob: float          # [0, 1] calibrated escalation probability
    graph_risk_score: float = 0.0   # [0, 100] community/graph risk
    similar_suspicious_strength: float = 0.0  # [0, 1] retrieval signal
    p50_hours: float = 24.0         # expected resolution hours (p50)
    time_penalty_hours: float | None = None  # backward-compatible alias for p50_hours
    uncertainty: float = 0.0        # model uncertainty proxy [0, 1]
    regulatory_urgency: float = 0.0 # [0, 1] sanctions/CTRA urgency flag

    # Extra raw signals for transparency
    metadata: dict[str, Any] = field(default_factory=dict)


class PriorityFormula:
    """Compute composite priority score from multiple ML and rule signals."""

    def __init__(self, weights: PriorityWeights | None = None) -> None:
        self._weights = weights or PriorityWeights()
        self._weights.validate()

    def compute(self, signals: PrioritySignals) -> float:
        """Return a priority score in [0, 100]."""
        w = self._weights

        # Escalation contribution
        esc = float(np.clip(signals.escalation_prob, 0.0, 1.0)) * 100.0

        # Graph risk contribution (already in [0, 100])
        graph = float(np.clip(signals.graph_risk_score, 0.0, 100.0))

        # Similar suspicious contribution
        sim = float(np.clip(signals.similar_suspicious_strength, 0.0, 1.0)) * 100.0

        # Time penalty: longer cases penalize priority slightly
        # 24h = neutral (0), 168h (1 week) = max penalty of -30
        max_penalty = 30.0
        penalty_hours = signals.time_penalty_hours if signals.time_penalty_hours is not None else signals.p50_hours
        time_penalty = min((float(penalty_hours) / 168.0) * max_penalty, max_penalty)

        # Regulatory urgency: typology/country boosts (from governance_service)
        reg_boost = float(np.clip(signals.regulatory_urgency, 0.0, 1.0)) * 100.0

        # Uncertainty penalty: penalize low-confidence predictions
        unc_penalty = float(np.clip(signals.uncertainty, 0.0, 1.0)) * 100.0

        score = (
            w.escalation_prob * esc
            + w.graph_risk * graph
            + w.similar_suspicious * sim
            - w.time_penalty * time_penalty
            + w.regulatory_boost * reg_boost
            - w.uncertainty_penalty * unc_penalty
        )
        return float(np.clip(score, 0.0, 100.0))

    def compute_batch(self, signals_df: pd.DataFrame) -> pd.Series:
        """Vectorized batch computation from a DataFrame of signals.

        Expected columns:
            escalation_prob, graph_risk_score, similar_suspicious_strength,
            p50_hours, uncertainty, regulatory_urgency
        """
        def _safe(col: str, default: float) -> pd.Series:
            if col in signals_df.columns:
                return pd.to_numeric(signals_df[col], errors="coerce").fillna(default)
            return pd.Series(default, index=signals_df.index)

        w = self._weights
        esc = _safe("escalation_prob", 0.0).clip(0, 1) * 100.0
        graph = _safe("graph_risk_score", 0.0).clip(0, 100)
        sim = _safe("similar_suspicious_strength", 0.0).clip(0, 1) * 100.0
        time_penalty_col = "time_penalty_hours" if "time_penalty_hours" in signals_df.columns else "p50_hours"
        time_pen = (_safe(time_penalty_col, 24.0) / 168.0 * 30.0).clip(0, 30)
        reg = _safe("regulatory_urgency", 0.0).clip(0, 1) * 100.0
        unc = _safe("uncertainty", 0.0).clip(0, 1) * 100.0

        scores = (
            w.escalation_prob * esc
            + w.graph_risk * graph
            + w.similar_suspicious * sim
            - w.time_penalty * time_pen
            + w.regulatory_boost * reg
            - w.uncertainty_penalty * unc
        ).clip(0.0, 100.0)

        return scores

    @staticmethod
    def priority_bucket(score: float) -> str:
        """Map a priority score to a named bucket."""
        if score >= 85:
            return "CRITICAL"
        if score >= 65:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"
