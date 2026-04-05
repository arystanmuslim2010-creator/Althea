"""Calibration monitor — tracks probability calibration drift over time.

A well-calibrated model produces probabilities where P(escalation | score=0.7)
is actually 70%. Calibration drift means the model is systematically over-
or under-confident, which degrades queue ranking quality.

Metrics:
    expected_calibration_error (ECE)  — overall calibration quality
    max_calibration_error (MCE)       — worst bin calibration
    reliability_diagram               — per-bin (pred, actual) pairs
    calibration_drift                 — ECE delta vs reference period
    overconfidence_flag               — model assigns too-high probs
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("althea.monitoring.calibration")

_POSITIVE_DECISIONS = frozenset({"true_positive", "escalated", "sar_filed", "confirmed_suspicious"})


class CalibrationMonitor:
    """Monitor probability calibration on finalized outcome windows."""

    def __init__(self, repository, n_bins: int = 10) -> None:
        self._repository = repository
        self._n_bins = n_bins

    def compute(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
        reference_ece: float | None = None,
    ) -> dict[str, Any]:
        """Compute calibration metrics.

        Parameters
        ----------
        reference_ece : ECE from the original evaluation run; used to compute drift.
        """
        df = self._fetch_scored_outcomes(tenant_id, lookback_days, model_version)
        if df.empty or len(df) < 20:
            return {
                "status": "insufficient_data",
                "tenant_id": tenant_id,
                "model_version": model_version,
                "n_outcomes": len(df),
            }

        y_true = df["escalation_label"].to_numpy()
        y_prob = np.clip(df["risk_score"].to_numpy() / 100.0, 0.0, 1.0)

        ece = self._ece(y_true, y_prob)
        mce = self._mce(y_true, y_prob)
        reliability = self._reliability_diagram(y_true, y_prob)

        # Overconfidence: mean predicted prob significantly > actual positive rate
        mean_pred = float(y_prob.mean())
        actual_rate = float(y_true.mean())
        overconfidence = mean_pred > (actual_rate * 1.5) and mean_pred > 0.1

        metrics: dict[str, Any] = {
            "tenant_id": tenant_id,
            "model_version": model_version,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": lookback_days,
            "n_outcomes": int(len(df)),
            "ece": round(ece, 6),
            "mce": round(mce, 6),
            "mean_predicted_prob": round(mean_pred, 4),
            "actual_positive_rate": round(actual_rate, 4),
            "overconfidence_flag": overconfidence,
            "reliability_diagram": reliability,
        }

        if reference_ece is not None:
            metrics["calibration_drift"] = round(ece - reference_ece, 6)
            metrics["recalibration_recommended"] = abs(ece - reference_ece) > 0.05 or ece > 0.10

        logger.info(
            json.dumps(
                {
                    "event": "calibration_monitor_computed",
                    "tenant_id": tenant_id,
                    "ece": ece,
                    "overconfidence": overconfidence,
                },
                ensure_ascii=True,
            )
        )
        return metrics

    def _ece(self, y_true: np.ndarray, y_prob: np.ndarray) -> float:
        bins = np.linspace(0.0, 1.0, self._n_bins + 1)
        ece = 0.0
        n = len(y_true)
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (y_prob >= lo) & (y_prob < hi)
            if mask.sum() == 0:
                continue
            ece += (mask.sum() / n) * abs(y_prob[mask].mean() - y_true[mask].mean())
        return float(ece)

    def _mce(self, y_true: np.ndarray, y_prob: np.ndarray) -> float:
        bins = np.linspace(0.0, 1.0, self._n_bins + 1)
        mce = 0.0
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (y_prob >= lo) & (y_prob < hi)
            if mask.sum() == 0:
                continue
            mce = max(mce, abs(y_prob[mask].mean() - y_true[mask].mean()))
        return float(mce)

    def _reliability_diagram(self, y_true: np.ndarray, y_prob: np.ndarray) -> list[dict[str, float]]:
        bins = np.linspace(0.0, 1.0, self._n_bins + 1)
        diagram = []
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (y_prob >= lo) & (y_prob < hi)
            if mask.sum() == 0:
                continue
            diagram.append({
                "bin_lower": round(lo, 2),
                "bin_upper": round(hi, 2),
                "mean_predicted": round(float(y_prob[mask].mean()), 4),
                "actual_fraction": round(float(y_true[mask].mean()), 4),
                "count": int(mask.sum()),
            })
        return diagram

    def _fetch_scored_outcomes(
        self, tenant_id: str, lookback_days: int, model_version: str | None
    ) -> pd.DataFrame:
        with self._repository.session(tenant_id=tenant_id) as session:
            mv_filter = "AND model_version = :mv" if model_version else ""
            rows = session.execute(
                text(
                    f"""
                    SELECT analyst_decision, risk_score_at_decision
                    FROM alert_outcomes
                    WHERE tenant_id = :tenant_id
                      AND timestamp >= NOW() - INTERVAL '{lookback_days} days'
                      AND analyst_decision IS NOT NULL
                      AND risk_score_at_decision IS NOT NULL
                      {mv_filter}
                    LIMIT 50000
                    """
                ),
                {"tenant_id": tenant_id, **({"mv": model_version} if model_version else {})},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([
            {
                "escalation_label": 1 if str(r[0] or "").lower() in _POSITIVE_DECISIONS else 0,
                "risk_score": float(r[1] or 0.0),
            }
            for r in rows
        ])
