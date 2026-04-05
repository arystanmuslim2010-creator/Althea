"""Performance monitor — tracks model classification/ranking quality over time.

Operates on finalized outcomes joined to scores at decision time.
All metrics are computed on finalized (non-pending) outcome rows only.

Metrics tracked:
    pr_auc              precision-recall AUC
    precision_at_k      precision at k=10, 25, 50, 100
    recall_at_k         recall at k=10, 25, 50, 100
    lift_top_decile     lift at top 10%
    suspicious_capture_top_20pct
    score_drift         mean score delta vs reference period
    rank_correlation    Spearman rank correlation with risk score
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("althea.monitoring.performance")

_POSITIVE_DECISIONS = frozenset({"true_positive", "escalated", "sar_filed", "confirmed_suspicious"})


class PerformanceMonitor:
    """Compute model performance metrics from finalized analyst outcomes."""

    def __init__(self, repository) -> None:
        self._repository = repository

    def compute(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
        k_values: tuple[int, ...] = (10, 25, 50, 100),
        typology_breakdown: bool = True,
        segment_breakdown: bool = True,
    ) -> dict[str, Any]:
        """Compute performance metrics for finalized outcomes in the lookback window."""
        joined = self._fetch_scored_outcomes(tenant_id, lookback_days, model_version)
        if joined.empty:
            return {
                "status": "insufficient_data",
                "tenant_id": tenant_id,
                "model_version": model_version,
                "lookback_days": lookback_days,
            }

        y_true = joined["escalation_label"].to_numpy()
        y_prob = np.clip(joined["risk_score"].to_numpy() / 100.0, 0.0, 1.0)

        metrics: dict[str, Any] = {
            "tenant_id": tenant_id,
            "model_version": model_version,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": lookback_days,
            "n_outcomes": int(len(joined)),
            "positive_rate": float(y_true.mean()),
        }

        # PR-AUC
        try:
            from sklearn.metrics import average_precision_score
            if y_true.sum() > 0 and (1 - y_true).sum() > 0:
                metrics["pr_auc"] = float(average_precision_score(y_true, y_prob))
            else:
                metrics["pr_auc"] = None
        except Exception:
            metrics["pr_auc"] = None

        # Precision@k / Recall@k
        sorted_idx = np.argsort(-y_prob)
        total_pos = max(int(y_true.sum()), 1)
        pk = {}
        rk = {}
        for k in k_values:
            if k > len(y_true):
                continue
            top_k = y_true[sorted_idx[:k]]
            pk[f"precision_at_{k}"] = float(top_k.mean())
            rk[f"recall_at_{k}"] = float(top_k.sum() / total_pos)
        metrics["precision_at_k"] = pk
        metrics["recall_at_k"] = rk

        # Lift at top decile
        n10 = max(1, int(len(y_true) * 0.10))
        top10 = y_true[sorted_idx[:n10]]
        baseline = y_true.mean()
        metrics["lift_top_decile"] = float(top10.mean() / baseline) if baseline > 0 else None

        # Suspicious capture top 20%
        n20 = max(1, int(len(y_true) * 0.20))
        metrics["suspicious_capture_top_20pct"] = float(y_true[sorted_idx[:n20]].sum() / total_pos)

        # Score drift vs prior period
        metrics["mean_score"] = float(joined["risk_score"].mean())
        metrics["score_std"] = float(joined["risk_score"].std())

        # Per-typology breakdown
        if typology_breakdown and "typology" in joined.columns:
            breakdown: dict[str, Any] = {}
            for typo, sub in joined.groupby("typology"):
                if len(sub) < 5:
                    continue
                yt = sub["escalation_label"].to_numpy()
                yp = np.clip(sub["risk_score"].to_numpy() / 100.0, 0.0, 1.0)
                try:
                    from sklearn.metrics import average_precision_score
                    ap = float(average_precision_score(yt, yp)) if yt.sum() > 0 and (1 - yt).sum() > 0 else None
                except Exception:
                    ap = None
                breakdown[str(typo)] = {
                    "pr_auc": ap,
                    "n": len(sub),
                    "positive_rate": float(yt.mean()),
                }
            metrics["by_typology"] = breakdown

        logger.info(
            json.dumps(
                {
                    "event": "performance_monitor_computed",
                    "tenant_id": tenant_id,
                    "n_outcomes": metrics["n_outcomes"],
                    "pr_auc": metrics.get("pr_auc"),
                    "suspicious_capture_top_20pct": metrics.get("suspicious_capture_top_20pct"),
                },
                ensure_ascii=True,
            )
        )
        return metrics

    def _fetch_scored_outcomes(
        self,
        tenant_id: str,
        lookback_days: int,
        model_version: str | None,
    ) -> pd.DataFrame:
        """Fetch outcomes joined with their scores at decision time."""
        with self._repository.session(tenant_id=tenant_id) as session:
            mv_filter = "AND o.model_version = :model_version" if model_version else ""
            rows = session.execute(
                text(
                    f"""
                    SELECT
                        o.alert_id,
                        o.analyst_decision,
                        o.risk_score_at_decision   AS risk_score,
                        o.model_version,
                        a.payload_json
                    FROM alert_outcomes o
                    LEFT JOIN alerts a
                        ON a.alert_id = o.alert_id AND a.tenant_id = o.tenant_id
                    WHERE o.tenant_id = :tenant_id
                      AND o.timestamp >= NOW() - INTERVAL '{lookback_days} days'
                      AND o.analyst_decision IS NOT NULL
                      {mv_filter}
                    ORDER BY o.timestamp DESC
                    LIMIT 50000
                    """
                ),
                {"tenant_id": tenant_id, **({"model_version": model_version} if model_version else {})},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            decision = str(r[1] or "").lower()
            payload = r[4] or {}
            if isinstance(payload, str):
                import json as _json
                try:
                    payload = _json.loads(payload)
                except Exception:
                    payload = {}
            records.append({
                "alert_id": str(r[0]),
                "analyst_decision": decision,
                "risk_score": float(r[2] or 0.0),
                "model_version": str(r[3] or ""),
                "typology": str(payload.get("typology") or ""),
                "segment": str(payload.get("segment") or ""),
                "escalation_label": 1 if decision in _POSITIVE_DECISIONS else 0,
            })

        return pd.DataFrame(records)
