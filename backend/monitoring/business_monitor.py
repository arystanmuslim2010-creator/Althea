"""Business monitor — tracks operational impact metrics.

These metrics translate model quality into business outcomes:
    queue_compression_ratio     fraction of alerts actively worked
    analyst_hours_saved         estimated investigator time saved
    sar_capture_top_20pct       SARs filed in top 20% of queue
    false_positive_suppression  suppressed alerts that were FPs (correct suppressions)
    missed_escalations          escalated alerts in suppressed tier (wrong suppressions)
    per_tenant_breakdown        above metrics per tenant (admin view)
    per_typology_breakdown      above metrics per typology
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("althea.monitoring.business")

_SAR_DECISIONS = frozenset({"sar_filed"})
_ESCALATED_DECISIONS = frozenset({"true_positive", "escalated", "sar_filed", "confirmed_suspicious"})
_FP_DECISIONS = frozenset({"false_positive", "benign_activity"})


class BusinessMonitor:
    """Compute business impact metrics from alerts and outcomes."""

    def __init__(
        self,
        repository,
        analyst_hours_per_alert: float = 2.0,
    ) -> None:
        self._repository = repository
        self._analyst_hours = analyst_hours_per_alert

    def compute(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
        typology_breakdown: bool = True,
    ) -> dict[str, Any]:
        """Compute business metrics for the specified time window."""
        df = self._fetch_alerts_with_outcomes(tenant_id, lookback_days, model_version)
        if df.empty:
            return {
                "status": "insufficient_data",
                "tenant_id": tenant_id,
                "model_version": model_version,
            }

        metrics: dict[str, Any] = {
            "tenant_id": tenant_id,
            "model_version": model_version,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": lookback_days,
            "total_alerts": int(len(df)),
        }

        total = len(df)
        labeled = df.dropna(subset=["analyst_decision"])

        # Queue compression
        in_queue = df[df.get("in_queue", pd.Series(True, index=df.index)).astype(bool)] if "in_queue" in df.columns else df
        suppressed = df[~df.index.isin(in_queue.index)] if "in_queue" in df.columns else pd.DataFrame()
        metrics["queue_compression_ratio"] = round(len(in_queue) / max(total, 1), 4)
        metrics["suppressed_count"] = len(suppressed)
        metrics["queued_count"] = len(in_queue)

        # Analyst hours saved
        total_hours = total * self._analyst_hours
        worked_hours = len(in_queue) * self._analyst_hours
        metrics["estimated_analyst_hours_full_queue"] = round(total_hours, 1)
        metrics["estimated_analyst_hours_actual"] = round(worked_hours, 1)
        metrics["estimated_analyst_hours_saved"] = round(total_hours - worked_hours, 1)

        if not labeled.empty:
            decisions = labeled["analyst_decision"].astype(str).str.lower()
            risk_scores = pd.to_numeric(labeled["risk_score"], errors="coerce").fillna(0.0)

            # SAR capture in top 20%
            top20_count = max(1, int(len(labeled) * 0.20))
            sorted_labeled = labeled.sort_values("risk_score", ascending=False).head(top20_count)
            sar_decisions_top20 = sorted_labeled["analyst_decision"].astype(str).str.lower()
            total_sars = int((decisions.isin(_SAR_DECISIONS)).sum())
            sars_in_top20 = int((sar_decisions_top20.isin(_SAR_DECISIONS)).sum())
            metrics["sar_capture_top_20pct"] = round(sars_in_top20 / max(total_sars, 1), 4)
            metrics["total_sar_filed"] = total_sars

            # Escalation capture top 20%
            total_escalated = int((decisions.isin(_ESCALATED_DECISIONS)).sum())
            esc_in_top20 = int((sorted_labeled["analyst_decision"].astype(str).str.lower().isin(_ESCALATED_DECISIONS)).sum())
            metrics["escalation_capture_top_20pct"] = round(esc_in_top20 / max(total_escalated, 1), 4)

            # Suppression quality: what fraction of suppressed alerts were correct?
            if "in_queue" in df.columns and not suppressed.empty:
                supp_labeled = suppressed.dropna(subset=["analyst_decision"])
                if not supp_labeled.empty:
                    supp_decisions = supp_labeled["analyst_decision"].astype(str).str.lower()
                    correct_supp = int((supp_decisions.isin(_FP_DECISIONS)).sum())
                    missed_esc = int((supp_decisions.isin(_ESCALATED_DECISIONS)).sum())
                    metrics["suppression_precision"] = round(correct_supp / max(len(supp_labeled), 1), 4)
                    metrics["missed_escalations_in_suppressed"] = missed_esc
                    metrics["missed_sar_in_suppressed"] = int((supp_decisions.isin(_SAR_DECISIONS)).sum())

        # Per-typology breakdown
        if typology_breakdown and "typology" in df.columns and not labeled.empty:
            breakdown: dict[str, Any] = {}
            for typo, sub in labeled.groupby("typology"):
                if len(sub) < 3:
                    continue
                sub_decisions = sub["analyst_decision"].astype(str).str.lower()
                breakdown[str(typo)] = {
                    "n": len(sub),
                    "escalation_rate": round(float((sub_decisions.isin(_ESCALATED_DECISIONS)).mean()), 4),
                    "sar_rate": round(float((sub_decisions.isin(_SAR_DECISIONS)).mean()), 4),
                    "fp_rate": round(float((sub_decisions.isin(_FP_DECISIONS)).mean()), 4),
                    "mean_risk_score": round(float(pd.to_numeric(sub["risk_score"], errors="coerce").mean()), 2),
                }
            metrics["by_typology"] = breakdown

        logger.info(
            json.dumps(
                {
                    "event": "business_monitor_computed",
                    "tenant_id": tenant_id,
                    "total_alerts": total,
                    "queue_compression_ratio": metrics.get("queue_compression_ratio"),
                    "hours_saved": metrics.get("estimated_analyst_hours_saved"),
                    "sar_capture_top_20pct": metrics.get("sar_capture_top_20pct"),
                },
                ensure_ascii=True,
            )
        )
        return metrics

    def _fetch_alerts_with_outcomes(
        self, tenant_id: str, lookback_days: int, model_version: str | None
    ) -> pd.DataFrame:
        with self._repository.session(tenant_id=tenant_id) as session:
            rows = session.execute(
                text(
                    f"""
                    SELECT
                        a.alert_id,
                        a.risk_score,
                        a.risk_band,
                        a.priority,
                        a.status,
                        a.payload_json,
                        o.analyst_decision,
                        o.model_version
                    FROM alerts a
                    LEFT JOIN alert_outcomes o
                        ON a.alert_id = o.alert_id AND a.tenant_id = o.tenant_id
                    WHERE a.tenant_id = :tenant_id
                      AND a.created_at >= NOW() - INTERVAL '{lookback_days} days'
                    ORDER BY a.risk_score DESC
                    LIMIT 100000
                    """
                ),
                {"tenant_id": tenant_id},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            payload = r[5] or {}
            if isinstance(payload, str):
                import json as _json
                try:
                    payload = _json.loads(payload)
                except Exception:
                    payload = {}
            records.append({
                "alert_id": str(r[0]),
                "risk_score": float(r[1] or 0.0),
                "risk_band": str(r[2] or ""),
                "priority": str(r[3] or ""),
                "status": str(r[4] or ""),
                "typology": str(payload.get("typology") or ""),
                "analyst_decision": r[6],
                "model_version": r[7],
            })
        return pd.DataFrame(records)
