"""Model monitoring service — façade over monitoring sub-modules.

Provides a unified API for:
    - PSI-based score drift (legacy pipeline integration point)
    - PerformanceMonitor metrics
    - CalibrationMonitor metrics
    - BusinessMonitor metrics

Existing callers (pipeline_service, workers) use record_run_monitoring()
which is preserved with the same return contract. New callers can access
the specialized monitors directly via compute_*_report().
"""
from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np

from storage.postgres_repository import EnterpriseRepository

logger = logging.getLogger("althea.monitoring.service")


class ModelMonitoringService:
    """Façade over the monitoring sub-modules.

    Preserves the existing record_run_monitoring() contract while
    delegating to specialized monitors for richer analysis.
    """

    def __init__(
        self,
        repository: EnterpriseRepository,
        performance_monitor=None,
        calibration_monitor=None,
        business_monitor=None,
    ) -> None:
        self._repository = repository
        self._performance_monitor = performance_monitor
        self._calibration_monitor = calibration_monitor
        self._business_monitor = business_monitor

    def record_run_monitoring(
        self,
        tenant_id: str,
        run_id: str,
        model_version: str,
        scores: list[float],
    ) -> dict[str, Any]:
        """Record score distribution metrics for a completed pipeline run."""
        current = np.asarray(scores, dtype=float)
        current = current[np.isfinite(current)]
        previous = np.asarray([], dtype=float)

        for run in self._repository.list_pipeline_runs(tenant_id, limit=25):
            if (
                run.get("run_id")
                and run.get("run_id") != run_id
                and str(run.get("status", "")).lower() == "completed"
            ):
                payloads = self._repository.list_alert_payloads_by_run(
                    tenant_id, run["run_id"], limit=50000
                )
                prev_scores = [float(p.get("risk_score", 0.0) or 0.0) for p in payloads]
                if prev_scores:
                    previous = np.asarray(prev_scores, dtype=float)
                    previous = previous[np.isfinite(previous)]
                    break

        psi_score = self._psi(previous, current) if previous.size else 0.0
        drift_score = float(abs(current.mean() - previous.mean())) if previous.size else 0.0
        degradation_flag = bool(psi_score > 0.25 or drift_score > 15)

        metrics = {
            "psi": float(psi_score),
            "mean_score": float(current.mean()) if current.size else 0.0,
            "baseline_mean_score": float(previous.mean()) if previous.size else 0.0,
            "drift_score": float(drift_score),
            "degradation_flag": degradation_flag,
            "n_current": int(current.size),
            "n_baseline": int(previous.size),
        }

        if degradation_flag:
            logger.warning(
                json.dumps(
                    {
                        "event": "model_degradation_detected",
                        "tenant_id": tenant_id,
                        "run_id": run_id,
                        "model_version": model_version,
                        "psi": psi_score,
                        "drift_score": drift_score,
                    },
                    ensure_ascii=True,
                )
            )

        record = self._repository.save_model_monitoring(
            {
                "tenant_id": tenant_id,
                "run_id": run_id,
                "model_version": model_version,
                "psi_score": float(psi_score),
                "drift_score": float(drift_score),
                "degradation_flag": degradation_flag,
                "metrics_json": metrics,
            }
        )
        return {"record": record, "metrics": metrics}

    def compute_performance_report(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
    ) -> dict[str, Any]:
        """Delegate to PerformanceMonitor if available."""
        if self._performance_monitor is None:
            return {"status": "performance_monitor_not_configured"}
        return self._performance_monitor.compute(
            tenant_id=tenant_id,
            model_version=model_version,
            lookback_days=lookback_days,
        )

    def compute_calibration_report(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
    ) -> dict[str, Any]:
        """Delegate to CalibrationMonitor if available."""
        if self._calibration_monitor is None:
            return {"status": "calibration_monitor_not_configured"}
        return self._calibration_monitor.compute(
            tenant_id=tenant_id,
            model_version=model_version,
            lookback_days=lookback_days,
        )

    def compute_business_report(
        self,
        tenant_id: str,
        model_version: str | None = None,
        lookback_days: int = 90,
    ) -> dict[str, Any]:
        """Delegate to BusinessMonitor if available."""
        if self._business_monitor is None:
            return {"status": "business_monitor_not_configured"}
        return self._business_monitor.compute(
            tenant_id=tenant_id,
            model_version=model_version,
            lookback_days=lookback_days,
        )

    def record_monitoring(
        self,
        tenant_id: str,
        model_version: str,
        model_drift: float,
        score_distribution_shift: float,
        alert_outcome_feedback: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Compatibility shim used by governance_service lifecycle calls."""
        logger.info(
            json.dumps(
                {
                    "event": "governance_monitoring_record",
                    "tenant_id": tenant_id,
                    "model_version": model_version,
                    "model_drift": model_drift,
                    "score_distribution_shift": score_distribution_shift,
                    **(metadata or {}),
                },
                ensure_ascii=True,
            )
        )

    @staticmethod
    def _psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
        if expected.size == 0 or actual.size == 0:
            return 0.0
        breaks = np.quantile(expected, np.linspace(0, 1, bins + 1))
        breaks[0] = -np.inf
        breaks[-1] = np.inf
        expected_hist, _ = np.histogram(expected, bins=breaks)
        actual_hist, _ = np.histogram(actual, bins=breaks)
        expected_pct = np.clip(expected_hist / max(expected_hist.sum(), 1), 1e-6, None)
        actual_pct = np.clip(actual_hist / max(actual_hist.sum(), 1), 1e-6, None)
        return float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))
