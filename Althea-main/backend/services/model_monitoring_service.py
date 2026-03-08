from __future__ import annotations

from typing import Any

import numpy as np

from storage.postgres_repository import EnterpriseRepository


class ModelMonitoringService:
    def __init__(self, repository: EnterpriseRepository) -> None:
        self._repository = repository

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

    def record_run_monitoring(self, tenant_id: str, run_id: str, model_version: str, scores: list[float]) -> dict[str, Any]:
        current = np.asarray(scores, dtype=float)
        previous = np.asarray([], dtype=float)

        for run in self._repository.list_pipeline_runs(tenant_id, limit=25):
            if run.get("run_id") and run.get("run_id") != run_id and str(run.get("status", "")).lower() == "completed":
                payloads = self._repository.list_alert_payloads_by_run(tenant_id, run["run_id"], limit=50000)
                prev_scores = [float(p.get("risk_score", 0.0) or 0.0) for p in payloads]
                if prev_scores:
                    previous = np.asarray(prev_scores, dtype=float)
                    break

        psi_score = self._psi(previous, current) if previous.size else 0.0
        drift_score = float(abs(current.mean() - previous.mean())) if previous.size else 0.0
        degradation_flag = bool(psi_score > 0.25 or drift_score > 15)

        metrics = {
            "psi": psi_score,
            "mean_score": float(current.mean()) if current.size else 0.0,
            "baseline_mean_score": float(previous.mean()) if previous.size else 0.0,
            "drift_score": drift_score,
            "degradation_flag": degradation_flag,
            "n_current": int(current.size),
            "n_baseline": int(previous.size),
        }

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
