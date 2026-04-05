"""Model evaluator for escalation and time models.

Computes classification, ranking, and business metrics for the
escalation model, and regression metrics for the time model.

Metrics produced:
  Classification : PR-AUC, ROC-AUC, precision@k, recall@k, F1
  Ranking        : lift at top decile, suspicious capture in top N%
  Calibration    : ECE (expected calibration error)
  Business       : estimated analyst hours saved, queue compression ratio
  Regression     : MAE, RMSE, MAPE, median AE (for time model)
"""
from __future__ import annotations

import json
import logging
import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

logger = logging.getLogger("althea.training.evaluator")


class ModelEvaluator:
    """Compute a comprehensive evaluation report for a trained model."""

    def evaluate_escalation(
        self,
        y_true: np.ndarray | pd.Series,
        y_prob: np.ndarray,
        y_pred: np.ndarray | None = None,
        segments: pd.Series | None = None,
        typologies: pd.Series | None = None,
        k_values: tuple[int, ...] = (10, 25, 50, 100),
        top_n_pct: float = 0.20,
    ) -> dict[str, Any]:
        """Evaluate a binary escalation model.

        Parameters
        ----------
        y_true      : true binary labels
        y_prob      : predicted probabilities (class 1)
        y_pred      : optional binary predictions (threshold = 0.5)
        segments    : optional segment column for breakdown metrics
        typologies  : optional typology column for breakdown metrics
        k_values    : values of k for precision@k / recall@k
        top_n_pct   : top fraction to use for capture-rate metrics
        """
        y_true = np.asarray(y_true, dtype=int)
        y_prob = np.clip(np.asarray(y_prob, dtype=float), 0.0, 1.0)

        if y_pred is None:
            y_pred = (y_prob >= 0.5).astype(int)

        if len(y_true) == 0:
            return {"error": "empty_evaluation_set"}

        metrics: dict[str, Any] = {}

        # --- Core classification metrics ---
        if y_true.sum() > 0 and (1 - y_true).sum() > 0:
            metrics["pr_auc"] = float(average_precision_score(y_true, y_prob))
            metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
        else:
            metrics["pr_auc"] = None
            metrics["roc_auc"] = None
            metrics["warning"] = "single_class_in_eval_set"

        metrics["f1"] = float(f1_score(y_true, y_pred, zero_division=0))
        metrics["precision"] = float(precision_score(y_true, y_pred, zero_division=0))
        metrics["recall"] = float(recall_score(y_true, y_pred, zero_division=0))
        metrics["positive_rate"] = float(y_true.mean())
        metrics["n_samples"] = int(len(y_true))
        metrics["n_positive"] = int(y_true.sum())

        # --- Precision@k and Recall@k ---
        sorted_idx = np.argsort(-y_prob)
        pk_metrics: dict[str, float] = {}
        rk_metrics: dict[str, float] = {}
        for k in k_values:
            if k > len(y_true):
                continue
            top_k = y_true[sorted_idx[:k]]
            pk_metrics[f"precision_at_{k}"] = float(top_k.mean())
            total_pos = max(y_true.sum(), 1)
            rk_metrics[f"recall_at_{k}"] = float(top_k.sum() / total_pos)
        metrics["precision_at_k"] = pk_metrics
        metrics["recall_at_k"] = rk_metrics

        # --- Lift at top decile ---
        metrics["lift_top_decile"] = self._lift_at_top_n(y_true, y_prob, pct=0.10)

        # --- Suspicious capture in top N% ---
        metrics[f"suspicious_capture_top_{int(top_n_pct * 100)}pct"] = self._capture_rate(
            y_true, y_prob, pct=top_n_pct
        )

        # --- SAR/escalated capture in top 10% ---
        metrics["suspicious_capture_top_10pct"] = self._capture_rate(y_true, y_prob, pct=0.10)
        metrics["suspicious_capture_top_20pct"] = self._capture_rate(y_true, y_prob, pct=0.20)

        # --- Calibration error ---
        metrics["expected_calibration_error"] = self._ece(y_true, y_prob, n_bins=10)

        # --- Business metrics ---
        metrics["business"] = self._business_metrics(y_true, y_prob, top_n_pct=top_n_pct)

        # --- Segment breakdown ---
        if segments is not None and len(segments) == len(y_true):
            metrics["by_segment"] = self._breakdown(y_true, y_prob, pd.Series(segments))

        # --- Typology breakdown ---
        if typologies is not None and len(typologies) == len(y_true):
            metrics["by_typology"] = self._breakdown(y_true, y_prob, pd.Series(typologies))

        logger.info(
            json.dumps(
                {
                    "event": "escalation_evaluation_complete",
                    "pr_auc": metrics.get("pr_auc"),
                    "roc_auc": metrics.get("roc_auc"),
                    "precision_at_50": pk_metrics.get("precision_at_50"),
                    "suspicious_capture_top_20pct": metrics.get("suspicious_capture_top_20pct"),
                },
                ensure_ascii=True,
            )
        )
        return metrics

    def evaluate_time_model(
        self,
        y_true: np.ndarray | pd.Series,
        y_pred_log: np.ndarray,
        label_transform: str = "log1p",
    ) -> dict[str, Any]:
        """Evaluate a time regression model.

        Parameters
        ----------
        y_true       : true resolution_hours values (original scale)
        y_pred_log   : predicted values in log scale
        label_transform : the transform applied to y_true during training
        """
        y_true_hours = np.asarray(y_true, dtype=float)

        if label_transform == "log1p":
            y_pred_hours = np.expm1(np.asarray(y_pred_log, dtype=float))
        else:
            y_pred_hours = np.asarray(y_pred_log, dtype=float)

        y_pred_hours = np.clip(y_pred_hours, 0.0, None)

        metrics: dict[str, Any] = {
            "n_samples": int(len(y_true_hours)),
            "mae_hours": float(np.mean(np.abs(y_true_hours - y_pred_hours))),
            "rmse_hours": float(np.sqrt(np.mean((y_true_hours - y_pred_hours) ** 2))),
            "median_ae_hours": float(np.median(np.abs(y_true_hours - y_pred_hours))),
            "true_median_hours": float(np.median(y_true_hours)),
            "pred_median_hours": float(np.median(y_pred_hours)),
        }

        # MAPE — guard against zero denominators
        nonzero = y_true_hours > 0.5
        if nonzero.sum() > 0:
            mape = float(np.mean(np.abs(y_true_hours[nonzero] - y_pred_hours[nonzero]) / y_true_hours[nonzero]))
            metrics["mape"] = mape
        else:
            metrics["mape"] = None

        # Quantile coverage (are true values inside predicted intervals?)
        metrics["pct_within_2x_pred"] = float(
            np.mean((y_true_hours <= y_pred_hours * 2) & (y_true_hours >= y_pred_hours * 0.5))
        )

        logger.info(
            json.dumps(
                {
                    "event": "time_model_evaluation_complete",
                    "mae_hours": metrics["mae_hours"],
                    "rmse_hours": metrics["rmse_hours"],
                },
                ensure_ascii=True,
            )
        )
        return metrics

    # ------------------------------------------------------------------
    # Ranking / business helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lift_at_top_n(
        y_true: np.ndarray, y_prob: np.ndarray, pct: float = 0.10
    ) -> float | None:
        """Lift = (precision in top k%) / (overall positive rate)."""
        n = max(1, int(len(y_true) * pct))
        sorted_idx = np.argsort(-y_prob)
        top_k = y_true[sorted_idx[:n]]
        overall_rate = y_true.mean()
        if overall_rate <= 0.0:
            return None
        return float(top_k.mean() / overall_rate)

    @staticmethod
    def _capture_rate(
        y_true: np.ndarray, y_prob: np.ndarray, pct: float = 0.20
    ) -> float:
        """Fraction of all positives captured within the top pct% by score."""
        n = max(1, int(len(y_true) * pct))
        sorted_idx = np.argsort(-y_prob)
        top_k = y_true[sorted_idx[:n]]
        total_pos = y_true.sum()
        if total_pos == 0:
            return 0.0
        return float(top_k.sum() / total_pos)

    @staticmethod
    def _ece(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
        """Expected Calibration Error across uniform bins."""
        bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
        ece = 0.0
        n = len(y_true)
        for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
            mask = (y_prob >= lo) & (y_prob < hi)
            if mask.sum() == 0:
                continue
            avg_pred = y_prob[mask].mean()
            avg_true = y_true[mask].mean()
            ece += (mask.sum() / n) * abs(avg_pred - avg_true)
        return float(ece)

    @staticmethod
    def _business_metrics(
        y_true: np.ndarray,
        y_prob: np.ndarray,
        top_n_pct: float,
        analyst_hours_per_alert: float = 2.0,
    ) -> dict[str, Any]:
        """Estimate business impact metrics.

        Assumptions:
        - Working the entire queue costs N × analyst_hours_per_alert hours.
        - Working only the top-N% queue, we capture some fraction of true positives
          while skipping the bottom 1-N%.
        - Analyst time saved = time not spent on bottom (1-N%) minus time spent
          on false negatives in top (N%).
        """
        n = len(y_true)
        top_k = max(1, int(n * top_n_pct))
        sorted_idx = np.argsort(-y_prob)

        top_positives = y_true[sorted_idx[:top_k]].sum()
        bottom_positives = y_true[sorted_idx[top_k:]].sum()

        total_positives = y_true.sum()
        total_hours_full_queue = n * analyst_hours_per_alert
        total_hours_top_queue = top_k * analyst_hours_per_alert

        hours_saved = total_hours_full_queue - total_hours_top_queue
        missed_positives = int(bottom_positives)
        capture_rate = float(top_positives / max(total_positives, 1))

        return {
            "estimated_analyst_hours_full_queue": round(total_hours_full_queue, 1),
            "estimated_analyst_hours_top_queue": round(total_hours_top_queue, 1),
            "estimated_analyst_hours_saved": round(hours_saved, 1),
            "queue_compression_ratio": round(top_n_pct, 3),
            "positive_capture_rate": round(capture_rate, 4),
            "missed_positives_in_bottom": missed_positives,
        }

    @staticmethod
    def _breakdown(
        y_true: np.ndarray,
        y_prob: np.ndarray,
        group: pd.Series,
    ) -> dict[str, dict[str, Any]]:
        """Compute PR-AUC per group for segment/typology analysis."""
        result: dict[str, dict[str, Any]] = {}
        df = pd.DataFrame({"y_true": y_true, "y_prob": y_prob, "group": group.values})
        for name, sub in df.groupby("group"):
            if sub["y_true"].nunique() < 2 or len(sub) < 5:
                continue
            try:
                ap = float(average_precision_score(sub["y_true"], sub["y_prob"]))
                result[str(name)] = {
                    "pr_auc": ap,
                    "n_samples": len(sub),
                    "positive_rate": float(sub["y_true"].mean()),
                }
            except Exception:
                pass
        return result
