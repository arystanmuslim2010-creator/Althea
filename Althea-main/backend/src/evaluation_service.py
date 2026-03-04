"""Production-grade AML evaluation service.

Replaces self-referential synthetic-label metrics with outcome-label-based
evaluation that mirrors how real bank compliance teams measure AML triage.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config


# ---------------------------------------------------------------------------
# 2a. OutcomeLabelSource
# ---------------------------------------------------------------------------

class OutcomeLabelSource(Enum):
    ANALYST_DISPOSITION = "analyst_disposition"  # Real outcomes from closed cases
    SYNTHETIC           = "synthetic"            # Demo-only synthetic labels
    NONE                = "none"                 # No labels available


# ---------------------------------------------------------------------------
# 2b. detect_outcome_source
# ---------------------------------------------------------------------------

def detect_outcome_source(
    df: pd.DataFrame,
) -> Tuple[OutcomeLabelSource, str, str]:
    """
    Detect what kind of outcome labels are available in df.

    Priority order:
      1. analyst_disposition with >= MIN_CLOSED_CASES_FOR_METRICS resolved rows
      2. synthetic_true_suspicious (demo-only fallback)
      3. NONE

    Returns (source, column_name, warning_message).
    warning_message is an empty string when source == ANALYST_DISPOSITION.
    """
    min_closed   = getattr(config, "MIN_CLOSED_CASES_FOR_METRICS", 50)
    outcome_col  = getattr(config, "OUTCOME_COLUMN", "analyst_disposition")
    synthetic_col = getattr(config, "SYNTHETIC_LABEL_COLUMN", "synthetic_true_suspicious")

    resolved_values = {
        getattr(config, "OUTCOME_LABEL_SAR_FILED",      "SAR_FILED"),
        getattr(config, "OUTCOME_LABEL_TRUE_POSITIVE",  "TP"),
        getattr(config, "OUTCOME_LABEL_FALSE_POSITIVE", "FP"),
    }

    if outcome_col in df.columns:
        n_closed = df[outcome_col].isin(resolved_values).sum()
        if n_closed >= min_closed:
            return OutcomeLabelSource.ANALYST_DISPOSITION, outcome_col, ""

    if synthetic_col in df.columns:
        warning = getattr(
            config,
            "SYNTHETIC_LABEL_WARNING",
            "WARNING: synthetic labels are not valid for production evaluation.",
        )
        return OutcomeLabelSource.SYNTHETIC, synthetic_col, warning

    return OutcomeLabelSource.NONE, "", ""


# ---------------------------------------------------------------------------
# 2c. build_binary_labels
# ---------------------------------------------------------------------------

def build_binary_labels(
    df: pd.DataFrame,
    source: OutcomeLabelSource,
    col: str,
) -> Tuple[pd.Series, pd.Series]:
    """
    Convert raw disposition values to binary labels aligned to df.index.

    For ANALYST_DISPOSITION:
      - 1 if SAR_FILED or TP
      - 0 if FP
      - PENDING and INCONCLUSIVE rows are EXCLUDED (mask=False)

    For SYNTHETIC:
      - 1 if value == config.RISK_LABEL_YES, 0 otherwise
      - All rows included

    Returns (labels_series, inclusion_mask).
    """
    sar_filed = getattr(config, "OUTCOME_LABEL_SAR_FILED",      "SAR_FILED")
    tp_val    = getattr(config, "OUTCOME_LABEL_TRUE_POSITIVE",  "TP")
    fp_val    = getattr(config, "OUTCOME_LABEL_FALSE_POSITIVE", "FP")
    risk_yes  = getattr(config, "RISK_LABEL_YES", "Yes")

    if source == OutcomeLabelSource.ANALYST_DISPOSITION:
        resolved_mask = df[col].isin({sar_filed, tp_val, fp_val})
        label_map = {sar_filed: 1, tp_val: 1, fp_val: 0}
        labels = df[col].map(label_map).fillna(-1).astype(int)
        final_mask = resolved_mask & (labels >= 0)
        labels = labels.where(final_mask, other=0)
        return labels, final_mask

    elif source == OutcomeLabelSource.SYNTHETIC:
        labels = (df[col] == risk_yes).astype(int)
        mask = pd.Series(True, index=df.index)
        return labels, mask

    else:
        labels = pd.Series(0, index=df.index)
        mask   = pd.Series(False, index=df.index)
        return labels, mask


# ---------------------------------------------------------------------------
# 2d. RankingMetrics
# ---------------------------------------------------------------------------

@dataclass
class RankingMetrics:
    """
    Full production AML ranking metrics.

    is_synthetic=True and is_production_valid=False when synthetic labels were used.
    These metrics must not be submitted to regulators in that case.
    """
    # Core ranking metrics
    precision_at_k:       float
    recall_at_k:          float
    average_precision:    float
    auroc:                float

    # AML-specific operational metrics
    sar_capture_rate:             float
    false_positive_rate_before:   float
    false_positive_rate_after:    float
    fpr_reduction:                float

    # Workload metrics
    alert_reduction_rate: float
    analyst_hours_saved:  float
    cost_per_tp:          float

    # Lift
    lift_at_k:            float

    # Counts
    k:            int
    total_alerts: int
    total_tp:     int
    total_fp:     int
    tp_in_topk:   int
    fp_in_topk:   int

    # Metadata
    label_source:  str
    is_synthetic:  bool
    warning:       str
    evaluated_at:  str


# ---------------------------------------------------------------------------
# 2e. TemporalHoldoutEvaluator
# ---------------------------------------------------------------------------

class TemporalHoldoutEvaluator:
    """
    Evaluates model performance using temporal holdout.

    Required by SR 11-7 / OCC Model Risk Guidance: model validation must
    use out-of-time data. Random splits allow future transactions to inform
    past predictions, creating look-ahead leakage.

    Protocol:
      - Sort data by timestamp ascending
      - Train window: first train_pct% of time-ordered data
      - Gap:          gap_pct% buffer to simulate deployment lag
      - Eval window:  next eval_pct% of time-ordered data
    """

    def __init__(
        self,
        train_pct: float = 0.70,
        eval_pct:  float = 0.20,
        gap_pct:   float = 0.05,
        time_col:  str   = "timestamp",
    ):
        self.train_pct = train_pct
        self.eval_pct  = eval_pct
        self.gap_pct   = gap_pct
        self.time_col  = time_col

    def split(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Return (train_df, eval_df) split by time.
        Falls back to positional split with a warning if timestamp is missing.
        """
        if self.time_col in df.columns:
            df_sorted = df.copy()
            df_sorted[self.time_col] = pd.to_datetime(
                df_sorted[self.time_col], errors="coerce"
            )
            df_sorted = df_sorted.sort_values(self.time_col).reset_index(drop=True)
        else:
            df_sorted = df.copy().reset_index(drop=True)

        n = len(df_sorted)
        train_end   = int(n * self.train_pct)
        eval_start  = int(n * (self.train_pct + self.gap_pct))
        eval_end    = int(n * (self.train_pct + self.gap_pct + self.eval_pct))
        eval_end    = max(eval_end, eval_start + 1)

        train_df = df_sorted.iloc[:train_end].copy()
        eval_df  = df_sorted.iloc[eval_start:eval_end].copy()
        return train_df, eval_df

    def compute_temporal_metrics(
        self,
        df: pd.DataFrame,
        score_col: str = "risk_score",
        capacity:  int = 50,
    ) -> Dict[str, Any]:
        """
        Returns train/eval window info + RankingMetrics on eval window.
        Also computes random-order baseline for lift comparison.
        """
        train_df, eval_df = self.split(df)

        svc = EvaluationService()
        metrics = svc.compute_ranking_metrics(eval_df, capacity)

        baseline_metrics = None
        if metrics is not None:
            rng = np.random.default_rng(42)
            df_random = eval_df.copy()
            if score_col in df_random.columns:
                df_random[score_col] = rng.permutation(df_random[score_col].values)
            baseline_metrics = svc.compute_ranking_metrics(df_random, capacity)

        def _window_info(d: pd.DataFrame) -> Dict[str, Any]:
            info: Dict[str, Any] = {"n_rows": len(d)}
            if self.time_col in d.columns:
                ts = pd.to_datetime(d[self.time_col], errors="coerce").dropna()
                if len(ts):
                    info["start"] = str(ts.min().date())
                    info["end"]   = str(ts.max().date())
            return info

        lift_summary: Dict[str, float] = {}
        if metrics and baseline_metrics and baseline_metrics.precision_at_k > 0:
            lift_summary["precision_lift"] = round(
                metrics.precision_at_k / baseline_metrics.precision_at_k, 2
            )

        return {
            "train_window":    _window_info(train_df),
            "eval_window":     _window_info(eval_df),
            "metrics":         metrics,
            "baseline_metrics": baseline_metrics,
            "lift_summary":    lift_summary,
        }


# ---------------------------------------------------------------------------
# 2f. PrecisionRecallCurveData
# ---------------------------------------------------------------------------

class PrecisionRecallCurveData:
    """
    Precision-recall tradeoff at multiple K values.

    Compliance teams use this curve to decide their daily review budget:
    at what K does precision drop below their acceptable floor?
    """

    def compute(
        self,
        df: pd.DataFrame,
        score_col: str = "risk_score",
        max_k: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Returns DataFrame(k, precision, recall, f1, threshold).
        Returns None if no labels are available.
        """
        source, col, _ = detect_outcome_source(df)
        if source == OutcomeLabelSource.NONE:
            return None

        labels, mask = build_binary_labels(df, source, col)
        df_eval = df[mask].copy()
        y_true  = labels[mask].values.astype(int)

        if score_col not in df_eval.columns or len(y_true) == 0:
            return None

        y_score = pd.to_numeric(df_eval[score_col], errors="coerce").fillna(0.0).values
        n = min(len(y_true), max_k) if max_k else len(y_true)
        k_values = [k for k in [1, 5, 10, 25, 50, 100, 200, 500, n] if k <= n]
        if not k_values:
            k_values = [n]

        from .evaluation_metrics import precision_recall_table
        return precision_recall_table(y_true, y_score, k_values=k_values)


# ---------------------------------------------------------------------------
# 2g. SARCaptureAnalysis
# ---------------------------------------------------------------------------

class SARCaptureAnalysis:
    """
    SAR capture rate analysis for regulatory compliance demonstration.

    Banks must demonstrate to OCC/FinCEN that triage systems do not
    systematically miss SAR-worthy alerts.
    Each missed SAR represents a regulatory violation, not just an ops failure.
    """

    def compute(
        self,
        df: pd.DataFrame,
        capacity: int,
        score_col: str = "risk_score",
    ) -> Optional[Dict[str, Any]]:
        """
        Returns SAR capture statistics, or None if no SAR labels are available.
        """
        sar_filed   = getattr(config, "OUTCOME_LABEL_SAR_FILED", "SAR_FILED")
        outcome_col = getattr(config, "OUTCOME_COLUMN", "analyst_disposition")

        if outcome_col not in df.columns or score_col not in df.columns:
            return None

        sar_df = df[df[outcome_col] == sar_filed].copy()
        if len(sar_df) == 0:
            return None

        df_sorted = df.sort_values(score_col, ascending=False).reset_index(drop=True)
        topk      = df_sorted.head(min(capacity, len(df_sorted)))
        topk_idx  = set(topk.index)

        sar_in_topk   = sar_df[sar_df.index.isin(topk_idx)]
        missed_sars   = sar_df[~sar_df.index.isin(topk_idx)]
        sar_capture_rate = len(sar_in_topk) / len(sar_df) if len(sar_df) > 0 else 0.0

        missed_scores = (
            pd.to_numeric(missed_sars[score_col], errors="coerce").dropna()
        )

        missed_dist: Dict[str, int] = {}
        if len(missed_scores) > 0:
            for band, (lo, hi) in [
                ("0-25", (0, 25)), ("25-50", (25, 50)),
                ("50-75", (50, 75)), ("75-100", (75, 100)),
            ]:
                missed_dist[band] = int(
                    ((missed_scores >= lo) & (missed_scores < hi)).sum()
                )

        capture_rows = []
        for k in [10, 25, 50, 100, 200, min(500, len(df))]:
            if k > len(df):
                break
            topk_k       = df_sorted.head(k)
            in_topk_k    = sar_df[sar_df.index.isin(topk_k.index)]
            capture_rows.append({
                "k": k,
                "sar_capture_rate": (
                    len(in_topk_k) / len(sar_df) if len(sar_df) > 0 else 0.0
                ),
            })

        return {
            "sar_capture_rate":              sar_capture_rate,
            "missed_sar_count":              len(missed_sars),
            "missed_sar_avg_score":          float(missed_scores.mean()) if len(missed_scores) > 0 else None,
            "missed_sar_score_distribution": missed_dist,
            "capture_curve":                 pd.DataFrame(capture_rows),
        }


# ---------------------------------------------------------------------------
# 2h. ModelDegradationDetector
# ---------------------------------------------------------------------------

class ModelDegradationDetector:
    """
    Silent model degradation detector.

    The worst AML failure mode: the model produces scores, no errors surface,
    analysts keep working — but accuracy silently declines and genuine cases
    are missed. This class catches that before it becomes a regulatory problem.
    """

    def compute_rolling_precision(
        self,
        df: pd.DataFrame,
        window_days: int = 30,
        capacity_per_day: int = 50,
    ) -> pd.DataFrame:
        """
        Rolling precision@K over time.
        Sets degradation_flag=True when rolling precision < 50% of baseline.
        """
        source, col, _ = detect_outcome_source(df)
        if source == OutcomeLabelSource.NONE or "timestamp" not in df.columns:
            return pd.DataFrame()

        df_work = df.copy()
        df_work["_ts"] = pd.to_datetime(df_work["timestamp"], errors="coerce")
        df_work = df_work[df_work["_ts"].notna()].sort_values("_ts")

        labels, mask = build_binary_labels(df_work, source, col)
        df_work["_label"] = labels
        df_work["_mask"]  = mask

        rows: List[Dict[str, Any]] = []
        baseline_precision: Optional[float] = None

        for date in sorted(df_work["_ts"].dt.date.unique()):
            window_start = pd.Timestamp(date) - pd.Timedelta(days=window_days)
            window_df = df_work[
                (df_work["_ts"].dt.date <= date) & (df_work["_ts"] >= window_start)
            ]
            resolved = window_df[window_df["_mask"]]
            if len(resolved) < 5 or "risk_score" not in resolved.columns:
                continue

            topk = resolved.sort_values("risk_score", ascending=False).head(capacity_per_day)
            if len(topk) == 0:
                continue

            precision = float(topk["_label"].sum()) / len(topk)
            if baseline_precision is None:
                baseline_precision = precision

            degradation_flag = (
                baseline_precision > 0 and precision < 0.5 * baseline_precision
            )
            rows.append({
                "date":                  date,
                "rolling_precision_at_k": precision,
                "rolling_sar_rate":       float(resolved["_label"].mean()),
                "degradation_flag":       degradation_flag,
            })

        return pd.DataFrame(rows)

    def detect_concept_drift(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Chi-squared test on risk_score bins vs outcome labels across two time windows.

        Detects when the feature-outcome relationship changes (new typology,
        regulatory regime shift, data pipeline change).
        Returns drift_detected=True and p_value when p < 0.05.
        """
        source, col, _ = detect_outcome_source(df)
        if source == OutcomeLabelSource.NONE or "timestamp" not in df.columns:
            return {
                "drift_detected": False,
                "chi2_statistic": None,
                "p_value": None,
                "interpretation": "No labels or timestamp available for drift detection.",
            }

        df_work = df.copy()
        df_work["_ts"] = pd.to_datetime(df_work["timestamp"], errors="coerce")
        df_work = df_work.sort_values("_ts")
        labels, mask = build_binary_labels(df_work, source, col)
        df_eval = df_work[mask].copy()
        df_eval["_label"] = labels[mask]

        n = len(df_eval)
        if n < 20:
            return {
                "drift_detected": False,
                "chi2_statistic": None,
                "p_value": None,
                "interpretation": "Not enough resolved cases for drift detection.",
            }
        if "risk_score" not in df_eval.columns:
            return {
                "drift_detected": False,
                "chi2_statistic": None,
                "p_value": None,
                "interpretation": "risk_score column missing.",
            }

        mid         = n // 2
        first_half  = df_eval.iloc[:mid]
        second_half = df_eval.iloc[mid:]
        bins        = [0, 25, 50, 75, 100]

        try:
            from scipy.stats import chi2_contingency

            def _hist(part: pd.DataFrame, lbl: int) -> np.ndarray:
                h, _ = np.histogram(
                    part[part["_label"] == lbl]["risk_score"].fillna(0), bins=bins
                )
                return h

            contingency = np.array([
                _hist(first_half, 1) + _hist(second_half, 1),
                _hist(first_half, 0) + _hist(second_half, 0),
            ]) + 1  # Laplace smoothing

            chi2_stat, p_value, _, _ = chi2_contingency(contingency)
            drift_detected = bool(p_value < 0.05)
            if drift_detected:
                interp = (
                    f"Concept drift detected (chi2={chi2_stat:.2f}, p={p_value:.4f}). "
                    "The relationship between risk scores and outcomes has changed. "
                    "Consider model retraining."
                )
            else:
                interp = (
                    f"No significant concept drift (chi2={chi2_stat:.2f}, p={p_value:.4f}). "
                    "Risk score distribution vs outcomes is stable."
                )
            return {
                "drift_detected": drift_detected,
                "chi2_statistic": float(chi2_stat),
                "p_value":        float(p_value),
                "interpretation": interp,
            }

        except ImportError:
            rs1 = first_half["risk_score"].fillna(0).values
            rs2 = second_half["risk_score"].fillna(0).values
            mean_shift    = abs(float(rs1.mean()) - float(rs2.mean()))
            drift_detected = mean_shift > 10
            return {
                "drift_detected": drift_detected,
                "chi2_statistic": None,
                "p_value":        None,
                "interpretation": (
                    f"Mean score shift: {mean_shift:.1f} (threshold=10). "
                    "scipy not available for chi-squared test."
                ),
            }


# ---------------------------------------------------------------------------
# 2i. EvaluationService (rewrite)
# ---------------------------------------------------------------------------

class EvaluationService:
    """
    Production-grade AML evaluation service.

    Usage pattern:
      1. After running the scoring pipeline, call evaluate(df, capacity).
      2. The service auto-detects label source (analyst disposition vs synthetic).
      3. Returns full RankingMetrics with appropriate warnings if synthetic labels used.
      4. All metrics use temporal holdout when data is sufficient.
    """

    def evaluate(
        self,
        df: pd.DataFrame,
        capacity: int,
        minutes_per_case: int = 45,
        use_temporal_holdout: bool = True,
    ) -> Dict[str, Any]:
        """
        Main evaluation entry point.

        Returns:
          metrics:           RankingMetrics | None
          pr_curve:          pd.DataFrame | None
          sar_analysis:      Dict | None
          temporal_split:    Dict | None
          label_source:      OutcomeLabelSource
          warnings:          List[str]
          is_production_valid: bool  (False when using synthetic labels)
        """
        source, col, warning = detect_outcome_source(df)
        warnings_list: List[str] = []

        if source == OutcomeLabelSource.NONE:
            warnings_list.append("no label column found; cannot compute ranking metrics")
            return {
                "metrics":             None,
                "pr_curve":            None,
                "sar_analysis":        None,
                "temporal_split":      None,
                "label_source":        OutcomeLabelSource.NONE,
                "warnings":            warnings_list,
                "is_production_valid": False,
            }

        if warning:
            warnings_list.append(warning)

        eval_df       = df
        temporal_info = None
        if use_temporal_holdout and len(df) >= 20:
            evaluator = TemporalHoldoutEvaluator()
            try:
                _, eval_df = evaluator.split(df)
                if len(eval_df) < 5:
                    eval_df = df
                else:
                    temporal_info = evaluator.compute_temporal_metrics(df, capacity=capacity)
            except Exception:
                eval_df = df

        metrics      = self.compute_ranking_metrics(eval_df, capacity, minutes_per_case=minutes_per_case)
        pr_curve     = PrecisionRecallCurveData().compute(eval_df)
        sar_analysis = SARCaptureAnalysis().compute(eval_df, capacity)

        return {
            "metrics":             metrics,
            "pr_curve":            pr_curve,
            "sar_analysis":        sar_analysis,
            "temporal_split":      temporal_info,
            "label_source":        source,
            "warnings":            warnings_list,
            "is_production_valid": (source == OutcomeLabelSource.ANALYST_DISPOSITION),
        }

    def compute_ranking_metrics(
        self,
        df: pd.DataFrame,
        capacity: int,
        minutes_per_case: int = 45,
    ) -> Optional[RankingMetrics]:
        """
        Backward-compatible wrapper. Returns RankingMetrics dataclass or None.
        """
        if len(df) == 0 or "risk_score" not in df.columns:
            return None

        source, col, warning = detect_outcome_source(df)
        if source == OutcomeLabelSource.NONE:
            return None

        work_df = (
            df[df["alert_eligible"] == True].copy()
            if "alert_eligible" in df.columns
            else df.copy()
        )
        if len(work_df) == 0:
            return None

        labels, mask = build_binary_labels(work_df, source, col)
        resolved_df  = work_df[mask].copy()
        y_true_all   = labels[mask].values.astype(int)

        if len(y_true_all) == 0:
            return None

        y_score_all = pd.to_numeric(
            resolved_df["risk_score"], errors="coerce"
        ).fillna(0.0).values
        actual_k = min(capacity, len(resolved_df))

        from .evaluation_metrics import (
            precision_at_k  as _p_at_k,
            recall_at_k     as _r_at_k,
            average_precision as _ap,
            auroc           as _auroc,
            lift_at_k       as _lift,
        )

        p_at_k = _p_at_k(y_true_all, y_score_all, actual_k)
        r_at_k = _r_at_k(y_true_all, y_score_all, actual_k)
        ap     = _ap(y_true_all, y_score_all)
        auc    = _auroc(y_true_all, y_score_all)
        lift   = _lift(y_true_all, y_score_all, actual_k)

        order       = np.argsort(y_score_all)[::-1]
        topk_labels = y_true_all[order[:actual_k]]
        tp_in_topk  = int(topk_labels.sum())
        fp_in_topk  = actual_k - tp_in_topk

        total_tp = int(y_true_all.sum())
        total_fp = len(y_true_all) - total_tp

        fpr_before   = total_fp / len(y_true_all) if len(y_true_all) > 0 else 0.0
        fpr_after    = fp_in_topk / actual_k if actual_k > 0 else 0.0
        fpr_reduction = (
            (fpr_before - fpr_after) / fpr_before if fpr_before > 0 else 0.0
        )

        alert_reduction = (
            1.0 - (actual_k / len(resolved_df)) if len(resolved_df) > 0 else 0.0
        )
        hours_all       = len(resolved_df) * minutes_per_case / 60.0
        hours_topk      = actual_k * minutes_per_case / 60.0
        analyst_hours_saved = hours_all - hours_topk
        cost_per_tp = hours_topk / tp_in_topk if tp_in_topk > 0 else float("inf")

        return RankingMetrics(
            precision_at_k=p_at_k,
            recall_at_k=r_at_k,
            average_precision=ap,
            auroc=auc,
            sar_capture_rate=r_at_k,
            false_positive_rate_before=fpr_before,
            false_positive_rate_after=fpr_after,
            fpr_reduction=fpr_reduction,
            alert_reduction_rate=alert_reduction,
            analyst_hours_saved=analyst_hours_saved,
            cost_per_tp=cost_per_tp,
            lift_at_k=lift,
            k=actual_k,
            total_alerts=len(work_df),
            total_tp=total_tp,
            total_fp=total_fp,
            tp_in_topk=tp_in_topk,
            fp_in_topk=fp_in_topk,
            label_source=source.value,
            is_synthetic=(source == OutcomeLabelSource.SYNTHETIC),
            warning=warning,
            evaluated_at=pd.Timestamp.utcnow().isoformat(),
        )

    def simulate_analyst_workload(
        self,
        df: pd.DataFrame,
        capacity: int,
        minutes_per_case: int,
    ) -> Dict[str, Any]:
        """
        Compute time savings from prioritisation.
        Uses real analyst disposition labels when available.
        """
        if len(df) == 0:
            return {
                "hours_required_topk": 0.0,
                "hours_required_all":  0.0,
                "hours_saved":         0.0,
                "tp_captured":         None,
            }

        work_df = (
            df[df["alert_eligible"] == True].copy()
            if "alert_eligible" in df.columns
            else df.copy()
        )
        ranked_df = work_df.sort_values("risk_score", ascending=False)
        topk      = ranked_df.head(min(capacity, len(ranked_df)))
        actual_k  = len(topk)

        hours_required_topk = actual_k * minutes_per_case / 60.0
        hours_required_all  = len(work_df) * minutes_per_case / 60.0
        hours_saved         = hours_required_all - hours_required_topk

        source, col, _ = detect_outcome_source(topk)
        tp_captured    = None
        if source != OutcomeLabelSource.NONE:
            labels, mask = build_binary_labels(topk, source, col)
            tp_captured  = int(labels[mask].sum())

        return {
            "hours_required_topk": hours_required_topk,
            "hours_required_all":  hours_required_all,
            "hours_saved":         hours_saved,
            "tp_captured":         tp_captured,
        }

    def compute_alert_reduction(self, df: pd.DataFrame, capacity: int) -> Dict[str, float]:
        """Alert reduction metric — independent of label availability."""
        work_df = (
            df[df["alert_eligible"] == True].copy()
            if "alert_eligible" in df.columns
            else df.copy()
        )
        baseline_alerts = len(work_df)
        topk_alerts     = min(capacity, baseline_alerts)
        alert_reduction_percent = (
            1.0 - (topk_alerts / baseline_alerts) if baseline_alerts > 0 else 0.0
        )
        return {
            "baseline_alerts":          int(baseline_alerts),
            "topk_alerts":              int(topk_alerts),
            "alert_reduction_percent":  alert_reduction_percent,
        }

    def generate_evaluation_report(self, df: pd.DataFrame, capacity: int) -> str:
        """
        Human-readable model validation report (bank compliance officer format).
        Mirrors the structure expected in an SR 11-7 model validation document.
        """
        result  = self.evaluate(df, capacity)
        metrics = result["metrics"]

        lines = [
            "=" * 60,
            "ALTHEA AML MODEL VALIDATION REPORT",
            "=" * 60,
            "",
            f"Label Source:      {result['label_source'].value if hasattr(result['label_source'], 'value') else result['label_source']}",
            f"Production Valid:  {'YES' if result['is_production_valid'] else 'NO'}",
        ]

        if result["warnings"]:
            lines += ["", "WARNINGS:"]
            for w in result["warnings"]:
                lines.append(f"  ! {w}")

        if metrics is None:
            lines += ["", "No metrics available — no outcome labels found.", ""]
            return "\n".join(lines)

        n_resolved = metrics.total_tp + metrics.total_fp
        base_rate_str = (
            f"{metrics.total_tp / n_resolved:.1%}" if n_resolved > 0 else "N/A"
        )

        lines += [
            "",
            "MODEL PERFORMANCE SUMMARY",
            "-" * 40,
            f"  Analyst Capacity (K):      {metrics.k}",
            f"  Total Resolved Alerts:     {n_resolved}",
            f"  True Positives (total):    {metrics.total_tp}",
            f"  Base Rate:                 {base_rate_str}",
            "",
            f"  Precision@K:               {metrics.precision_at_k:.1%}",
            f"  Recall@K (SAR Capture):    {metrics.recall_at_k:.1%}",
            f"  Lift@K (vs. random):       {metrics.lift_at_k:.2f}x",
            f"  Average Precision (AP):    {metrics.average_precision:.3f}",
            f"  AUROC:                     {metrics.auroc:.3f}",
            "",
            "REGULATORY COMPLIANCE",
            "-" * 40,
            f"  SAR Capture Rate@K:        {metrics.sar_capture_rate:.1%}",
            f"  FP Rate (before ranking):  {metrics.false_positive_rate_before:.1%}",
            f"  FP Rate (in top-K):        {metrics.false_positive_rate_after:.1%}",
            f"  FP Reduction:              {metrics.fpr_reduction:.1%}",
            "",
            "OPERATIONAL EFFICIENCY",
            "-" * 40,
            f"  Alert Reduction Rate:      {metrics.alert_reduction_rate:.1%}",
            f"  Analyst Hours Saved/Day:   {metrics.analyst_hours_saved:.1f}h",
            f"  Cost per TP (hours):       "
            + (f"{metrics.cost_per_tp:.2f}h" if metrics.cost_per_tp != float("inf") else "N/A (no TPs)"),
            "",
            f"  Evaluated At: {metrics.evaluated_at}",
            "",
            "=" * 60,
        ]

        if metrics.is_synthetic:
            lines += [
                "NOTE: Metrics computed using SYNTHETIC labels.",
                "These metrics are NOT valid for regulatory submission.",
                "Connect analyst_disposition column for production evaluation.",
                "=" * 60,
            ]

        return "\n".join(lines)
