"""Phase G: Model Health & Ops Monitoring (bank-grade lite).

Provides operational monitoring for:
- Risk distribution (detect saturation/collapse)
- Alert rate (volume anomalies vs baseline)
- Drift proxy (PSI on risk_score distribution)
- Rule hit distribution (hit rate per rule, spikes)
- Health alerts with severity (GREEN/AMBER/RED)
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config


# =============================================================================
# Monitor Status Constants
# =============================================================================
STATUS_GREEN = "GREEN"
STATUS_AMBER = "AMBER"
STATUS_RED = "RED"


# =============================================================================
# Helper Functions
# =============================================================================

def _get_threshold(name: str, default: float) -> float:
    """Get threshold from config with fallback to default."""
    return float(getattr(config, name, default))


def _compute_z_score(value: float, baseline_mean: float, baseline_std: float) -> float:
    """Compute z-score safely."""
    if baseline_std < 1e-9:
        return 0.0
    return (value - baseline_mean) / baseline_std


def _compute_pct_change(current: float, baseline: float) -> float:
    """Compute percentage change safely."""
    if baseline < 1e-9:
        return 0.0
    return (current - baseline) / baseline


def _compute_psi(
    expected: np.ndarray,
    actual: np.ndarray,
    n_bins: int = 10,
) -> float:
    """
    Compute Population Stability Index (PSI) between two distributions.
    
    PSI < 0.1: No significant change
    PSI 0.1-0.25: Moderate change (AMBER)
    PSI > 0.25: Significant change (RED)
    
    Args:
        expected: Baseline distribution (array of values)
        actual: Current distribution (array of values)
        n_bins: Number of bins for histogram
        
    Returns:
        PSI value
    """
    if len(expected) == 0 or len(actual) == 0:
        return 0.0
    
    # Create bins based on expected distribution
    min_val = min(expected.min(), actual.min())
    max_val = max(expected.max(), actual.max())
    
    if max_val - min_val < 1e-9:
        return 0.0
    
    bins = np.linspace(min_val, max_val, n_bins + 1)
    
    # Compute histograms
    expected_counts, _ = np.histogram(expected, bins=bins)
    actual_counts, _ = np.histogram(actual, bins=bins)
    
    # Normalize to proportions
    expected_pct = expected_counts / max(len(expected), 1)
    actual_pct = actual_counts / max(len(actual), 1)
    
    # Avoid division by zero and log(0)
    eps = 1e-6
    expected_pct = np.clip(expected_pct, eps, 1.0)
    actual_pct = np.clip(actual_pct, eps, 1.0)
    
    # PSI = sum((actual - expected) * ln(actual / expected))
    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    
    return float(psi)


# =============================================================================
# G2: Risk Distribution Monitor
# =============================================================================

def monitor_risk_distribution(
    alerts_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Monitor risk score distribution for saturation and collapse.
    
    Metrics:
    - saturation_rate: Share of alerts with risk_score >= 95
    - collapse_rate: Share with risk_score in narrow band (e.g., 80-85)
    - top1_share: Max bin share in histogram (proxy for clustering)
    
    Args:
        alerts_df: DataFrame with risk_score column
        
    Returns:
        Signal dict with name, status, value, threshold, details
    """
    if len(alerts_df) == 0 or "risk_score" not in alerts_df.columns:
        return {
            "name": "risk_distribution",
            "status": STATUS_GREEN,
            "value": None,
            "threshold": None,
            "details": {"error": "No risk_score data available"},
        }
    
    risk_scores = pd.to_numeric(alerts_df["risk_score"], errors="coerce").dropna()
    
    if len(risk_scores) == 0:
        return {
            "name": "risk_distribution",
            "status": STATUS_GREEN,
            "value": None,
            "threshold": None,
            "details": {"error": "No valid risk_score values"},
        }
    
    n = len(risk_scores)
    
    # Compute metrics
    saturation_rate = float((risk_scores >= 95).sum() / n)
    collapse_rate = float(((risk_scores >= 80) & (risk_scores <= 85)).sum() / n)
    
    # Histogram for top bin share (10 bins)
    hist, _ = np.histogram(risk_scores, bins=10, range=(0, 100))
    top1_share = float(hist.max() / n) if n > 0 else 0.0
    
    # Get thresholds
    max_saturation = _get_threshold("HEALTH_MAX_SATURATION_RATE", 0.30)
    max_topbin = _get_threshold("HEALTH_MAX_TOPBIN_SHARE", 0.50)
    saturation_amber = _get_threshold("HEALTH_SATURATION_AMBER", 0.20)
    topbin_amber = _get_threshold("HEALTH_TOPBIN_AMBER", 0.40)
    
    # Determine status
    status = STATUS_GREEN
    triggered_metric = None
    
    if saturation_rate >= max_saturation:
        status = STATUS_RED
        triggered_metric = f"saturation_rate={saturation_rate:.2%} >= {max_saturation:.0%}"
    elif top1_share >= max_topbin:
        status = STATUS_RED
        triggered_metric = f"top1_share={top1_share:.2%} >= {max_topbin:.0%}"
    elif saturation_rate >= saturation_amber:
        status = STATUS_AMBER
        triggered_metric = f"saturation_rate={saturation_rate:.2%} >= {saturation_amber:.0%}"
    elif top1_share >= topbin_amber:
        status = STATUS_AMBER
        triggered_metric = f"top1_share={top1_share:.2%} >= {topbin_amber:.0%}"
    
    return {
        "name": "risk_distribution",
        "status": status,
        "value": {
            "saturation_rate": saturation_rate,
            "collapse_rate": collapse_rate,
            "top1_share": top1_share,
        },
        "threshold": {
            "max_saturation": max_saturation,
            "saturation_amber": saturation_amber,
            "max_topbin": max_topbin,
            "topbin_amber": topbin_amber,
        },
        "details": {
            "n_alerts": n,
            "triggered": triggered_metric,
            "recommendation": _get_risk_recommendation(status) if status != STATUS_GREEN else None,
        },
    }


def _get_risk_recommendation(status: str) -> str:
    """Get recommendation for risk distribution issues."""
    if status == STATUS_RED:
        return "Check calibration / risk mapping. Model may need retraining."
    elif status == STATUS_AMBER:
        return "Monitor risk distribution closely. Review recent data changes."
    return ""


# =============================================================================
# G3: Alert Rate Monitor
# =============================================================================

def monitor_alert_rate(
    current_stats: Dict[str, int],
    baseline_stats: List[Dict[str, int]],
) -> Dict[str, Any]:
    """
    Monitor alert rate for volume anomalies vs baseline.
    
    Compares current run stats to trailing baseline (last 7 runs).
    Uses z-score and percent-change thresholds.
    
    Args:
        current_stats: Dict with total_alerts, in_queue, mandatory_review, suppressed
        baseline_stats: List of dicts from previous runs
        
    Returns:
        Signal dict with name, status, value, threshold, details
    """
    if not baseline_stats or len(baseline_stats) < 2:
        return {
            "name": "alert_rate",
            "status": STATUS_GREEN,
            "value": current_stats.get("total_alerts", 0),
            "threshold": None,
            "details": {"info": "Insufficient baseline data (need at least 2 prior runs)"},
        }
    
    current_total = current_stats.get("total_alerts", 0)
    baseline_totals = [s.get("total_alerts", 0) for s in baseline_stats]
    
    baseline_mean = float(np.mean(baseline_totals))
    baseline_std = float(np.std(baseline_totals))
    
    z_score = _compute_z_score(current_total, baseline_mean, baseline_std)
    pct_change = _compute_pct_change(current_total, baseline_mean)
    
    # Get thresholds
    max_z = _get_threshold("HEALTH_ALERT_RATE_MAX_Z", 3.0)
    max_pct = _get_threshold("HEALTH_ALERT_RATE_MAX_PCT_CHANGE", 0.50)
    amber_z = _get_threshold("HEALTH_ALERT_RATE_AMBER_Z", 2.0)
    amber_pct = _get_threshold("HEALTH_ALERT_RATE_AMBER_PCT", 0.30)
    
    # Determine status
    status = STATUS_GREEN
    triggered = None
    
    abs_z = abs(z_score)
    abs_pct = abs(pct_change)
    
    if abs_z >= max_z or abs_pct >= max_pct:
        status = STATUS_RED
        triggered = f"z={z_score:.2f}, pct_change={pct_change:.1%}"
    elif abs_z >= amber_z or abs_pct >= amber_pct:
        status = STATUS_AMBER
        triggered = f"z={z_score:.2f}, pct_change={pct_change:.1%}"
    
    return {
        "name": "alert_rate",
        "status": status,
        "value": {
            "current_total": current_total,
            "baseline_mean": baseline_mean,
            "z_score": z_score,
            "pct_change": pct_change,
        },
        "threshold": {
            "max_z": max_z,
            "amber_z": amber_z,
            "max_pct": max_pct,
            "amber_pct": amber_pct,
        },
        "details": {
            "baseline_window": len(baseline_stats),
            "triggered": triggered,
            "recommendation": "Investigate data shift or upstream changes" if status != STATUS_GREEN else None,
        },
    }


# =============================================================================
# G4: Drift Proxy Monitor (PSI)
# =============================================================================

def monitor_drift_psi(
    current_scores: np.ndarray,
    baseline_scores: np.ndarray,
) -> Dict[str, Any]:
    """
    Monitor drift using PSI on risk_score distribution.
    
    Args:
        current_scores: Current run risk scores
        baseline_scores: Baseline window risk scores
        
    Returns:
        Signal dict with name, status, value, threshold, details
    """
    if len(current_scores) == 0 or len(baseline_scores) == 0:
        return {
            "name": "drift_psi",
            "status": STATUS_GREEN,
            "value": None,
            "threshold": None,
            "details": {"info": "Insufficient data for PSI calculation"},
        }
    
    psi = _compute_psi(baseline_scores, current_scores)
    
    # Get thresholds
    psi_red = _get_threshold("HEALTH_PSI_RED", 0.25)
    psi_amber = _get_threshold("HEALTH_PSI_AMBER", 0.10)
    
    # Determine status
    if psi >= psi_red:
        status = STATUS_RED
        triggered = f"PSI={psi:.3f} >= {psi_red:.2f}"
    elif psi >= psi_amber:
        status = STATUS_AMBER
        triggered = f"PSI={psi:.3f} >= {psi_amber:.2f}"
    else:
        status = STATUS_GREEN
        triggered = None
    
    return {
        "name": "drift_psi",
        "status": status,
        "value": psi,
        "threshold": {
            "psi_red": psi_red,
            "psi_amber": psi_amber,
        },
        "details": {
            "n_current": len(current_scores),
            "n_baseline": len(baseline_scores),
            "triggered": triggered,
            "recommendation": "Investigate data shift. Consider model recalibration." if status != STATUS_GREEN else None,
        },
    }


def monitor_drift_from_df(
    alerts_df: pd.DataFrame,
    baseline_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute drift PSI from DataFrames.
    
    If baseline_df is None, uses first 50% of alerts_df as baseline.
    
    Args:
        alerts_df: Current alerts DataFrame
        baseline_df: Optional baseline DataFrame
        
    Returns:
        Drift signal dict
    """
    if len(alerts_df) == 0 or "risk_score" not in alerts_df.columns:
        return monitor_drift_psi(np.array([]), np.array([]))
    
    current_scores = pd.to_numeric(alerts_df["risk_score"], errors="coerce").dropna().values
    
    if baseline_df is not None and len(baseline_df) > 0 and "risk_score" in baseline_df.columns:
        baseline_scores = pd.to_numeric(baseline_df["risk_score"], errors="coerce").dropna().values
    else:
        # Use first 50% as baseline (synthetic baseline for testing)
        n = len(current_scores)
        if n >= 20:
            split_idx = n // 2
            baseline_scores = current_scores[:split_idx]
            current_scores = current_scores[split_idx:]
        else:
            baseline_scores = current_scores
    
    return monitor_drift_psi(current_scores, baseline_scores)


# =============================================================================
# G5: Rule Hit Distribution Monitor
# =============================================================================

def parse_rules_json(rules_json_col: pd.Series) -> pd.DataFrame:
    """
    Parse rules_json column into structured format.
    
    Args:
        rules_json_col: Series containing rules_json (list of dicts or JSON strings)
        
    Returns:
        DataFrame with columns: alert_idx, rule_id, hit
    """
    records = []
    
    for idx, val in rules_json_col.items():
        if val is None:
            continue
            
        # Parse if string
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                continue
        
        # Handle list of rule results
        if isinstance(val, list):
            for rule in val:
                if isinstance(rule, dict):
                    records.append({
                        "alert_idx": idx,
                        "rule_id": rule.get("rule_id", "unknown"),
                        "hit": bool(rule.get("hit", False)),
                    })
    
    if not records:
        return pd.DataFrame(columns=["alert_idx", "rule_id", "hit"])
    
    return pd.DataFrame(records)


def compute_rule_hit_rates(
    alerts_df: pd.DataFrame,
    segment_col: Optional[str] = "segment",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute rule hit rates overall and by segment.
    
    Args:
        alerts_df: DataFrame with rules_json column
        segment_col: Column name for segmentation (optional)
        
    Returns:
        Tuple of (overall_rates, segment_rates) DataFrames
    """
    if "rules_json" not in alerts_df.columns:
        return pd.DataFrame(), pd.DataFrame()
    
    rules_df = parse_rules_json(alerts_df["rules_json"])
    
    if len(rules_df) == 0:
        return pd.DataFrame(), pd.DataFrame()
    
    n_alerts = len(alerts_df)
    
    # Overall hit rates
    overall_rates = (
        rules_df.groupby("rule_id")["hit"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "n_hits", "count": "n_alerts"})
    )
    overall_rates["hit_rate"] = overall_rates["n_hits"] / n_alerts
    overall_rates = overall_rates.reset_index()
    
    # Segment hit rates (if segment column exists)
    segment_rates = pd.DataFrame()
    if segment_col and segment_col in alerts_df.columns:
        # Join segment to rules_df
        rules_df = rules_df.merge(
            alerts_df[[segment_col]].reset_index().rename(columns={"index": "alert_idx"}),
            on="alert_idx",
            how="left",
        )
        
        segment_rates = (
            rules_df.groupby(["rule_id", segment_col])["hit"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "n_hits", "count": "n_alerts"})
        )
        segment_rates["hit_rate"] = segment_rates["n_hits"] / segment_rates["n_alerts"]
        segment_rates = segment_rates.reset_index()
    
    return overall_rates, segment_rates


def monitor_rule_hits(
    current_rates: pd.DataFrame,
    baseline_rates: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Monitor rule hit distribution for spikes.
    
    Args:
        current_rates: DataFrame with rule_id, hit_rate columns
        baseline_rates: Optional baseline DataFrame
        
    Returns:
        Signal dict with name, status, value, threshold, details
    """
    if len(current_rates) == 0:
        return {
            "name": "rule_hit_distribution",
            "status": STATUS_GREEN,
            "value": None,
            "threshold": None,
            "details": {"info": "No rule hit data available"},
        }
    
    # Get thresholds
    max_pct_change = _get_threshold("HEALTH_RULE_HIT_MAX_PCT_CHANGE", 1.0)
    max_z = _get_threshold("HEALTH_RULE_HIT_MAX_Z", 3.0)
    amber_pct_change = _get_threshold("HEALTH_RULE_HIT_AMBER_PCT", 0.5)
    amber_z = _get_threshold("HEALTH_RULE_HIT_AMBER_Z", 2.0)
    
    spikes = []
    status = STATUS_GREEN
    
    if baseline_rates is not None and len(baseline_rates) > 0:
        # Compare current to baseline
        merged = current_rates.merge(
            baseline_rates[["rule_id", "hit_rate"]],
            on="rule_id",
            how="left",
            suffixes=("", "_baseline"),
        )
        
        for _, row in merged.iterrows():
            rule_id = row["rule_id"]
            current = row["hit_rate"]
            baseline = row.get("hit_rate_baseline", 0.0)
            
            if pd.isna(baseline) or baseline < 1e-9:
                continue
            
            pct_change = _compute_pct_change(current, baseline)
            
            if abs(pct_change) >= max_pct_change:
                spikes.append({
                    "rule_id": rule_id,
                    "current": current,
                    "baseline": baseline,
                    "pct_change": pct_change,
                    "severity": STATUS_RED,
                })
                status = STATUS_RED
            elif abs(pct_change) >= amber_pct_change:
                spikes.append({
                    "rule_id": rule_id,
                    "current": current,
                    "baseline": baseline,
                    "pct_change": pct_change,
                    "severity": STATUS_AMBER,
                })
                if status == STATUS_GREEN:
                    status = STATUS_AMBER
    
    # Always report top rules by hit rate
    top_rules = current_rates.nlargest(5, "hit_rate").to_dict("records")
    
    return {
        "name": "rule_hit_distribution",
        "status": status,
        "value": {
            "top_rules": top_rules,
            "n_rules": len(current_rates),
            "spikes": spikes,
        },
        "threshold": {
            "max_pct_change": max_pct_change,
            "amber_pct_change": amber_pct_change,
            "max_z": max_z,
            "amber_z": amber_z,
        },
        "details": {
            "has_baseline": baseline_rates is not None and len(baseline_rates) > 0,
            "n_spikes": len(spikes),
            "recommendation": "Rule threshold drift detected. Review rule configurations." if status != STATUS_GREEN else None,
        },
    }


# =============================================================================
# G6: Health Aggregation
# =============================================================================

def aggregate_health_status(signals: List[Dict[str, Any]]) -> str:
    """
    Aggregate overall health status from individual signals.
    
    RED if any RED signal, else AMBER if any AMBER, else GREEN.
    
    Args:
        signals: List of signal dicts with status field
        
    Returns:
        Aggregated status string
    """
    if any(s.get("status") == STATUS_RED for s in signals):
        return STATUS_RED
    if any(s.get("status") == STATUS_AMBER for s in signals):
        return STATUS_AMBER
    return STATUS_GREEN


def get_action_recommendations(signals: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Get recommended actions for non-GREEN signals.
    
    Args:
        signals: List of signal dicts
        
    Returns:
        List of {signal_name, status, recommendation} dicts
    """
    recommendations = []
    
    for signal in signals:
        status = signal.get("status", STATUS_GREEN)
        if status == STATUS_GREEN:
            continue
        
        details = signal.get("details", {})
        rec = details.get("recommendation", "")
        
        if not rec:
            # Default recommendations by signal name
            name = signal.get("name", "unknown")
            if name == "risk_distribution":
                rec = "Check calibration / risk mapping"
            elif name == "alert_rate":
                rec = "Investigate data shift or upstream changes"
            elif name == "drift_psi":
                rec = "Investigate data shift. Consider model recalibration."
            elif name == "rule_hit_distribution":
                rec = "Rule threshold drift detected. Review rule configurations."
            else:
                rec = "Review and investigate"
        
        recommendations.append({
            "signal_name": signal.get("name", "unknown"),
            "status": status,
            "recommendation": rec,
        })
    
    return recommendations


# =============================================================================
# Main Function: compute_health_report
# =============================================================================

def compute_health_report(
    alerts_df: pd.DataFrame,
    daily_stats: Optional[List[Dict[str, int]]] = None,
    baseline_df: Optional[pd.DataFrame] = None,
    baseline_rule_rates: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute comprehensive health report from persisted DB state.
    
    This function uses only persisted data (alerts + rules_json + risk fields).
    No recomputation on UI rerun.
    
    Args:
        alerts_df: DataFrame loaded from alerts table (with risk_score, rules_json, etc.)
        daily_stats: List of daily stats dicts from previous runs (optional)
        baseline_df: Baseline alerts DataFrame for drift comparison (optional)
        baseline_rule_rates: Baseline rule hit rates DataFrame (optional)
        
    Returns:
        Health report dict:
        {
            "status": "GREEN|AMBER|RED",
            "signals": [
                {"name": "...", "status": "...", "value": ..., "threshold": ..., "details": {...}},
                ...
            ],
            "generated_at": "...",
            "data_window": {"n_alerts": ..., "time_range": ...},
            "recommendations": [{"signal_name": ..., "status": ..., "recommendation": ...}],
        }
    """
    signals: List[Dict[str, Any]] = []
    
    # G2: Risk distribution monitor
    risk_signal = monitor_risk_distribution(alerts_df)
    signals.append(risk_signal)
    
    # G3: Alert rate monitor
    current_stats = {
        "total_alerts": len(alerts_df),
        "in_queue": int(alerts_df.get("in_queue", pd.Series([False])).sum()) if "in_queue" in alerts_df.columns else 0,
        "suppressed": int((~alerts_df.get("in_queue", pd.Series([True]))).sum()) if "in_queue" in alerts_df.columns else 0,
    }
    alert_signal = monitor_alert_rate(current_stats, daily_stats or [])
    signals.append(alert_signal)
    
    # G4: Drift proxy monitor (PSI)
    drift_signal = monitor_drift_from_df(alerts_df, baseline_df)
    signals.append(drift_signal)
    
    # G5: Rule hit distribution monitor
    current_rule_rates, _ = compute_rule_hit_rates(alerts_df)
    rule_signal = monitor_rule_hits(current_rule_rates, baseline_rule_rates)
    signals.append(rule_signal)
    
    # G6: Aggregate health status
    overall_status = aggregate_health_status(signals)
    recommendations = get_action_recommendations(signals)
    
    # Data window info
    time_range = None
    if "created_at" in alerts_df.columns and len(alerts_df) > 0:
        try:
            created_at = pd.to_datetime(alerts_df["created_at"], errors="coerce")
            valid_times = created_at.dropna()
            if len(valid_times) > 0:
                time_range = {
                    "min": str(valid_times.min()),
                    "max": str(valid_times.max()),
                }
        except Exception:
            pass
    
    return {
        "status": overall_status,
        "signals": signals,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "data_window": {
            "n_alerts": len(alerts_df),
            "time_range": time_range,
        },
        "recommendations": recommendations,
    }


# =============================================================================
# Daily Stats Helpers
# =============================================================================

def compute_daily_stats(alerts_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute daily stats for current pipeline run.
    
    Args:
        alerts_df: DataFrame with alerts
        
    Returns:
        Dict with date, total_alerts, in_queue, mandatory_review, suppressed
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    
    total = len(alerts_df)
    in_queue = 0
    suppressed = 0
    mandatory_review = 0
    
    if "in_queue" in alerts_df.columns:
        in_queue = int(alerts_df["in_queue"].sum())
        suppressed = total - in_queue
    
    if "risk_band" in alerts_df.columns:
        mandatory_review = int((alerts_df["risk_band"].isin(["critical", "high"])).sum())
    
    return {
        "date": today,
        "total_alerts": total,
        "in_queue": in_queue,
        "mandatory_review": mandatory_review,
        "suppressed": suppressed,
    }


def compute_rule_hit_stats(alerts_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Compute rule hit stats for current pipeline run.
    
    Args:
        alerts_df: DataFrame with rules_json column
        
    Returns:
        List of dicts with run_date, rule_id, hit_rate, n_alerts, n_hits
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    
    overall_rates, _ = compute_rule_hit_rates(alerts_df)
    
    if len(overall_rates) == 0:
        return []
    
    stats = []
    for _, row in overall_rates.iterrows():
        stats.append({
            "run_date": today,
            "rule_id": row["rule_id"],
            "hit_rate": float(row["hit_rate"]),
            "n_alerts": int(row.get("n_alerts", 0)),
            "n_hits": int(row.get("n_hits", 0)),
        })
    
    return stats
