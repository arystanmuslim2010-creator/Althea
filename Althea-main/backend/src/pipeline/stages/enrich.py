"""
ENRICH stage: Build context_json before scoring (behavioral baseline, historical alerts, peer, external, typology_likelihood).
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd

from ...external_data import load_all_configured_sources
from ...external_data.constraints import compute_external_flags_and_versions, external_versions_snapshot
from ...observability.logging import get_logger

logger = get_logger("enrich")

# Default typology names for deterministic likelihood (no ML)
DEFAULT_TYPOLOGIES = [
    "sanctions", "cross_border", "flow_through", "rapid_velocity", "structuring",
    "high_amount_outlier", "burst_activity", "smurfing", "bank_alert", "dormant",
]

# Severity-like weights for external/behavior boosts (deterministic)
BOOST_HIGH_RISK_COUNTRY = 0.05
BOOST_HIGH_AMOUNT = 0.10
BASELINE_P = 0.05


def _compute_typology_likelihood(
    row: pd.Series,
    alert_id: str,
    flags: Dict[str, bool],
    amount_high: bool,
    typologies: List[str],
) -> tuple[Dict[str, float], Dict[str, Any]]:
    """
    Deterministic typology likelihood from external signals and behavior.
    Returns (typology_likelihood dict, top_typology {"name", "p"}).
    """
    p_map = {t: BASELINE_P for t in typologies}
    if flags.get("sanctions_hit"):
        p_map["sanctions"] = 1.0
    if flags.get("high_risk_country_critical"):
        for t in ("cross_border", "flow_through"):
            if t in p_map:
                p_map[t] = min(1.0, p_map[t] + BOOST_HIGH_RISK_COUNTRY)
    if amount_high:
        for t in ("rapid_velocity", "structuring"):
            if t in p_map:
                p_map[t] = min(1.0, p_map[t] + BOOST_HIGH_AMOUNT)
    # Clamp all to [0, 1]
    for k in p_map:
        p_map[k] = max(0.0, min(1.0, float(p_map[k])))
    # top_typology = argmax p
    best_name = max(p_map, key=lambda k: p_map[k])
    top_typology = {"name": best_name, "p": p_map[best_name]}
    return p_map, top_typology


def run_enrich(
    df: pd.DataFrame,
    run_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    storage: Any = None,
) -> pd.DataFrame:
    """
    Build context per alert: behavioral baseline (placeholder from segment), historical alerts count,
    peer comparison (segment percentile), external signals (risk country, sanctions from external_data).
    Store context in context_json (or features_json if no dedicated column).
    """
    config = config or {}
    out = df.copy()

    try:
        loaded_sources = load_all_configured_sources()
    except Exception:
        loaded_sources = {}
    external_versions = external_versions_snapshot(loaded_sources)
    flags_per_alert, _ = compute_external_flags_and_versions(out, loaded_sources, country_col="country", user_id_col="user_id")

    # Per-entity historical alerts count (from current df only for this run)
    if "user_id" in out.columns:
        hist_count = out.groupby("user_id").size().to_dict()
        out["_hist_alert_count"] = out["user_id"].map(hist_count).fillna(0).astype(int)
    else:
        out["_hist_alert_count"] = 0

    # Segment percentile (peer comparison)
    if "segment" in out.columns and "risk_score" not in out.columns:
        out["_segment_pct"] = 0.5  # placeholder until we have risk_score
    elif "segment" in out.columns and "risk_score" in out.columns:
        out["_segment_pct"] = out.groupby("segment")["risk_score"].rank(pct=True).fillna(0.5)
    else:
        out["_segment_pct"] = 0.5

    # Typology set: union of default and present typology column (deterministic)
    typologies = list(DEFAULT_TYPOLOGIES)
    if "typology" in out.columns:
        for t in out["typology"].dropna().astype(str).unique():
            t = (t or "").strip()
            if t and t not in typologies:
                typologies.append(t)
    typologies = sorted(typologies)

    # Amount high: above 90th percentile (deterministic)
    if "amount" in out.columns:
        amt = pd.to_numeric(out["amount"], errors="coerce").fillna(0)
        thresh = amt.quantile(0.9) if len(amt) > 0 else 0
        out["_amount_high"] = (amt >= thresh) & (amt > 0)
    else:
        out["_amount_high"] = False

    context_list = []
    for idx in out.index:
        row = out.loc[idx]
        alert_id = str(row.get("alert_id", idx))
        entity_id = str(row.get("entity_id", row.get("user_id", "")))
        n_hist_val = row.get("n_hist", row.get("_hist_alert_count", 0))
        if n_hist_val is None or (isinstance(n_hist_val, float) and pd.isna(n_hist_val)):
            n_hist_val = 0
        behavioral_baseline = {"segment": str(row.get("segment", "")), "n_hist": int(n_hist_val)}
        historical_alerts = {"count": int(n_hist_val), "prior_dispositions": []}
        segment_pct = row.get("peer_segment_percentile", row.get("_segment_pct", 0.5))
        if segment_pct is None or (isinstance(segment_pct, float) and pd.isna(segment_pct)):
            segment_pct = 0.5
        peer_comparison = {"segment_percentile": float(segment_pct)}
        flags = flags_per_alert.get(alert_id, {})
        external_signals = {
            "risk_country": flags.get("high_risk_country_critical", False),
            "sanctions_flags": flags.get("sanctions_hit", False),
        }
        typology_likelihood_map, top_typology = _compute_typology_likelihood(
            row, alert_id, flags,
            amount_high=bool(row.get("_amount_high", False)),
            typologies=typologies,
        )
        context_list.append({
            "behavioral_baseline": behavioral_baseline,
            "historical_alerts": historical_alerts,
            "peer_comparison": peer_comparison,
            "external_signals": external_signals,
            "typology_likelihood": typology_likelihood_map,
            "top_typology": top_typology,
        })

    out["context_json"] = [json.dumps(c) if isinstance(c, dict) else c for c in context_list]
    out["external_versions_json"] = [json.dumps(external_versions)] * len(out)

    # Drop temp columns
    for c in ["_hist_alert_count", "_segment_pct", "_amount_high"]:
        if c in out.columns:
            out = out.drop(columns=[c])
    return out
