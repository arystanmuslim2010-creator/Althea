"""Canonical risk pipeline: raw -> calibrated prob -> meta-risk -> governed 0-100 score -> band."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from . import config
from . import calibration


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return np.clip(1.0 / (1.0 + np.exp(-np.clip(x, -500, 500))), 0.0, 1.0)


def _logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    p = np.clip(p, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))


# Default segment log-odds priors (additive)
DEFAULT_SEGMENT_LOG_ODDS: Dict[str, float] = {
    "retail_low": -0.2,
    "retail_high": -0.1,
    "smb": 0.0,
    "corporate": 0.2,
    "unknown": 0.0,
}

# Rule severity -> additive log-odds
SEVERITY_LOG_ODDS: Dict[str, float] = {
    "INFO": 0.0,
    "LOW": 0.1,
    "MEDIUM": 0.3,
    "HIGH": 0.5,
    "CRITICAL": 0.8,
}

# External risk level -> bounded log-odds prior (small, monotonicity-preserving)
EXTERNAL_RISK_LOG_ODDS: Dict[str, float] = {
    "HIGH": 0.25,
    "CRITICAL": 0.4,
    "MEDIUM": 0.1,
}


def _external_country_prior(
    country_series: pd.Series,
    loaded_sources: Dict[str, Any],
) -> np.ndarray:
    """
    Build per-row country log-odds prior from external high_risk_countries source (bounded).
    """
    n = len(country_series)
    prior = np.zeros(n, dtype=float)
    if "high_risk_countries" not in loaded_sources:
        return prior
    data = loaded_sources["high_risk_countries"].get("data", {})
    records = data.get("records", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    country_to_log_odds: Dict[str, float] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        cc = rec.get("country_code")
        level = str(rec.get("risk_level", "")).upper()
        if cc:
            country_to_log_odds[str(cc).upper()] = EXTERNAL_RISK_LOG_ODDS.get(level, 0.1)
    country_codes = country_series.astype(str).str.upper()
    for i in range(n):
        c = country_codes.iloc[i] if i < len(country_codes) else ""
        prior[i] = country_to_log_odds.get(c, 0.0)
    return prior


def compute_risk(
    alert_df: pd.DataFrame,
    policy_params: Optional[Dict[str, Any]] = None,
    external_sources: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Single canonical risk pipeline:
    raw_model_score -> calibrated_probability -> meta-risk (incl. external priors) -> governed 0-100 score -> risk_band.

    Expects alert_df to have at least:
    - risk_score_raw (float): raw model/anomaly blend before calibration
    - risk_prob (float): calibrated probability in [0,1]

    Optional: segment, country, rules_json (list of RuleResult dicts), created_at, alert_id.

    external_sources: Optional loaded external data from load_all_configured_sources();
      used for country risk log-odds priors and recorded in risk_explain_json (base_prob, external_priors, final_prob).

    Returns df with:
    - risk_score_raw, risk_prob, risk_score (0-100), risk_band, risk_score_rank, risk_explain_json.
    """
    policy_params = policy_params or {}
    t1 = int(policy_params.get("risk_band_t1", getattr(config, "RISK_BAND_T1", 40)))
    t2 = int(policy_params.get("risk_band_t2", getattr(config, "RISK_BAND_T2", 70)))
    t3 = int(policy_params.get("risk_band_t3", getattr(config, "RISK_BAND_T3", 90)))
    mapping_kind = str(policy_params.get("score_mapping_kind", getattr(config, "SCORE_MAPPING_KIND", "logit_stretch")))
    segment_priors = policy_params.get("segment_log_odds", DEFAULT_SEGMENT_LOG_ODDS)

    df = alert_df.copy()
    n = len(df)

    # Ensure required columns
    risk_prob = np.clip(
        pd.to_numeric(df.get("risk_prob", 0.5), errors="coerce").fillna(0.5).to_numpy(),
        1e-6,
        1.0 - 1e-6,
    )
    risk_score_raw = pd.to_numeric(df.get("risk_score_raw", 0.0), errors="coerce").fillna(0.0).to_numpy()

    # --- D3: Meta-risk in log-odds space ---
    base_log_odds = _logit(risk_prob)
    meta_prior = np.zeros(n, dtype=float)

    # Segment prior
    segment = df.get("segment", pd.Series(["unknown"] * n, index=df.index))
    segment = segment.astype(str).str.lower()
    seg_prior_arr = np.array([segment_priors.get(s, segment_priors.get("unknown", 0.0)) for s in segment])
    meta_prior += seg_prior_arr

    # Country prior: from external_sources if provided (versioned), else config
    country = df.get("country", pd.Series([""] * n, index=df.index)).astype(str).str.upper()
    if external_sources and "high_risk_countries" in external_sources:
        country_prior = _external_country_prior(country, external_sources)
    else:
        high_risk_countries = list(getattr(config, "HIGH_RISK_COUNTRIES", ["AE", "TR", "PA", "CY", "RU"]))
        country_prior = np.where(country.isin(high_risk_countries), 0.4, 0.0)
    meta_prior += country_prior

    # Rule severity aggregation from rules_json
    rules_json_col = df.get("rules_json", None)
    if rules_json_col is not None:
        for i in range(n):
            rlist = rules_json_col.iloc[i] if hasattr(rules_json_col, "iloc") else (rules_json_col[i] if i < len(rules_json_col) else [])
            if not isinstance(rlist, list):
                continue
            for r in rlist:
                if not isinstance(r, dict) or not r.get("hit"):
                    continue
                sev = r.get("severity", "INFO")
                meta_prior[i] += SEVERITY_LOG_ODDS.get(sev, 0.0)

    combined_log_odds = base_log_odds + meta_prior
    risk_prob_meta = _sigmoid(combined_log_odds)
    risk_prob_meta = np.clip(risk_prob_meta, 0.0, 1.0)

    # --- D4: Anti-saturation mapping to 0-100 (monotonic) ---
    risk_score = calibration.score_mapping(risk_prob_meta, kind=mapping_kind)
    risk_score = np.clip(risk_score, 0.0, 100.0)

    df["risk_score_raw"] = risk_score_raw
    df["risk_prob"] = risk_prob_meta
    df["risk_score"] = risk_score

    # --- D5: Risk bands and deterministic ranking ---
    df["risk_band"] = np.select(
        [risk_score < t1, risk_score < t2, risk_score < t3],
        ["LOW", "MEDIUM", "HIGH"],
        default="CRITICAL",
    )

    # Deterministic rank: risk_score desc, then created_at asc, then alert_id asc
    created_at = df.get("created_at", pd.Series(range(n), index=df.index))
    if pd.api.types.is_datetime64_any_dtype(created_at):
        created_at = created_at.astype("int64")
    else:
        created_at = pd.to_numeric(created_at, errors="coerce").fillna(0)
    alert_id = df.get("alert_id", df.index.astype(str)).astype(str)
    rank_df = pd.DataFrame({
        "_rs": risk_score,
        "_ca": created_at.values if hasattr(created_at, "values") else created_at,
        "_aid": alert_id.values if hasattr(alert_id, "values") else alert_id,
    }, index=df.index)
    rank_df = rank_df.sort_values(["_rs", "_ca", "_aid"], ascending=[False, True, True])
    rank_df["_ord"] = range(1, len(rank_df) + 1)
    df["risk_score_rank"] = rank_df.reindex(df.index)["_ord"].fillna(0).astype(int)

    # --- risk_explain_json: base_prob, external_priors, final_prob for audit (Phase F) ---
    use_external = bool(external_sources and "high_risk_countries" in external_sources)
    explain_list = []
    for i in range(n):
        explain_entry = {
            "risk_score_raw": float(risk_score_raw[i]),
            "risk_prob_calibrated": float(risk_prob[i]),
            "base_prob": float(risk_prob[i]),
            "meta_prior_log_odds": float(meta_prior[i]),
            "segment_prior": float(seg_prior_arr[i]) if i < len(seg_prior_arr) else 0.0,
            "country_prior": float(country_prior[i]),
            "risk_prob_meta": float(risk_prob_meta[i]),
            "final_prob": float(risk_prob_meta[i]),
            "risk_score": float(risk_score[i]),
            "risk_band": str(df["risk_band"].iloc[i]),
            "score_mapping_kind": mapping_kind,
            "band_thresholds": {"t1": t1, "t2": t2, "t3": t3},
        }
        if use_external:
            explain_entry["external_priors"] = {"high_risk_countries": float(country_prior[i])}
        else:
            explain_entry["external_priors"] = {}
        explain_list.append(explain_entry)
    df["risk_explain_json"] = explain_list

    return df


def compute_risk_explain_serialized(explain_obj: Any) -> str:
    """Serialize risk_explain_json for DB storage (TEXT)."""
    if explain_obj is None:
        return "{}"
    if isinstance(explain_obj, str):
        return explain_obj
    if isinstance(explain_obj, dict):
        return json.dumps(explain_obj)
    if isinstance(explain_obj, list):
        return json.dumps(explain_obj)
    return json.dumps({})
