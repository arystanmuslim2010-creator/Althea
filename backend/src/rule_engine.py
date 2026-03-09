"""Canonical AML rule engine: modular rules, RuleResult aggregation, versioning."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from . import config
from .rules import (
    run_dormant,
    run_flow_through,
    run_high_risk_country,
    run_low_buyer_diversity,
    run_rapid_withdraw,
    run_structuring,
)

# Map modular rule_id -> legacy R00x for scoring backward compatibility
RULE_ID_TO_LEGACY: Dict[str, str] = {
    "structuring": "R001",
    "rapid_withdraw": "R002",
    "dormant": "R003",
    "flow_through": "R004",
    "high_risk_country": "R005",
}

RESULT_COLS = [
    "rule_structuring_result",
    "rule_dormant_result",
    "rule_rapid_withdraw_result",
    "rule_flow_through_result",
    "rule_low_buyer_diversity_result",
    "rule_high_risk_country_result",
]


def _ensure_ts_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
        return out
    for col in config.TIME_COL_CANDIDATES:
        if col in out.columns:
            parsed = pd.to_datetime(out[col], errors="coerce")
            if parsed.notna().any():
                out["ts"] = parsed
                return out
    out["ts"] = pd.NaT
    return out


def _row_results_to_list(row: pd.Series) -> List[Dict[str, Any]]:
    """Collect all rule result dicts for one row (from rule_*_result columns)."""
    out: List[Dict[str, Any]] = []
    for col in RESULT_COLS:
        if col not in row.index:
            continue
        v = row[col]
        if isinstance(v, dict):
            out.append(v)
        elif isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    out.append(x)
    return out


def _build_rules_json_and_legacy(
    df: pd.DataFrame,
    policy_params: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Build rules_json, rule_evidence_json, rule_hits per row;
    add rule_R001_hit .. rule_R005_hit and rule_score_total for scoring compat.
    """
    policy_params = policy_params or {}
    weights = {
        "R001": float(policy_params.get("RULE_WEIGHT_R001", getattr(config, "RULE_WEIGHT_R001", 0.30))),
        "R002": float(policy_params.get("RULE_WEIGHT_R002", getattr(config, "RULE_WEIGHT_R002", 0.20))),
        "R003": float(policy_params.get("RULE_WEIGHT_R003", getattr(config, "RULE_WEIGHT_R003", 0.20))),
        "R004": float(policy_params.get("RULE_WEIGHT_R004", getattr(config, "RULE_WEIGHT_R004", 0.15))),
        "R005": float(policy_params.get("RULE_WEIGHT_R005", getattr(config, "RULE_WEIGHT_R005", 0.15))),
    }
    weight_sum = max(sum(weights.values()), 1e-9)
    weights = {k: v / weight_sum for k, v in weights.items()}

    rules_json_list: List[List[Dict[str, Any]]] = []
    rule_evidence_list: List[Dict[str, Any]] = []
    rule_hits_list: List[List[str]] = []
    r001_hit = []
    r002_hit = []
    r003_hit = []
    r004_hit = []
    r005_hit = []
    rule_score_total_list = []

    for idx in df.index:
        row = df.loc[idx]
        results = _row_results_to_list(row)
        rules_json_list.append(results)
        rule_evidence_list.append({r["rule_id"]: r.get("evidence", {}) for r in results})
        hits = [r["rule_id"] for r in results if r.get("hit")]
        rule_hits_list.append(hits)

        # Legacy R001-R005
        hit_by_legacy = {RULE_ID_TO_LEGACY.get(r["rule_id"]): r.get("hit", False) for r in results}
        r001_hit.append(1 if hit_by_legacy.get("R001") else 0)
        r002_hit.append(1 if hit_by_legacy.get("R002") else 0)
        r003_hit.append(1 if hit_by_legacy.get("R003") else 0)
        r004_hit.append(1 if hit_by_legacy.get("R004") else 0)
        r005_hit.append(1 if hit_by_legacy.get("R005") else 0)

        # rule_score_total: weighted sum of R001..R005 scores, scaled 0-100
        score_total = 0.0
        for r in results:
            leg = RULE_ID_TO_LEGACY.get(r["rule_id"])
            if leg and leg in weights:
                score_total += weights[leg] * float(r.get("score", 0.0))
        rule_score_total_list.append(min(1.0, score_total) * 100.0)

    df = df.copy()
    df["rules_json"] = rules_json_list
    df["rule_evidence_json"] = rule_evidence_list
    df["rule_hits"] = rule_hits_list
    df["rule_R001_hit"] = r001_hit
    df["rule_R002_hit"] = r002_hit
    df["rule_R003_hit"] = r003_hit
    df["rule_R004_hit"] = r004_hit
    df["rule_R005_hit"] = r005_hit
    df["rule_score_total"] = rule_score_total_list
    df["rule_hits_count"] = [len(h) for h in rule_hits_list]
    return df


def _empty_rule_results() -> List[Dict[str, Any]]:
    """No-hit placeholder when rules cannot run (e.g. no timestamps)."""
    return [
        {"rule_id": rid, "rule_version": "1.0.0", "hit": False, "severity": "INFO", "score": 0.0, "evidence": {}, "thresholds": {}, "window": {}}
        for rid in ["structuring", "dormant", "rapid_withdraw", "flow_through", "low_buyer_diversity", "high_risk_country"]
    ]


def run_all_rules(df: pd.DataFrame, cfg, policy_params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Run all modular rules and aggregate RuleResults into rules_json, rule_evidence_json, legacy columns."""
    out = _ensure_ts_column(df)
    if out["ts"].isna().all():
        for name in [
            "structuring",
            "dormant",
            "rapid_withdraw",
            "flow_through",
            "low_buyer_diversity",
            "high_risk_country",
        ]:
            out[f"rule_{name}_hit"] = 0
            out[f"rule_{name}_score"] = 0.0
            out[f"rule_{name}_evidence"] = "none"
            out[f"rule_{name}_result"] = None
        n = len(out)
        empty = _empty_rule_results()
        out["rules_json"] = [empty] * n
        out["rule_evidence_json"] = [{r["rule_id"]: r["evidence"] for r in empty}] * n
        out["rule_hits"] = [[]] * n
        out["rule_R001_hit"] = [0] * n
        out["rule_R002_hit"] = [0] * n
        out["rule_R003_hit"] = [0] * n
        out["rule_R004_hit"] = [0] * n
        out["rule_R005_hit"] = [0] * n
        out["rule_score_total"] = [0.0] * n
        out["rule_hits_count"] = [0] * n
        return out
    out = run_structuring(out, cfg)
    out = run_dormant(out, cfg)
    out = run_rapid_withdraw(out, cfg)
    out = run_flow_through(out, cfg)
    out = run_low_buyer_diversity(out, cfg)
    out = run_high_risk_country(out, cfg)
    out = _build_rules_json_and_legacy(out, policy_params)
    return out


def run_rules(
    df: pd.DataFrame,
    cfg,
    policy_params: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Canonical rules pipeline: run all rules, aggregate RuleResults.
    Returns (df with rules_json, rule_evidence_json, rule_hits; rule_hits_summary).
    policy_params may contain enabled_rules: {rule_id: {min_version, params}} and threshold overrides.
    """
    out = run_all_rules(df, cfg, policy_params)
    # Summary: list of {rule_id, hit_count, ...} per rule (optional)
    rule_hits_summary: List[Dict[str, Any]] = []
    if "rule_hits" in out.columns:
        all_hits = out["rule_hits"].explode().dropna()
        if len(all_hits) > 0:
            for rule_id in all_hits.unique():
                rule_hits_summary.append({
                    "rule_id": rule_id,
                    "hit_count": int((out["rule_hits"].apply(lambda x: rule_id in x if isinstance(x, list) else False)).sum()),
                })
    return out, rule_hits_summary


def aggregate_rule_score(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Aggregate rule_score_raw, rule_top_hit, rule_evidence, rule_hit_any from rule_*_score columns."""
    out = df.copy()
    score_cols = [c for c in out.columns if c.startswith("rule_") and c.endswith("_score") and "_result" not in c]
    hit_cols = [c for c in out.columns if c.startswith("rule_") and c.endswith("_hit") and not c.startswith("rule_R00")]
    evidence_cols = [c for c in out.columns if c.startswith("rule_") and c.endswith("_evidence")]

    if not score_cols:
        out["rule_score_raw"] = 0.0
        out["rule_top_hit"] = "none"
        out["rule_evidence"] = "none"
        out["rule_hit_any"] = 0
        return out

    scores = out[score_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    out["rule_score_raw"] = scores.max(axis=1).clip(0.0, 1.0)
    top_idx = scores.values.argmax(axis=1)
    rule_names = [c.replace("rule_", "").replace("_score", "") for c in score_cols]
    out["rule_top_hit"] = [rule_names[i] if out["rule_score_raw"].iat[idx] > 0 else "none" for idx, i in enumerate(top_idx)]

    evidence_map = {}
    for col in evidence_cols:
        name = col.replace("rule_", "").replace("_evidence", "")
        evidence_map[name] = col
    evidence = []
    for idx, name in enumerate(out["rule_top_hit"].tolist()):
        if name == "none" or name not in evidence_map:
            evidence.append("none")
        else:
            evidence.append(str(out.loc[out.index[idx], evidence_map[name]]))
    out["rule_evidence"] = evidence

    if hit_cols:
        hits = out[hit_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
        out["rule_hit_any"] = (hits.max(axis=1) > 0).astype(int)
    else:
        out["rule_hit_any"] = 0

    return out
