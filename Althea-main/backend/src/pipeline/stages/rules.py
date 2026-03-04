"""
RULES stage: Run rules against normalized+context data; standardize evidence (rule_id, name, severity, triggered, evidence).
Evidence must include external source + version when used.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pandas as pd

from ... import config
from ...rule_engine import run_all_rules, aggregate_rule_score
from ...external_data.constraints import high_risk_countries_for_rule
from ...external_data import load_all_configured_sources
from ...observability.logging import get_logger

logger = get_logger("rules")


def _standardize_evidence(row_rules: Any, external_high_risk: Optional[Dict] = None) -> List[Dict[str, Any]]:
    """Convert rule results to standard format: rule_id, name, severity, triggered, evidence (with external_source, version)."""
    out: List[Dict[str, Any]] = []
    if isinstance(row_rules, str):
        try:
            row_rules = json.loads(row_rules) if row_rules else []
        except Exception:
            row_rules = []
    if not isinstance(row_rules, list):
        return out
    ext_src = (external_high_risk or {}).get("source_name", "")
    ext_ver = (external_high_risk or {}).get("version", "")
    for r in row_rules:
        if not isinstance(r, dict):
            continue
        rule_id = r.get("rule_id", "")
        evidence = r.get("evidence") or {}
        if isinstance(evidence, str):
            evidence = {}
        if rule_id == "high_risk_country" and ext_src:
            evidence["external_source"] = ext_src
            evidence["external_version"] = ext_ver
        out.append({
            "rule_id": rule_id,
            "name": rule_id.replace("_", " ").title(),
            "severity": r.get("severity", "INFO"),
            "triggered": bool(r.get("hit", False)),
            "evidence": evidence,
        })
    return out


def run_rules_stage(
    df: pd.DataFrame,
    run_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Run rule engine and standardize rule_evidence_json format."""
    cfg = config or {}
    cfg_ns = type("Config", (), {**{k: getattr(config, k) for k in dir(config) if k.isupper()}, **cfg})()
    df = run_all_rules(df, cfg_ns, policy_params=None)
    df = aggregate_rule_score(df, cfg_ns)

    try:
        loaded = load_all_configured_sources()
        external_hr = high_risk_countries_for_rule(loaded)
    except Exception:
        external_hr = None

    standardized = []
    for idx in df.index:
        row = df.loc[idx]
        rules_raw = row.get("rules_json", [])
        standardized.append(_standardize_evidence(rules_raw, external_hr))
    df["rule_evidence_standard"] = standardized
    df["rule_evidence_json"] = [json.dumps(x) for x in standardized]
    return df
