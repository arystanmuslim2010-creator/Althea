from __future__ import annotations

import pandas as pd

from .base import RuleResult

RULE_ID = "high_risk_country"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "HIGH"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """
    High-risk country rule. Uses external_high_risk_countries from cfg when provided
    (source name + version in evidence); otherwise falls back to config HIGH_RISK_COUNTRIES.
    """
    out = df.copy()
    hit_col = "rule_high_risk_country_hit"
    score_col = "rule_high_risk_country_score"
    evidence_col = "rule_high_risk_country_evidence"
    result_col = "rule_high_risk_country_result"

    required = {"country", "direction"}
    if not required.issubset(out.columns):
        out[hit_col] = 0
        out[score_col] = 0.0
        out[evidence_col] = "none"
        out[result_col] = None
        return out

    # Do not hardcode country lists; read from loader (cfg.external_high_risk_countries) or config
    external = getattr(cfg, "external_high_risk_countries", None)
    if external and isinstance(external, dict):
        countries = list(external.get("country_codes", []))
        source_name = str(external.get("source_name", "high_risk_countries"))
        source_version = str(external.get("version", ""))
    else:
        countries = list(getattr(cfg, "HIGH_RISK_COUNTRIES", ["AE", "TR", "PA", "CY", "RU"]))
        source_name = ""
        source_version = ""

    thresholds = {"high_risk_countries": countries}
    window = {}

    country = out["country"].astype(str).str.upper()
    direction = out["direction"].astype(str).str.lower()
    hits = country.isin(countries)
    out[hit_col] = hits.astype(int)
    out[score_col] = hits.astype(float)
    evidence = []
    result_list = []
    for c, d, h in zip(country, direction, hits):
        evidence.append(f"country={c}; direction={d}" if h else "none")
        ev = ({"country": c, "direction": d} if h else {}).copy()
        if source_name:
            ev["external_source"] = source_name
        if source_version:
            ev["external_version"] = source_version
        result_list.append(
            RuleResult(
                rule_id=RULE_ID,
                rule_version=RULE_VERSION,
                hit=bool(h),
                severity=DEFAULT_SEVERITY,
                score=1.0 if h else 0.0,
                evidence=ev,
                thresholds=thresholds,
                window=window,
            ).to_dict()
        )
    out[evidence_col] = evidence
    out[result_col] = result_list

    return out
