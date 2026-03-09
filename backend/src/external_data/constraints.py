"""
Compute hard-constraint external flags and version snapshot from loaded external sources.
Used by queue_service and app to pass into evaluate_hard_constraints and to persist external_versions_json.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _records(loaded: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get records list from loaded source (either data['records'] or data as list)."""
    data = loaded.get("data", loaded)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "records" in data:
        return data["records"]
    return []


def external_versions_snapshot(loaded_sources: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    Build the version snapshot to persist per alert: { source_name: { "version": str, "hash": str } }.
    """
    out: Dict[str, Dict[str, str]] = {}
    for name, meta in loaded_sources.items():
        out[name] = {
            "version": meta.get("version", ""),
            "hash": meta.get("hash", ""),
        }
    return out


def compute_external_flags_and_versions(
    df: pd.DataFrame,
    loaded_sources: Dict[str, Dict[str, Any]],
    country_col: str = "country",
    user_id_col: str = "user_id",
) -> Tuple[Dict[str, Dict[str, bool]], Dict[str, Dict[str, str]]]:
    """
    Compute per-alert external flags (for hard constraints) and the version snapshot for persistence.

    Uses:
    - sanctions: alert country in sanctions list countries -> sanctions_hit
    - high_risk_countries: alert country in records with risk_level CRITICAL -> high_risk_country_critical

    Args:
        df: Alert DataFrame with at least alert_id; optional country_col, user_id_col.
        loaded_sources: Result of load_all_configured_sources().
        country_col: Column name for country code (e.g. country).
        user_id_col: Column name for user/entity id (for future sanctions name match).

    Returns:
        (external_flags_per_alert, external_versions_snapshot)
        - external_flags_per_alert: alert_id -> { sanctions_hit, high_risk_country_critical, mandatory_rule_hit }
        - external_versions_snapshot: source_name -> { version, hash } for external_versions_json
    """
    versions = external_versions_snapshot(loaded_sources)
    flags_per_alert: Dict[str, Dict[str, bool]] = {}

    # Sanctions: set of countries that appear in sanctions list
    sanctions_countries: set = set()
    if "sanctions" in loaded_sources:
        for rec in _records(loaded_sources["sanctions"]):
            if isinstance(rec, dict) and rec.get("country"):
                sanctions_countries.add(str(rec["country"]).upper())

    # High-risk CRITICAL countries
    critical_countries: set = set()
    if "high_risk_countries" in loaded_sources:
        for rec in _records(loaded_sources["high_risk_countries"]):
            if isinstance(rec, dict) and str(rec.get("risk_level", "")).upper() == "CRITICAL":
                cc = rec.get("country_code")
                if cc:
                    critical_countries.add(str(cc).upper())

    alert_ids = df["alert_id"].astype(str).tolist() if "alert_id" in df.columns else []
    country_series = df[country_col].astype(str).str.upper() if country_col in df.columns else pd.Series([""] * len(df), index=df.index)

    for i, idx in enumerate(df.index):
        aid = alert_ids[i] if i < len(alert_ids) else str(idx)
        country = country_series.iloc[i] if i < len(country_series) else ""
        row = df.iloc[i]
        # Use pilot-injected flags when present (e.g. PILOT_TEST_MODE synthetic alerts)
        sanctions_hit = country in sanctions_countries
        if "sanctions_hit" in df.columns and pd.notna(row.get("sanctions_hit")):
            sanctions_hit = bool(row["sanctions_hit"])
        high_risk_country_critical = country in critical_countries
        if "high_risk_country" in df.columns and pd.notna(row.get("high_risk_country")):
            high_risk_country_critical = bool(row["high_risk_country"])
        flags_per_alert[aid] = {
            "sanctions_hit": sanctions_hit,
            "high_risk_country_critical": high_risk_country_critical,
            "mandatory_rule_hit": False,
        }

    return flags_per_alert, versions


def high_risk_countries_for_rule(loaded_sources: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Build the dict for rule_engine high_risk_country rule: country_codes, source_name, version.
    Use this to set cfg.external_high_risk_countries so the rule does not hardcode lists.
    Returns None if high_risk_countries not in loaded_sources.
    """
    if "high_risk_countries" not in loaded_sources:
        return None
    meta = loaded_sources["high_risk_countries"]
    records = _records(meta)
    codes = []
    for rec in records:
        if isinstance(rec, dict) and rec.get("country_code"):
            codes.append(str(rec["country_code"]).upper())
    return {
        "country_codes": list(dict.fromkeys(codes)),
        "source_name": "high_risk_countries",
        "version": meta.get("version", ""),
    }
