"""Hard constraints: alerts that cannot be suppressed (mandatory review)."""
from __future__ import annotations

from typing import Any, Dict, Optional


def evaluate_hard_constraints(
    alert_row: Any,
    external_flags: Dict[str, bool],
    external_versions: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Evaluate whether an alert hits any hard constraint (non-suppressible).

    Hard constraints force in_queue=1, governance_status=MANDATORY_REVIEW,
    and skip all suppression rules. They override suppression unconditionally.

    Args:
        alert_row: Row as dict-like (e.g. pd.Series) with optional columns:
            sanctions_hit, mandatory_rule_hit, high_risk_country_critical
            (if missing, taken from external_flags or treated as False).
        external_flags: Dict of external flags, e.g.:
            - sanctions_hit: True if alert hit sanctions list
            - mandatory_rule_hit: True if mandatory regulatory rule fired
            - high_risk_country_critical: True if critical high-risk country
        external_versions: Optional dict source_name -> {"version": str, "hash": str}
            for recording which external data versions were applied.

    Returns:
        {
            "hard_hit": bool,
            "hard_reason": str,
            "hard_code": str,
            "external_source": str (if hit from external list),
            "external_version": str (if hit from external list),
        }
    """
    external_versions = external_versions or {}

    def _get_flag(name: str) -> bool:
        try:
            if name in external_flags and external_flags[name] is True:
                return True
        except (TypeError, KeyError):
            pass
        try:
            val = alert_row.get(name, False)
            if val is True or (isinstance(val, (int, float)) and val != 0):
                return True
        except (TypeError, AttributeError, KeyError):
            pass
        return False

    def _ver(source: str) -> Dict[str, Any]:
        out = {"hard_hit": True, "hard_reason": "", "hard_code": "", "external_source": source, "external_version": ""}
        if source in external_versions:
            out["external_version"] = external_versions[source].get("version", "")
        return out

    # Check each hard constraint (order defines priority for reason/code)
    # External list hits must record source and version for reproducibility.
    if _get_flag("sanctions_hit"):
        out = _ver("sanctions")
        out["hard_reason"] = "Sanctions list hit; mandatory review required"
        out["hard_code"] = "SANCTIONS_HIT"
        return out
    if _get_flag("mandatory_rule_hit"):
        out = _ver("mandatory_rule")
        out["hard_reason"] = "Mandatory regulatory rule fired; cannot suppress"
        out["hard_code"] = "MANDATORY_RULE_HIT"
        return out
    if _get_flag("high_risk_country_critical"):
        out = _ver("high_risk_countries")
        out["hard_reason"] = "Critical high-risk country; mandatory review"
        out["hard_code"] = "HIGH_RISK_COUNTRY_CRITICAL"
        return out

    return {
        "hard_hit": False,
        "hard_reason": "",
        "hard_code": "",
        "external_source": "",
        "external_version": "",
    }
