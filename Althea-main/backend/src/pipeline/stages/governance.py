"""
GOVERNANCE stage: Hard constraints (unsuppressible) then suppression/prioritization.
Output: governance_status (eligible/suppressed/MANDATORY_REVIEW), suppression_code, suppression_reason, in_queue, policy_version.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from ... import config
from ...hard_constraints import evaluate_hard_constraints
from ...external_data import load_all_configured_sources
from ...external_data.constraints import compute_external_flags_and_versions
from ...queue_governance import apply_alert_governance
from ...observability.logging import get_logger

logger = get_logger("governance")


def run_governance(
    df: pd.DataFrame,
    run_id: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Apply hard constraints first (sanctions_hit, critical high-risk country) -> MANDATORY_REVIEW, in_queue=1.
    Then apply suppression (capacity, daily budget, per-entity cap, dedupe, segment throttle).
    """
    cfg = config_overrides or {}
    policy_version = str(cfg.get("policy_version", getattr(config, "CURRENT_POLICY_VERSION", "1.0")))

    try:
        loaded = load_all_configured_sources()
        flags_per_alert, external_versions = compute_external_flags_and_versions(df, loaded)
    except Exception:
        flags_per_alert = {}
        external_versions = {}

    out = df.copy()
    out["policy_version"] = policy_version
    # Keep external_versions_json from enrich; if missing, set from this run's snapshot
    if "external_versions_json" not in out.columns or out["external_versions_json"].isna().all():
        import json
        out["external_versions_json"] = [json.dumps(external_versions)] * len(out)

    # Hard constraints: set MANDATORY_REVIEW, in_queue=1, and explicit columns for audit
    hard_alert_ids = set()
    out["hard_constraint"] = 0
    out["hard_constraint_reason"] = ""
    out["hard_constraint_code"] = ""
    for idx in out.index:
        row = out.loc[idx]
        alert_id = str(row.get("alert_id", idx))
        flags = flags_per_alert.get(alert_id, {})
        result = evaluate_hard_constraints(row, flags, external_versions)
        if result.get("hard_hit"):
            hard_alert_ids.add(alert_id)
            out.at[idx, "governance_status"] = "MANDATORY_REVIEW"
            out.at[idx, "suppression_code"] = result.get("hard_code", "HARD_CONSTRAINT")
            out.at[idx, "suppression_reason"] = result.get("hard_reason", "")
            out.at[idx, "in_queue"] = True
            out.at[idx, "hard_constraint"] = 1
            out.at[idx, "hard_constraint_reason"] = result.get("hard_reason", "")
            out.at[idx, "hard_constraint_code"] = result.get("hard_code", "HARD_CONSTRAINT")

    # Then run queue governance (suppression only for non-hard-constraint rows)
    if "governance_status" not in out.columns:
        out["governance_status"] = "eligible"
    if "in_queue" not in out.columns:
        out["in_queue"] = False
    governance_config = cfg.get("governance") or {}
    out = apply_alert_governance(out, governance_config=governance_config)

    # in_queue: single source of truth = eligible or MANDATORY_REVIEW
    out["in_queue"] = out["governance_status"].astype(str).str.lower().isin(["eligible", "mandatory_review"])

    return out
