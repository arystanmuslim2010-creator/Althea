"""Legacy entrypoint: delegates to canonical rule_engine. Do not add new logic here."""
from __future__ import annotations

from typing import Any

import pandas as pd

from .. import config
from .. import rule_engine


def run_rule_engine(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the canonical rule engine (modular rules + RuleResult aggregation).
    Returns df with rules_json, rule_evidence_json, rule_R001_hit..R005_hit, rule_score_total, etc.
    """
    cfg: Any = config
    df = rule_engine.run_all_rules(df, cfg, policy_params=None)
    df = rule_engine.aggregate_rule_score(df, cfg)
    return df
