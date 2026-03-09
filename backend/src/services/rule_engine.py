"""AML Typology Rule Engine - delegates to canonical src/rule_engine.

This class is a thin wrapper for backward compatibility.
All rule logic lives in src/rules/ (modular) and is orchestrated by src/rule_engine.py.
Do not add new rule logic here - add it to src/rules/ instead.
"""
from __future__ import annotations

import pandas as pd

from .. import config
from .. import rule_engine as _canonical_rule_engine


class RuleEngine:
    """Thin wrapper around the canonical AML rule engine for backward compatibility.

    Delegates all rule execution to src/rule_engine.run_all_rules() which orchestrates
    the modular rules in src/rules/ (structuring, dormant, flow_through, rapid_withdraw,
    high_risk_country, low_buyer_diversity).
    """

    def __init__(self, cfg=None):
        """
        Args:
            cfg: Config object (unused - canonical engine reads from src/config directly).
                 Kept for backward-compatible call signatures.
        """
        self.cfg = cfg or config

    def apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all AML typology rules via the canonical modular rule engine.

        Delegates to rule_engine.run_all_rules() which runs all six modular rules
        (structuring, dormant, flow_through, rapid_withdraw, high_risk_country,
        low_buyer_diversity) and produces rules_json, rule_evidence_json,
        rule_R001_hit..R005_hit, rule_score_total, and rule_{id}_result columns.

        Args:
            df: DataFrame with alert data. Required columns depend on rules:
                - user_id, ts/alert_created_at (all rules)
                - direction, amount (structuring, flow_through, rapid_withdraw)
                - country (high_risk_country)
                - segment, counterparty_id (low_buyer_diversity)

        Returns:
            DataFrame with all rule output columns added.
        """
        df = _canonical_rule_engine.run_all_rules(df, self.cfg, policy_params=None)
        df = _canonical_rule_engine.aggregate_rule_score(df, self.cfg)
        return df
