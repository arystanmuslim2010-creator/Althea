"""
Tests for the modular AML rule engine: src/rules/*.

Each rule is tested for:
1. Hit condition (correct detection)
2. No-hit condition (correct non-detection)
3. Graceful no-hit when required columns are missing
4. Output schema (hit_col, score_col, evidence_col, result_col all present)
5. Rule version and rule_id in result dict
"""
from __future__ import annotations

import pandas as pd
import numpy as np
import pytest
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

from src.rules.structuring import run_rule as run_structuring
from src.rules.dormant import run_rule as run_dormant
from src.rules.flow_through import run_rule as run_flow_through
from src.rules.rapid_withdraw import run_rule as run_rapid_withdraw
from src.rules.high_risk_country import run_rule as run_high_risk_country
from src.rules.low_buyer_diversity import run_rule as run_low_buyer_diversity
from src import config as cfg


# ─── Helpers ────────────────────────────────────────────────────────────────

def _ts(dates):
    return pd.to_datetime(dates)


# ─── structuring ────────────────────────────────────────────────────────────

class TestStructuringRule:

    def _base_df(self):
        """3 near-threshold OUT transactions within 60 days for one user."""
        return pd.DataFrame({
            "user_id":   ["U1"] * 4,
            "ts":        _ts(["2024-01-05", "2024-01-10", "2024-01-15", "2024-01-20"]),
            "direction": ["out", "out", "out", "out"],
            "amount":    [9600.0, 9700.0, 9800.0, 9500.0],
        })

    def test_hit_when_3_near_threshold_in_window(self):
        df = self._base_df()
        out = run_structuring(df, cfg)
        # By the 3rd row there should be a hit (count >= 3)
        assert out["rule_structuring_hit"].sum() >= 1

    def test_no_hit_below_count_threshold(self):
        """Only 2 qualifying transactions — should not hit."""
        df = pd.DataFrame({
            "user_id":   ["U1", "U1"],
            "ts":        _ts(["2024-01-05", "2024-01-10"]),
            "direction": ["out", "out"],
            "amount":    [9600.0, 9700.0],
        })
        out = run_structuring(df, cfg)
        assert out["rule_structuring_hit"].sum() == 0

    def test_no_hit_when_amount_above_threshold(self):
        """Amounts above reporting threshold — not structuring."""
        df = pd.DataFrame({
            "user_id":   ["U1"] * 4,
            "ts":        _ts(["2024-01-05", "2024-01-10", "2024-01-15", "2024-01-20"]),
            "direction": ["out"] * 4,
            "amount":    [11000.0, 12000.0, 15000.0, 20000.0],
        })
        out = run_structuring(df, cfg)
        assert out["rule_structuring_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        """Missing required columns must return zero hits, not raise."""
        df = pd.DataFrame({"user_id": ["U1", "U2"]})
        out = run_structuring(df, cfg)
        assert "rule_structuring_hit" in out.columns
        assert out["rule_structuring_hit"].sum() == 0

    def test_output_schema_and_result_version(self):
        df = self._base_df()
        out = run_structuring(df, cfg)
        for col in ["rule_structuring_hit", "rule_structuring_score",
                    "rule_structuring_evidence", "rule_structuring_result"]:
            assert col in out.columns, f"Missing column: {col}"
        # result dict must have rule_id and rule_version
        result = out["rule_structuring_result"].dropna().iloc[0]
        assert isinstance(result, dict)
        assert result["rule_id"] == "structuring"
        assert "rule_version" in result


# ─── dormant ────────────────────────────────────────────────────────────────

class TestDormantRule:

    def test_hit_after_long_inactivity_then_burst(self):
        """31-day gap followed by burst activity should hit."""
        df = pd.DataFrame({
            "user_id": ["U1"] * 10,
            "ts": _ts([
                "2024-01-01",   # last activity
                # 31-day gap
                "2024-02-01", "2024-02-01", "2024-02-01", "2024-02-01",
                "2024-02-01", "2024-02-01", "2024-02-01", "2024-02-01",
                "2024-02-01",  # burst: 9 transactions on same day
            ]),
        })
        out = run_dormant(df, cfg)
        assert out["rule_dormant_hit"].sum() >= 1

    def test_no_hit_for_continuous_activity(self):
        """Daily activity — no dormancy gap — should not hit."""
        df = pd.DataFrame({
            "user_id": ["U1"] * 10,
            "ts": _ts(pd.date_range("2024-01-01", periods=10, freq="D")),
        })
        out = run_dormant(df, cfg)
        assert out["rule_dormant_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        df = pd.DataFrame({"amount": [100.0, 200.0]})
        out = run_dormant(df, cfg)
        assert "rule_dormant_hit" in out.columns
        assert out["rule_dormant_hit"].sum() == 0

    def test_output_schema(self):
        df = pd.DataFrame({
            "user_id": ["U1"] * 3,
            "ts": _ts(["2024-01-01", "2024-02-10", "2024-02-10"]),
        })
        out = run_dormant(df, cfg)
        for col in ["rule_dormant_hit", "rule_dormant_score",
                    "rule_dormant_evidence", "rule_dormant_result"]:
            assert col in out.columns


# ─── flow_through ────────────────────────────────────────────────────────────

class TestFlowThroughRule:

    def test_hit_when_high_ratio_and_sufficient_volume(self):
        """Equal in and out amounts with sufficient volume should hit."""
        df = pd.DataFrame({
            "user_id":   ["U1"] * 4,
            "ts":        _ts(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
            "direction": ["in", "out", "in", "out"],
            "amount":    [10000.0, 9800.0, 10000.0, 9900.0],
        })
        out = run_flow_through(df, cfg)
        assert out["rule_flow_through_hit"].sum() >= 1

    def test_no_hit_when_low_volume(self):
        """High ratio but very small amounts — below volume threshold."""
        df = pd.DataFrame({
            "user_id":   ["U1"] * 4,
            "ts":        _ts(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
            "direction": ["in", "out", "in", "out"],
            "amount":    [10.0, 10.0, 10.0, 10.0],
        })
        out = run_flow_through(df, cfg)
        assert out["rule_flow_through_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        df = pd.DataFrame({"user_id": ["U1"]})
        out = run_flow_through(df, cfg)
        assert "rule_flow_through_hit" in out.columns
        assert out["rule_flow_through_hit"].sum() == 0

    def test_output_schema(self):
        df = pd.DataFrame({
            "user_id":   ["U1"] * 2,
            "ts":        _ts(["2024-01-01", "2024-01-02"]),
            "direction": ["in", "out"],
            "amount":    [5000.0, 4800.0],
        })
        out = run_flow_through(df, cfg)
        for col in ["rule_flow_through_hit", "rule_flow_through_score",
                    "rule_flow_through_evidence", "rule_flow_through_result"]:
            assert col in out.columns


# ─── rapid_withdraw ──────────────────────────────────────────────────────────

class TestRapidWithdrawRule:

    def test_hit_when_withdrawal_within_threshold_minutes(self):
        """OUT within 20 minutes of IN should hit."""
        df = pd.DataFrame({
            "user_id":   ["U1", "U1"],
            "ts":        [
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-01 10:20:00"),  # 20 minutes later
            ],
            "direction": ["in", "out"],
            "amount":    [5000.0, 4800.0],
        })
        out = run_rapid_withdraw(df, cfg)
        assert out["rule_rapid_withdraw_hit"].iloc[1] == 1

    def test_no_hit_when_withdrawal_too_late(self):
        """OUT 2 hours after IN — beyond 30-minute threshold."""
        df = pd.DataFrame({
            "user_id":   ["U1", "U1"],
            "ts":        [
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-01 12:00:00"),  # 2 hours later
            ],
            "direction": ["in", "out"],
            "amount":    [5000.0, 4800.0],
        })
        out = run_rapid_withdraw(df, cfg)
        assert out["rule_rapid_withdraw_hit"].iloc[1] == 0

    def test_no_hit_for_in_transactions(self):
        """IN transactions should never hit rapid_withdraw."""
        df = pd.DataFrame({
            "user_id":   ["U1", "U1"],
            "ts":        [
                pd.Timestamp("2024-01-01 10:00:00"),
                pd.Timestamp("2024-01-01 10:05:00"),
            ],
            "direction": ["in", "in"],
            "amount":    [5000.0, 3000.0],
        })
        out = run_rapid_withdraw(df, cfg)
        assert out["rule_rapid_withdraw_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        df = pd.DataFrame({"user_id": ["U1"]})
        out = run_rapid_withdraw(df, cfg)
        assert "rule_rapid_withdraw_hit" in out.columns

    def test_output_schema(self):
        df = pd.DataFrame({
            "user_id":   ["U1", "U1"],
            "ts":        [pd.Timestamp("2024-01-01 10:00"), pd.Timestamp("2024-01-01 10:15")],
            "direction": ["in", "out"],
            "amount":    [5000.0, 4800.0],
        })
        out = run_rapid_withdraw(df, cfg)
        for col in ["rule_rapid_withdraw_hit", "rule_rapid_withdraw_score",
                    "rule_rapid_withdraw_evidence", "rule_rapid_withdraw_result"]:
            assert col in out.columns


# ─── high_risk_country ────────────────────────────────────────────────────────

class TestHighRiskCountryRule:

    def test_hit_for_known_high_risk_country(self):
        """AE (UAE) is in the default high-risk list."""
        df = pd.DataFrame({
            "country":   ["AE", "US"],
            "direction": ["out", "out"],
        })
        out = run_high_risk_country(df, cfg)
        assert out["rule_high_risk_country_hit"].iloc[0] == 1
        assert out["rule_high_risk_country_hit"].iloc[1] == 0

    def test_hit_for_multiple_default_countries(self):
        """All default high-risk countries should hit."""
        default_countries = ["AE", "TR", "PA", "CY", "RU"]
        df = pd.DataFrame({
            "country":   default_countries,
            "direction": ["out"] * len(default_countries),
        })
        out = run_high_risk_country(df, cfg)
        assert out["rule_high_risk_country_hit"].sum() == len(default_countries)

    def test_no_hit_for_low_risk_country(self):
        """US, GB, DE should not hit."""
        df = pd.DataFrame({
            "country":   ["US", "GB", "DE"],
            "direction": ["out"] * 3,
        })
        out = run_high_risk_country(df, cfg)
        assert out["rule_high_risk_country_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        df = pd.DataFrame({"amount": [100.0]})
        out = run_high_risk_country(df, cfg)
        assert "rule_high_risk_country_hit" in out.columns
        assert out["rule_high_risk_country_hit"].sum() == 0

    def test_versioned_external_source_in_evidence(self):
        """When external source is provided, evidence must include external_source key."""
        external_cfg = type("Cfg", (), {
            "external_high_risk_countries": {
                "country_codes": ["XX"],
                "source_name": "FATF_2024",
                "version": "v2024-Q1",
            }
        })()
        df = pd.DataFrame({
            "country":   ["XX", "US"],
            "direction": ["out", "out"],
        })
        out = run_high_risk_country(df, external_cfg)
        assert out["rule_high_risk_country_hit"].iloc[0] == 1
        result = out["rule_high_risk_country_result"].iloc[0]
        assert isinstance(result, dict)
        assert result.get("evidence", {}).get("external_source") == "FATF_2024"

    def test_output_schema(self):
        df = pd.DataFrame({"country": ["AE"], "direction": ["out"]})
        out = run_high_risk_country(df, cfg)
        for col in ["rule_high_risk_country_hit", "rule_high_risk_country_score",
                    "rule_high_risk_country_evidence", "rule_high_risk_country_result"]:
            assert col in out.columns


# ─── low_buyer_diversity ─────────────────────────────────────────────────────

class TestLowBuyerDiversityRule:

    def test_hit_for_smb_with_low_diversity(self):
        """SMB with 20+ transactions and only 2 unique counterparties should hit."""
        n = 25
        df = pd.DataFrame({
            "user_id":        ["U1"] * n,
            "ts":             _ts(pd.date_range("2024-01-01", periods=n, freq="6h")),
            "segment":        ["smb"] * n,
            "counterparty_id": ["CP1" if i % 2 == 0 else "CP2" for i in range(n)],
        })
        out = run_low_buyer_diversity(df, cfg)
        assert out["rule_low_buyer_diversity_hit"].sum() >= 1

    def test_no_hit_for_non_smb_segment(self):
        """Rule only fires for SMB — corporate should not hit."""
        n = 25
        df = pd.DataFrame({
            "user_id":        ["U1"] * n,
            "ts":             _ts(pd.date_range("2024-01-01", periods=n, freq="6h")),
            "segment":        ["corporate"] * n,
            "counterparty_id": ["CP1"] * n,
        })
        out = run_low_buyer_diversity(df, cfg)
        assert out["rule_low_buyer_diversity_hit"].sum() == 0

    def test_no_hit_when_high_diversity(self):
        """SMB with 20+ transactions but 10 unique counterparties — should not hit."""
        n = 25
        df = pd.DataFrame({
            "user_id":        ["U1"] * n,
            "ts":             _ts(pd.date_range("2024-01-01", periods=n, freq="6h")),
            "segment":        ["smb"] * n,
            "counterparty_id": [f"CP{i}" for i in range(n)],  # all unique
        })
        out = run_low_buyer_diversity(df, cfg)
        assert out["rule_low_buyer_diversity_hit"].sum() == 0

    def test_graceful_no_hit_missing_columns(self):
        df = pd.DataFrame({"user_id": ["U1", "U2"]})
        out = run_low_buyer_diversity(df, cfg)
        assert "rule_low_buyer_diversity_hit" in out.columns
        assert out["rule_low_buyer_diversity_hit"].sum() == 0

    def test_output_schema(self):
        n = 25
        df = pd.DataFrame({
            "user_id":        ["U1"] * n,
            "ts":             _ts(pd.date_range("2024-01-01", periods=n, freq="6h")),
            "segment":        ["smb"] * n,
            "counterparty_id": ["CP1"] * n,
        })
        out = run_low_buyer_diversity(df, cfg)
        for col in ["rule_low_buyer_diversity_hit", "rule_low_buyer_diversity_score",
                    "rule_low_buyer_diversity_evidence", "rule_low_buyer_diversity_result"]:
            assert col in out.columns


# ─── Integration: canonical rule_engine runs all 6 ───────────────────────────

class TestCanonicalRuleEngineIntegration:

    def test_run_all_rules_produces_expected_output_columns(self):
        """rule_engine.run_all_rules() must produce rules_json and legacy R001–R005 columns."""
        from src import rule_engine
        df = pd.DataFrame({
            "user_id":        ["U1"] * 5,
            "ts":             _ts(pd.date_range("2024-01-01", periods=5, freq="D")),
            "direction":      ["out"] * 5,
            "amount":         [9600.0, 9700.0, 9800.0, 9500.0, 9650.0],
            "country":        ["AE"] * 5,
            "segment":        ["smb"] * 5,
            "counterparty_id": ["CP1"] * 5,
        })
        out = rule_engine.run_all_rules(df, cfg, policy_params=None)
        out = rule_engine.aggregate_rule_score(out, cfg)
        assert "rules_json" in out.columns
        assert "rule_score_total" in out.columns
        for legacy_col in ["rule_R001_hit", "rule_R002_hit", "rule_R003_hit",
                           "rule_R004_hit", "rule_R005_hit"]:
            assert legacy_col in out.columns, f"Missing legacy column: {legacy_col}"

    def test_services_rule_engine_delegates_to_canonical(self):
        """services/rule_engine.py RuleEngine.apply_rules() must call canonical engine."""
        from src.services.rule_engine import RuleEngine
        re = RuleEngine()
        df = pd.DataFrame({
            "user_id":   ["U1"] * 3,
            "ts":        _ts(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "direction": ["in", "out", "in"],
            "amount":    [1000.0, 900.0, 1100.0],
            "country":   ["US"] * 3,
        })
        out = re.apply_rules(df)
        # Must produce canonical columns, not the old typology/rule_score columns only
        assert "rules_json" in out.columns or "rule_structuring_hit" in out.columns
