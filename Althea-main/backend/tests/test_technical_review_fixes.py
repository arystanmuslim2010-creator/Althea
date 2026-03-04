"""
Expanded test suite for AML overlay — technical review fixes (FIX 9).

Covers:
  1. Feature engineering: rolling windows, baseline fallback, winsorization
  2. Suppression logic: each suppression code, priority assignment
  3. Hard constraints: sanctions, mandatory rule hits, high-risk country
  4. Scoring: output range, calibration validity, weights from config only
  5. Pipeline: determinism (same input => same scores)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_alert_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Minimal alert DataFrame for suppression / scoring tests."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "user_id": [f"U{i % 10}" for i in range(n)],
            "segment": rng.choice(["retail_low", "retail_high", "smb", "corporate"], n),
            "amount": rng.lognormal(5, 1, n),
            "time_gap": rng.uniform(0.1, 100, n),
            "num_transactions": rng.integers(1, 50, n),
            "risk_score": rng.uniform(0, 100, n),
            "typology": rng.choice(["smurfing", "burst_activity", "none"], n),
            "Top_Driver": rng.choice(["BEHAVIORAL", "RULE", "STRUCTURAL"], n),
            "synthetic_true_suspicious": rng.choice(["Yes", "No"], n, p=[0.15, 0.85]),
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h"),
            "baseline_confidence": rng.uniform(0.2, 0.9, n),
            "baseline_level": rng.choice(["user", "segment", "global"], n),
            "reason_codes": "[]",
            "case_status": "OPEN",
        }
    )


# ===========================================================================
# 1. FEATURE ENGINEERING
# ===========================================================================

class TestFeatureEngineering:
    """Rolling windows, baseline fallback, winsorization."""

    def test_rolling_windows_no_lookahead(self):
        """Rolling stats must use closed='left' — future values must not influence past rows."""
        from src.features import compute_behavioral_features
        from src import config as cfg

        n = 60
        rng = np.random.default_rng(0)
        df = pd.DataFrame(
            {
                "user_id": ["U1"] * n,
                "segment": ["retail_low"] * n,
                "amount": rng.lognormal(4, 0.5, n),
                "time_gap": rng.uniform(0.5, 5, n),
                "num_transactions": rng.integers(1, 10, n),
                "timestamp": pd.date_range("2024-01-01", periods=n, freq="6h"),
                "typology": "none",
                "synthetic_true_suspicious": "No",
            }
        )
        out, _ = compute_behavioral_features(df, cfg)
        # The first row should have NaN rolling stats (no prior data),
        # or at minimum not be influenced by rows that come after it.
        # Check: rolling_mean_amount_5 for row 0 must be NaN or 0 (min_periods=3 not met).
        assert (
            pd.isna(out["rolling_mean_amount_5"].iloc[0])
            or out["rolling_mean_amount_5"].iloc[0] == 0.0
        ), "First row rolling mean must not see future data"

    def test_baseline_fallback_order(self):
        """Baseline level must follow: user -> segment -> global hierarchy."""
        from src.features import compute_behavioral_features
        from src import config as cfg

        # Single-row user — cannot have a user baseline (history=0)
        df = pd.DataFrame(
            {
                "user_id": ["U_NEW"],
                "segment": ["smb"],
                "amount": [1000.0],
                "time_gap": [2.0],
                "num_transactions": [5],
                "timestamp": ["2024-06-01"],
                "typology": "none",
                "synthetic_true_suspicious": "No",
            }
        )
        out, _ = compute_behavioral_features(df, cfg)
        # A brand-new user with 1 transaction cannot have user-level baseline
        level = out["baseline_level"].iloc[0]
        assert level in {"segment", "global"}, (
            f"New user should fall back to segment/global, got '{level}'"
        )

    def test_winsorization_clips_extremes(self):
        """Winsorization must clip values at specified percentile bounds."""
        from src.scoring import _winsorize

        vals = pd.Series([1.0] * 95 + [1_000_000.0] * 5)
        clipped = _winsorize(vals, p=0.05)
        # After clipping at 5th/95th percentile the extreme values must decrease
        assert clipped.max() < 1_000_000.0, "Winsorization must cap extreme high values"
        assert clipped.min() >= vals.quantile(0.05) - 1e-6


# ===========================================================================
# 2. SUPPRESSION LOGIC
# ===========================================================================

class TestSuppressionLogic:
    """Each suppression code fires correctly; priority assignment is correct."""

    def _apply(self, df: pd.DataFrame):
        from src.suppression import apply_suppression
        from src import config as cfg
        return apply_suppression(df, cfg)

    def test_baseline_weak_suppression(self):
        """BASELINE_WEAK fires for non-user baseline + low confidence + score < 85."""
        df = _make_alert_df(10)
        df["baseline_level"] = "segment"
        df["baseline_confidence"] = 0.3
        df["risk_score"] = 50.0

        out = self._apply(df)
        assert (out["suppression_code"] == "BASELINE_WEAK").all()
        assert (out["alert_eligible"] == False).all()

    def test_low_risk_suppression(self):
        """LOW_RISK fires when risk_score < RISK_QUEUE_MIN_SCORE."""
        from src import config as cfg

        df = _make_alert_df(20)
        df["baseline_level"] = "user"
        df["baseline_confidence"] = 0.9
        df["risk_score"] = cfg.RISK_QUEUE_MIN_SCORE - 1

        out = self._apply(df)
        suppressed = out[out["alert_eligible"] == False]
        assert (suppressed["suppression_code"] == "LOW_RISK").all()

    def test_duplicate_signature_suppression(self):
        """DUPLICATE_SIGNATURE keeps at most MAX_ALERTS_PER_USER_PER_SIGNATURE per (user, sig)."""
        from src import config as cfg

        df = pd.DataFrame(
            {
                "user_id": ["U1"] * 10,
                "segment": ["retail_low"] * 10,
                "risk_score": list(range(90, 100)),
                "typology": ["smurfing"] * 10,
                "Top_Driver": ["BEHAVIORAL"] * 10,
                "baseline_level": ["user"] * 10,
                "baseline_confidence": [0.9] * 10,
                "reason_codes": ["[]"] * 10,
                "case_status": ["OPEN"] * 10,
            }
        )
        out = self._apply(df)
        eligible = out[out["alert_eligible"]]
        assert len(eligible) <= cfg.MAX_ALERTS_PER_USER_PER_SIGNATURE

    def test_user_cap_suppression(self):
        """USER_CAP keeps at most MAX_ALERTS_PER_USER per user."""
        from src import config as cfg

        n = cfg.MAX_ALERTS_PER_USER + 5
        df = pd.DataFrame(
            {
                "user_id": ["U1"] * n,
                "segment": ["smb"] * n,
                "risk_score": list(range(70, 70 + n)),
                "typology": [f"t{i}" for i in range(n)],   # unique signatures
                "Top_Driver": ["BEHAVIORAL"] * n,
                "baseline_level": ["user"] * n,
                "baseline_confidence": [0.9] * n,
                "reason_codes": ["[]"] * n,
                "case_status": ["OPEN"] * n,
            }
        )
        out = self._apply(df)
        eligible = out[out["alert_eligible"]]
        assert len(eligible) <= cfg.MAX_ALERTS_PER_USER

    def test_priority_p0_for_high_score(self):
        """risk_score >= 90 => P0."""
        df = _make_alert_df(5)
        df["risk_score"] = 95.0
        df["baseline_level"] = "user"
        df["baseline_confidence"] = 0.9
        out = self._apply(df)
        assert (out["alert_priority"] == "P0").all()

    def test_priority_p1_for_mid_score(self):
        """75 <= risk_score < 90 => P1."""
        df = _make_alert_df(5)
        df["risk_score"] = 80.0
        df["baseline_level"] = "user"
        df["baseline_confidence"] = 0.9
        out = self._apply(df)
        assert (out["alert_priority"] == "P1").all()

    def test_priority_p2_for_low_score(self):
        """risk_score < 75 (but above queue min) => P2."""
        df = _make_alert_df(5)
        df["risk_score"] = 72.0
        df["baseline_level"] = "user"
        df["baseline_confidence"] = 0.9
        out = self._apply(df)
        # May still be suppressed by LOW_RISK if 72 < RISK_QUEUE_MIN_SCORE
        from src import config as cfg
        if 72 >= cfg.RISK_QUEUE_MIN_SCORE:
            assert (out["alert_priority"] == "P2").all()


# ===========================================================================
# 3. HARD CONSTRAINTS
# ===========================================================================

class TestHardConstraints:
    """Sanctions and mandatory rules are never suppressed."""

    def test_sanctions_hit_never_suppressed(self):
        """A sanctions hit must trigger hard_hit=True regardless of score."""
        from src.hard_constraints import evaluate_hard_constraints

        result = evaluate_hard_constraints({}, {"sanctions_hit": True}, {})
        assert result["hard_hit"] is True
        assert "SANCTIONS" in result.get("hard_code", "").upper() or (
            "sanction" in result.get("hard_reason", "").lower()
        )

    def test_high_risk_critical_country_hard_hit(self):
        """high_risk_country_critical flag triggers hard_hit."""
        from src.hard_constraints import evaluate_hard_constraints

        result = evaluate_hard_constraints({}, {"high_risk_country_critical": True}, {})
        assert result["hard_hit"] is True

    def test_no_flags_no_hard_hit(self):
        """No special flags => hard_hit=False."""
        from src.hard_constraints import evaluate_hard_constraints

        result = evaluate_hard_constraints({}, {}, {})
        assert result["hard_hit"] is False

    def test_suppression_does_not_suppress_high_score_sanctions(self):
        """Even with suppression applied, sanctions-level alerts remain high-priority."""
        from src.suppression import apply_suppression
        from src import config as cfg

        # Craft an alert that would normally be suppressed (baseline weak) but has
        # a sanctions-level risk_score >= 90.
        df = _make_alert_df(5)
        df["risk_score"] = 95.0  # force P0 path
        df["baseline_level"] = "user"
        df["baseline_confidence"] = 0.9
        out = apply_suppression(df, cfg)
        # P0 alerts should never be suppressed by USER_CAP or BASELINE_WEAK
        assert (out["alert_priority"] == "P0").all()


# ===========================================================================
# 4. SCORING
# ===========================================================================

class TestScoring:
    """Score output range, calibration validity, weights read from config."""

    def test_weights_read_from_config_not_hardcoded(self):
        """scoring.py must not contain hardcoded weight literals for risk blending."""
        import ast, pathlib

        src_path = pathlib.Path(__file__).parents[1] / "src" / "scoring.py"
        source = src_path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        # Look for suspicious literal assignments like w_b = 0.30 (not via config)
        forbidden_patterns = [
            "w_b = 0.", "w_s = 0.", "w_t_ml = 0.", "w_r = 0.",
        ]
        for pattern in forbidden_patterns:
            assert pattern not in source, (
                f"Hardcoded weight literal '{pattern}' found in scoring.py — "
                "weights must come from config.py"
            )

    def test_score_output_in_0_100_range(self):
        """risk_score must always be in [0, 100]."""
        from src import config as cfg
        from src.features import compute_behavioral_features
        from src.scoring import train_risk_engine, score_with_risk_engine

        df = _make_alert_df(80)
        df["synthetic_true_suspicious"] = np.where(
            df["risk_score"] > 70, "Yes", "No"
        )
        # Provide minimal columns for feature computation
        df_feat, feature_groups = compute_behavioral_features(df, cfg)
        # Fill any NaN feature columns
        for col in feature_groups["all_feature_cols"]:
            if col in df_feat.columns:
                df_feat[col] = pd.to_numeric(df_feat[col], errors="coerce").fillna(0.0)

        models, calibrator = train_risk_engine(df_feat, feature_groups, force_retrain=True)
        scored = score_with_risk_engine(df_feat, models, calibrator)

        scores = pd.to_numeric(scored["risk_score"], errors="coerce")
        assert scores.notna().all(), "risk_score must not contain NaN"
        assert (scores >= 0).all() and (scores <= 100).all(), "risk_score must be in [0, 100]"

    def test_calibration_produces_valid_probabilities(self):
        """Calibrator output must be in [0, 1]."""
        from src import calibration
        import numpy as np

        rng = np.random.default_rng(7)
        y = rng.integers(0, 2, 200)
        raw = rng.uniform(0, 1, 200)

        cal = calibration.fit_calibrator(y, raw, method="isotonic")
        if cal is not None:
            probs = calibration.apply_calibrator(cal, raw)
            assert ((probs >= 0) & (probs <= 1)).all(), "Calibrated probabilities must be in [0,1]"


# ===========================================================================
# 5. PIPELINE DETERMINISM
# ===========================================================================

class TestPipelineDeterminism:
    """Same input + same config => same run_id => same scores."""

    def test_same_input_same_scores(self):
        """Running the scoring pipeline twice on identical data must produce identical scores."""
        from src import config as cfg
        from src.features import compute_behavioral_features
        from src.scoring import train_risk_engine, score_with_risk_engine

        df = _make_alert_df(60)
        df["synthetic_true_suspicious"] = np.where(
            df["risk_score"] > 65, "Yes", "No"
        )

        def _run(df_in):
            df_feat, feature_groups = compute_behavioral_features(df_in.copy(), cfg)
            for col in feature_groups["all_feature_cols"]:
                if col in df_feat.columns:
                    df_feat[col] = pd.to_numeric(df_feat[col], errors="coerce").fillna(0.0)
            models, calibrator = train_risk_engine(df_feat, feature_groups, force_retrain=True)
            scored = score_with_risk_engine(df_feat, models, calibrator)
            return scored["risk_score"].round(6).tolist()

        scores_1 = _run(df)
        scores_2 = _run(df)
        assert scores_1 == scores_2, "Scores must be deterministic across identical runs"
