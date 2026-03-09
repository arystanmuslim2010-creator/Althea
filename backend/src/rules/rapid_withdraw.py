from __future__ import annotations

import numpy as np
import pandas as pd

from .base import RuleResult

RULE_ID = "rapid_withdraw"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "HIGH"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    out = df.copy()
    hit_col = "rule_rapid_withdraw_hit"
    score_col = "rule_rapid_withdraw_score"
    evidence_col = "rule_rapid_withdraw_evidence"
    result_col = "rule_rapid_withdraw_result"

    required = {"user_id", "ts", "direction", "amount"}
    if not required.issubset(out.columns):
        out[hit_col] = 0
        out[score_col] = 0.0
        out[evidence_col] = "none"
        out[result_col] = None
        return out

    minutes_thr = float(getattr(cfg, "RAPID_WITHDRAW_MINUTES", 30))
    thresholds = {"minutes_thr": minutes_thr}
    window = {"minutes": minutes_thr}

    out = out.sort_values(["user_id", "ts"], kind="mergesort").copy()
    out[hit_col] = 0
    out[score_col] = 0.0
    out[evidence_col] = "none"
    out[result_col] = None

    is_in = out["direction"].astype(str).str.lower() == "in"
    is_out = out["direction"].astype(str).str.lower() == "out"

    # Mask ts and amount to only IN rows, then ffill within each user group
    out["_ts_in"] = out["ts"].where(is_in)
    out["_amt_in"] = pd.to_numeric(out["amount"], errors="coerce").where(is_in)
    out["_ts_in"] = out.groupby("user_id", sort=False)["_ts_in"].ffill()
    out["_amt_in"] = out.groupby("user_id", sort=False)["_amt_in"].ffill()

    minutes = (out["ts"] - out["_ts_in"]).dt.total_seconds().div(60.0)
    hits = is_out & minutes.notna() & (minutes <= minutes_thr)
    scores = hits.astype(float)

    out[hit_col] = hits.astype(int).to_numpy()
    out[score_col] = scores.to_numpy()

    evidence = []
    result_list = []
    for m, a, h, sc in zip(
        minutes.fillna(0).to_numpy(), out["amount"].to_numpy(), hits.to_numpy(), scores.to_numpy()
    ):
        evidence.append(f"in->out_minutes={int(m)}; out_amt={float(a):.0f}" if h else "none")
        result_list.append(
            RuleResult(
                rule_id=RULE_ID,
                rule_version=RULE_VERSION,
                hit=bool(h),
                severity=DEFAULT_SEVERITY,
                score=float(sc),
                evidence={"in_out_minutes": float(m), "out_amt": float(a)} if h else {},
                thresholds=thresholds,
                window=window,
            ).to_dict()
        )
    out[evidence_col] = evidence
    out[result_col] = result_list

    # Drop temporary columns
    out = out.drop(columns=["_ts_in", "_amt_in"])
    return out
