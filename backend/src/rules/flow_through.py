from __future__ import annotations

import numpy as np
import pandas as pd

from .base import RuleResult

RULE_ID = "flow_through"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "MEDIUM"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    out = df.copy()
    hit_col = "rule_flow_through_hit"
    score_col = "rule_flow_through_score"
    evidence_col = "rule_flow_through_evidence"
    result_col = "rule_flow_through_result"

    required = {"user_id", "ts", "direction", "amount"}
    if not required.issubset(out.columns):
        out[hit_col] = 0
        out[score_col] = 0.0
        out[evidence_col] = "none"
        out[result_col] = None
        return out

    window_days = int(getattr(cfg, "FLOW_THROUGH_WINDOW_DAYS", 7))
    ratio_min = float(getattr(cfg, "FLOW_THROUGH_RATIO_MIN", 0.90))
    volume_min = float(getattr(cfg, "FLOW_THROUGH_VOLUME_MIN", 5000))
    thresholds = {"ratio_min": ratio_min, "volume_min": volume_min}
    window = {"days": window_days}

    out = out.sort_values(["user_id", "ts"], kind="mergesort").copy()
    out[hit_col] = 0
    out[score_col] = 0.0
    out[evidence_col] = "none"
    out[result_col] = None

    out_ts = out.set_index("ts")
    amounts = pd.to_numeric(out_ts["amount"], errors="coerce").fillna(0.0)
    is_in = out_ts["direction"].astype(str).str.lower() == "in"
    is_out = out_ts["direction"].astype(str).str.lower() == "out"
    in_amt = amounts.where(is_in, 0.0)
    out_amt = amounts.where(is_out, 0.0)
    out_ts["_in"] = in_amt
    out_ts["_out"] = out_amt

    sum_in = (
        out_ts.groupby("user_id", sort=False)["_in"]
        .rolling(f"{window_days}D")
        .sum()
        .fillna(0.0)
        .reset_index(level=0, drop=True)
    )
    sum_out = (
        out_ts.groupby("user_id", sort=False)["_out"]
        .rolling(f"{window_days}D")
        .sum()
        .fillna(0.0)
        .reset_index(level=0, drop=True)
    )
    total = sum_in + sum_out
    ratio = np.where(
        (sum_in > 0) & (sum_out > 0),
        np.minimum(sum_in, sum_out) / np.maximum(sum_in, sum_out),
        0.0,
    )
    hits = (ratio >= ratio_min) & (np.asarray(total) >= volume_min)
    scores = np.clip(ratio, 0.0, 1.0)

    out[hit_col] = np.asarray(hits).astype(int)
    out[score_col] = np.asarray(scores)

    evidence = []
    result_list = []
    for s_in, s_out, r, h, sc in zip(
        sum_in.to_numpy(), sum_out.to_numpy(), ratio, hits, scores
    ):
        evidence.append(
            f"7d_in={float(s_in):.0f}; 7d_out={float(s_out):.0f}; ratio={float(r):.2f}"
            if h
            else "none"
        )
        result_list.append(
            RuleResult(
                rule_id=RULE_ID,
                rule_version=RULE_VERSION,
                hit=bool(h),
                severity=DEFAULT_SEVERITY,
                score=float(sc),
                evidence={"sum_in_7d": float(s_in), "sum_out_7d": float(s_out), "ratio": float(r)}
                if h
                else {},
                thresholds=thresholds,
                window=window,
            ).to_dict()
        )
    out[evidence_col] = evidence
    out[result_col] = result_list

    return out
