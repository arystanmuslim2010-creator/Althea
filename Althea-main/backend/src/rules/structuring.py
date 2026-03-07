from __future__ import annotations

import numpy as np
import pandas as pd

from .base import RuleResult

RULE_ID = "structuring"
RULE_VERSION = "1.0.0"
DEFAULT_SEVERITY = "HIGH"


def run_rule(df: pd.DataFrame, cfg) -> pd.DataFrame:
    hit_col = "rule_structuring_hit"
    score_col = "rule_structuring_score"
    evidence_col = "rule_structuring_evidence"
    result_col = "rule_structuring_result"

    required = {"user_id", "ts", "direction", "amount"}
    if not required.issubset(df.columns):
        df = df.copy()
        df[hit_col] = 0
        df[score_col] = 0.0
        df[evidence_col] = "none"
        df[result_col] = None
        return df

    window_days = int(getattr(cfg, "STRUCTURING_WINDOW_DAYS", 60))
    low = float(getattr(cfg, "STRUCTURING_LOW", 9500))
    high = float(getattr(cfg, "STRUCTURING_THRESHOLD", 10000))
    min_count = int(getattr(cfg, "STRUCTURING_COUNT_MIN", 3))
    thresholds = {"amount_low": low, "amount_high": high, "count_thr": min_count}
    window = {"days": window_days}

    df = df.sort_values(["user_id", "ts"], kind="mergesort").copy()
    df[hit_col] = 0
    df[score_col] = 0.0
    df[evidence_col] = "none"
    df[result_col] = None

    df_ts = df.set_index("ts")
    direction_out = df_ts["direction"].astype(str).str.lower() == "out"
    amount_numeric = pd.to_numeric(df_ts["amount"], errors="coerce")
    qualifies = (
        direction_out & (amount_numeric >= low) & (amount_numeric < high)
    ).astype(float)
    df_ts["_q"] = qualifies

    rolling = (
        df_ts.groupby("user_id", sort=False)["_q"]
        .rolling(f"{window_days}D")
        .sum()
        .fillna(0)
    )
    rolling = rolling.reset_index(level=0, drop=True)
    counts = rolling.to_numpy()
    hits = counts >= min_count
    scores = np.minimum(1.0, counts / max(min_count * 2, 1))

    df[hit_col] = hits.astype(int)
    df[score_col] = scores

    evidence_list = []
    result_list = []
    for c, h, sc in zip(counts, hits, scores):
        if h:
            evidence_list.append(f"count_{window_days}d={int(c)}; near_thr_amt={int(low)}-{int(high-1)}")
            ev = {"count_60d": int(c), "near_threshold_amounts": int(c), "max_amount": high - 1}
            result_list.append(
                RuleResult(
                    rule_id=RULE_ID,
                    rule_version=RULE_VERSION,
                    hit=True,
                    severity=DEFAULT_SEVERITY,
                    score=float(sc),
                    evidence=ev,
                    thresholds=thresholds,
                    window=window,
                ).to_dict()
            )
        else:
            evidence_list.append("none")
            result_list.append(
                RuleResult(
                    rule_id=RULE_ID,
                    rule_version=RULE_VERSION,
                    hit=False,
                    severity=DEFAULT_SEVERITY,
                    score=0.0,
                    evidence={},
                    thresholds=thresholds,
                    window=window,
                ).to_dict()
            )
    df[evidence_col] = evidence_list
    df[result_col] = result_list

    return df
